package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// ConvertAudio converts a LINE audio message to a Matrix audio message.
func (h *Handler) ConvertAudio(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data line.Message, decryptedBody string, relatesTo *event.RelatesTo) (*bridgev2.ConvertedMessage, error) {
	client := h.NewClient()
	oid := data.ContentMetadata["OID"]
	isPlainMedia := oid == ""

	// If OID is not in ContentMetadata, check decrypted body (E2EE path)
	if oid == "" && decryptedBody != "" && strings.Contains(decryptedBody, "OID") {
		var decryptInfo struct {
			OID         string `json:"OID"`
			KeyMaterial string `json:"keyMaterial"`
		}
		if err := json.Unmarshal([]byte(decryptedBody), &decryptInfo); err == nil && decryptInfo.OID != "" {
			oid = decryptInfo.OID
			isPlainMedia = false
		}
	}

	// For plain media, the audio is stored at r/talk/m/{messageID}
	if isPlainMedia {
		oid = data.ID
	}

	if oid == "" {
		return nil, nil
	}

	sid := "ema"
	if isPlainMedia {
		sid = "m"
	}
	audioData, err := client.DownloadOBSWithSID(ctx, oid, data.ID, sid)

	if newClient, ok := h.tryRecoverClient(ctx, err); ok {
		client = newClient
		audioData, err = client.DownloadOBSWithSID(ctx, oid, data.ID, sid)
	}
	_ = client

	if err != nil {
		h.Log.Warn().
			Err(err).
			Str("oid", oid).
			Str("msg_id", data.ID).
			Bool("plain_media", isPlainMedia).
			Msg("Failed to download audio from OBS, sending placeholder")
		return &bridgev2.ConvertedMessage{
			Parts: []*bridgev2.ConvertedMessagePart{
				{
					Type: event.EventMessage,
					Content: &event.MessageEventContent{
						MsgType:   event.MsgNotice,
						Body:      "[Audio unavailable — LINE media expired before it could be bridged]",
						RelatesTo: relatesTo,
					},
				},
			},
		}, nil
	}

	// Decrypt audio if it has keyMaterial (E2EE)
	decrypted := false
	if decryptedBody != "" && strings.Contains(decryptedBody, "keyMaterial") {
		var decryptInfo struct {
			KeyMaterial string `json:"keyMaterial"`
		}
		if err := json.Unmarshal([]byte(decryptedBody), &decryptInfo); err == nil && decryptInfo.KeyMaterial != "" {
			decryptedAudio, err := h.DecryptMedia(audioData, decryptInfo.KeyMaterial)
			if err != nil {
				h.Log.Error().Err(err).Msg("Failed to decrypt audio data")
				return nil, fmt.Errorf("failed to decrypt audio data: %w", err)
			}
			audioData = decryptedAudio
			decrypted = true
		}
	}

	// ENC_KM is a fallback when the in-body keyMaterial path didn't decrypt
	// (e.g. E2EE chunk decryption failed). Running it unconditionally would
	// double-decrypt for bridge-sent LSON audio and corrupt the bytes.
	if !decrypted {
		if encKM := data.ContentMetadata["ENC_KM"]; encKM != "" && len(audioData) > 32 {
			decryptedAudio, err := h.DecryptMedia(audioData, encKM)
			if err != nil {
				h.Log.Warn().Err(err).Msg("ENC_KM fallback decrypt failed, sending raw audio")
			} else {
				audioData = decryptedAudio
			}
		}
	}

	var duration int
	if durationStr := data.ContentMetadata["DURATION"]; durationStr != "" {
		if d, err := strconv.Atoi(durationStr); err == nil {
			duration = d
		}
	}

	mxc, file, err := intent.UploadMedia(ctx, portal.MXID, audioData, "audio.m4a", "audio/mp4")
	if err != nil {
		return nil, fmt.Errorf("failed to upload audio to matrix: %w", err)
	}

	audioInfo := &event.FileInfo{
		MimeType: "audio/mp4",
		Size:     len(audioData),
	}
	if duration > 0 {
		audioInfo.Duration = duration
	}

	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{
			{
				Type: event.EventMessage,
				Content: &event.MessageEventContent{
					MsgType:   event.MsgAudio,
					Body:      "audio.m4a",
					URL:       mxc,
					File:      file,
					Info:      audioInfo,
					RelatesTo: relatesTo,
				},
			},
		},
	}, nil
}
