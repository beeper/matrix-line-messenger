package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// ConvertVideo converts a LINE video message to a Matrix video message.
func (h *Handler) ConvertVideo(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data line.Message, decryptedBody string, relatesTo *event.RelatesTo) (*bridgev2.ConvertedMessage, error) {
	client := h.NewClient()
	oid := data.ContentMetadata["OID"]
	isPlainMedia := oid == ""

	if oid == "" && decryptedBody != "" && strings.Contains(decryptedBody, "OID") {
		var decryptInfo struct {
			OID         string `json:"OID"`
			KeyMaterial string `json:"keyMaterial"`
			FileName    string `json:"fileName"`
		}
		if err := json.Unmarshal([]byte(decryptedBody), &decryptInfo); err == nil && decryptInfo.OID != "" {
			oid = decryptInfo.OID
			isPlainMedia = false
		}
	}

	// For plain media, the video is stored at r/talk/m/{messageID}
	if isPlainMedia {
		oid = data.ID
	}

	if oid == "" {
		return nil, nil
	}

	sid := "emv"
	if isPlainMedia {
		sid = "m"
	}
	dlStart := time.Now()
	videoData, err := client.DownloadOBSWithSID(ctx, oid, data.ID, sid)

	if newClient, ok := h.tryRecoverClient(ctx, err); ok {
		client = newClient
		videoData, err = client.DownloadOBSWithSID(ctx, oid, data.ID, sid)
	}
	_ = client

	if err != nil {
		h.Log.Warn().
			Err(err).
			Str("oid", oid).
			Str("msg_id", data.ID).
			Bool("plain_media", isPlainMedia).
			Dur("download_duration", time.Since(dlStart)).
			Msg("Failed to download video from OBS, sending placeholder")
		return &bridgev2.ConvertedMessage{
			Parts: []*bridgev2.ConvertedMessagePart{
				{
					Type: event.EventMessage,
					Content: &event.MessageEventContent{
						MsgType:   event.MsgNotice,
						Body:      "[Video unavailable — LINE media expired before it could be bridged]",
						RelatesTo: relatesTo,
					},
				},
			},
		}, nil
	}

	decrypted := false
	if decryptedBody != "" && strings.Contains(decryptedBody, "keyMaterial") {
		var decryptInfo struct {
			KeyMaterial string `json:"keyMaterial"`
			FileName    string `json:"fileName"`
		}
		if err := json.Unmarshal([]byte(decryptedBody), &decryptInfo); err == nil && decryptInfo.KeyMaterial != "" {
			h.Log.Debug().
				Str("key_material_len", fmt.Sprintf("%d", len(decryptInfo.KeyMaterial))).
				Str("file_name", decryptInfo.FileName).
				Msg("Decrypting E2EE video")

			decryptedVideo, err := h.DecryptMedia(videoData, decryptInfo.KeyMaterial)
			if err != nil {
				h.Log.Error().Err(err).Msg("Failed to decrypt video data")
				return nil, fmt.Errorf("failed to decrypt video data: %w", err)
			}
			videoData = decryptedVideo
			decrypted = true
			h.Log.Info().Int("decrypted_size", len(videoData)).Msg("Successfully decrypted video")
		}
	}

	// ENC_KM is a fallback when the in-body keyMaterial path didn't decrypt
	// (e.g. E2EE chunk decryption failed). Running it unconditionally would
	// double-decrypt for bridge-sent LSON videos and corrupt the bytes.
	if !decrypted {
		if encKM := data.ContentMetadata["ENC_KM"]; encKM != "" && len(videoData) > 32 {
			h.Log.Debug().
				Str("enc_km_preview", encKM[:min(20, len(encKM))]+"...").
				Msg("Decrypting video using ENC_KM from metadata (fallback)")

			decryptedVideo, err := h.DecryptMedia(videoData, encKM)
			if err != nil {
				h.Log.Warn().Err(err).Msg("ENC_KM fallback decrypt failed, sending raw video")
			} else {
				videoData = decryptedVideo
				h.Log.Info().Int("decrypted_size", len(videoData)).Msg("Successfully decrypted video from ENC_KM")
			}
		}
	}

	fileName := data.ContentMetadata["FILE_NAME"]

	if fileName == "" && decryptedBody != "" && strings.Contains(decryptedBody, "fileName") {
		var decryptInfo struct {
			FileName string `json:"fileName"`
		}
		if err := json.Unmarshal([]byte(decryptedBody), &decryptInfo); err == nil && decryptInfo.FileName != "" {
			fileName = decryptInfo.FileName
		}
	}

	if fileName == "" {
		fileName = "video.mp4"
	}

	mimeType := "video/mp4"
	if strings.HasSuffix(strings.ToLower(fileName), ".webm") {
		mimeType = "video/webm"
	}

	mxc, file, err := intent.UploadMedia(ctx, portal.MXID, videoData, fileName, mimeType)
	if err != nil {
		h.Log.Error().Err(err).Int("size_bytes", len(videoData)).Msg("Failed to upload video to Matrix")
		return nil, fmt.Errorf("failed to upload video to matrix: %w", err)
	}

	h.Log.Info().
		Str("mxc", mxc.ParseOrIgnore().String()).
		Str("file_name", fileName).
		Int("size", len(videoData)).
		Dur("download_duration", time.Since(dlStart)).
		Msg("Successfully uploaded video to Matrix")

	var duration int
	if durationStr := data.ContentMetadata["DURATION"]; durationStr != "" {
		if d, err := strconv.Atoi(durationStr); err == nil {
			duration = d
		}
	}

	videoInfo := &event.FileInfo{
		MimeType: mimeType,
		Size:     len(videoData),
	}
	if duration > 0 {
		videoInfo.Duration = duration
	}
	if thumbInfo := data.ContentMetadata["MEDIA_THUMB_INFO"]; thumbInfo != "" {
		var info struct {
			Width  int `json:"width"`
			Height int `json:"height"`
		}
		if json.Unmarshal([]byte(thumbInfo), &info) == nil && info.Width > 0 && info.Height > 0 {
			videoInfo.Width = info.Width
			videoInfo.Height = info.Height
		}
	}

	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{
			{
				Type: event.EventMessage,
				Content: &event.MessageEventContent{
					MsgType:   event.MsgVideo,
					Body:      fileName,
					URL:       mxc,
					File:      file,
					Info:      videoInfo,
					RelatesTo: relatesTo,
				},
			},
		},
	}, nil
}
