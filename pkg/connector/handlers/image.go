package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// ConvertImage converts a LINE image message to a Matrix image message.
func (h *Handler) ConvertImage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data line.Message, decryptedBody string, relatesTo *event.RelatesTo) (*bridgev2.ConvertedMessage, error) {
	client := h.NewClient()
	oid := data.ContentMetadata["OID"]
	isPlainMedia := oid == ""

	// For plain media, the image is stored at r/talk/m/{messageID}
	if isPlainMedia {
		oid = data.ID
	}

	if oid == "" {
		return nil, nil
	}

	var imgData []byte
	var err error
	if isPlainMedia {
		imgData, err = client.DownloadOBSWithSID(ctx, oid, data.ID, "m")
	} else {
		imgData, err = client.DownloadOBS(ctx, oid, data.ID)
	}

	// Refresh token if we get a 401
	if newClient, ok := h.tryRecoverClient(ctx, err); ok {
		client = newClient
		if isPlainMedia {
			imgData, err = client.DownloadOBSWithSID(ctx, oid, data.ID, "m")
		} else {
			imgData, err = client.DownloadOBS(ctx, oid, data.ID)
		}
	}

	if err != nil {
		h.Log.Warn().
			Err(err).
			Str("oid", oid).
			Str("msg_id", data.ID).
			Bool("plain_media", isPlainMedia).
			Msg("Failed to download image from OBS, sending placeholder")
		return &bridgev2.ConvertedMessage{
			Parts: []*bridgev2.ConvertedMessagePart{
				{
					Type: event.EventMessage,
					Content: &event.MessageEventContent{
						MsgType:   event.MsgNotice,
						Body:      "[Image unavailable — LINE media expired before it could be bridged]",
						RelatesTo: relatesTo,
					},
				},
			},
		}, nil
	}

	// Decrypt image if it has keyMaterial (E2EE)
	if decryptedBody != "" && strings.Contains(decryptedBody, "keyMaterial") {
		var decryptInfo struct {
			KeyMaterial string `json:"keyMaterial"`
			FileName    string `json:"fileName"`
		}
		if err := json.Unmarshal([]byte(decryptedBody), &decryptInfo); err == nil && decryptInfo.KeyMaterial != "" {
			decryptedImg, err := h.DecryptMedia(imgData, decryptInfo.KeyMaterial)
			if err != nil {
				h.Log.Error().Err(err).Msg("Failed to decrypt image data")
				return nil, fmt.Errorf("failed to decrypt image data: %w", err)
			}
			imgData = decryptedImg
		}
	}

	// Upload to Matrix
	mxc, file, err := intent.UploadMedia(ctx, portal.MXID, imgData, "image.jpg", "image/jpeg")
	if err != nil {
		h.Log.Error().Err(err).Int("size_bytes", len(imgData)).Msg("Failed to upload image to Matrix")
		return nil, fmt.Errorf("failed to upload image to matrix: %w", err)
	}

	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{
			{
				Type: event.EventMessage,
				Content: &event.MessageEventContent{
					MsgType:   event.MsgImage,
					Body:      "image.jpg",
					URL:       mxc,
					File:      file,
					RelatesTo: relatesTo,
				},
			},
		},
	}, nil
}
