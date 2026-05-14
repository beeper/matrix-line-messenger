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

// ConvertFile converts a LINE file message to a Matrix file message.
func (h *Handler) ConvertFile(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data line.Message, decryptedBody string, relatesTo *event.RelatesTo) (*bridgev2.ConvertedMessage, error) {
	client := h.NewClient()
	oid := data.ContentMetadata["OID"]
	isPlainMedia := oid == ""

	if oid == "" && decryptedBody != "" && strings.Contains(decryptedBody, "fileName") {
		h.Log.Debug().Msg("File message with encrypted payload, OID in metadata")
	}

	// For plain media, the file is stored at r/talk/m/{messageID}
	if isPlainMedia {
		oid = data.ID
	}

	if oid == "" {
		return nil, nil
	}

	sid := "emf"
	if isPlainMedia {
		sid = "m"
	}
	fileData, err := client.DownloadOBSWithSID(ctx, oid, data.ID, sid)
	if err != nil {
		h.Log.Warn().
			Err(err).
			Str("oid", oid).
			Bool("plain_media", isPlainMedia).
			Msg("Failed to download file from OBS, sending placeholder")
		return &bridgev2.ConvertedMessage{
			Parts: []*bridgev2.ConvertedMessagePart{
				{
					Type: event.EventMessage,
					Content: &event.MessageEventContent{
						MsgType:   event.MsgNotice,
						Body:      "[File unavailable — LINE media expired before it could be bridged]",
						RelatesTo: relatesTo,
					},
				},
			},
		}, nil
	}

	// Try to decrypt using keyMaterial from encrypted payload
	var fileName string
	if decryptedBody != "" && strings.Contains(decryptedBody, "keyMaterial") {
		var decryptInfo struct {
			KeyMaterial string `json:"keyMaterial"`
			FileName    string `json:"fileName"`
		}
		if err := json.Unmarshal([]byte(decryptedBody), &decryptInfo); err != nil {
			h.Log.Error().Err(err).Msg("Failed to parse file payload JSON")
			return nil, fmt.Errorf("failed to parse file payload: %w", err)
		}

		if decryptInfo.KeyMaterial != "" {
			keyPreview := decryptInfo.KeyMaterial
			if len(keyPreview) > 20 {
				keyPreview = keyPreview[:20] + "..."
			}
			h.Log.Debug().
				Str("key_material_preview", keyPreview).
				Msg("Decrypting file using keyMaterial from payload")

			decryptedFile, err := h.DecryptMedia(fileData, decryptInfo.KeyMaterial)
			if err != nil {
				h.Log.Error().Err(err).Msg("Failed to decrypt file data")
				return nil, fmt.Errorf("failed to decrypt file data: %w", err)
			}
			fileData = decryptedFile
			h.Log.Info().Int("decrypted_size", len(fileData)).Msg("Successfully decrypted file")
		}

		if decryptInfo.FileName != "" {
			fileName = decryptInfo.FileName
		}
	}

	if fileName == "" {
		fileName = data.ContentMetadata["FILE_NAME"]
	}

	if fileName == "" {
		fileName = "file.bin"
	}

	// Detect MIME type from file extension
	mimeType := "application/octet-stream"
	if strings.HasSuffix(strings.ToLower(fileName), ".pdf") {
		mimeType = "application/pdf"
	}

	mxc, file, err := intent.UploadMedia(ctx, portal.MXID, fileData, fileName, mimeType)
	if err != nil {
		h.Log.Error().Err(err).Int("size_bytes", len(fileData)).Msg("Failed to upload file to Matrix")
		return nil, fmt.Errorf("failed to upload file to matrix: %w", err)
	}

	h.Log.Info().
		Str("mxc", mxc.ParseOrIgnore().String()).
		Str("file_name", fileName).
		Int("size", len(fileData)).
		Msg("Successfully uploaded file to Matrix")

	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{
			{
				Type: event.EventMessage,
				Content: &event.MessageEventContent{
					MsgType: event.MsgFile,
					Body:    fileName,
					URL:     mxc,
					File:    file,
					Info: &event.FileInfo{
						MimeType: mimeType,
						Size:     len(fileData),
					},
					RelatesTo: relatesTo,
				},
			},
		},
	}, nil
}
