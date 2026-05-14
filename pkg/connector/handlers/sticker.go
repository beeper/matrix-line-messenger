package handlers

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// ConvertSticker converts a LINE sticker message to a Matrix image or text fallback.
func (h *Handler) ConvertSticker(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data line.Message, relatesTo *event.RelatesTo) (*bridgev2.ConvertedMessage, error) {
	stkID := data.ContentMetadata["STKID"]
	stkTxt := data.ContentMetadata["STKTXT"]
	stkOpt := data.ContentMetadata["STKOPT"]
	if stkTxt == "" {
		stkTxt = "[Sticker]"
	}

	if stkID != "" {
		isAnimated := strings.Contains(stkOpt, "A")
		baseURL := "https://stickershop.line-scdn.net/stickershop/v1/sticker/%s/android/sticker_animation.png"
		fallbackURL := "https://stickershop.line-scdn.net/stickershop/v1/sticker/%s/android/sticker.png"
		if !isAnimated {
			baseURL, fallbackURL = fallbackURL, baseURL
		}

		url := fmt.Sprintf(baseURL, stkID)
		resp, err := h.HTTPClient.Get(url)
		if (err != nil || resp.StatusCode != 200) && fallbackURL != "" {
			if resp != nil {
				resp.Body.Close()
			}
			url = fmt.Sprintf(fallbackURL, stkID)
			resp, err = h.HTTPClient.Get(url)
		}

		if err != nil {
			h.Log.Warn().Err(err).Str("stk_id", stkID).Msg("Failed to download sticker")
		} else if resp.StatusCode != 200 {
			h.Log.Warn().Int("status_code", resp.StatusCode).Str("stk_id", stkID).Msg("Failed to download sticker")
			resp.Body.Close()
		} else {
			defer resp.Body.Close()
			stkData, err := io.ReadAll(resp.Body)
			if err != nil {
				h.Log.Warn().Err(err).Str("stk_id", stkID).Msg("Failed to read sticker body")
			} else {
				if isAnimated {
					stkData = forceAPNGLoop(stkData)
				}

				mimeType := resp.Header.Get("Content-Type")
				if mimeType == "" {
					mimeType = "image/png"
				}

				var ext string
				switch mimeType {
				case "image/jpeg", "image/jpg":
					ext = "jpg"
				case "image/webp":
					ext = "webp"
				case "image/gif":
					ext = "gif"
				case "image/png":
					ext = "png"
				default:
					ext = "png"
				}

				bodyName := "sticker." + ext

				mxc, file, err := intent.UploadMedia(ctx, portal.MXID, stkData, bodyName, mimeType)
				if err != nil {
					h.Log.Warn().Err(err).Msg("Failed to upload sticker to Matrix")
				} else {
					return &bridgev2.ConvertedMessage{
						Parts: []*bridgev2.ConvertedMessagePart{
							{
								Type: event.EventMessage,
								Content: &event.MessageEventContent{
									MsgType: event.MsgImage,
									Body:    bodyName,
									URL:     mxc,
									File:    file,
									Info: &event.FileInfo{
										MimeType: mimeType,
										Size:     len(stkData),
									},
									RelatesTo: relatesTo,
								},
							},
						},
					}, nil
				}
			}
		}
	}

	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{
			{
				Type: event.EventMessage,
				Content: &event.MessageEventContent{
					MsgType:   event.MsgText,
					Body:      stkTxt,
					RelatesTo: relatesTo,
				},
			},
		},
	}, nil
}

// forceAPNGLoop sets the APNG loop count to 0 (infinite) in an acTL chunk.
func forceAPNGLoop(data []byte) []byte {
	if len(data) < 8 || string(data[:8]) != "\x89PNG\r\n\x1a\n" {
		return data
	}

	offset := 8
	for offset < len(data) {
		if offset+8 > len(data) {
			break
		}
		length := binary.BigEndian.Uint32(data[offset : offset+4])
		chunkType := string(data[offset+4 : offset+8])

		if chunkType == "acTL" {
			if length >= 8 && offset+8+8 <= len(data) {
				binary.BigEndian.PutUint32(data[offset+8+4:offset+8+8], 0)

				crc := crc32.NewIEEE()
				crc.Write(data[offset+4 : offset+8+int(length)])
				newCRC := crc.Sum32()

				binary.BigEndian.PutUint32(data[offset+8+int(length):offset+8+int(length)+4], newCRC)
			}
			break
		}

		offset += 4 + 4 + int(length) + 4
	}
	return data
}
