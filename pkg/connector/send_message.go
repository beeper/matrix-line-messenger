package connector

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"regexp"
	"strconv"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

type mentionEntry struct {
	S string `json:"S"`
	E string `json:"E"`
	M string `json:"M,omitempty"`
	A string `json:"A,omitempty"`
}

var mentionLinkRegex = regexp.MustCompile(`<a\s+[^>]*href="https://matrix\.to/#/([^"]+)"[^>]*>([^<]+)</a>`)

func (lc *LineClient) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	client := line.NewClient(lc.AccessToken)
	portalMid := string(msg.Portal.ID)
	fromMid := lc.midOrFallback()

	lowerPortalID := strings.ToLower(portalMid)
	isGroup := strings.HasPrefix(lowerPortalID, "c") || strings.HasPrefix(lowerPortalID, "r")

	// Determine whether we need to send as plain text (peer/group has Letter Sealing off).
	plainText := false
	if lc.E2EE == nil {
		plainText = true
		lc.UserLogin.Bridge.Log.Warn().Msg("E2EE not initialized, sending as plain text")
	} else if isGroup {
		if lc.isGroupNoE2EE(portalMid) {
			plainText = true
		}
	} else {
		// 1:1 — probe peer key to determine E2EE support
		_, _, errPeer := lc.ensurePeerKey(ctx, portalMid)
		if errPeer != nil && line.IsNoUsableE2EEPublicKey(errPeer) {
			plainText = true
		} else if errPeer != nil {
			return nil, fmt.Errorf("failed to get peer key: %w", errPeer)
		}
	}

	var chunks []string
	var err error
	contentType := int(ContentText)
	contentMetadata := map[string]string{}
	if !plainText {
		contentMetadata["e2eeVersion"] = "2"
	}

	// Detect media files sent as MsgFile and handle them with the correct LINE content type.
	// Matrix clients often send media as MsgFile (e.g. drag-and-drop), which would otherwise
	// be sent as ContentFile (14) instead of the appropriate media type on LINE.
	effectiveMsgType := msg.Content.MsgType
	if effectiveMsgType == event.MsgFile && msg.Content.Info != nil {
		mime := msg.Content.Info.MimeType
		if strings.HasPrefix(mime, "audio/") {
			effectiveMsgType = event.MsgAudio
		} else if strings.HasPrefix(mime, "video/") {
			effectiveMsgType = event.MsgVideo
		} else if strings.HasPrefix(mime, "image/") {
			effectiveMsgType = event.MsgImage
		}
	}

	// For non-text messages, check with the server whether to use E2EE or plain media upload.
	// This must happen before media processing since it affects the upload path.
	useE2EEMedia := !plainText
	if useE2EEMedia && effectiveMsgType != event.MsgText {
		mediaContentType := contentTypeForMsgType(effectiveMsgType)
		if !lc.shouldUseE2EEMediaFlow(portalMid, mediaContentType) {
			lc.UserLogin.Bridge.Log.Info().
				Str("portal", portalMid).
				Int("content_type", mediaContentType).
				Msg("Server indicates plain media flow for this chat")
			useE2EEMedia = false
			plainText = true
			delete(contentMetadata, "e2eeVersion")
		}
	}

	// For plain text, we set lineMsg.Text directly; payload is used only for E2EE.
	var payload []byte
	var plainTextBody string // used when plainText == true for text messages

	// For plain media: we send the message first, then upload media to r/talk/m/{msgId}.
	var plainMediaData []byte // raw media data to upload after sending
	var plainThumbData []byte // raw thumbnail data to upload after sending

	// For group chats, save original data in case E2EE encryption fails and we fall back to plain.
	var originalMediaData []byte
	var originalThumbData []byte

	// For file uploads, save the raw data so we can retry with ZIP wrapping if LINE rejects
	// the raw file (some file types require ZIP, others like PDF work directly).
	var rawFileData []byte
	var rawFileName string

	switch effectiveMsgType {
	case event.MsgText:
		contentType = int(ContentText)
		if plainText {
			plainTextBody = msg.Content.Body
		} else {
			payload, err = json.Marshal(map[string]string{"text": msg.Content.Body})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal text payload: %w", err)
			}
		}

	case event.MsgImage:
		data, err := lc.UserLogin.Bridge.Bot.DownloadMedia(ctx, msg.Content.URL, msg.Content.File)
		if err != nil {
			return nil, fmt.Errorf("failed to download media from matrix: %w", err)
		}

		mimeType := msg.Content.Info.MimeType
		isGif := mimeType == "image/gif"
		isAnimated := isGif && isAnimatedGif(data)

		extension := "jpg"
		if isGif {
			extension = "gif"
		} else if mimeType == "image/png" {
			extension = "png"
		}

		contentType = int(ContentImage)

		if plainText {
			// Plain media: save data for post-send upload to r/talk/m/{msgId}
			plainMediaData = data

			thumbnailData, thumbWidth, thumbHeight, err := generateThumbnail(data)
			if err != nil {
				lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to generate thumbnail, continuing without it")
			} else {
				plainThumbData = thumbnailData
				mediaThumbInfo := map[string]interface{}{
					"width":  thumbWidth,
					"height": thumbHeight,
				}
				if thumbInfoJSON, err := json.Marshal(mediaThumbInfo); err == nil {
					contentMetadata["MEDIA_THUMB_INFO"] = string(thumbInfoJSON)
				}
			}

			contentMetadata["FILE_SIZE"] = fmt.Sprintf("%d", len(data))
			contentMetadata["contentType"] = fmt.Sprintf("%d", ContentImage)

			fileName := msg.Content.GetFileName()
			if fileName == "" {
				fileName = "image." + extension
			}
			contentMetadata["FILE_NAME"] = fileName

			mediaContentInfo := map[string]interface{}{
				"category":  "original",
				"fileSize":  len(data),
				"extension": extension,
			}
			if isAnimated {
				mediaContentInfo["animated"] = true
			}
			if mediaInfoJSON, err := json.Marshal(mediaContentInfo); err == nil {
				contentMetadata["MEDIA_CONTENT_INFO"] = string(mediaInfoJSON)
			}
		} else {
			// E2EE: encrypt, upload to OBS first, send with OID
			// Save original data for potential group E2EE fallback
			if isGroup {
				originalMediaData = data
				if thumbData, _, _, tErr := generateThumbnail(data); tErr == nil {
					originalThumbData = thumbData
				}
			}

			uploadData, keyMaterialB64, err := lc.encryptFileData(data)
			if err != nil {
				return nil, fmt.Errorf("failed to encrypt image data: %w", err)
			}

			oid, err := client.UploadOBS(uploadData)
			if err != nil {
				return nil, fmt.Errorf("failed to upload image to OBS: %w", err)
			}

			thumbnailData, thumbWidth, thumbHeight, err := generateThumbnail(data)
			if err != nil {
				lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to generate thumbnail, continuing without it")
			} else if thumbToUpload, err := encryptThumbnail(thumbnailData, keyMaterialB64); err != nil {
				lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to encrypt thumbnail, continuing without it")
			} else {
				previewOID := fmt.Sprintf("%s__ud-preview", oid)
				if err := client.UploadOBSWithOID(thumbToUpload, previewOID); err != nil {
					lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to upload preview, continuing without it")
				} else {
					mediaThumbInfo := map[string]interface{}{
						"width":  thumbWidth,
						"height": thumbHeight,
					}
					if thumbInfoJSON, err := json.Marshal(mediaThumbInfo); err == nil {
						contentMetadata["MEDIA_THUMB_INFO"] = string(thumbInfoJSON)
					}

					lc.UserLogin.Bridge.Log.Info().
						Str("preview_oid", previewOID).
						Int("thumb_size", len(thumbnailData)).
						Int("thumb_width", thumbWidth).
						Int("thumb_height", thumbHeight).
						Msg("Uploaded preview thumbnail")
				}
			}

			contentMetadata["OID"] = oid
			contentMetadata["SID"] = "emi"
			contentMetadata["FILE_SIZE"] = fmt.Sprintf("%d", len(uploadData))
			contentMetadata["contentType"] = fmt.Sprintf("%d", ContentImage)
			contentMetadata["ENC_KM"] = keyMaterialB64

			fileName := msg.Content.GetFileName()
			if fileName == "" {
				fileName = "image." + extension
			}
			contentMetadata["FILE_NAME"] = fileName

			mediaContentInfo := map[string]interface{}{
				"category":  "original",
				"fileSize":  len(uploadData),
				"extension": extension,
			}
			if isAnimated {
				mediaContentInfo["animated"] = true
			}
			if mediaInfoJSON, err := json.Marshal(mediaContentInfo); err == nil {
				contentMetadata["MEDIA_CONTENT_INFO"] = string(mediaInfoJSON)
			}

			imgPayload := map[string]string{"keyMaterial": keyMaterialB64}
			payload, _ = json.Marshal(imgPayload)
		}

	case event.MsgFile:
		data, err := lc.UserLogin.Bridge.Bot.DownloadMedia(ctx, msg.Content.URL, msg.Content.File)
		if err != nil {
			return nil, fmt.Errorf("failed to download file from matrix: %w", err)
		}

		contentType = int(ContentFile)

		fileName := msg.Content.GetFileName()
		if fileName == "" {
			fileName = "file.bin"
		}
		rawFileData = data
		rawFileName = fileName

		contentMetadata["FILE_NAME"] = fileName
		contentMetadata["FILE_SIZE"] = fmt.Sprintf("%d", len(data))
		contentMetadata["contentType"] = fmt.Sprintf("%d", ContentFile)

		if plainText {
			// Plain media: save data for post-send upload
			plainMediaData = data

			lc.UserLogin.Bridge.Log.Info().
				Str("file_name", fileName).
				Bool("plain_text", plainText).
				Int("original_size", len(data)).
				Msg("Prepared plain file message")
		} else {
			if isGroup {
				originalMediaData = data
			}

			uploadData, keyMaterialB64, err := lc.encryptFileData(data)
			if err != nil {
				return nil, fmt.Errorf("failed to encrypt file data: %w", err)
			}

			oid, err := client.UploadOBSWithSID(uploadData, "emf")
			if err != nil {
				return nil, fmt.Errorf("failed to upload file to OBS: %w", err)
			}

			contentMetadata["OID"] = oid
			contentMetadata["SID"] = "emf"
			contentMetadata["ENC_KM"] = keyMaterialB64

			filePayload := map[string]string{
				"keyMaterial": keyMaterialB64,
				"fileName":    fileName,
			}
			payloadBytes, _ := json.Marshal(filePayload)
			payload = payloadBytes

			lc.UserLogin.Bridge.Log.Info().
				Str("oid", oid).
				Str("file_name", fileName).
				Bool("plain_text", plainText).
				Int("upload_size", len(uploadData)).
				Int("original_size", len(data)).
				Msg("Prepared file message")
		}

	case event.MsgVideo:
		data, err := lc.UserLogin.Bridge.Bot.DownloadMedia(ctx, msg.Content.URL, msg.Content.File)
		if err != nil {
			return nil, fmt.Errorf("failed to download video from matrix: %w", err)
		}

		contentType = int(ContentVideo)
		contentMetadata["FILE_SIZE"] = fmt.Sprintf("%d", len(data))
		contentMetadata["contentType"] = fmt.Sprintf("%d", ContentVideo)

		if msg.Content.Info.Duration > 0 {
			contentMetadata["DURATION"] = fmt.Sprintf("%d", msg.Content.Info.Duration)
		}

		if plainText {
			// Plain media: save data for post-send upload
			plainMediaData = data

			thumbnailData, thumbWidth, thumbHeight, err := extractVideoThumbnail(data)
			if err != nil {
				lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to extract video thumbnail, using placeholder")
				thumbWidth = 384
				thumbHeight = 384
				placeholderImg := image.NewRGBA(image.Rect(0, 0, thumbWidth, thumbHeight))
				var thumbBuf bytes.Buffer
				jpeg.Encode(&thumbBuf, placeholderImg, &jpeg.Options{Quality: 30})
				thumbnailData = thumbBuf.Bytes()
			}
			if len(thumbnailData) > 0 {
				plainThumbData = thumbnailData
				mediaThumbInfo := map[string]interface{}{
					"width":  thumbWidth,
					"height": thumbHeight,
				}
				if thumbInfoJSON, err := json.Marshal(mediaThumbInfo); err == nil {
					contentMetadata["MEDIA_THUMB_INFO"] = string(thumbInfoJSON)
				}
			}

			lc.UserLogin.Bridge.Log.Info().
				Bool("plain_text", plainText).
				Int("original_size", len(data)).
				Msg("Prepared plain video message")
		} else {
			if isGroup {
				originalMediaData = data
				if thumbData, _, _, tErr := extractVideoThumbnail(data); tErr == nil {
					originalThumbData = thumbData
				}
			}

			uploadData, keyMaterialB64, err := lc.encryptVideoData(data)
			if err != nil {
				return nil, fmt.Errorf("failed to encrypt video data: %w", err)
			}

			oid, err := client.UploadOBSWithSID(uploadData, "emv")
			if err != nil {
				return nil, fmt.Errorf("failed to upload video to OBS: %w", err)
			}

			chunkHashes := generateChunkHashes(uploadData[:len(uploadData)-32])
			if len(chunkHashes) > 0 {
				hashOID := fmt.Sprintf("%s__ud-hash", oid)
				if err := client.UploadOBSWithOIDAndSID(chunkHashes, hashOID, "emv"); err != nil {
					lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to upload video hash, continuing without it")
				} else {
					lc.UserLogin.Bridge.Log.Info().
						Str("hash_oid", hashOID).
						Int("hash_size", len(chunkHashes)).
						Msg("Uploaded video chunk hashes")
				}
			}

			thumbnailData, thumbWidth, thumbHeight, err := extractVideoThumbnail(data)
			if err != nil {
				lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to extract video thumbnail, using placeholder")
				thumbWidth = 384
				thumbHeight = 384
				placeholderImg := image.NewRGBA(image.Rect(0, 0, thumbWidth, thumbHeight))
				var thumbBuf bytes.Buffer
				jpeg.Encode(&thumbBuf, placeholderImg, &jpeg.Options{Quality: 30})
				thumbnailData = thumbBuf.Bytes()
			}

			if len(thumbnailData) > 0 {
				if thumbToUpload, err := encryptThumbnail(thumbnailData, keyMaterialB64); err != nil {
					lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to encrypt video thumbnail, continuing without it")
				} else {
					previewOID := fmt.Sprintf("%s__ud-preview", oid)
					if err := client.UploadOBSWithOIDAndSID(thumbToUpload, previewOID, "emv"); err != nil {
						lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to upload video preview, continuing without it")
					} else {
						mediaThumbInfo := map[string]interface{}{
							"width":  thumbWidth,
							"height": thumbHeight,
						}
						if thumbInfoJSON, err := json.Marshal(mediaThumbInfo); err == nil {
							contentMetadata["MEDIA_THUMB_INFO"] = string(thumbInfoJSON)
						}

						lc.UserLogin.Bridge.Log.Info().
							Str("preview_oid", previewOID).
							Int("preview_size", len(thumbToUpload)).
							Int("thumb_width", thumbWidth).
							Int("thumb_height", thumbHeight).
							Msg("Uploaded video preview")
					}
				}
			}

			contentMetadata["OID"] = oid
			contentMetadata["SID"] = "emv"
			contentMetadata["ENC_KM"] = keyMaterialB64

			vidPayload := map[string]string{"keyMaterial": keyMaterialB64}
			payload, _ = json.Marshal(vidPayload)

			lc.UserLogin.Bridge.Log.Info().
				Str("oid", oid).
				Bool("plain_text", plainText).
				Int("upload_size", len(uploadData)).
				Msg("Prepared video message")
		}

	case event.MsgAudio:
		data, err := lc.UserLogin.Bridge.Bot.DownloadMedia(ctx, msg.Content.URL, msg.Content.File)
		if err != nil {
			return nil, fmt.Errorf("failed to download audio from matrix: %w", err)
		}

		contentType = int(ContentAudio)
		contentMetadata["FILE_SIZE"] = fmt.Sprintf("%d", len(data))
		contentMetadata["contentType"] = fmt.Sprintf("%d", ContentAudio)

		if msg.Content.Info != nil && msg.Content.Info.Duration > 0 {
			contentMetadata["DURATION"] = fmt.Sprintf("%d", msg.Content.Info.Duration)
			contentMetadata["AUDLEN"] = fmt.Sprintf("%d", msg.Content.Info.Duration)
		}

		if plainText {
			plainMediaData = data
		} else {
			if isGroup {
				originalMediaData = data
			}

			uploadData, keyMaterialB64, err := lc.encryptFileData(data)
			if err != nil {
				return nil, fmt.Errorf("failed to encrypt audio data: %w", err)
			}

			oid, err := client.UploadOBSWithSID(uploadData, "ema")
			if err != nil {
				return nil, fmt.Errorf("failed to upload audio to OBS: %w", err)
			}

			contentMetadata["OID"] = oid
			contentMetadata["SID"] = "ema"
			contentMetadata["ENC_KM"] = keyMaterialB64

			audioPayload := map[string]string{"keyMaterial": keyMaterialB64}
			payload, _ = json.Marshal(audioPayload)

			lc.UserLogin.Bridge.Log.Info().
				Str("oid", oid).
				Int("upload_size", len(uploadData)).
				Msg("Prepared audio message")
		}

	default:
		return nil, fmt.Errorf("message type %s not implemented", effectiveMsgType)
	}

	// Process mentions — set MENTION content metadata for LINE notification
	if msg.Content.Mentions == nil {
		msg.Content.Mentions = &event.Mentions{}
	}
	// Detect implicit @room/@all/@everyone in body for LINE group-wide mentions
	if !msg.Content.Mentions.Room {
		lowerBody := strings.ToLower(msg.Content.Body)
		if strings.Contains(lowerBody, "@room") || strings.Contains(lowerBody, "@all") || strings.Contains(lowerBody, "@everyone") {
			msg.Content.Mentions.Room = true
		}
	}
	mentionMeta := lc.buildMentionMetadata(ctx, msg.Content.Body, msg.Content.FormattedBody, msg.Content.Mentions)
	for k, v := range mentionMeta {
		contentMetadata[k] = v
	}

	// Encryption phase — skip entirely for plain text
	if !plainText {
		if isGroup {
			if errFetch := lc.fetchAndUnwrapGroupKey(ctx, portalMid, 0); errFetch != nil {
				lc.UserLogin.Bridge.Log.Debug().Err(errFetch).Str("chat_mid", portalMid).Msg("fetchAndUnwrapGroupKey before encrypt failed")
			}
			if contentType != int(ContentText) {
				chunks, err = lc.E2EE.EncryptGroupMessageRaw(portalMid, fromMid, contentType, payload)
			} else {
				chunks, err = lc.E2EE.EncryptGroupMessage(portalMid, fromMid, msg.Content.Body)
			}
			if err != nil {
				if errFetch := lc.fetchAndUnwrapGroupKey(ctx, portalMid, 0); errFetch == nil {
					if contentType != int(ContentText) {
						chunks, err = lc.E2EE.EncryptGroupMessageRaw(portalMid, fromMid, contentType, payload)
					} else {
						chunks, err = lc.E2EE.EncryptGroupMessage(portalMid, fromMid, msg.Content.Body)
					}
				}
				if err != nil {
					// E2EE setup failed — fall back to plain text
					lc.markGroupNoE2EE(portalMid)
					lc.UserLogin.Bridge.Log.Warn().Err(err).Str("chat_mid", portalMid).
						Msg("Failed to set up E2EE for group, falling back to plain text")
					plainText = true
					chunks = nil
					err = nil
					delete(contentMetadata, "e2eeVersion")
					if contentType == int(ContentText) {
						plainTextBody = msg.Content.Body
					} else {
						delete(contentMetadata, "OID")
						delete(contentMetadata, "SID")
						delete(contentMetadata, "ENC_KM")
						plainMediaData = originalMediaData
						plainThumbData = originalThumbData
					}
				}
			}
		} else {
			// 1-1 Encryption (peer key already fetched above)
			myRaw, myKeyID, errKey := lc.E2EE.MyKeyIDs()
			if errKey != nil {
				return nil, fmt.Errorf("missing own E2EE key: %w", errKey)
			}
			peerRaw, peerPub, errPeer := lc.ensurePeerKey(ctx, portalMid)
			if errPeer != nil {
				return nil, fmt.Errorf("failed to get peer key: %w", errPeer)
			}

			chunks, err = lc.E2EE.EncryptMessageV2Raw(portalMid, fromMid, myKeyID, peerPub, myRaw, peerRaw, contentType, payload)
		}

		if err != nil {
			return nil, fmt.Errorf("encrypt failed: %w", err)
		}
	}

	if plainText {
		lc.UserLogin.Bridge.Log.Info().Str("portal", portalMid).Int("content_type", contentType).Msg("Sending plain text message (no E2EE)")
	}

	now := time.Now().UnixMilli()
	lineMsg := &line.Message{
		ID:              fmt.Sprintf("local-%d", now),
		From:            lc.midOrFallback(),
		To:              portalMid,
		ToType:          int(guessToType(portalMid)),
		SessionID:       0,
		CreatedTime:     json.Number(strconv.FormatInt(now, 10)),
		ContentType:     contentType,
		HasContent:      contentType != int(ContentText),
		ContentMetadata: contentMetadata,
	}

	if plainText {
		lineMsg.Text = plainTextBody
	} else {
		lineMsg.Chunks = chunks
	}

	var relatedMsg *database.Message

	if msg.ReplyTo != nil {
		relatedMsg = msg.ReplyTo
	} else if msg.Content.RelatesTo != nil && msg.Content.RelatesTo.InReplyTo != nil {
		replyToMXID := msg.Content.RelatesTo.InReplyTo.EventID
		if replyToMXID != "" {
			dbMsg, err := lc.UserLogin.Bridge.DB.Message.GetPartByMXID(ctx, replyToMXID)
			if err == nil && dbMsg != nil {
				relatedMsg = dbMsg
			}
		}
	}

	if relatedMsg != nil && relatedMsg.ID != "" && !strings.HasPrefix(string(relatedMsg.ID), "local-") {
		lineMsg.RelatedMessageID = string(relatedMsg.ID)
		lineMsg.MessageRelationType = 3
		lineMsg.RelatedMessageServiceCode = 1
	}

	reqSeq := int(now % 1_000_000_000)
	lc.reqSeqMu.Lock()
	if lc.sentReqSeqs == nil {
		lc.sentReqSeqs = make(map[int]time.Time)
	}
	lc.sentReqSeqs[reqSeq] = time.Now()
	lc.reqSeqMu.Unlock()

	sentMsg, err := client.SendMessage(int64(reqSeq), lineMsg)

	// LINE rejects some file types from the Chrome Extension client.
	// Retry by wrapping the file in a ZIP archive (matching Chrome Extension behavior).
	if err != nil && strings.Contains(err.Error(), "Extension does not support file upload") && rawFileData != nil {
		lc.UserLogin.Bridge.Log.Info().Str("file_name", rawFileName).Msg("File upload rejected by LINE, retrying with ZIP wrapping")

		zipData, zipErr := wrapInZip(rawFileName, rawFileData)
		if zipErr != nil {
			return nil, fmt.Errorf("failed to wrap file in zip: %w", zipErr)
		}
		zipFileName := rawFileName + ".zip"

		contentMetadata["FILE_NAME"] = zipFileName
		contentMetadata["FILE_SIZE"] = fmt.Sprintf("%d", len(zipData))
		lineMsg.ContentMetadata = contentMetadata

		if plainText {
			plainMediaData = zipData
		} else {
			// Re-encrypt and re-upload the zipped data
			uploadData, keyMaterialB64, encErr := lc.encryptFileData(zipData)
			if encErr != nil {
				return nil, fmt.Errorf("failed to encrypt zipped file: %w", encErr)
			}
			oid, uploadErr := client.UploadOBSWithSID(uploadData, "emf")
			if uploadErr != nil {
				return nil, fmt.Errorf("failed to upload zipped file to OBS: %w", uploadErr)
			}
			contentMetadata["OID"] = oid
			contentMetadata["ENC_KM"] = keyMaterialB64
			lineMsg.ContentMetadata = contentMetadata

			zipPayload, _ := json.Marshal(map[string]string{
				"keyMaterial": keyMaterialB64,
				"fileName":    zipFileName,
			})

			if isGroup {
				chunks, err = lc.E2EE.EncryptGroupMessageRaw(portalMid, fromMid, contentType, zipPayload)
			} else {
				myRaw, myKeyID, errKey := lc.E2EE.MyKeyIDs()
				if errKey != nil {
					return nil, fmt.Errorf("missing own E2EE key for zip retry: %w", errKey)
				}
				peerRaw, peerPub, errPeer := lc.ensurePeerKey(ctx, portalMid)
				if errPeer != nil {
					return nil, fmt.Errorf("failed to get peer key for zip retry: %w", errPeer)
				}
				chunks, err = lc.E2EE.EncryptMessageV2Raw(portalMid, fromMid, myKeyID, peerPub, myRaw, peerRaw, contentType, zipPayload)
			}
			if err != nil {
				return nil, fmt.Errorf("failed to encrypt zipped file message: %w", err)
			}
			lineMsg.Chunks = chunks
		}

		retryReqSeq := int(time.Now().UnixMilli() % 1_000_000_000)
		lc.reqSeqMu.Lock()
		lc.sentReqSeqs[retryReqSeq] = time.Now()
		lc.reqSeqMu.Unlock()

		sentMsg, err = client.SendMessage(int64(retryReqSeq), lineMsg)
	}

	// If LINE rejects with "group key is not registered" (code 99),
	// auto-register a group key and retry the send once.
	if err != nil && isGroup && line.IsGroupKeyNotRegisteredError(err) {
		lc.UserLogin.Bridge.Log.Info().Str("chat_mid", portalMid).
			Msg("SendMessage failed: group key not registered, registering and retrying")
		if regErr := lc.autoRegisterGroupKey(ctx, portalMid); regErr == nil {
			lc.UserLogin.Bridge.Log.Info().Str("chat_mid", portalMid).
				Msg("autoRegisterGroupKey succeeded, retrying send")
			if !plainText && lc.E2EE != nil {
				if fetchErr := lc.fetchAndUnwrapGroupKey(ctx, portalMid, 0); fetchErr == nil {
					if contentType != int(ContentText) {
						chunks, err = lc.E2EE.EncryptGroupMessageRaw(portalMid, fromMid, contentType, payload)
					} else {
						chunks, err = lc.E2EE.EncryptGroupMessage(portalMid, fromMid, msg.Content.Body)
					}
					if err == nil {
						lineMsg.Chunks = chunks
						lineMsg.Text = ""
					}
				}
			}
			if err == nil {
				retryReqSeq := int(time.Now().UnixMilli() % 1_000_000_000)
				lc.reqSeqMu.Lock()
				lc.sentReqSeqs[retryReqSeq] = time.Now()
				lc.reqSeqMu.Unlock()
				sentMsg, err = client.SendMessage(int64(retryReqSeq), lineMsg)
			}
		} else {
			lc.UserLogin.Bridge.Log.Warn().Str("chat_mid", portalMid).
				Err(regErr).Msg("autoRegisterGroupKey failed, giving up on retry")
		}
	}

	if err != nil {
		return nil, err
	}

	// For plain media: upload media to r/talk/m/{serverMessageId} after sending
	if plainText && plainMediaData != nil && sentMsg.ID != "" {
		obsType := "image"
		switch contentType {
		case int(ContentVideo):
			obsType = "video"
		case int(ContentAudio):
			obsType = "audio"
		case int(ContentFile):
			obsType = "file"
		}

		if err := client.UploadOBSPlain(plainMediaData, sentMsg.ID, obsType); err != nil {
			return nil, fmt.Errorf("failed to upload plain media to OBS: %w", err)
		}
		lc.UserLogin.Bridge.Log.Info().
			Str("message_id", sentMsg.ID).
			Str("obs_type", obsType).
			Int("media_size", len(plainMediaData)).
			Msg("Uploaded plain media after sending")

		if plainThumbData != nil {
			previewID := fmt.Sprintf("%s__ud-preview", sentMsg.ID)
			if err := client.UploadOBSPlain(plainThumbData, previewID, obsType); err != nil {
				lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to upload plain media thumbnail, continuing without it")
			}
		}
	}

	return &bridgev2.MatrixMessageResponse{
		DB: &database.Message{
			ID:        networkid.MessageID(sentMsg.ID),
			SenderID:  makeUserID(string(lc.UserLogin.ID)),
			Timestamp: time.UnixMilli(now),
		},
	}, nil
}

// wrapInZip creates a ZIP archive containing a single file with the given name and data.
// LINE's Chrome Extension wraps file uploads in ZIP before sending.
func wrapInZip(fileName string, data []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	fw, err := zw.Create(fileName)
	if err != nil {
		return nil, err
	}
	if _, err := fw.Write(data); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func contentTypeForMsgType(msgType event.MessageType) int {
	switch msgType {
	case event.MsgImage:
		return int(ContentImage)
	case event.MsgVideo:
		return int(ContentVideo)
	case event.MsgAudio:
		return int(ContentAudio)
	case event.MsgFile:
		return int(ContentFile)
	default:
		return int(ContentText)
	}
}

func (lc *LineClient) HandleMatrixMessageRemove(ctx context.Context, msg *bridgev2.MatrixMessageRemove) error {
	client := line.NewClient(lc.AccessToken)

	reqSeq := int(time.Now().UnixMilli() % 1_000_000_000)
	lc.reqSeqMu.Lock()
	if lc.sentReqSeqs == nil {
		lc.sentReqSeqs = make(map[int]time.Time)
	}
	lc.sentReqSeqs[reqSeq] = time.Now()
	lc.reqSeqMu.Unlock()

	err := client.UnsendMessage(int64(reqSeq), string(msg.TargetMessage.ID))
	if err != nil && strings.Contains(err.Error(), "message too old") {
		return bridgev2.WrapErrorInStatus(fmt.Errorf("message too old to unsend on LINE (24h limit)")).
			WithStatus(event.MessageStatusFail).
			WithErrorReason(event.MessageStatusTooOld).
			WithIsCertain(true).
			WithSendNotice(true)
	}
	return err
}

func (lc *LineClient) HandleMatrixLeaveRoom(ctx context.Context, portal *bridgev2.Portal) error {
	client := line.NewClient(lc.AccessToken)

	reqSeq := int(time.Now().UnixMilli() % 1_000_000_000)
	lc.reqSeqMu.Lock()
	if lc.sentReqSeqs == nil {
		lc.sentReqSeqs = make(map[int]time.Time)
	}
	lc.sentReqSeqs[reqSeq] = time.Now()
	lc.reqSeqMu.Unlock()

	return client.SendChatRemoved(int64(reqSeq), string(portal.ID), "0", 0)
}

func (lc *LineClient) buildMentionMetadata(ctx context.Context, body, formattedBody string, mentions *event.Mentions) map[string]string {
	if mentions == nil || (len(mentions.UserIDs) == 0 && !mentions.Room) {
		return nil
	}

	var mentionees []mentionEntry

	if mentions.Room {
		lowerBody := strings.ToLower(body)
		roomMentionTexts := []string{"@room", "@all", "@everyone"}
		pos := -1
		matchLen := 0
		for _, text := range roomMentionTexts {
			if p := strings.Index(lowerBody, text); p >= 0 {
				pos = p
				matchLen = len(text)
				break
			}
		}
		if pos >= 0 {
			mentionees = append(mentionees, mentionEntry{
				S: strconv.Itoa(pos),
				E: strconv.Itoa(pos + matchLen),
				A: "1",
			})
		}
	}

	nameByUserID := map[string]string{}
	if formattedBody != "" {
		for _, m := range mentionLinkRegex.FindAllStringSubmatch(formattedBody, -1) {
			if len(m) >= 3 {
				nameByUserID[m[1]] = m[2]
			}
		}
	}

	for _, userID := range mentions.UserIDs {
		mid, ok := lc.UserLogin.Bridge.Matrix.ParseGhostMXID(userID)
		if !ok {
			continue
		}

		displayName := nameByUserID[string(userID)]

		if displayName == "" {
			existingGhost, err := lc.UserLogin.Bridge.GetExistingGhostByID(ctx, mid)
			if err == nil && existingGhost != nil && existingGhost.Name != "" {
				displayName = existingGhost.Name
			}
		}

		if displayName == "" {
			continue
		}

		searchStr := "@" + displayName
		pos := strings.Index(body, searchStr)
		if pos >= 0 {
			mentionees = append(mentionees, mentionEntry{
				S: strconv.Itoa(pos),
				E: strconv.Itoa(pos + len(searchStr)),
				M: string(mid),
			})
		}
	}

	if len(mentionees) == 0 {
		return nil
	}

	mentionData := map[string]interface{}{
		"MENTIONEES": mentionees,
	}
	mentionJSON, err := json.Marshal(mentionData)
	if err != nil {
		return nil
	}

	return map[string]string{
		"MENTION": string(mentionJSON),
	}
}
