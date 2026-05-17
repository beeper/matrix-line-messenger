package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/highesttt/matrix-line-messenger/pkg/connector/handlers"
	"github.com/highesttt/matrix-line-messenger/pkg/e2ee"
	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

func (lc *LineClient) newMessageHandler() *handlers.Handler {
	return &handlers.Handler{
		Log:               lc.UserLogin.Bridge.Log,
		HTTPClient:        lc.HTTPClient,
		RecoverToken:      lc.recoverToken,
		IsRefreshRequired: lc.isRefreshRequired,
		IsLoggedOut:       lc.isLoggedOut,
		NewClient:         func() *line.Client { return line.NewClient(lc.AccessToken) },
		DecryptMedia:      lc.decryptImageData,
	}
}

func (lc *LineClient) queueIncomingMessage(msg *line.Message, opType int) {
	// Only process known content types; skip system messages (group created, member invited, etc.)
	switch ContentType(msg.ContentType) {
	case ContentText, ContentImage, ContentVideo, ContentAudio,
		ContentSticker, ContentContact, ContentFile, ContentLocation:
		// supported — continue
	default:
		// Let call and contact notifications through regardless of content type,
		// as LINE may wrap them in non-standard content type values.
		if msg.ContentMetadata["ORGCONTP"] == "CALL" || msg.ContentMetadata["ORGCONTP"] == "CONTACT" {
			// pass through — the ConvertMessageFunc will handle routing
		} else {
			lc.UserLogin.Bridge.Log.Debug().
				Int("content_type", msg.ContentType).
				Str("msg_id", msg.ID).
				Interface("content_metadata", msg.ContentMetadata).
				Str("text", msg.Text).
				Int("chunk_count", len(msg.Chunks)).
				Msg("Skipping unsupported content type")
			return
		}
	}

	senderID := makeUserID(msg.From)

	portalIDStr := msg.From
	// If I sent it (Type 25), the portal is the recipient (msg.To)
	if OperationType(opType) == OpSendMessage {
		portalIDStr = msg.To
	}
	// If it's a group (ToType 1 or 2), the portal is msg.To
	if ToType(msg.ToType) == ToRoom || ToType(msg.ToType) == ToGroup {
		portalIDStr = msg.To
	}

	portalKey := networkid.PortalKey{ID: makePortalID(portalIDStr), Receiver: lc.UserLogin.ID}

	// Handle Content
	bodyText := msg.Text
	if bodyText == "" && len(msg.Chunks) > 0 {
		bodyText = "[Unable to decrypt message. Open an issue on GitHub.]"
		if lc.E2EE != nil {
			// Ensure peer keys are available before attempting decryption
			lc.ensurePeerKeyForMessage(context.Background(), msg)

			// If we receive an encrypted group message, clear its noE2EE cache
			// so future sends will attempt E2EE again.
			if (ToType(msg.ToType) == ToRoom || ToType(msg.ToType) == ToGroup) && lc.isGroupNoE2EE(portalIDStr) {
				lc.UserLogin.Bridge.Log.Info().Str("chat_mid", portalIDStr).Msg("Received encrypted group message, clearing noE2EE cache")
				lc.clearGroupNoE2EE(portalIDStr)
			}

			if ToType(msg.ToType) == ToRoom || ToType(msg.ToType) == ToGroup {
				// Group Decryption
				if len(msg.Chunks) >= 5 {
					if gkID, err := e2ee.DecodeKeyID(msg.Chunks[len(msg.Chunks)-1]); err == nil && gkID != 0 {
						if errFetch := lc.fetchAndUnwrapGroupKey(context.Background(), portalIDStr, gkID); errFetch != nil {
							lc.UserLogin.Bridge.Log.Debug().Err(errFetch).Int("key_id", gkID).Str("chat_mid", portalIDStr).Msg("Prefetch group key before decrypt failed")
						}
					}
				}

				pt, keyID, err := lc.E2EE.DecryptGroupMessage(msg, portalIDStr)
				if err == nil {
					bodyText = pt
				} else {
					lc.UserLogin.Bridge.Log.Debug().Err(err).Int("key_id", keyID).Str("chat_mid", portalIDStr).Msg("DecryptGroupMessage failed, trying to fetch key")
					if keyID != 0 {
						if errFetch := lc.fetchAndUnwrapGroupKey(context.Background(), portalIDStr, keyID); errFetch != nil {
							lc.UserLogin.Bridge.Log.Warn().Err(errFetch).Int("key_id", keyID).Str("chat_mid", portalIDStr).Msg("Failed to fetch/unwrap group key")
						} else if ptRetry, _, errRetry := lc.E2EE.DecryptGroupMessage(msg, portalIDStr); errRetry == nil {
							bodyText = ptRetry
						}
					}
				}
			} else {
				// 1-1 Decryption
				if pt, err := lc.E2EE.DecryptMessageV2(msg); err == nil {
					bodyText = pt
				} else {
					lc.UserLogin.Bridge.Log.Debug().Err(err).Msg("DecryptMessageV2 failed on first attempt")
					if _, _, errKey := lc.E2EE.MyKeyIDs(); errKey != nil {
						lc.UserLogin.Bridge.Log.Error().Msg("E2EE own key not loaded — cannot decrypt any messages. Re-login required.")
					} else {
						peerMid := msg.From
						if peerMid == lc.Mid || peerMid == string(lc.UserLogin.ID) {
							peerMid = msg.To
						}
						// Fetch the EXACT keyID the message used (handles peer key rotation)
						// before falling back to negotiating the peer's current key.
						fetched := false
						if len(msg.Chunks) >= 5 {
							if receiverKeyID, errKID := e2ee.DecodeKeyID(msg.Chunks[len(msg.Chunks)-1]); errKID == nil && receiverKeyID != 0 {
								if _, _, errPeer := lc.ensurePeerKeyByID(context.Background(), peerMid, receiverKeyID); errPeer == nil {
									fetched = true
								} else {
									lc.UserLogin.Bridge.Log.Debug().Err(errPeer).Str("peer", peerMid).Int("key_id", receiverKeyID).Msg("ensurePeerKeyByID failed on retry, falling back to NegotiateE2EEPublicKey")
								}
							}
						}
						if !fetched {
							if _, _, errPeer := lc.ensurePeerKey(context.Background(), peerMid); errPeer != nil {
								lc.UserLogin.Bridge.Log.Warn().Err(errPeer).Str("peer", peerMid).Msg("Failed to force-fetch peer key for retry")
							}
						}
						if ptRetry, errRetry := lc.E2EE.DecryptMessageV2(msg); errRetry == nil {
							bodyText = ptRetry
						} else {
							lc.UserLogin.Bridge.Log.Warn().Err(errRetry).Msg("DecryptMessageV2 failed on retry")
						}
					}
				}
			}
		}
	}

	// unwrap JSON payload
	unwrappedText := bodyText
	if strings.HasPrefix(bodyText, "{") {
		var wrapper map[string]any
		if err := json.Unmarshal([]byte(bodyText), &wrapper); err == nil {
			if t, ok := wrapper["text"].(string); ok {
				unwrappedText = t
			}
		}
	}
	decryptedBody := bodyText

	var ts time.Time
	if tsInt, err := msg.CreatedTime.Int64(); err != nil {
		lc.UserLogin.Bridge.Log.Warn().
			Err(err).
			Str("msg_id", msg.ID).
			Msg("Failed to convert message CreatedTime to int64, using current time")
		ts = time.Now()
	} else {
		ts = time.UnixMilli(tsInt)
		if ts.IsZero() {
			ts = time.Now()
		}
	}

	lc.UserLogin.Bridge.QueueRemoteEvent(lc.UserLogin, &simplevent.Message[line.Message]{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventMessage,
			LogContext:   func(c zerolog.Context) zerolog.Context { return c.Str("msg_id", msg.ID) },
			PortalKey:    portalKey,
			CreatePortal: true,
			Sender:       bridgev2.EventSender{Sender: senderID, IsFromMe: OperationType(opType) == OpSendMessage},
			Timestamp:    ts,
		},
		Data: *msg,
		ID:   networkid.MessageID(msg.ID),
		ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data line.Message) (*bridgev2.ConvertedMessage, error) {
			h := lc.newMessageHandler()
			replyRelatesTo := lc.resolveReplyRelatesTo(ctx, &data)

			// Handle call events (ORGCONTP == "CALL")
			if data.ContentMetadata["ORGCONTP"] == "CALL" {
				return h.ConvertCall(data, replyRelatesTo)
			}

			// Dispatch to content-type-specific handlers
			switch ContentType(data.ContentType) {
			case ContentImage:
				return h.ConvertImage(ctx, portal, intent, data, decryptedBody, replyRelatesTo)
			case ContentVideo:
				return h.ConvertVideo(ctx, portal, intent, data, decryptedBody, replyRelatesTo)
			case ContentAudio:
				return h.ConvertAudio(ctx, portal, intent, data, decryptedBody, replyRelatesTo)
			case ContentFile:
				return h.ConvertFile(ctx, portal, intent, data, decryptedBody, replyRelatesTo)
			case ContentSticker:
				return h.ConvertSticker(ctx, portal, intent, data, replyRelatesTo)
			case ContentLocation:
				return h.ConvertLocation(data, replyRelatesTo)
			case ContentContact:
				return h.ConvertContact(data, replyRelatesTo)
			}

			// Handle device/phone contact shared via ORGCONTP (contentType 0 with vCard)
			if data.ContentMetadata["ORGCONTP"] == "CONTACT" {
				return h.ConvertDeviceContact(ctx, portal, intent, data, unwrappedText, replyRelatesTo)
			}

			// Handle inline emoji/stamp embedded in text messages
			if data.ContentMetadata["STKID"] != "" || data.ContentMetadata["STKPKGID"] != "" ||
				data.ContentMetadata["STICON_OWNERSHIP"] != "" {
				if data.ContentMetadata["STICON_OWNERSHIP"] != "" {
					h.Log.Debug().
						Str("body_text", bodyText).
						Str("unwrapped_text", unwrappedText).
						Interface("content_metadata", data.ContentMetadata).
						Msg("STICON_OWNERSHIP: full message body")
				}
				return h.ConvertInlineEmoji(ctx, portal, intent, data, unwrappedText, bodyText, replyRelatesTo)
			}

			// Skip empty/whitespace-only text messages (system messages that fell through)
			if strings.TrimSpace(unwrappedText) == "" {
				return nil, nil
			}

			// Default to text
			converted, err := h.ConvertText(unwrappedText, replyRelatesTo)
			if err != nil {
				return nil, err
			}

			if mentionStr := data.ContentMetadata["MENTION"]; mentionStr != "" && len(converted.Parts) > 0 {
				lc.UserLogin.Bridge.Log.Debug().Str("raw_mention", mentionStr).Msg("Processing inbound LINE MENTION metadata")
				var mentionData struct {
					MENTIONEES []struct {
						M string `json:"M,omitempty"`
						A string `json:"A,omitempty"`
						S string `json:"S,omitempty"`
						E string `json:"E,omitempty"`
					} `json:"MENTIONEES"`
				}
				if err := json.Unmarshal([]byte(mentionStr), &mentionData); err != nil {
					lc.UserLogin.Bridge.Log.Debug().Err(err).Msg("Failed to unmarshal MENTION metadata")
				} else {
					ghostFormatter, ok := lc.UserLogin.Bridge.Matrix.(interface {
						FormatGhostMXID(networkid.UserID) id.UserID
					})
					lc.UserLogin.Bridge.Log.Debug().Bool("formatter_ok", ok).Msg("Checking FormatGhostMXID availability")
					mentions := &event.Mentions{}
					type mentionEntry struct {
						start int
						end   int
						mxid  string
					}
					var entries []mentionEntry
					for _, ment := range mentionData.MENTIONEES {
						lc.UserLogin.Bridge.Log.Debug().
							Str("mid", ment.M).
							Str("a", ment.A).
							Str("s", ment.S).
							Str("e", ment.E).
							Bool("has_formatter", ok).
							Msg("Processing mention entry")
						if ment.M != "" {
							var mxid id.UserID
							switch {
							case ment.M == lc.Mid || ment.M == string(lc.UserLogin.ID):
								mxid = lc.UserLogin.UserMXID
								lc.UserLogin.Bridge.Log.Debug().Str("mxid", string(mxid)).Msg("Mention targets bridge user, using real MXID")
							case ok:
								mxid = ghostFormatter.FormatGhostMXID(networkid.UserID(ment.M))
							default:
								lc.UserLogin.Bridge.Log.Debug().Msg("Skip mention: unknown MID and no formatter available")
								continue
							}
							lc.UserLogin.Bridge.Log.Debug().Str("mxid", string(mxid)).Msg("Formatted MXID from LINE MID")
							mentions.UserIDs = append(mentions.UserIDs, mxid)
							if s, errS := strconv.Atoi(ment.S); errS == nil && s >= 0 {
								if e, errE := strconv.Atoi(ment.E); errE == nil && e <= len(unwrappedText) && e > s {
									entries = append(entries, mentionEntry{start: s, end: e, mxid: string(mxid)})
								}
							}
						}
						if ment.A == "1" {
							mentions.Room = true
							if s, errS := strconv.Atoi(ment.S); errS == nil && s >= 0 {
								if e, errE := strconv.Atoi(ment.E); errE == nil && e <= len(unwrappedText) && e > s {
									entries = append(entries, mentionEntry{start: s, end: e, mxid: "@room"})
								}
							}
						}
					}
					if len(mentions.UserIDs) > 0 || mentions.Room {
						logEvt := lc.UserLogin.Bridge.Log.Debug().
							Int("user_count", len(mentions.UserIDs)).
							Bool("is_room", mentions.Room)
						if len(entries) > 0 {
							logEvt = logEvt.Int("formatted_body_entries", len(entries))
						}
						logEvt.Msg("Setting mentions on converted message")
						var formattedBody string
						if len(entries) > 0 {
							sort.Slice(entries, func(i, j int) bool { return entries[i].start < entries[j].start })
							var fb strings.Builder
							lastEnd := 0
							for _, entry := range entries {
								if entry.start >= lastEnd && entry.start >= 0 && entry.end <= len(unwrappedText) && entry.start < entry.end {
									fb.WriteString(html.EscapeString(unwrappedText[lastEnd:entry.start]))
									fb.WriteString(fmt.Sprintf(`<a href="https://matrix.to/#/%s">%s</a>`, html.EscapeString(entry.mxid), html.EscapeString(unwrappedText[entry.start:entry.end])))
									lastEnd = entry.end
								}
							}
							fb.WriteString(html.EscapeString(unwrappedText[lastEnd:]))
							formattedBody = fb.String()
						}
						// Replace room mention text in body with @room for client-side highlighting.
						// Process end-to-start to preserve positions for earlier entries.
						if mentions.Room && len(entries) > 0 {
							body := converted.Parts[0].Content.Body
							for i := len(entries) - 1; i >= 0; i-- {
								if entries[i].mxid == "@room" && entries[i].start >= 0 && entries[i].end <= len(body) {
									body = body[:entries[i].start] + "@room" + body[entries[i].end:]
								}
							}
							for _, part := range converted.Parts {
								part.Content.Body = body
							}
						}
						for _, part := range converted.Parts {
							part.Content.Mentions = mentions
							if formattedBody != "" {
								part.Content.Format = event.FormatHTML
								part.Content.FormattedBody = formattedBody
							}
						}
					}
				}
			}

			return converted, nil
		},
	})
}

// resolveReplyRelatesTo looks up the Matrix event ID for a replied-to LINE message.
func (lc *LineClient) resolveReplyRelatesTo(ctx context.Context, data *line.Message) *event.RelatesTo {
	if data == nil {
		return nil
	}

	relatedID := data.RelatedMessageID
	if relatedID == "" && data.ContentMetadata != nil {
		relatedID = data.ContentMetadata["message_relation_server_message_id"]
	}

	if relatedID == "" {
		return nil
	}

	if data.MessageRelationType != 0 && data.MessageRelationType != 3 {
		return nil
	}

	dbMsg, err := lc.UserLogin.Bridge.DB.Message.GetPartByID(ctx, lc.UserLogin.ID, networkid.MessageID(relatedID), "")
	if err != nil {
		lc.UserLogin.Bridge.Log.Debug().Err(err).Str("related_msg_id", relatedID).Msg("Failed to lookup reply target")
		return nil
	}
	if dbMsg == nil || dbMsg.MXID == "" {
		lc.UserLogin.Bridge.Log.Debug().Str("related_msg_id", relatedID).Msg("No Matrix event found for reply target")
		return nil
	}

	return &event.RelatesTo{InReplyTo: &event.InReplyTo{EventID: dbMsg.MXID}}
}
