package handlers

import (
	"fmt"
	"strconv"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// ConvertCall converts a LINE call event to a Matrix notice message.
func (h *Handler) ConvertCall(data line.Message, relatesTo *event.RelatesTo) (*bridgev2.ConvertedMessage, error) {
	callType := "Voice"
	beeperCallType := event.BeeperActionMessageCallTypeVoice
	if data.ContentMetadata["TYPE"] == "V" {
		callType = "Video"
		beeperCallType = event.BeeperActionMessageCallTypeVideo
	}

	durationMs, _ := strconv.Atoi(data.ContentMetadata["DURATION"])
	duration := durationMs / 1000
	result := data.ContentMetadata["RESULT"]

	var body string
	switch {
	case duration > 0:
		mins := duration / 60
		secs := duration % 60
		if mins > 0 {
			body = fmt.Sprintf("%s call (%dm%02ds)", callType, mins, secs)
		} else {
			body = fmt.Sprintf("%s call (%ds)", callType, secs)
		}
	case result == "CANCELED":
		body = fmt.Sprintf("Missed %s call", strings.ToLower(callType))
	default:
		body = fmt.Sprintf("Missed %s call", strings.ToLower(callType))
	}

	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{
			{
				Type: event.EventMessage,
				Content: &event.MessageEventContent{
					MsgType:   event.MsgNotice,
					Body:      body,
					RelatesTo: relatesTo,
					BeeperActionMessage: &event.BeeperActionMessage{
						Type:     event.BeeperActionMessageCall,
						CallType: beeperCallType,
					},
				},
			},
		},
	}, nil
}
