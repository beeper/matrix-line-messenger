package line

import "encoding/json"

// SticonOwnership represents a sticon ownership entry returned by
// getSticonOwnershipByMid, mapping an ownership token to sticon details.
type SticonOwnership struct {
	ProductID      string `json:"productId"`
	EmojiID        string `json:"emojiId"`
	OwnershipToken string `json:"ownershipToken"`
	ResourceType   int    `json:"resourceType,omitempty"`
	Version        int    `json:"version,omitempty"`
}

type ReactionPayload struct {
	ChatMid string          `json:"chatMid"`
	Curr    *ReactionDetail `json:"curr,omitempty"`
}

type ReactionDetail struct {
	PaidReactionType       *PaidReactionType `json:"paidReactionType,omitempty"`
	PredefinedReactionType *int              `json:"predefinedReactionType,omitempty"`
}

type PaidReactionType struct {
	ProductID    string `json:"productId"`
	EmojiID      string `json:"emojiId"`
	ResourceType int    `json:"resourceType"`
	Version      int    `json:"version"`
}

// PredefinedReactionEmoji maps LINE predefinedReactionType values to Unicode emoji.
var PredefinedReactionEmoji = map[int]string{
	2: "\U0001F44D",   // 👍 LIKE
	3: "\u2764\uFE0F", // ❤️ LOVE
	4: "\U0001F606",   // 😆 LAUGH
	5: "\U0001F62E",   // 😮 SURPRISE
	6: "\U0001F622",   // 😢 SAD
	7: "\U0001F621",   // 😡 ANGRY
}

// PredefinedReactionName maps predefinedReactionType values to human-readable names.
var PredefinedReactionName = map[int]string{
	2: "LIKE",
	3: "LOVE",
	4: "LAUGH",
	5: "SURPRISE",
	6: "SAD",
	7: "ANGRY",
}

func ParseReactionParam2(data string) (*ReactionPayload, error) {
	var p ReactionPayload
	if err := json.Unmarshal([]byte(data), &p); err != nil {
		return nil, err
	}
	return &p, nil
}
