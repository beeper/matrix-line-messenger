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
	PaidReactionType *PaidReactionType `json:"paidReactionType,omitempty"`
}

type PaidReactionType struct {
	ProductID    string `json:"productId"`
	EmojiID      string `json:"emojiId"`
	ResourceType int    `json:"resourceType"`
	Version      int    `json:"version"`
}

func ParseReactionParam2(data string) (*ReactionPayload, error) {
	var p ReactionPayload
	if err := json.Unmarshal([]byte(data), &p); err != nil {
		return nil, err
	}
	return &p, nil
}
