package connector

import (
	"context"
	"fmt"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

var _ bridgev2.GroupCreatingNetworkAPI = (*LineClient)(nil)

func (lc *LineClient) CreateGroup(ctx context.Context, params *bridgev2.GroupCreateParams) (*bridgev2.CreateChatResponse, error) {
	participantMids := make([]string, len(params.Participants))
	for i, p := range params.Participants {
		participantMids[i] = string(p)
	}

	name := ""
	if params.Name != nil {
		name = params.Name.Name
	}

	client := line.NewClient(lc.AccessToken)
	var chat *line.Chat
	var err error
	chatType := 1 // ROOM: members join automatically.
	lineName := name
	chat, err = client.CreateChat(participantMids, lineName, chatType)
	if err != nil && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
		if errRecover := lc.recoverToken(ctx); errRecover == nil {
			client = line.NewClient(lc.AccessToken)
			chat, err = client.CreateChat(participantMids, lineName, chatType)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create LINE chat: %w", err)
	}

	lc.UserLogin.Bridge.Log.Info().
		Str("chat_mid", chat.ChatMid).
		Str("name", chat.ChatName).
		Int("participants", len(participantMids)).
		Msg("LINE group chat created")

	// Cache the member list so auto-registration can fall back to it
	// when GetChats withMembers returns empty data.
	groupMembers := make([]string, 0, len(participantMids)+1)
	groupMembers = append(groupMembers, lc.Mid)
	groupMembers = append(groupMembers, participantMids...)
	lc.cacheMu.Lock()
	if lc.groupMemberCache == nil {
		lc.groupMemberCache = make(map[string][]string)
	}
	if lc.generatedGroupNameCache == nil {
		lc.generatedGroupNameCache = make(map[string]bool)
	}
	lc.groupMemberCache[chat.ChatMid] = groupMembers
	lc.generatedGroupNameCache[chat.ChatMid] = name == ""
	lc.cacheMu.Unlock()

	// Register E2EE group key so members can decrypt group messages.
	// This is best-effort: if E2EE isn't available for a member we skip them,
	// and if the entire registration fails we log a warning without aborting.
	if lc.E2EE != nil && len(participantMids) > 0 {
		if err := lc.registerGroupKey(ctx, chat.ChatMid, participantMids); err != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(err).
				Str("chat_mid", chat.ChatMid).
				Msg("Failed to register E2EE group key, continuing without E2EE")
		}
	}

	portalKey := networkid.PortalKey{
		ID:       makePortalID(chat.ChatMid),
		Receiver: lc.UserLogin.ID,
	}

	portal, err := lc.UserLogin.Bridge.GetPortalByKey(ctx, portalKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get portal for new chat: %w", err)
	}

	members := make([]bridgev2.ChatMember, 0, len(participantMids)+1)
	members = append(members, bridgev2.ChatMember{
		EventSender: bridgev2.EventSender{
			IsFromMe: true,
			Sender:   networkid.UserID(lc.UserLogin.ID),
		},
		Membership: event.MembershipJoin,
	})

	for _, mid := range participantMids {
		if mid == lc.Mid || mid == string(lc.UserLogin.ID) {
			continue
		}
		lowerMid := strings.ToLower(mid)
		if strings.HasPrefix(lowerMid, "c") || strings.HasPrefix(lowerMid, "r") {
			continue
		}
		members = append(members, bridgev2.ChatMember{
			EventSender: bridgev2.EventSender{
				Sender: makeUserID(mid),
			},
			Membership: event.MembershipJoin,
		})
	}

	ct := database.RoomTypeGroupDM
	chatName := name
	if chatName == "" {
		chatName = lc.generateNameFromMemberList(ctx, groupMembers)
	}
	if chatName == "" {
		chatName = chat.ChatName
	}

	return &bridgev2.CreateChatResponse{
		PortalKey: portalKey,
		Portal:    portal,
		PortalInfo: &bridgev2.ChatInfo{
			Type: &ct,
			Name: &chatName,
			Members: &bridgev2.ChatMemberList{
				IsFull:  true,
				Members: members,
			},
		},
	}, nil
}

// registerGroupKey generates a random 32-byte group key, wraps it for each member
// using ECDH + AES-256-CBC, and registers it with the LINE server so all members
// can decrypt group messages. The bridge user's own MID is excluded since the
// creator already has the group key.
func (lc *LineClient) registerGroupKey(ctx context.Context, chatMid string, members []string) error {
	// Exclude the bridge user's own MID — the creator already has the group key.
	otherMembers := make([]string, 0, len(members))
	for _, mid := range members {
		if mid != lc.Mid {
			otherMembers = append(otherMembers, mid)
		}
	}
	if len(otherMembers) == 0 {
		return fmt.Errorf("no other members to register group key for")
	}
	members = otherMembers

	client := line.NewClient(lc.AccessToken)

	// Fetch current E2EE public keys for all other members as a batch. If the batch
	// call fails (e.g. server 500 for a specific member), fall back to fetching
	// each member's key individually via NegotiateE2EEPublicKey.
	pubKeysReq := line.GetLastE2EEPublicKeysRequest{
		ChatMid: chatMid,
		Members: members,
	}
	pubKeys, err := client.GetLastE2EEPublicKeys(pubKeysReq)
	if err != nil {
		if lc.isRefreshRequired(err) || lc.isLoggedOut(err) {
			if errRecover := lc.recoverToken(ctx); errRecover == nil {
				client = line.NewClient(lc.AccessToken)
				pubKeys, err = client.GetLastE2EEPublicKeys(pubKeysReq)
			}
		}
	}
	if err != nil {
		// Batch call failed — try individual key negotiation per member
		lc.UserLogin.Bridge.Log.Warn().Err(err).
			Str("chat_mid", chatMid).
			Int("members", len(members)).
			Msg("Batch GetLastE2EEPublicKeys failed, falling back to per-member NegotiateE2EEPublicKey")
		pubKeys = make(map[string]line.E2EEPeerPublicKey)
		for _, mid := range members {
			res, nErr := client.NegotiateE2EEPublicKey(mid)
			if nErr != nil {
				if line.IsNoUsableE2EEPublicKey(nErr) {
					lc.UserLogin.Bridge.Log.Debug().Str("member", mid).Msg("Member has Letter Sealing disabled, skipping")
					continue
				}
				if lc.isRefreshRequired(nErr) || lc.isLoggedOut(nErr) {
					if errRecover := lc.recoverToken(ctx); errRecover == nil {
						client = line.NewClient(lc.AccessToken)
						res, nErr = client.NegotiateE2EEPublicKey(mid)
					}
				}
				if nErr != nil {
					lc.UserLogin.Bridge.Log.Warn().Err(nErr).Str("member", mid).Msg("Failed to negotiate key for member, skipping")
					continue
				}
			}
			keyID, nErr := res.KeyID.Int64()
			if nErr != nil {
				lc.UserLogin.Bridge.Log.Warn().Err(nErr).Str("member", mid).Msg("Failed to parse key ID, skipping")
				continue
			}
			pubKeys[mid] = line.E2EEPeerPublicKey{KeyID: int(keyID), KeyData: res.PublicKey}
		}
	}

	// Generate group key in WASM (same approach as LINE Chrome Extension).
	// The generated key is a Curve25519Key object stored in the WASM module.
	groupKeyID, err := lc.E2EE.GenerateGroupKey()
	if err != nil {
		return fmt.Errorf("failed to generate group key: %w", err)
	}

	// Wrap the group key for each member that has a public key
	apiMembers := make([]string, 0, len(members))
	keyIds := make([]int, 0, len(members))
	encryptedKeys := make([]string, 0, len(members))

	for _, mid := range members {
		pk, ok := pubKeys[mid]
		if !ok {
			lc.UserLogin.Bridge.Log.Debug().Str("member", mid).Msg("No E2EE public key for member, skipping")
			continue
		}
		if pk.KeyData == "" {
			lc.UserLogin.Bridge.Log.Debug().Str("member", mid).Int("key_id", pk.KeyID).Msg("Empty public key data for member, skipping")
			continue
		}

		encryptedKey, err := lc.E2EE.WrapGroupKeyForMember(pk.KeyData, groupKeyID)
		if err != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(err).Str("member", mid).Msg("Failed to wrap group key for member, skipping")
			continue
		}

		apiMembers = append(apiMembers, mid)
		keyIds = append(keyIds, pk.KeyID)
		encryptedKeys = append(encryptedKeys, encryptedKey)
	}

	if len(apiMembers) == 0 {
		return fmt.Errorf("no members with valid E2EE keys")
	}

	// LINE's registerE2EEGroupKey requires the caller's own key entry as well — without it the
	// server rejects the request with "empty caller key". The Chrome extension wraps the group
	// key for every member returned by getLastE2EEPublicKeys, which includes the caller. Mirror
	// that by wrapping the group key for our own public key and appending ourselves.
	selfRawID, selfPub, err := lc.E2EE.MyPublicKey()
	if err != nil {
		return fmt.Errorf("get own E2EE key: %w", err)
	}
	selfEncryptedKey, err := lc.E2EE.WrapGroupKeyForMember(selfPub, groupKeyID)
	if err != nil {
		return fmt.Errorf("wrap group key for self: %w", err)
	}
	apiMembers = append(apiMembers, lc.Mid)
	keyIds = append(keyIds, selfRawID)
	encryptedKeys = append(encryptedKeys, selfEncryptedKey)

	if err := client.RegisterE2EEGroupKey(1, chatMid, apiMembers, keyIds, encryptedKeys); err != nil {
		if lc.isRefreshRequired(err) || lc.isLoggedOut(err) {
			if errRecover := lc.recoverToken(ctx); errRecover == nil {
				client = line.NewClient(lc.AccessToken)
				err = client.RegisterE2EEGroupKey(1, chatMid, apiMembers, keyIds, encryptedKeys)
			}
		}
		if err != nil {
			return fmt.Errorf("registerE2EEGroupKey failed: %w", err)
		}
	}

	lc.UserLogin.Bridge.Log.Info().
		Str("chat_mid", chatMid).
		Int("members", len(apiMembers)).
		Msg("Registered E2EE group key")

	return nil
}
