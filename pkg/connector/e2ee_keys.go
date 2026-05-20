package connector

import (
	"context"
	"fmt"
	"time"

	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/highesttt/matrix-line-messenger/pkg/e2ee"
	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

const noE2EETTL = 1 * time.Hour

// fetchAndUnwrapGroupKey retrieves a specific group key (or the latest when groupKeyID == 0)
// and unwraps it so the E2EE manager can encrypt/decrypt group messages.
// If no group key exists yet (TalkException code 5), it auto-registers one and retries.
func (lc *LineClient) fetchAndUnwrapGroupKey(ctx context.Context, chatMid string, groupKeyID int) error {
	if lc.E2EE == nil {
		return fmt.Errorf("E2EE manager not initialized")
	}

	client := line.NewClient(lc.AccessToken)
	fetch := func() (*line.E2EEGroupSharedKey, error) {
		if groupKeyID > 0 {
			return client.GetE2EEGroupSharedKey(chatMid, groupKeyID)
		}
		return client.GetLastE2EEGroupSharedKey(chatMid)
	}

	sharedKey, err := fetch()
	// No group key exists yet — auto-register one so the group can use E2EE.
	if err != nil && line.IsGroupKeyNotFound(err) {
		lc.UserLogin.Bridge.Log.Info().Str("chat_mid", chatMid).
			Msg("No group key found, auto-registering")
		if registerErr := lc.autoRegisterGroupKey(ctx, chatMid); registerErr != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(registerErr).Str("chat_mid", chatMid).
				Msg("Auto-register group key failed, returning original error")
			return err
		}
		sharedKey, err = fetch()
	}
	// Token recovery for other error types
	if err != nil && !line.IsNoUsableE2EEGroupKey(err) && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
		if errRecover := lc.recoverToken(ctx); errRecover == nil {
			client = line.NewClient(lc.AccessToken)
			sharedKey, err = fetch()
		} else {
			return fmt.Errorf("failed to recover token before fetching group key: %w", errRecover)
		}
	}
	if err != nil {
		return err
	}
	if sharedKey == nil {
		return fmt.Errorf("no group shared key returned for %s", chatMid)
	}

	lc.UserLogin.Bridge.Log.Debug().
		Str("chat_mid", chatMid).
		Int("group_key_id", sharedKey.GroupKeyID).
		Int("creator_key_id", sharedKey.CreatorKeyID).
		Int("receiver_key_id", sharedKey.ReceiverKeyID).
		Msg("Fetched group shared key")

	if _, _, err := lc.ensurePeerKey(ctx, sharedKey.Creator); err != nil {
		return fmt.Errorf("failed to ensure creator key: %w", err)
	}
	if _, _, err := lc.ensurePeerKeyByID(ctx, sharedKey.Creator, sharedKey.CreatorKeyID); err != nil {
		return fmt.Errorf("failed to ensure creator key id %d: %w", sharedKey.CreatorKeyID, err)
	}

	unwrappedID, err := lc.E2EE.UnwrapGroupSharedKey(chatMid, sharedKey)
	if err != nil {
		return fmt.Errorf("failed to unwrap group key: %w", err)
	}

	lc.UserLogin.Bridge.Log.Debug().
		Str("chat_mid", chatMid).
		Int("group_key_id", sharedKey.GroupKeyID).
		Int("receiver_key_id", sharedKey.ReceiverKeyID).
		Int("unwrapped_id", unwrappedID).
		Msg("Unwrapped group shared key")

	return nil
}

func (lc *LineClient) ensurePeerKey(_ context.Context, mid string) (int, string, error) {
	lc.cacheMu.Lock()
	if lc.peerKeys == nil {
		lc.peerKeys = make(map[string]peerKeyInfo)
	}
	cached, ok := lc.peerKeys[mid]
	lc.cacheMu.Unlock()
	if ok {
		// Cached as Letter Sealing off — return error unless TTL expired
		if cached.noE2EE {
			if time.Since(cached.checkedAt) < noE2EETTL {
				return 0, "", line.ErrNoUsableE2EEPublicKey
			}
			// TTL expired, re-negotiate below
		} else if cached.raw != 0 && cached.pub != "" {
			if lc.E2EE != nil {
				lc.E2EE.RegisterPeerPublicKey(cached.raw, cached.pub)
			}
			return cached.raw, cached.pub, nil
		}
	}
	client := line.NewClient(lc.AccessToken)
	res, err := client.NegotiateE2EEPublicKey(mid)
	if err != nil {
		// Cache negative result so we don't keep hitting the API
		if line.IsNoUsableE2EEPublicKey(err) {
			lc.cacheMu.Lock()
			lc.peerKeys[mid] = peerKeyInfo{noE2EE: true, checkedAt: time.Now()}
			lc.cacheMu.Unlock()
			lc.UserLogin.Bridge.Log.Info().Str("peer", mid).Msg("Peer has Letter Sealing disabled, will send plain text")
		}
		return 0, "", err
	}
	keyID, err := res.KeyID.Int64()
	if err != nil {
		return 0, "", err
	}
	pk := peerKeyInfo{raw: int(keyID), pub: res.PublicKey}
	lc.cacheMu.Lock()
	lc.peerKeys[mid] = pk
	lc.cacheMu.Unlock()
	if lc.E2EE != nil {
		lc.E2EE.RegisterPeerPublicKey(pk.raw, pk.pub)
	}
	return pk.raw, pk.pub, nil
}

// isGroupNoE2EE checks if a group is cached as having no E2EE shared key.
func (lc *LineClient) isGroupNoE2EE(chatMid string) bool {
	lc.cacheMu.Lock()
	defer lc.cacheMu.Unlock()
	if lc.noE2EEGroups == nil {
		return false
	}
	checkedAt, ok := lc.noE2EEGroups[chatMid]
	return ok && time.Since(checkedAt) < noE2EETTL
}

// markGroupNoE2EE caches a group as having no E2EE shared key.
func (lc *LineClient) markGroupNoE2EE(chatMid string) {
	lc.cacheMu.Lock()
	defer lc.cacheMu.Unlock()
	if lc.noE2EEGroups == nil {
		lc.noE2EEGroups = make(map[string]time.Time)
	}
	lc.noE2EEGroups[chatMid] = time.Now()
}

// clearGroupNoE2EE removes a group from the noE2EE cache (e.g., when we receive encrypted messages).
func (lc *LineClient) clearGroupNoE2EE(chatMid string) {
	lc.cacheMu.Lock()
	defer lc.cacheMu.Unlock()
	delete(lc.noE2EEGroups, chatMid)
}

// getChatMemberMIDs fetches the member and invitee MIDs for a group chat via GetChats.
// Invitees are included because group key registration must happen before they accept,
// otherwise the key won't be available when they start sending messages.
func (lc *LineClient) getChatMemberMIDs(ctx context.Context, chatMid string) ([]string, error) {
	client := line.NewClient(lc.AccessToken)
	chats, err := client.GetChats([]string{chatMid}, true, true)
	if err != nil {
		if lc.isRefreshRequired(err) || lc.isLoggedOut(err) {
			if errRecover := lc.recoverToken(ctx); errRecover == nil {
				client = line.NewClient(lc.AccessToken)
				chats, err = client.GetChats([]string{chatMid}, true, true)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("getChats failed for %s: %w", chatMid, err)
		}
	}
	if len(chats.Chats) == 0 {
		return nil, fmt.Errorf("chat %s not found", chatMid)
	}
	chat := chats.Chats[0]
	if chat.Extra.GroupExtra == nil {
		return nil, fmt.Errorf("chat %s has no group extra", chatMid)
	}
	seen := make(map[string]struct{})
	for mid := range chat.Extra.GroupExtra.MemberMids {
		seen[mid] = struct{}{}
	}
	for mid := range chat.Extra.GroupExtra.InviteeMids {
		seen[mid] = struct{}{}
	}
	// Always include the bridge user's own MID since we're definitely a member
	if _, ok := seen[lc.Mid]; !ok {
		seen[lc.Mid] = struct{}{}
	}
	mids := make([]string, 0, len(seen))
	for mid := range seen {
		mids = append(mids, mid)
	}
	if len(mids) == 0 {
		return nil, fmt.Errorf("chat %s has no members or invitees", chatMid)
	}

	// Cache the successful result for fallback use.
	lc.cacheMu.Lock()
	if lc.groupMemberCache == nil {
		lc.groupMemberCache = make(map[string][]string)
	}
	lc.groupMemberCache[chatMid] = mids
	lc.cacheMu.Unlock()

	return mids, nil
}

// autoRegisterGroupKey fetches group members, then registers a new E2EE group key
// for the chat. This is called when fetchAndUnwrapGroupKey finds no key exists.
func (lc *LineClient) autoRegisterGroupKey(ctx context.Context, chatMid string) error {
	members, err := lc.getChatMemberMIDs(ctx, chatMid)
	if err != nil {
		return fmt.Errorf("getChatMemberMIDs: %w", err)
	}

	// If getChatMemberMIDs returned only ourself, the server likely returned
	// an empty MemberMids map (known LINE API issue). Fall back to cached
	// member list from CreateGroup or a prior successful fetch.
	if len(members) == 1 && members[0] == lc.Mid {
		lc.cacheMu.Lock()
		cached, ok := lc.groupMemberCache[chatMid]
		lc.cacheMu.Unlock()
		if ok && len(cached) > 1 {
			lc.UserLogin.Bridge.Log.Warn().Str("chat_mid", chatMid).
				Msg("GetChats returned only self MID, falling back to cached member list")
			members = cached
		}
	}

	// Last resort: query Matrix room members via the bridge API.
	if len(members) == 1 && members[0] == lc.Mid {
		matrixMembers, err := lc.getGroupMemberMIDsViaMatrix(ctx, chatMid)
		if err != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(err).Str("chat_mid", chatMid).
				Msg("Matrix member fallback also failed")
		} else if len(matrixMembers) > 1 {
			lc.UserLogin.Bridge.Log.Warn().Str("chat_mid", chatMid).
				Int("members", len(matrixMembers)).
				Msg("GetChats returned only self MID, falling back to Matrix room members")
			members = matrixMembers
		}
	}

	return lc.registerGroupKey(ctx, chatMid, members)
}

// getGroupMemberMIDsViaMatrix queries the Matrix room's member list via the bridge
// API and converts ghost user IDs back to LINE MIDs. This is a fallback when the
// LINE API's GetChats withMembers returns an empty MemberMids map.
func (lc *LineClient) getGroupMemberMIDsViaMatrix(ctx context.Context, chatMid string) (_ []string, err error) {
	portalKey := networkid.PortalKey{
		ID:       makePortalID(chatMid),
		Receiver: lc.UserLogin.ID,
	}
	portal, err := lc.UserLogin.Bridge.GetPortalByKey(ctx, portalKey)
	if err != nil {
		return nil, fmt.Errorf("get portal: %w", err)
	}
	if portal == nil || portal.MXID == "" {
		return nil, fmt.Errorf("portal has no Matrix room")
	}

	matrixMembers, err := lc.UserLogin.Bridge.Matrix.GetMembers(ctx, portal.MXID)
	if err != nil {
		return nil, fmt.Errorf("get matrix members: %w", err)
	}

	mids := make([]string, 0, len(matrixMembers))
	for mxid := range matrixMembers {
		// Skip the bridge user's own Matrix account if present.
		if mxid == lc.UserLogin.UserMXID {
			continue
		}
		// Parse the ghost MXID back to a network ID (LINE MID).
		if netID, ok := lc.UserLogin.Bridge.Matrix.ParseGhostMXID(mxid); ok {
			mid := string(netID)
			if mid != lc.Mid && mid != string(lc.UserLogin.ID) {
				mids = append(mids, mid)
			}
		}
	}

	if len(mids) == 0 {
		return nil, fmt.Errorf("no LINE members found via Matrix")
	}

	// Always include the bridge user's own MID.
	mids = append(mids, lc.Mid)

	lc.UserLogin.Bridge.Log.Debug().Str("chat_mid", chatMid).
		Int("matrix_members", len(matrixMembers)).
		Int("resolved_mids", len(mids)).
		Msg("Resolved group members via Matrix room")

	return mids, nil
}

func (lc *LineClient) ensurePeerKeyByID(_ context.Context, mid string, keyID int) (int, string, error) {
	lc.cacheMu.Lock()
	if lc.peerKeys == nil {
		lc.peerKeys = make(map[string]peerKeyInfo)
	}
	cached, ok := lc.peerKeys[mid]
	lc.cacheMu.Unlock()
	if ok && cached.raw == keyID && cached.pub != "" {
		if lc.E2EE != nil {
			lc.E2EE.RegisterPeerPublicKey(cached.raw, cached.pub)
		}
		return cached.raw, cached.pub, nil
	}

	client := line.NewClient(lc.AccessToken)
	// keyVersion 1
	res, err := client.GetE2EEPublicKey(mid, 1, keyID)
	if err != nil {
		return 0, "", err
	}

	resKeyID, err := res.KeyID.Int64()
	if err != nil {
		return 0, "", err
	}

	if int(resKeyID) != keyID {
		return 0, "", fmt.Errorf("fetched key ID %d does not match requested %d", resKeyID, keyID)
	}

	pk := peerKeyInfo{raw: int(resKeyID), pub: res.PublicKey}
	// Cache the fetched key so subsequent lookups reuse it.
	lc.cacheMu.Lock()
	lc.peerKeys[mid] = pk
	lc.cacheMu.Unlock()
	if lc.E2EE != nil {
		lc.E2EE.RegisterPeerPublicKey(pk.raw, pk.pub)
	}
	return pk.raw, pk.pub, nil
}

func (lc *LineClient) ensurePeerKeyForMessage(ctx context.Context, msg *line.Message) {
	if lc.E2EE == nil || len(msg.Chunks) < 5 {
		return
	}

	// If we receive an encrypted message from a peer we cached as noE2EE,
	// they must have enabled Letter Sealing — invalidate the cache.
	lc.cacheMu.Lock()
	if lc.peerKeys != nil {
		if cached, ok := lc.peerKeys[msg.From]; ok && cached.noE2EE {
			delete(lc.peerKeys, msg.From)
			lc.cacheMu.Unlock()
			lc.UserLogin.Bridge.Log.Info().Str("peer", msg.From).Msg("Received encrypted message from peer previously cached as noE2EE, invalidating cache")
		} else {
			lc.cacheMu.Unlock()
		}
	} else {
		lc.cacheMu.Unlock()
	}

	senderKeyID, err1 := e2ee.DecodeKeyID(msg.Chunks[len(msg.Chunks)-2])
	if err1 != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err1).Msg("Failed to decode sender key ID")
		return
	}
	myRaw, _, errMy := lc.E2EE.MyKeyIDs()
	if errMy != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(errMy).Msg("Failed to get own key IDs")
		return
	}

	// For group messages, chunks are [first, body, tag, senderKeyID, groupKeyID].
	// We only need the sender's public key to create the decryption channel.
	if ToType(msg.ToType) == ToRoom || ToType(msg.ToType) == ToGroup {
		if lc.E2EE.IsMyKey(senderKeyID) {
			return
		}
		if lc.E2EE.HasPeerPublicKey(senderKeyID) {
			return
		}
		lc.UserLogin.Bridge.Log.Debug().Int("peer_key_id", senderKeyID).Str("peer_mid", msg.From).Msg("Fetching peer public key for group decrypt")
		if _, _, err := lc.ensurePeerKeyByID(ctx, msg.From, senderKeyID); err != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(err).Str("peer", msg.From).Int("key_id", senderKeyID).Msg("Failed to fetch sender peer key for group decrypt")
		}
		return
	}

	// 1:1 message handling
	receiverKeyID, err2 := e2ee.DecodeKeyID(msg.Chunks[len(msg.Chunks)-1])
	if err2 != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err2).Msg("Failed to decode receiver key ID")
		return
	}
	peerRaw := senderKeyID
	peerMid := msg.From
	// Treat the message as self-sent if the sender key is any of OUR devices'
	// keys (the user's keychain shares private keys across devices). myRaw
	// only points to the latest one, so we must check the full set.
	if lc.E2EE.IsMyKey(senderKeyID) {
		peerRaw = receiverKeyID
		peerMid = msg.To
	}
	if peerRaw == 0 || peerRaw == myRaw {
		return
	}
	if lc.E2EE.HasPeerPublicKey(peerRaw) {
		return
	}
	lc.UserLogin.Bridge.Log.Debug().Int("peer_key_id", peerRaw).Str("peer_mid", peerMid).Msg("Fetching peer public key for decrypt")
	if _, _, err := lc.ensurePeerKeyByID(ctx, peerMid, peerRaw); err != nil {
		lc.UserLogin.Bridge.Log.Debug().Err(err).Int("key_id", peerRaw).Msg("ensurePeerKeyByID failed, trying NegotiateE2EEPublicKey")
		if _, _, err2 := lc.ensurePeerKey(ctx, peerMid); err2 != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(err2).Str("peer", peerMid).Int("key_id", peerRaw).Msg("Failed to fetch peer key for decrypt")
		}
	}
}
