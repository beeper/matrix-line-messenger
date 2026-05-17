package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/util/ptr"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

func (lc *LineClient) syncDMChats(ctx context.Context) {
	defer lc.wg.Done()

	client := line.NewClient(lc.AccessToken)
	opts := line.MessageBoxesOptions{
		ActiveOnly:                     true,
		MessageBoxCountLimit:           100,
		WithUnreadCount:                false,
		LastMessagesPerMessageBoxCount: 0,
	}

	res, err := client.GetMessageBoxes(opts)
	if err != nil && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
		if errRecover := lc.recoverToken(ctx); errRecover == nil {
			client = line.NewClient(lc.AccessToken)
			res, err = client.GetMessageBoxes(opts)
		}
	}
	if err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to fetch message boxes for DM sync")
		return
	}

	for _, box := range res.MessageBoxes {
		mid := box.ID
		lowerMid := strings.ToLower(mid)
		// Skip group chats — they're handled by syncChats
		if strings.HasPrefix(lowerMid, "c") || strings.HasPrefix(lowerMid, "r") {
			continue
		}

		contact := lc.getContact(ctx, mid)
		var avatar *bridgev2.Avatar
		if contact.PicturePath != "" {
			picturePath := contact.PicturePath
			avatar = &bridgev2.Avatar{
				ID: networkid.AvatarID(picturePath),
				Get: func(ctx context.Context) ([]byte, error) {
					return lc.GetAvatar(ctx, networkid.AvatarID(picturePath))
				},
			}
		}

		dmType := database.RoomTypeDM
		chatName := contact.EffectiveDisplayName()
		portalKey := networkid.PortalKey{ID: makePortalID(mid), Receiver: lc.UserLogin.ID}
		lc.UserLogin.Bridge.QueueRemoteEvent(lc.UserLogin, &simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{
				Type:      bridgev2.RemoteEventChatResync,
				PortalKey: portalKey,
				Timestamp: time.Now(),
			},
			ChatInfo: &bridgev2.ChatInfo{
				Type:   &dmType,
				Name:   &chatName,
				Avatar: avatar,
				Members: &bridgev2.ChatMemberList{
					IsFull:                     true,
					ExcludeChangesFromTimeline: true,
					Members: []bridgev2.ChatMember{
						{
							EventSender: bridgev2.EventSender{
								IsFromMe: true,
								Sender:   networkid.UserID(lc.UserLogin.ID),
							},
							Membership: event.MembershipJoin,
							PowerLevel: ptr.Ptr(100),
						},
						{
							EventSender: bridgev2.EventSender{
								Sender: makeUserID(mid),
							},
							Membership: event.MembershipJoin,
							PowerLevel: ptr.Ptr(0),
						},
					},
				},
				ExcludeChangesFromTimeline: true,
			},
		})
	}
}

func (lc *LineClient) prefetchMessages(ctx context.Context) {
	defer lc.wg.Done()

	client := line.NewClient(lc.AccessToken)
	opts := line.MessageBoxesOptions{
		ActiveOnly:                     true,
		MessageBoxCountLimit:           100,
		WithUnreadCount:                true,
		LastMessagesPerMessageBoxCount: 0,
	}

	res, err := client.GetMessageBoxes(opts)
	if err != nil && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
		if errRecover := lc.recoverToken(ctx); errRecover == nil {
			client = line.NewClient(lc.AccessToken)
			res, err = client.GetMessageBoxes(opts)
		}
	}
	if err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to prefetch message boxes")
		return
	}

	for _, box := range res.MessageBoxes {
		// Fetch recent messages for all active chats to ensure history is populated
		msgs, err := client.GetRecentMessagesV2(box.ID, 50)
		if err != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(err).Str("chat_mid", box.ID).Msg("Failed to fetch recent messages")
			continue
		}

		// Reverse messages to process oldest first
		for i := len(msgs) - 1; i >= 0; i-- {
			msg := msgs[i]
			if msg.ContentType == 18 {
				lc.cacheGroupMembersFromSystemMessage(msg)
			}

			existing, err := lc.UserLogin.Bridge.DB.Message.GetPartByID(ctx, lc.UserLogin.ID, networkid.MessageID(msg.ID), "")
			if err == nil && existing != nil {
				continue
			}

			opType := OpReceiveMessage
			if msg.From == lc.Mid {
				opType = OpSendMessage
			}
			lc.queueIncomingMessage(msg, int(opType))
		}
	}
}

func (lc *LineClient) syncChats(ctx context.Context) {
	defer lc.wg.Done()

	client := line.NewClient(lc.AccessToken)
	midsResp, err := client.GetAllChatMids(true, true)
	if err != nil && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
		if errRecover := lc.recoverToken(ctx); errRecover == nil {
			client = line.NewClient(lc.AccessToken)
			midsResp, err = client.GetAllChatMids(true, true)
		}
	}
	if err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to fetch all chat mids")
		return
	}

	allMids := append(midsResp.MemberChatMids, midsResp.InvitedChatMids...)
	if len(allMids) == 0 {
		return
	}

	chunkSize := 20
	for i := 0; i < len(allMids); i += chunkSize {
		end := i + chunkSize
		if end > len(allMids) {
			end = len(allMids)
		}
		batch := allMids[i:end]
		chatsResp, err := client.GetChats(batch, true, true)
		if err != nil && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
			if errRecover := lc.recoverToken(ctx); errRecover == nil {
				client = line.NewClient(lc.AccessToken)
				chatsResp, err = client.GetChats(batch, true, true)
			}
		}
		if err != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to fetch batch of chats")
			continue
		}

		for _, chat := range chatsResp.Chats {
			portalKey := networkid.PortalKey{ID: makePortalID(chat.ChatMid), Receiver: lc.UserLogin.ID}

			info := lc.chatToChatInfo(ctx, &chat, true)
			lc.UserLogin.Bridge.QueueRemoteEvent(lc.UserLogin, &simplevent.ChatResync{
				EventMeta: simplevent.EventMeta{
					Type:      bridgev2.RemoteEventChatResync,
					PortalKey: portalKey,
					Timestamp: time.Now(),
				},
				ChatInfo: info,
			})
		}
	}
}

func (lc *LineClient) chatToChatInfo(ctx context.Context, chat *line.Chat, excludeFromTimeline bool) *bridgev2.ChatInfo {
	var avatar *bridgev2.Avatar
	if chat.PicturePath != "" {
		avatar = &bridgev2.Avatar{
			ID: networkid.AvatarID(chat.PicturePath),
			Get: func(ctx context.Context) ([]byte, error) {
				return lc.GetAvatar(ctx, networkid.AvatarID(chat.PicturePath))
			},
		}
	}

	members := []bridgev2.ChatMember{
		{
			EventSender: bridgev2.EventSender{
				IsFromMe: true,
				Sender:   networkid.UserID(lc.UserLogin.ID),
			},
			Membership: event.MembershipJoin,
			PowerLevel: ptr.Ptr(0),
		},
	}

	var groupMemberMids []string
	if chat.Extra.GroupExtra != nil {
		if chat.Extra.GroupExtra.CreatorMid == lc.Mid {
			members[0].PowerLevel = ptr.Ptr(100)
		}
		// If the bridge user is not a full member but is an invitee, mark as invite
		_, isMember := chat.Extra.GroupExtra.MemberMids[lc.Mid]
		if !isMember {
			_, isInvitee := chat.Extra.GroupExtra.InviteeMids[lc.Mid]
			if isInvitee {
				members[0].Membership = event.MembershipInvite
			}
		}

		// Populate group member cache for fallback use when GetChats
		// returns empty MemberMids (known LINE API issue).
		allMemberMids := make([]string, 0, len(chat.Extra.GroupExtra.MemberMids))
		for m := range chat.Extra.GroupExtra.MemberMids {
			if m == lc.Mid || m == string(lc.UserLogin.ID) || strings.HasPrefix(m, "c") || strings.HasPrefix(m, "r") {
				continue
			}
			allMemberMids = append(allMemberMids, m)
			members = append(members, bridgev2.ChatMember{
				EventSender: bridgev2.EventSender{
					Sender: makeUserID(m),
				},
				Membership: event.MembershipJoin,
			})
		}
		for m := range chat.Extra.GroupExtra.InviteeMids {
			if m == lc.Mid || m == string(lc.UserLogin.ID) || strings.HasPrefix(m, "c") || strings.HasPrefix(m, "r") {
				continue
			}
			allMemberMids = append(allMemberMids, m)
			membership := event.MembershipInvite
			if chat.Type == 1 {
				membership = event.MembershipJoin
			}
			members = append(members, bridgev2.ChatMember{
				EventSender: bridgev2.EventSender{
					Sender: makeUserID(m),
				},
				Membership: membership,
			})
		}
		if len(allMemberMids) == 0 {
			lc.cacheGroupMembersFromRecentMessages(ctx, chat.ChatMid)
			for _, m := range lc.getCachedGroupMembers(chat.ChatMid) {
				if m == lc.Mid || m == string(lc.UserLogin.ID) || strings.HasPrefix(m, "c") || strings.HasPrefix(m, "r") {
					continue
				}
				allMemberMids = append(allMemberMids, m)
				members = append(members, bridgev2.ChatMember{
					EventSender: bridgev2.EventSender{
						Sender: makeUserID(m),
					},
					Membership: event.MembershipJoin,
				})
			}
		}

		groupMemberMids = make([]string, 0, len(allMemberMids)+1)
		groupMemberMids = append(groupMemberMids, lc.Mid)
		groupMemberMids = append(groupMemberMids, allMemberMids...)
		lc.cacheMu.Lock()
		if lc.groupMemberCache == nil {
			lc.groupMemberCache = make(map[string][]string)
		}
		if lc.generatedGroupNameCache == nil {
			lc.generatedGroupNameCache = make(map[string]bool)
		}
		lc.groupMemberCache[chat.ChatMid] = groupMemberMids
		lc.cacheMu.Unlock()
	}

	name := chat.ChatName
	if chat.Extra.GroupExtra != nil && chat.Type == 1 {
		lc.cacheMu.Lock()
		generateName := lc.generatedGroupNameCache[chat.ChatMid]
		lc.cacheMu.Unlock()
		if generateName && len(groupMemberMids) > 1 {
			name = lc.generateNameFromMemberList(ctx, groupMemberMids)
		}
	}
	if name == "" && chat.Extra.GroupExtra != nil {
		name = lc.generateNameFromMemberList(ctx, groupMemberMids)
	}

	ct := database.RoomTypeGroupDM
	if chat.Extra.GroupExtra == nil {
		ct = database.RoomTypeDM
	}

	return &bridgev2.ChatInfo{
		Type:   &ct,
		Name:   &name,
		Avatar: avatar,
		Members: &bridgev2.ChatMemberList{
			IsFull:                     true,
			Members:                    members,
			ExcludeChangesFromTimeline: excludeFromTimeline,
		},
		ExcludeChangesFromTimeline: excludeFromTimeline,
	}
}

func (lc *LineClient) generateNameFromMemberList(ctx context.Context, members []string) string {
	var names []string
	count := 0
	seen := make(map[string]struct{}, len(members))
	for _, mid := range members {
		if mid == string(lc.UserLogin.ID) || mid == lc.Mid || strings.HasPrefix(mid, "c") || strings.HasPrefix(mid, "r") {
			continue
		}
		if _, ok := seen[mid]; ok {
			continue
		}
		seen[mid] = struct{}{}
		contact := lc.getContact(ctx, mid)
		name := contact.EffectiveDisplayName()
		if name != "" && name != mid {
			names = append(names, name)
		}
		count++
		if count >= 20 {
			break
		}
	}

	finalNames := names
	if len(names) > 3 {
		finalNames = names[:3]
	}

	if len(finalNames) == 0 {
		return ""
	}

	result := strings.Join(finalNames, ", ")
	actualMemberCount := 0
	seen = make(map[string]struct{}, len(members))
	for _, m := range members {
		if _, ok := seen[m]; ok {
			continue
		}
		seen[m] = struct{}{}
		if m != string(lc.UserLogin.ID) && !strings.HasPrefix(m, "c") && !strings.HasPrefix(m, "r") {
			actualMemberCount++
		}
	}
	remaining := actualMemberCount - len(finalNames)
	if remaining > 0 {
		result += fmt.Sprintf(" and %d others", remaining)
	}
	return result
}

func (lc *LineClient) getCachedGroupMembers(chatMid string) []string {
	lc.cacheMu.Lock()
	defer lc.cacheMu.Unlock()
	members := lc.groupMemberCache[chatMid]
	if len(members) == 0 {
		return nil
	}
	return append([]string(nil), members...)
}

func (lc *LineClient) cacheGroupMembersFromSystemMessage(msg *line.Message) {
	if msg == nil || msg.ContentMetadata == nil {
		return
	}
	chatMid := msg.To
	if !isChatMID(chatMid) {
		return
	}
	locKey := msg.ContentMetadata["LOC_KEY"]
	switch locKey {
	case "C_GI", "C_MI", "A_MI", "A_MC":
	default:
		return
	}

	seen := map[string]struct{}{
		lc.Mid: {},
	}
	for _, mid := range lc.getCachedGroupMembers(chatMid) {
		seen[mid] = struct{}{}
	}
	for _, mid := range midsFromSystemLocArgs(msg.ContentMetadata["LOC_ARGS"]) {
		seen[mid] = struct{}{}
	}
	if len(seen) <= 1 {
		return
	}

	members := make([]string, 0, len(seen))
	for mid := range seen {
		members = append(members, mid)
	}
	lc.cacheMu.Lock()
	if lc.groupMemberCache == nil {
		lc.groupMemberCache = make(map[string][]string)
	}
	lc.groupMemberCache[chatMid] = members
	lc.cacheMu.Unlock()
}

func (lc *LineClient) cacheGroupMembersFromRecentMessages(ctx context.Context, chatMid string) {
	if len(lc.getCachedGroupMembers(chatMid)) > 1 {
		return
	}
	client := line.NewClient(lc.AccessToken)
	msgs, err := client.GetRecentMessagesV2(chatMid, 50)
	if err != nil && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
		if errRecover := lc.recoverToken(ctx); errRecover == nil {
			client = line.NewClient(lc.AccessToken)
			msgs, err = client.GetRecentMessagesV2(chatMid, 50)
		}
	}
	if err != nil {
		lc.UserLogin.Bridge.Log.Debug().Err(err).Str("chat_mid", chatMid).Msg("Failed to fetch recent messages for group member cache")
		return
	}
	for i := len(msgs) - 1; i >= 0; i-- {
		msg := msgs[i]
		if msg.ContentType == 18 {
			lc.cacheGroupMembersFromSystemMessage(msg)
		}
	}
}

func midsFromSystemLocArgs(locArgs string) []string {
	fields := strings.FieldsFunc(locArgs, func(r rune) bool {
		return r == '\x1e' || r == '\x1f'
	})
	mids := make([]string, 0, len(fields))
	for _, field := range fields {
		if isUserMID(field) {
			mids = append(mids, field)
		}
	}
	return mids
}

func isUserMID(mid string) bool {
	return len(mid) > 1 && strings.HasPrefix(mid, "U")
}

func isChatMID(mid string) bool {
	if mid == "" {
		return false
	}
	lower := strings.ToLower(mid)
	return strings.HasPrefix(lower, "c") || strings.HasPrefix(lower, "r")
}

func (lc *LineClient) refreshGroupsForContact(ctx context.Context, mid string) {
	type groupUpdate struct {
		chatMid       string
		members       []string
		generatedName bool
	}
	var updates []groupUpdate

	lc.cacheMu.Lock()
	for chatMid, members := range lc.groupMemberCache {
		for _, member := range members {
			if member == mid {
				updates = append(updates, groupUpdate{
					chatMid:       chatMid,
					members:       append([]string(nil), members...),
					generatedName: lc.generatedGroupNameCache[chatMid],
				})
				break
			}
		}
	}
	lc.cacheMu.Unlock()

	for _, update := range updates {
		var name *string
		if update.generatedName {
			generatedName := lc.generateNameFromMemberList(ctx, update.members)
			if generatedName != "" {
				name = &generatedName
			}
		}
		if name == nil {
			continue
		}
		portalKey := networkid.PortalKey{ID: makePortalID(update.chatMid), Receiver: lc.UserLogin.ID}
		lc.UserLogin.Bridge.QueueRemoteEvent(lc.UserLogin, &simplevent.ChatInfoChange{
			EventMeta: simplevent.EventMeta{
				Type:      bridgev2.RemoteEventChatInfoChange,
				PortalKey: portalKey,
				Timestamp: time.Now(),
			},
			ChatInfoChange: &bridgev2.ChatInfoChange{
				ChatInfo: &bridgev2.ChatInfo{
					Name: name,
				},
			},
		})
	}
}

func (lc *LineClient) pollLoop(ctx context.Context) {
	defer lc.wg.Done()

	var localRev int64 = 0
	client := line.NewClient(lc.AccessToken)

	lc.UserLogin.Bridge.Log.Info().Msg("Starting LINE SSE loop...")
	rev, err := client.GetLastOpRevision()
	if err != nil && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
		if errRecover := lc.recoverToken(ctx); errRecover == nil {
			client = line.NewClient(lc.AccessToken)
			rev, err = client.GetLastOpRevision()
		} else {
			lc.UserLogin.Bridge.Log.Warn().Err(errRecover).Msg("Failed to recover token for getLastOpRevision")
		}
	}
	if err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to get last op revision")
	} else {
		localRev = rev
		lc.UserLogin.Bridge.Log.Info().Int64("local_rev", localRev).Msg("Seeded local revision from getLastOpRevision")
	}

	handler := func(eventType, data string) {
		// handle keep alives
		if eventType == "ping" || eventType == "connInfoRevision" {
			return
		}

		// handle fullsync requests
		if eventType == "fullSync" {
			lc.UserLogin.Bridge.Log.Info().Msg("Received fullSync request")

			var fsPayload struct {
				NextRevision string `json:"nextRevision"`
			}
			if err := json.Unmarshal([]byte(data), &fsPayload); err == nil && fsPayload.NextRevision != "" {
				if newRev, err := strconv.ParseInt(fsPayload.NextRevision, 10, 64); err == nil {
					lc.UserLogin.Bridge.Log.Info().Int64("old", localRev).Int64("new", newRev).Msg("Updating local revision from fullSync")

					localRev = newRev

				}
			}
			lc.wg.Add(3)
			go lc.syncChats(ctx)
			go lc.syncDMChats(ctx)
			go lc.prefetchMessages(ctx)
			return
		}

		// handle operations
		if eventType == "operation" {
			var op line.Operation
			if err := json.Unmarshal([]byte(data), &op); err != nil {
				lc.UserLogin.Bridge.Log.Error().Err(err).Msg("Failed to unmarshal op")
				return
			}

			rev, _ := op.Revision.Int64()
			if rev > localRev {
				localRev = rev
			}

			lc.handleOperation(ctx, op)
		}
	}

	for {
		err := client.ListenSSE(ctx, localRev, handler)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if err.Error() != "EOF" {
				lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("SSE Disconnected")

				isAuthErr := strings.Contains(err.Error(), "SSE error: 401") ||
					strings.Contains(err.Error(), "SSE error: 403") ||
					lc.isLoggedOut(err)

				if isAuthErr {
					if errRecover := lc.recoverToken(ctx); errRecover != nil {
						lc.UserLogin.Bridge.Log.Error().Err(errRecover).Msg("Failed to recover session, stopping poll loop")
						lc.UserLogin.BridgeState.Send(status.BridgeState{
							StateEvent: status.StateBadCredentials,
							Error:      "line-logged-out",
							Message:    "LINE session was invalidated (logged out by another client). Please re-authenticate the bridge.",
						})
						return
					}
					client = line.NewClient(lc.AccessToken)
				}
			}
			time.Sleep(3 * time.Second)
		}
	}
}

func (lc *LineClient) handleOperation(ctx context.Context, op line.Operation) {
	opType := OperationType(op.Type)

	if opType == OpSendMessage {
		lc.reqSeqMu.Lock()
		_, ok := lc.sentReqSeqs[op.ReqSeq]
		if ok {
			delete(lc.sentReqSeqs, op.ReqSeq)
			lc.reqSeqMu.Unlock()
			return
		}
		lc.reqSeqMu.Unlock()
	}

	switch opType {
	case OpContactUpdate:
		mid := op.Param1
		lc.cacheMu.Lock()
		delete(lc.contactCache, mid)
		lc.cacheMu.Unlock()
		contact := lc.getContact(ctx, mid)
		name := contact.EffectiveDisplayName()
		lc.UserLogin.Bridge.Log.Info().Str("mid", mid).Str("name", name).Msg("Contact updated")
		ghost, err := lc.UserLogin.Bridge.GetGhostByID(ctx, makeUserID(mid))
		if err == nil && ghost != nil {
			ghost.UpdateInfo(ctx, &bridgev2.UserInfo{
				Identifiers: []string{mid},
				Name:        &name,
			})
		}
		var avatar *bridgev2.Avatar
		if contact.PicturePath != "" {
			picturePath := contact.PicturePath
			avatar = &bridgev2.Avatar{
				ID: networkid.AvatarID(picturePath),
				Get: func(ctx context.Context) ([]byte, error) {
					return lc.GetAvatar(ctx, networkid.AvatarID(picturePath))
				},
			}
		}
		dmType := database.RoomTypeDM
		portalKey := networkid.PortalKey{ID: makePortalID(mid), Receiver: lc.UserLogin.ID}
		lc.UserLogin.Bridge.QueueRemoteEvent(lc.UserLogin, &simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{
				Type:      bridgev2.RemoteEventChatResync,
				PortalKey: portalKey,
				Timestamp: time.Now(),
			},
			ChatInfo: &bridgev2.ChatInfo{
				Type:   &dmType,
				Name:   &name,
				Avatar: avatar,
			},
		})
		lc.refreshGroupsForContact(ctx, mid)

	case OpDeleteSelfFromChat:
		lc.handleSelfLeave(op.Param1)

	case OpSendChatRemoved:
		lc.reqSeqMu.Lock()
		_, ok := lc.sentReqSeqs[op.ReqSeq]
		if ok {
			delete(lc.sentReqSeqs, op.ReqSeq)
			lc.reqSeqMu.Unlock()
			return
		}
		lc.reqSeqMu.Unlock()
		lc.handleSelfLeave(op.Param1)

	case OpDeleteOtherFromChat:
		lc.handleMemberLeave(op.Param1, op.Param2)

	case OpNotifiedLeaveChat:
		lower1 := strings.ToLower(op.Param1)
		if strings.HasPrefix(lower1, "c") || strings.HasPrefix(lower1, "r") {
			lc.handleMemberLeave(op.Param1, op.Param2)
		} else {
			lc.handleMemberLeave(op.Param2, op.Param1)
		}

	case OpNotifiedJoinChat:
		lc.handleMemberJoin(op.Param1, op.Param2)

	case OpCancelInvitation:
		lc.handleMemberLeave(op.Param1, op.Param3)

	case OpInviteIntoChat, OpNotifiedInviteIntoChat:
		lc.wg.Add(1)
		go func() {
			defer lc.wg.Done()
			lc.handleInvite(context.Background(), op.Param1)
		}()

	case OpChatUpdate, OpChatUpdate2:
		lc.UserLogin.Bridge.Log.Info().Str("chat_mid", op.Param1).Int("op_type", op.Type).Msg("Received chat update operation")
		lc.wg.Add(1)
		go func() {
			defer lc.wg.Done()
			lc.syncSingleChat(context.Background(), op)
		}()

	case OpReadReceipt:
		portalID := makePortalID(op.Param1)
		senderID := makeUserID(op.Param2)

		ts, _ := op.CreatedTime.Int64()
		lc.UserLogin.Bridge.QueueRemoteEvent(lc.UserLogin, &simplevent.Receipt{
			EventMeta: simplevent.EventMeta{
				Type: bridgev2.RemoteEventReadReceipt,
				PortalKey: networkid.PortalKey{
					ID:       portalID,
					Receiver: lc.UserLogin.ID,
				},
				Timestamp: time.UnixMilli(ts),
				Sender:    bridgev2.EventSender{Sender: senderID},
			},
			ReadUpTo: time.UnixMilli(ts),
		})

	case OpUnsendLocal, OpUnsendRemote:
		chatMid := op.Param1
		msgID := op.Param2
		lc.UserLogin.Bridge.Log.Info().Str("msg_id", msgID).Str("chat_mid", chatMid).Int("op_type", op.Type).Msg("Received unsend operation")

		ts, _ := op.CreatedTime.Int64()
		lc.UserLogin.Bridge.QueueRemoteEvent(lc.UserLogin, &simplevent.MessageRemove{
			EventMeta: simplevent.EventMeta{
				Type:      bridgev2.RemoteEventMessageRemove,
				PortalKey: networkid.PortalKey{ID: makePortalID(chatMid), Receiver: lc.UserLogin.ID},
				Timestamp: time.UnixMilli(ts),
			},
			TargetMessage: networkid.MessageID(msgID),
		})

	case OpReaction:
		lc.wg.Add(1)
		go func() {
			defer lc.wg.Done()

			param2, err := line.ParseReactionParam2(op.Param2)
			if err != nil {
				lc.UserLogin.Bridge.Log.Error().Err(err).Msg("Failed to parse reaction param2")
				return
			}
			if param2.Curr == nil || param2.Curr.PaidReactionType == nil {
				lc.UserLogin.Bridge.Log.Error().Msg("No current reaction or paid reaction type found")
				return
			}

			prt := param2.Curr.PaidReactionType
			url := fmt.Sprintf("https://stickershop.line-scdn.net/sticonshop/v1/sticon/%s/android/%s.png", prt.ProductID, prt.EmojiID)

			resp, err := lc.HTTPClient.Get(url)
			if err != nil {
				lc.UserLogin.Bridge.Log.Error().Err(err).Str("url", url).Msg("Failed to download reaction image")
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				lc.UserLogin.Bridge.Log.Error().Int("status_code", resp.StatusCode).Str("url", url).Msg("Failed to download reaction image: bad status code")
				return
			}

			data, err := io.ReadAll(resp.Body)
			if err != nil {
				lc.UserLogin.Bridge.Log.Error().Err(err).Msg("Failed to read reaction image body")
				return
			}

			mimeType := resp.Header.Get("Content-Type")
			if mimeType == "" {
				mimeType = "image/png"
			}

			senderID := makeUserID(op.Param3)
			ghost, err := lc.UserLogin.Bridge.GetGhostByID(ctx, senderID)
			if err != nil {
				lc.UserLogin.Bridge.Log.Error().Err(err).Msg("Failed to get ghost for reaction sender")
				return
			}

			portalKey := networkid.PortalKey{ID: makePortalID(param2.ChatMid), Receiver: lc.UserLogin.ID}
			portal, err := lc.UserLogin.Bridge.GetPortalByKey(ctx, portalKey)
			if err != nil || portal == nil {
				lc.UserLogin.Bridge.Log.Error().Err(err).Str("chat_mid", param2.ChatMid).Msg("Failed to get portal for reaction")
				return
			}

			if portal.MXID == "" {
				lc.UserLogin.Bridge.Log.Error().Msg("Portal MXID is empty, cannot upload media")
				return
			}

			mxc, uploadedFile, err := ghost.Intent.UploadMedia(ctx, "", data, "reaction.png", mimeType)
			if err != nil {
				lc.UserLogin.Bridge.Log.Error().Err(err).Int("data_len", len(data)).Msg("Failed to upload reaction image to Matrix")
				return
			}
			if mxc == "" && uploadedFile != nil && uploadedFile.URL != "" {
				mxc = id.ContentURIString(uploadedFile.URL)
			}
			if mxc == "" {
				lc.UserLogin.Bridge.Log.Error().Interface("uploaded_file", uploadedFile).Msg("UploadMedia returned empty MXC URI")
				return
			}

			ts, _ := op.CreatedTime.Int64()
			lc.UserLogin.Bridge.QueueRemoteEvent(lc.UserLogin, &simplevent.Reaction{
				EventMeta: simplevent.EventMeta{
					Type:      bridgev2.RemoteEventReaction,
					PortalKey: portalKey,
					Timestamp: time.UnixMilli(ts),
					Sender:    bridgev2.EventSender{Sender: senderID},
				},
				TargetMessage: networkid.MessageID(op.Param1),
				Emoji:         string(mxc),
			})
		}()

	case OpSendMessage, OpReceiveMessage:
		if op.Message != nil {
			if op.Message.ContentType == 18 {
				lc.handleSystemMessage(op)
			} else {
				lc.queueIncomingMessage(op.Message, op.Type)
			}
		}

	default:
		logEvt := lc.UserLogin.Bridge.Log.Debug().
			Int("op_type", op.Type).
			Str("param1", op.Param1).
			Str("param2", op.Param2).
			Str("param3", op.Param3)
		if op.Message != nil {
			logEvt = logEvt.Str("msg_from", op.Message.From).
				Int("msg_content_type", op.Message.ContentType).
				Interface("msg_metadata", op.Message.ContentMetadata)
		}
		logEvt.Msg("Unhandled SSE operation")
	}
}

func (lc *LineClient) syncSingleChat(ctx context.Context, op line.Operation) {
	chatMid := op.Param1
	client := line.NewClient(lc.AccessToken)
	chatsResp, err := client.GetChats([]string{chatMid}, true, true)
	if err != nil && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
		if errRecover := lc.recoverToken(ctx); errRecover == nil {
			client = line.NewClient(lc.AccessToken)
			chatsResp, err = client.GetChats([]string{chatMid}, true, true)
		}
	}
	if err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Str("chat_mid", chatMid).Msg("Failed to fetch chat info")
		// Only emit leave if we confirm the user is definitively not a member
		if line.IsNotAMemberError(err) {
			// Confirm via GetAllChatMids before emitting leave
			isMember, isInvitee := lc.checkChatMembership(ctx, chatMid)
			if !isMember && !isInvitee {
				lc.UserLogin.Bridge.Log.Info().Str("chat_mid", chatMid).Msg("Confirmed user not in chat, emitting leave")
				lc.handleSelfLeave(chatMid)
			} else if isInvitee {
				lc.UserLogin.Bridge.Log.Info().Str("chat_mid", chatMid).Msg("User is an invitee, handling invite")
				lc.handleInvite(ctx, chatMid)
			}
		}
		return
	}
	if len(chatsResp.Chats) == 0 {
		// Chat not returned — verify before emitting leave
		isMember, isInvitee := lc.checkChatMembership(ctx, chatMid)
		if !isMember && !isInvitee {
			lc.UserLogin.Bridge.Log.Info().Str("chat_mid", chatMid).Msg("Chat no longer available, user removed, emitting leave")
			lc.handleSelfLeave(chatMid)
		} else if isInvitee {
			lc.UserLogin.Bridge.Log.Info().Str("chat_mid", chatMid).Msg("User is an invitee (empty resp), handling invite")
			lc.handleInvite(ctx, chatMid)
		}
		return
	}
	chat := chatsResp.Chats[0]

	portalKey := networkid.PortalKey{ID: makePortalID(chat.ChatMid), Receiver: lc.UserLogin.ID}

	var avatar *bridgev2.Avatar
	if chat.PicturePath != "" {
		avatar = &bridgev2.Avatar{
			ID: networkid.AvatarID(chat.PicturePath),
			Get: func(ctx context.Context) ([]byte, error) {
				return lc.GetAvatar(ctx, networkid.AvatarID(chat.PicturePath))
			},
		}
	}

	// Use ChatInfoChange to only update avatar (and other non-name metadata).
	// Name updates are handled by handleGroupRename from contentType=18 messages,
	// which has the correct new name from LOC_ARGS.
	// No sender is set on either event to avoid ghost creation/invite issues.
	lc.UserLogin.Bridge.QueueRemoteEvent(lc.UserLogin, &simplevent.ChatInfoChange{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatInfoChange,
			PortalKey: portalKey,
			Timestamp: time.Now(),
		},
		ChatInfoChange: &bridgev2.ChatInfoChange{
			ChatInfo: &bridgev2.ChatInfo{
				Avatar: avatar,
			},
		},
	})
}

// checkChatMembership calls GetAllChatMids to verify whether the bridge user
// is a member or invitee of the given chat.
func (lc *LineClient) checkChatMembership(ctx context.Context, chatMid string) (isMember, isInvitee bool) {
	client := line.NewClient(lc.AccessToken)
	midsResp, err := client.GetAllChatMids(true, true)
	if err != nil && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
		if errRecover := lc.recoverToken(ctx); errRecover == nil {
			client = line.NewClient(lc.AccessToken)
			midsResp, err = client.GetAllChatMids(true, true)
		}
	}
	if err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("checkChatMembership: GetAllChatMids failed")
		return false, false
	}
	for _, mid := range midsResp.MemberChatMids {
		if mid == chatMid {
			return true, false
		}
	}
	for _, mid := range midsResp.InvitedChatMids {
		if mid == chatMid {
			return false, true
		}
	}
	return false, false
}

func (lc *LineClient) emitMemberChange(chatMid, userMid string, membership event.Membership, ts time.Time) {
	portalKey := networkid.PortalKey{ID: makePortalID(chatMid), Receiver: lc.UserLogin.ID}
	sender := bridgev2.EventSender{Sender: networkid.UserID(userMid)}
	if userMid == string(lc.UserLogin.ID) || userMid == lc.Mid {
		sender.IsFromMe = true
	}
	lc.UserLogin.Bridge.QueueRemoteEvent(lc.UserLogin, &simplevent.ChatInfoChange{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatInfoChange,
			PortalKey: portalKey,
			Timestamp: ts,
		},
		ChatInfoChange: &bridgev2.ChatInfoChange{
			MemberChanges: &bridgev2.ChatMemberList{
				Members: []bridgev2.ChatMember{
					{
						EventSender: sender,
						Membership:  membership,
					},
				},
			},
		},
	})
}

func (lc *LineClient) handleSelfLeave(chatMid string) {
	lc.emitMemberChange(chatMid, string(lc.UserLogin.ID), event.MembershipLeave, time.Now())
}

func (lc *LineClient) handleMemberLeave(chatMid, leaverMid string) {
	lower := strings.ToLower(chatMid)
	if !strings.HasPrefix(lower, "c") && !strings.HasPrefix(lower, "r") {
		return
	}
	if leaverMid == lc.Mid || leaverMid == string(lc.UserLogin.ID) {
		lc.handleSelfLeave(chatMid)
		return
	}
	lc.emitMemberChange(chatMid, leaverMid, event.MembershipLeave, time.Now())
}

func (lc *LineClient) handleMemberJoin(chatMid, joinerMid string) {
	lower := strings.ToLower(chatMid)
	if !strings.HasPrefix(lower, "c") && !strings.HasPrefix(lower, "r") {
		return
	}
	lc.emitMemberChange(chatMid, joinerMid, event.MembershipJoin, time.Now())
}

func (lc *LineClient) handleInvite(ctx context.Context, chatMid string) {
	client := line.NewClient(lc.AccessToken)
	chatsResp, err := client.GetChats([]string{chatMid}, true, true)
	if err != nil && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
		if errRecover := lc.recoverToken(ctx); errRecover == nil {
			client = line.NewClient(lc.AccessToken)
			chatsResp, err = client.GetChats([]string{chatMid}, true, true)
		}
	}
	if err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Str("chat_mid", chatMid).Msg("Failed to fetch invited chat info")
		return
	}
	if len(chatsResp.Chats) == 0 {
		return
	}
	chat := chatsResp.Chats[0]

	// Check if the bridge user is being invited vs. someone else
	isBridgeUserInvitee := false
	if chat.Extra.GroupExtra != nil {
		_, isBridgeUserInvitee = chat.Extra.GroupExtra.InviteeMids[lc.Mid]
	}

	if !isBridgeUserInvitee {
		// Someone else is being invited — emit MembershipInvite for each invitee
		if chat.Extra.GroupExtra != nil {
			membership := event.MembershipInvite
			if chat.Type == 1 {
				membership = event.MembershipJoin
			}
			for inviteeMid := range chat.Extra.GroupExtra.InviteeMids {
				if inviteeMid == lc.Mid || inviteeMid == string(lc.UserLogin.ID) {
					continue
				}
				lc.emitMemberChange(chat.ChatMid, inviteeMid, membership, time.Now())
			}
		}
		return
	}

	portalKey := networkid.PortalKey{ID: makePortalID(chat.ChatMid), Receiver: lc.UserLogin.ID}
	info := lc.chatToChatInfo(ctx, &chat, false)
	lc.UserLogin.Bridge.QueueRemoteEvent(lc.UserLogin, &simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatResync,
			PortalKey: portalKey,
			Timestamp: time.Now(),
		},
		ChatInfo: info,
	})
}

func (lc *LineClient) handleSystemMessage(op line.Operation) {
	msg := op.Message
	if msg.ContentMetadata == nil {
		return
	}
	locKey := msg.ContentMetadata["LOC_KEY"]
	ts, _ := msg.CreatedTime.Int64()
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	tsTime := time.UnixMilli(ts)
	switch locKey {
	case "C_PN":
		lc.handleGroupRename(op)
	case "C_MJ", "A_MJ":
		lc.emitMemberChange(msg.To, msg.From, event.MembershipJoin, tsTime)
	case "C_ML", "A_ML", "C_MR", "A_MR":
		lc.UserLogin.Bridge.Log.Debug().Str("loc_key", locKey).Str("chat_mid", msg.To).Str("leaver_mid", msg.From).Msg("System message: member leave")
		lc.emitMemberChange(msg.To, msg.From, event.MembershipLeave, tsTime)
	case "C_GI", "C_MI", "A_MI":
		// msg.From is the inviter, not the invitee.
		// Extract the invitee from LOC_ARGS, which has format: inviterMid\x1einviteeMid
		locArgs := msg.ContentMetadata["LOC_ARGS"]
		parts := strings.SplitN(locArgs, "\x1e", 2)
		if len(parts) == 2 {
			inviteeMid := parts[1]
			lc.emitMemberChange(msg.To, inviteeMid, event.MembershipInvite, tsTime)
		}
	case "C_IC":
		// Invitation cancelled — emit leave for the invitee
		// LOC_ARGS format: cancellerMid\x1einviteeMid
		locArgs := msg.ContentMetadata["LOC_ARGS"]
		parts := strings.SplitN(locArgs, "\x1e", 2)
		if len(parts) == 2 {
			inviteeMid := parts[1]
			lc.emitMemberChange(msg.To, inviteeMid, event.MembershipLeave, tsTime)
		}
	case "A_MC":
		// A_MC = Auto-join via call / member added.
		// msg.From is the person added.
		lc.emitMemberChange(msg.To, msg.From, event.MembershipJoin, tsTime)
	default:
		lc.UserLogin.Bridge.Log.Debug().
			Str("loc_key", locKey).
			Str("chat_mid", msg.To).
			Msg("Unhandled system message LOC_KEY")
	}
}

func (lc *LineClient) handleGroupRename(op line.Operation) {
	msg := op.Message
	locArgs := msg.ContentMetadata["LOC_ARGS"]
	// LOC_ARGS format: "<renamer_mid>\x1e<new_name>"
	parts := strings.SplitN(locArgs, "\x1e", 2)
	if len(parts) < 2 || parts[1] == "" {
		return
	}
	newName := parts[1]

	portalKey := networkid.PortalKey{ID: makePortalID(msg.To), Receiver: lc.UserLogin.ID}

	ts, _ := msg.CreatedTime.Int64()
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}

	lc.UserLogin.Bridge.Log.Debug().
		Str("new_name", newName).
		Str("chat_mid", msg.To).
		Str("from", msg.From).
		Msg("Handling group rename")

	lc.UserLogin.Bridge.QueueRemoteEvent(lc.UserLogin, &simplevent.ChatInfoChange{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatInfoChange,
			PortalKey: portalKey,
			Timestamp: time.UnixMilli(ts),
		},
		ChatInfoChange: &bridgev2.ChatInfoChange{
			ChatInfo: &bridgev2.ChatInfo{
				Name: &newName,
			},
		},
	})
}
