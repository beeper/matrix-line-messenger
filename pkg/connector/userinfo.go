package connector

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"go.mau.fi/util/jsontime"
	"go.mau.fi/util/ptr"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

var _ bridgev2.IdentifierResolvingNetworkAPI = (*LineClient)(nil)

func (lc *LineClient) HandleMatrixReadReceipt(ctx context.Context, read *bridgev2.MatrixReadReceipt) error {
	if read.ReadUpTo.IsZero() && read.EventID == "" {
		return nil
	}

	targetID := string(read.EventID)
	if read.EventID != "" {
		msg, err := lc.UserLogin.Bridge.DB.Message.GetPartByMXID(ctx, read.EventID)
		if err == nil && msg != nil && msg.ID != "" {
			targetID = string(msg.ID)
		}
	}

	if targetID == "" || strings.HasPrefix(targetID, "$") {
		return nil
	}

	client := line.NewClient(lc.AccessToken)
	return client.SendChatChecked(string(read.Portal.ID), targetID)
}

func (lc *LineClient) GetCapabilities(ctx context.Context, portal *bridgev2.Portal) *event.RoomFeatures {
	return &event.RoomFeatures{
		MaxTextLength:         5000,
		Reply:                 event.CapLevelFullySupported,
		Reaction:              event.CapLevelRejected,
		ReadReceipts:          true,
		Delete:                event.CapLevelFullySupported,
		DeleteMaxAge:          &jsontime.Seconds{Duration: 24 * time.Hour},
		DeleteChatForEveryone: true,
		LocationMessage:       event.CapLevelPartialSupport,
		File: event.FileFeatureMap{
			event.MsgImage: {
				Caption: event.CapLevelRejected,
				MimeTypes: map[string]event.CapabilitySupportLevel{
					"image/jpeg":    event.CapLevelFullySupported,
					"image/png":     event.CapLevelFullySupported,
					"image/gif":     event.CapLevelFullySupported,
					"image/webp":    event.CapLevelFullySupported,
					"image/avif":    event.CapLevelFullySupported,
					"image/bmp":     event.CapLevelFullySupported,
					"image/tiff":    event.CapLevelFullySupported,
					"image/svg+xml": event.CapLevelFullySupported,
					"image/heic":    event.CapLevelFullySupported,
					"image/heif":    event.CapLevelFullySupported,
					"image/jxl":     event.CapLevelFullySupported,
				},
			},
			event.MsgFile: {
				Caption: event.CapLevelRejected,
				MimeTypes: map[string]event.CapabilitySupportLevel{
					"image/gif": event.CapLevelFullySupported,
					"*/*":       event.CapLevelFullySupported,
				},
			},
			event.MsgVideo: {
				Caption: event.CapLevelRejected,
				MimeTypes: map[string]event.CapabilitySupportLevel{
					"video/mp4":        event.CapLevelFullySupported,
					"video/webm":       event.CapLevelFullySupported,
					"video/quicktime":  event.CapLevelFullySupported,
					"video/3gpp":       event.CapLevelFullySupported,
					"video/x-matroska": event.CapLevelFullySupported,
					"video/mpeg":       event.CapLevelFullySupported,
					"video/ogg":        event.CapLevelFullySupported,
					"video/x-msvideo":  event.CapLevelFullySupported,
				},
			},
			event.MsgAudio: {
				Caption: event.CapLevelRejected,
				MimeTypes: map[string]event.CapabilitySupportLevel{
					"audio/mpeg":  event.CapLevelFullySupported,
					"audio/ogg":   event.CapLevelFullySupported,
					"audio/mp4":   event.CapLevelFullySupported,
					"audio/x-m4a": event.CapLevelFullySupported,
					"audio/aac":   event.CapLevelFullySupported,
					"audio/wav":   event.CapLevelFullySupported,
					"audio/x-wav": event.CapLevelFullySupported,
					"audio/flac":  event.CapLevelFullySupported,
					"audio/opus":  event.CapLevelFullySupported,
					"audio/webm":  event.CapLevelFullySupported,
					"audio/amr":   event.CapLevelFullySupported,
					"audio/3gpp":  event.CapLevelFullySupported,
				},
			},
			event.CapMsgVoice: {
				Caption: event.CapLevelRejected,
				MimeTypes: map[string]event.CapabilitySupportLevel{
					"audio/ogg":   event.CapLevelFullySupported,
					"audio/mp4":   event.CapLevelFullySupported,
					"audio/mpeg":  event.CapLevelFullySupported,
					"audio/x-m4a": event.CapLevelFullySupported,
					"audio/aac":   event.CapLevelFullySupported,
					"audio/wav":   event.CapLevelFullySupported,
					"audio/x-wav": event.CapLevelFullySupported,
					"audio/flac":  event.CapLevelFullySupported,
					"audio/opus":  event.CapLevelFullySupported,
					"audio/webm":  event.CapLevelFullySupported,
					"audio/amr":   event.CapLevelFullySupported,
					"audio/3gpp":  event.CapLevelFullySupported,
				},
			},
		},
	}
}

func (lc *LineClient) IsThisUser(ctx context.Context, userID networkid.UserID) bool {
	return userID == networkid.UserID(lc.UserLogin.ID)
}

func (lc *LineClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	mid := string(portal.ID)
	lowerMid := strings.ToLower(mid)
	if strings.HasPrefix(lowerMid, "c") || strings.HasPrefix(lowerMid, "r") {
		client := line.NewClient(lc.AccessToken)
		res, err := client.GetChats([]string{mid}, true, true)
		if err != nil && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
			if errRecover := lc.recoverToken(ctx); errRecover == nil {
				client = line.NewClient(lc.AccessToken)
				res, err = client.GetChats([]string{mid}, true, true)
			}
		}
		if err != nil {
			return nil, err
		}
		if len(res.Chats) == 0 {
			return nil, fmt.Errorf("chat not found")
		}
		return lc.chatToChatInfo(&res.Chats[0], true), nil
	}

	contact := lc.getContact(ctx, string(portal.ID))
	var avatar *bridgev2.Avatar
	if contact.PicturePath != "" {
		avatar = &bridgev2.Avatar{
			ID: networkid.AvatarID(contact.PicturePath),
			Get: func(ctx context.Context) ([]byte, error) {
				return lc.GetAvatar(ctx, networkid.AvatarID(contact.PicturePath))
			},
		}
	}
	dmType := database.RoomTypeDM
	chatName := contact.EffectiveDisplayName()
	return &bridgev2.ChatInfo{
		Type:   &dmType,
		Name:   &chatName,
		Avatar: avatar,
		Members: &bridgev2.ChatMemberList{
			IsFull: true,
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
						Sender: networkid.UserID(portal.ID),
					},
					Membership: event.MembershipJoin,
					PowerLevel: ptr.Ptr(0),
				},
			},
		},
	}, nil
}

func (lc *LineClient) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	contact := lc.getContact(ctx, string(ghost.ID))
	var avatar *bridgev2.Avatar
	if contact.PicturePath != "" {
		avatar = &bridgev2.Avatar{
			ID: networkid.AvatarID(contact.PicturePath),
			Get: func(ctx context.Context) ([]byte, error) {
				return lc.GetAvatar(ctx, networkid.AvatarID(contact.PicturePath))
			},
		}
	}
	name := contact.EffectiveDisplayName()
	return &bridgev2.UserInfo{
		Identifiers: []string{string(ghost.ID)},
		Name:        &name,
		Avatar:      avatar,
	}, nil
}

func (lc *LineClient) getContact(ctx context.Context, mid string) line.Contact {
	lc.cacheMu.Lock()
	cached, ok := lc.contactCache[mid]
	lc.cacheMu.Unlock()
	if ok && time.Since(cached.cachedAt) < contactCacheTTL {
		return cached.Contact
	}

	// Use GetProfile for our own user data
	if mid == lc.Mid || mid == string(lc.UserLogin.ID) {
		client := line.NewClient(lc.AccessToken)
		profile, err := client.GetProfile()
		if err != nil && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
			if errRecover := lc.recoverToken(ctx); errRecover == nil {
				client = line.NewClient(lc.AccessToken)
				profile, err = client.GetProfile()
			}
		}
		if err == nil && profile != nil {
			contact := line.Contact{Mid: mid, DisplayName: profile.DisplayName, PicturePath: profile.PicturePath}
			lc.setCachedContact(mid, contact)
			return contact
		}
		return line.Contact{Mid: mid, DisplayName: mid}
	}

	client := line.NewClient(lc.AccessToken)
	res, err := client.GetContactsV2([]string{mid})
	if err != nil && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
		if errRecover := lc.recoverToken(ctx); errRecover == nil {
			client = line.NewClient(lc.AccessToken)
			res, err = client.GetContactsV2([]string{mid})
		}
	}
	if err == nil && res != nil && res.Contacts != nil {
		if wrapper, ok := res.Contacts[mid]; ok {
			lc.setCachedContact(mid, wrapper.Contact)
			return wrapper.Contact
		}
	}

	// Fall back to BuddyService for official/business accounts
	lc.UserLogin.Bridge.Log.Debug().Str("mid", mid).Msg("Contact not found via GetContactsV2, trying BuddyService")
	buddy, err := client.GetBuddyProfile(mid)
	if err != nil && (lc.isRefreshRequired(err) || lc.isLoggedOut(err)) {
		if errRecover := lc.recoverToken(ctx); errRecover == nil {
			client = line.NewClient(lc.AccessToken)
			buddy, err = client.GetBuddyProfile(mid)
		}
	}
	if err == nil && buddy != nil {
		lc.UserLogin.Bridge.Log.Debug().Str("mid", mid).Str("display_name", buddy.DisplayName).Str("picture_path", buddy.PicturePath).Msg("Got buddy profile")
		contact := line.Contact{Mid: mid, DisplayName: buddy.DisplayName, PicturePath: buddy.PicturePath}
		lc.setCachedContact(mid, contact)
		return contact
	}
	if err != nil {
		lc.UserLogin.Bridge.Log.Debug().Err(err).Str("mid", mid).Msg("BuddyService lookup also failed")
	}

	return line.Contact{Mid: mid, DisplayName: mid}
}

// setCachedContact stores a contact in the cache under lc.cacheMu.
func (lc *LineClient) setCachedContact(mid string, contact line.Contact) {
	lc.cacheMu.Lock()
	if lc.contactCache == nil {
		lc.contactCache = make(map[string]cachedContact)
	}
	lc.contactCache[mid] = cachedContact{Contact: contact, cachedAt: time.Now()}
	lc.cacheMu.Unlock()
}

func (lc *LineClient) ResolveIdentifier(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	userID := makeUserID(strings.TrimSpace(identifier))
	portalID := networkid.PortalKey{ID: makePortalID(string(userID)), Receiver: lc.UserLogin.ID}
	ghost, err := lc.UserLogin.Bridge.GetGhostByID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get ghost: %w", err)
	}
	portal, err := lc.UserLogin.Bridge.GetPortalByKey(ctx, portalID)
	if err != nil {
		return nil, fmt.Errorf("failed to get portal: %w", err)
	}
	ghostInfo, _ := lc.GetUserInfo(ctx, ghost)
	portalInfo, _ := lc.GetChatInfo(ctx, portal)
	return &bridgev2.ResolveIdentifierResponse{
		Ghost:    ghost,
		UserID:   userID,
		UserInfo: ghostInfo,
		Chat:     &bridgev2.CreateChatResponse{Portal: portal, PortalKey: portalID, PortalInfo: portalInfo},
	}, nil
}

func (lc *LineClient) GetAvatar(ctx context.Context, id networkid.AvatarID) ([]byte, error) {
	url := fmt.Sprintf("https://profile.line-scdn.net%s", id)
	resp, err := lc.HTTPClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
