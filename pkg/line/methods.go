package line

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	obsTokenMu     sync.Mutex
	obsTokenCache  string
	obsTokenExpiry time.Time
)

const obsTokenBuffer = 30 * time.Second

// InvalidateOBSTokenCache clears the cached OBS access token. The OBS token is
// derived from the main LINE access token; when the latter is rotated (refresh
// or re-login) any previously-issued OBS token is invalidated server-side, but
// the cache here would keep handing it out until its original TTL expires.
// Callers must invoke this after any successful re-authentication.
func InvalidateOBSTokenCache() {
	obsTokenMu.Lock()
	obsTokenCache = ""
	obsTokenExpiry = time.Time{}
	obsTokenMu.Unlock()
}

// LoginV2 performs the loginV2 RPC call to authenticate a user
func (c *Client) LoginV2(email, password, certificate, secret string) ([]byte, error) {
	return c.LoginV2WithType(2, email, password, certificate, secret)
}

func (c *Client) LoginV2WithType(loginType int, email, password, certificate, secret string) ([]byte, error) {
	req := LoginRequest{
		Type:             loginType,
		IdentityProvider: 1,
		Identifier:       email,
		Password:         password,
		KeepLoggedIn:     false,
		AccessLocation:   "",
		SystemName:       "Chrome",
		ModelName:        "",
		Certificate:      certificate,
		Verifier:         "",
		Secret:           secret, // PIN for Secret for type 2
		E2EEVersion:      1,
	}
	return c.callRPC("AuthService", "loginV2", req)
}

// LoginV2WithVerifier finalizes login using the verifier (post-E2EE confirm flow)
func (c *Client) LoginV2WithVerifier(verifier string) (*LoginResult, error) {
	req := LoginRequest{
		Type:             1,
		IdentityProvider: 1,
		Identifier:       "",
		Password:         "",
		KeepLoggedIn:     false,
		AccessLocation:   "",
		SystemName:       "Chrome",
		ModelName:        "",
		Certificate:      "",
		Verifier:         verifier,
		Secret:           "",
		E2EEVersion:      1,
	}

	respBytes, err := c.callRPC("AuthService", "loginV2", req)
	if err != nil {
		return nil, err
	}

	var wrapper struct {
		Code    int         `json:"code"`
		Message string      `json:"message"`
		Data    LoginResult `json:"data"`
	}
	if err := json.Unmarshal(respBytes, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse loginV2 (verifier) response: %w", err)
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("loginV2 with verifier failed: %s", wrapper.Message)
	}

	// Prefer the V3 token if present, otherwise fall back to legacy authToken
	if wrapper.Data.TokenV3IssueResult != nil && wrapper.Data.TokenV3IssueResult.AccessToken != "" {
		wrapper.Data.AuthToken = wrapper.Data.TokenV3IssueResult.AccessToken
		c.AccessToken = wrapper.Data.TokenV3IssueResult.AccessToken
	} else if wrapper.Data.AuthToken != "" {
		c.AccessToken = wrapper.Data.AuthToken
	}

	return &wrapper.Data, nil
}

// GetProfile fetches the user's profile information
func (c *Client) GetProfile() (*Profile, error) {
	resp, err := c.callRPC("TalkService", "getProfile", 2)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int     `json:"code"`
		Message string  `json:"message"`
		Data    Profile `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("getProfile failed: %s", wrapper.Message)
	}
	return &wrapper.Data, nil
}

// GetEncryptedIdentityV3 fetches wrapped nonce and KDF params used to derive storage key.
func (c *Client) GetEncryptedIdentityV3() (*EncryptedIdentityV3, error) {
	resp, err := c.callRPC("TalkService", "getEncryptedIdentityV3")
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int                 `json:"code"`
		Message string              `json:"message"`
		Data    EncryptedIdentityV3 `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, err
	}
	return &wrapper.Data, nil
}

func (c *Client) GetE2EEGroupSharedKey(chatMid string, groupKeyID int) (*E2EEGroupSharedKey, error) {
	// args: [1, chatMid, groupKeyID]
	resp, err := c.callRPC("TalkService", "getE2EEGroupSharedKey", 1, chatMid, groupKeyID)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != 0 {
		return nil, parseE2EEGroupKeyError("getE2EEGroupSharedKey", wrapper.Message, wrapper.Data)
	}
	var data E2EEGroupSharedKey
	if err := json.Unmarshal(wrapper.Data, &data); err != nil {
		return nil, err
	}
	return &data, nil
}

func (c *Client) GetLastE2EEGroupSharedKey(chatMid string) (*E2EEGroupSharedKey, error) {
	resp, err := c.callRPC("TalkService", "getLastE2EEGroupSharedKey", 1, chatMid)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != 0 {
		return nil, parseE2EEGroupKeyError("getLastE2EEGroupSharedKey", wrapper.Message, wrapper.Data)
	}
	var data E2EEGroupSharedKey
	if err := json.Unmarshal(wrapper.Data, &data); err != nil {
		return nil, err
	}
	return &data, nil
}

// NegotiateE2EEPublicKey fetches (or renews) the public key of the person you're talking to (E2EE).
func (c *Client) NegotiateE2EEPublicKey(mid string) (*E2EEPublicKey, error) {
	resp, err := c.callRPC("TalkService", "negotiateE2EEPublicKey", mid)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("negotiateE2EEPublicKey failed: %s", wrapper.Message)
	}
	return parseE2EEPublicKey(wrapper.Data)
}

func parseE2EEPublicKey(rawData []byte) (*E2EEPublicKey, error) {
	var data map[string]any
	if err := json.Unmarshal(rawData, &data); err != nil {
		return nil, err
	}

	var findString func(any) string
	findString = func(v any) string {
		switch t := v.(type) {
		case string:
			return t
		case map[string]any:
			for _, val := range t {
				if s := findString(val); s != "" {
					return s
				}
			}
		case []any:
			for _, val := range t {
				if s := findString(val); s != "" {
					return s
				}
			}
		}
		return ""
	}

	var findInt64 func(any) int64
	findInt64 = func(v any) int64 {
		switch t := v.(type) {
		case json.Number:
			if n, err := t.Int64(); err == nil {
				return n
			}
		case float64:
			return int64(t)
		case int64:
			return t
		case int:
			return int64(t)
		case string:
			if t == "" {
				return 0
			}
			if n, err := strconv.ParseInt(t, 10, 64); err == nil {
				return n
			}
		case map[string]any:
			for _, val := range t {
				if n := findInt64(val); n != 0 {
					return n
				}
			}
		case []any:
			for _, val := range t {
				if n := findInt64(val); n != 0 {
					return n
				}
			}
		}
		return 0
	}

	var findBool func(any) bool
	findBool = func(v any) bool {
		switch t := v.(type) {
		case bool:
			return t
		case string:
			b, err := strconv.ParseBool(t)
			return err == nil && b
		case map[string]any:
			for _, val := range t {
				if b := findBool(val); b {
					return true
				}
			}
		case []any:
			for _, val := range t {
				if b := findBool(val); b {
					return true
				}
			}
		}
		return false
	}

	pub := ""
	keyID := int64(0)
	if pk, ok := data["publicKey"].(map[string]any); ok {
		// Try keyData as a direct string first (most common LINE response shape).
		switch kd := pk["keyData"].(type) {
		case string:
			pub = kd
		case map[string]any:
			// Nested object — check known field names deterministically before
			// falling back to the non-deterministic recursive findString.
			for _, name := range []string{"keyData", "publicKey", "key", "data", "value"} {
				if s, ok := kd[name].(string); ok && s != "" {
					pub = s
					break
				}
			}
			if pub == "" {
				pub = findString(kd)
			}
		default:
			// keyData is absent or an unexpected type; findString on the whole pk object.
			pub = findString(pk)
		}
		if keyID == 0 {
			keyID = findInt64(pk["keyId"])
		}
	}
	if pub == "" {
		if kd, ok := data["keyData"].(string); ok {
			pub = kd
		} else {
			pub = findString(data["publicKey"])
		}
	}
	if keyID == 0 {
		keyID = findInt64(data["keyId"])
	}
	if keyID == 0 {
		keyID = findInt64(data)
	}
	if pub == "" || keyID == 0 {
		return nil, fmt.Errorf("%w: missing fields (pub=%t keyID=%d raw=%s)", ErrNoUsableE2EEPublicKey, pub != "", keyID, string(rawData))
	}

	// Reject obviously invalid public keys that the recursive search may pick up.
	if _, err := base64.StdEncoding.DecodeString(pub); err != nil {
		if _, err2 := base64.URLEncoding.DecodeString(pub); err2 != nil {
			return nil, fmt.Errorf("%w: public key is not valid base64: %v", ErrNoUsableE2EEPublicKey, err)
		}
	}

	return &E2EEPublicKey{
		KeyID:        json.Number(strconv.FormatInt(keyID, 10)),
		PublicKey:    pub,
		E2EEVersion:  int(findInt64(data["e2eeVersion"])),
		Expired:      findBool(data["expired"]),
		CreatedTime:  json.Number(strconv.FormatInt(findInt64(data["createdTime"]), 10)),
		RenewalCount: int(findInt64(data["renewalCount"])),
	}, nil
}

func (c *Client) GetE2EEPublicKey(mid string, keyVersion, keyID int) (*E2EEPublicKey, error) {
	resp, err := c.callRPC("TalkService", "getE2EEPublicKey", mid, keyVersion, keyID)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("getE2EEPublicKey failed: %s", wrapper.Message)
	}

	return parseE2EEPublicKey(wrapper.Data)
}

func (c *Client) SendMessage(reqSeq int64, msg *Message) (*Message, error) {
	resp, err := c.callRPC("TalkService", "sendMessage", reqSeq, msg)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int      `json:"code"`
		Message string   `json:"message"`
		Data    *Message `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("sendMessage failed: %s", wrapper.Message)
	}
	return wrapper.Data, nil
}

// SendChatChecked sends a read receipt for a message in a chat
func (c *Client) SendChatChecked(chatMid, messageID string) error {
	_, err := c.callRPC("TalkService", "sendChatChecked", 0, chatMid, messageID)
	return err
}

// GetContactsV2 fetches contact details for a list of MIDs.
func (c *Client) GetContactsV2(mids []string) (*ContactsResponse, error) {
	req := GetContactsV2Request{TargetUserMids: mids}
	resp, err := c.callRPC("TalkService", "getContactsV2", req, 2)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int              `json:"code"`
		Message string           `json:"message"`
		Data    ContactsResponse `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("getContactsV2 failed: %s", wrapper.Message)
	}
	return &wrapper.Data, nil
}

// GetBuddyProfile fetches the profile of a LINE official/business account (buddy).
func (c *Client) GetBuddyProfile(mid string) (*BuddyProfile, error) {
	resp, err := c.callRPC("BuddyService", "getBuddyProfile", mid)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int          `json:"code"`
		Message string       `json:"message"`
		Data    BuddyProfile `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("getBuddyProfile failed: %s", wrapper.Message)
	}
	return &wrapper.Data, nil
}

// GetSticonOwnershipByMid fetches sticon ownership entries for a user.
// Tries multiple method name variants since the exact LINE API endpoint may vary.
func (c *Client) GetSticonOwnershipByMid(mid string) ([]SticonOwnership, error) {
	attempts := []struct {
		client  func(service, method string, args ...any) ([]byte, error)
		service string
		method  string
		args    []any
	}{
		{c.callRPC, "TalkService", "getSticonOwnershipByMid", []any{1, mid}},
		{c.callRPC, "TalkService", "getSticonOwnershipByMid", []any{mid}},
		{c.callShopRPC, "ShopService", "getSticonOwnershipByMid", []any{mid}},
		{c.callShopRPC, "ShopService", "getOwnedProductSummaries", []any{mid}},
	}

	for _, a := range attempts {
		resp, err := a.client(a.service, a.method, a.args...)
		if err != nil {
			continue
		}
		var wrapper struct {
			Code    int             `json:"code"`
			Message string          `json:"message"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(resp, &wrapper); err != nil {
			continue
		}
		if wrapper.Code != 0 {
			continue
		}

		var ownerships []SticonOwnership
		if err := json.Unmarshal(wrapper.Data, &ownerships); err != nil {
			var wrapped struct {
				Ownerships []SticonOwnership `json:"ownerships"`
			}
			if err2 := json.Unmarshal(wrapper.Data, &wrapped); err2 != nil {
				continue
			}
			ownerships = wrapped.Ownerships
		}
		if len(ownerships) > 0 {
			return ownerships, nil
		}
	}

	return nil, fmt.Errorf("getSticonOwnershipByMid: all method variants failed for mid=%s", mid)
}

// OwnedProductSummary represents a product owned by the user.
type OwnedProductSummary struct {
	ProductID string `json:"productId"`
	Type      int    `json:"type"`
}

// GetOwnedProductSummaries fetches owned product summaries for a user.
func (c *Client) GetOwnedProductSummaries(mid string) ([]OwnedProductSummary, error) {
	resp, err := c.callShopRPC("ShopService", "getOwnedProductSummaries", mid)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse getOwnedProductSummaries response: %w", err)
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("getOwnedProductSummaries failed: %s", wrapper.Message)
	}
	var products []OwnedProductSummary
	if err := json.Unmarshal(wrapper.Data, &products); err != nil {
		return nil, fmt.Errorf("failed to unmarshal product summaries: %w", err)
	}
	return products, nil
}

func (c *Client) GetAllChatMids(withMemberChats, withInvitedChats bool) (*GetAllChatMidsResponse, error) {
	req := GetAllChatMidsRequest{
		WithMemberChats:  withMemberChats,
		WithInvitedChats: withInvitedChats,
	}
	resp, err := c.callRPC("TalkService", "getAllChatMids", req, 2)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int                    `json:"code"`
		Message string                 `json:"message"`
		Data    GetAllChatMidsResponse `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("getAllChatMids failed: %s", wrapper.Message)
	}
	return &wrapper.Data, nil
}

func (c *Client) GetChats(mids []string, withMembers, withInvitees bool) (*GetChatsResponse, error) {
	req := GetChatsRequest{
		ChatMids:     mids,
		WithMembers:  withMembers,
		WithInvitees: withInvitees,
	}
	resp, err := c.callRPC("TalkService", "getChats", req, 2)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int              `json:"code"`
		Message string           `json:"message"`
		Data    GetChatsResponse `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("getChats failed: %s", wrapper.Message)
	}
	return &wrapper.Data, nil
}

func (c *Client) GetLastOpRevision() (int64, error) {
	resp, err := c.callRPC("TalkService", "getLastOpRevision")
	if err != nil {
		return 0, err
	}
	var wrapper struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    string `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return 0, err
	}
	if wrapper.Code != 0 {
		return 0, fmt.Errorf("getLastOpRevision failed: %s", wrapper.Message)
	}
	if wrapper.Data == "" {
		return 0, fmt.Errorf("getLastOpRevision returned empty data")
	}
	rev, err := strconv.ParseInt(wrapper.Data, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("getLastOpRevision invalid data: %w", err)
	}
	return rev, nil
}

// this token is used to encrypt images, videos, and files uploaded to LINE's OBS storage
func (c *Client) AcquireEncryptedAccessToken() (string, error) {
	obsTokenMu.Lock()
	if obsTokenCache != "" && time.Now().Before(obsTokenExpiry) {
		cached := obsTokenCache
		obsTokenMu.Unlock()
		return cached, nil
	}
	obsTokenMu.Unlock()

	// 2 = FeatureType::OBS_Authorization.
	resp, err := c.callRPC("TalkService", "acquireEncryptedAccessToken", 2)
	if err != nil {
		return "", err
	}

	var wrapper struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    string `json:"data"` // Format: "expirySeconds\x1eToken"
	}

	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return "", fmt.Errorf("failed to decode acquireEncryptedAccessToken response: %w", err)
	}

	if wrapper.Code != 0 {
		return "", fmt.Errorf("acquireEncryptedAccessToken API error: %s", wrapper.Message)
	}

	parts := strings.Split(wrapper.Data, "\x1e")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid encrypted token format: missing separator")
	}

	token := parts[1]
	if expirySec, err := strconv.Atoi(parts[0]); err == nil && expirySec > 0 {
		obsTokenMu.Lock()
		obsTokenCache = token
		obsTokenExpiry = time.Now().Add(time.Duration(expirySec)*time.Second - obsTokenBuffer)
		obsTokenMu.Unlock()
	}

	return token, nil
}

func (c *Client) GetMessageBoxes(options MessageBoxesOptions) (*MessageBoxesResponse, error) {
	resp, err := c.callRPC("TalkService", "getMessageBoxes", options, 2)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int                  `json:"code"`
		Message string               `json:"message"`
		Data    MessageBoxesResponse `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("getMessageBoxes failed: %s", wrapper.Message)
	}
	return &wrapper.Data, nil
}

func (c *Client) GetRecentMessagesV2(chatMid string, limit int) ([]*Message, error) {
	resp, err := c.callRPC("TalkService", "getRecentMessagesV2", chatMid, limit)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int        `json:"code"`
		Message string     `json:"message"`
		Data    []*Message `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("getRecentMessagesV2 failed: %s", wrapper.Message)
	}
	return wrapper.Data, nil
}

func (c *Client) UnsendMessage(reqSeq int64, messageID string) error {
	_, err := c.callRPC("TalkService", "unsendMessage", reqSeq, messageID)
	return err
}

func (c *Client) SendChatRemoved(reqSeq int64, chatMid, lastReadMessageId string, lastReadMessageTime int64) error {
	_, err := c.callRPC("TalkService", "sendChatRemoved", reqSeq, chatMid, lastReadMessageId, lastReadMessageTime)
	return err
}

// CreateChat creates a new LINE group chat with the given members and name.
// The returned Chat will have a ChatMid starting with "c" (group) or "r" (room).
func (c *Client) CreateChat(mids []string, name string, chatType int) (*Chat, error) {
	req := CreateChatRequest{
		ReqSeq:         int(time.Now().UnixMilli() % 1_000_000_000),
		Type:           chatType,
		Name:           name,
		TargetUserMids: mids,
	}
	resp, err := c.callRPC("TalkService", "createChat", req)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int                 `json:"code"`
		Message string              `json:"message"`
		Data    CreateChatResponse2 `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse createChat response: %w", err)
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("createChat failed: %s", wrapper.Message)
	}
	return &wrapper.Data.Chat, nil
}

// GetLastE2EEPublicKeys fetches the latest E2EE public keys for all members of a chat.
// Returns a map of member MID → {keyId, keyData}.
func (c *Client) GetLastE2EEPublicKeys(req GetLastE2EEPublicKeysRequest) (map[string]E2EEPeerPublicKey, error) {
	resp, err := c.callRPC("TalkService", "getLastE2EEPublicKeys", req)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int                          `json:"code"`
		Message string                       `json:"message"`
		Data    map[string]E2EEPeerPublicKey `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse getLastE2EEPublicKeys response: %w", err)
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("getLastE2EEPublicKeys failed: %s", wrapper.Message)
	}
	return wrapper.Data, nil
}

// RegisterE2EEGroupKey registers a shared E2EE group key with the server.
// This must be called after creating a group so that members can decrypt group messages.
// The LINE API expects 5 positional arguments: keyVersion, chatMid, members, keyIds, encryptedSharedKeys.
func (c *Client) RegisterE2EEGroupKey(keyVersion int, chatMid string, members []string, keyIds []int, encryptedSharedKeys []string) error {
	resp, err := c.callRPC("TalkService", "registerE2EEGroupKey", keyVersion, chatMid, members, keyIds, encryptedSharedKeys)
	if err != nil {
		return err
	}
	var wrapper struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return fmt.Errorf("failed to parse registerE2EEGroupKey response: %w", err)
	}
	if wrapper.Code != 0 {
		return fmt.Errorf("registerE2EEGroupKey failed: %s", wrapper.Message)
	}
	return nil
}

// InviteIntoChat invites users into an existing LINE group chat.
func (c *Client) InviteIntoChat(chatMid string, mids []string) error {
	_, err := c.callRPC("TalkService", "inviteIntoChat", 1, chatMid, mids)
	return err
}

// FindContactByUserid looks up a LINE user by their user ID (not MID).
func (c *Client) FindContactByUserid(userid string) (*Contact, error) {
	resp, err := c.callRPC("TalkService", "findContactByUserid", userid)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int     `json:"code"`
		Message string  `json:"message"`
		Data    Contact `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse findContactByUserid response: %w", err)
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("findContactByUserid failed: %s", wrapper.Message)
	}
	return &wrapper.Data, nil
}

// GetAllContactIds returns the MIDs of all contacts.
func (c *Client) GetAllContactIds() ([]string, error) {
	resp, err := c.callRPC("TalkService", "getAllContactIds")
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int      `json:"code"`
		Message string   `json:"message"`
		Data    []string `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse getAllContactIds response: %w", err)
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("getAllContactIds failed: %s", wrapper.Message)
	}
	return wrapper.Data, nil
}

// GetBlockedContactIds returns the MIDs of all contacts blocked by the user.
func (c *Client) GetBlockedContactIds() ([]string, error) {
	resp, err := c.callRPC("TalkService", "getBlockedContactIds")
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int      `json:"code"`
		Message string   `json:"message"`
		Data    []string `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse getBlockedContactIds response: %w", err)
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("getBlockedContactIds failed: %s", wrapper.Message)
	}
	return wrapper.Data, nil
}

// DetermineMediaMessageFlow asks the server which upload path to use for media
// in a given chat. Flow value 2 = E2EE encrypted upload, 1 = plain upload.
func (c *Client) DetermineMediaMessageFlow(chatMid string) (*MediaMessageFlowResponse, error) {
	req := map[string]string{"chatMid": chatMid}
	resp, err := c.callRPC("TalkService", "determineMediaMessageFlow", req)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code    int                      `json:"code"`
		Message string                   `json:"message"`
		Data    MediaMessageFlowResponse `json:"data"`
	}
	if err := json.Unmarshal(resp, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse determineMediaMessageFlow response: %w", err)
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("determineMediaMessageFlow failed: %s", wrapper.Message)
	}
	return &wrapper.Data, nil
}
