package line

import "encoding/json"

type FlexibleMidMap map[string]bool

func (f *FlexibleMidMap) UnmarshalJSON(data []byte) error {
	var m map[string]bool
	if err := json.Unmarshal(data, &m); err == nil {
		*f = m
		return nil
	}

	var arr []string
	if err := json.Unmarshal(data, &arr); err == nil {
		*f = make(map[string]bool, len(arr))
		for _, mid := range arr {
			(*f)[mid] = true
		}
		return nil
	}

	return nil
}

type LoginRequest struct {
	Type             int    `json:"type"` // 0=Email/Password, 1=QRCode, 2=Secret
	IdentityProvider int    `json:"identityProvider"`
	Identifier       string `json:"identifier"` // Identifier from RSA
	Password         string `json:"password"`   // \sessionkeysize{sessionKey}+\emailsize{email}+\passwordsize{password}
	KeepLoggedIn     bool   `json:"keepLoggedIn"`
	AccessLocation   string `json:"accessLocation"`
	SystemName       string `json:"systemName"`
	ModelName        string `json:"modelName"`
	Certificate      string `json:"certificate"` // When device has been verified, Type=0
	Verifier         string `json:"verifier"`
	Secret           string `json:"secret"` // PIN for Type=2 login
	E2EEVersion      int    `json:"e2eeVersion"`
}

type Profile struct {
	Mid                          string            `json:"mid"`
	UserID                       string            `json:"userid"`
	RegionCode                   string            `json:"regionCode"`
	DisplayName                  string            `json:"displayName"`
	StatusMessage                string            `json:"statusMessage"`
	AllowSearchByUserID          bool              `json:"allowSearchByUserid"`
	AllowSearchByEmail           bool              `json:"allowSearchByEmail"`
	PicturePath                  string            `json:"picturePath"`
	StatusMessageContentMetadata map[string]string `json:"statusMessageContentMetadata"`
	NFTProfile                   bool              `json:"nftProfile"`
	ProfileID                    string            `json:"profileId"`
	ProfileType                  int               `json:"profileType"`
	CreatedTimeMillis            string            `json:"createdTimeMillis"`
}

type Operation struct {
	Revision    json.Number `json:"revision"`
	Type        int         `json:"type"` // 25=Send, 26=Receive
	ReqSeq      int         `json:"reqSeq"`
	Message     *Message    `json:"message,omitempty"`
	Param1      string      `json:"param1,omitempty"`
	Param2      string      `json:"param2,omitempty"`
	Param3      string      `json:"param3,omitempty"`
	CreatedTime json.Number `json:"createdTime"`
}

type MessageLocation struct {
	Title     string  `json:"title"`
	Address   string  `json:"address"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type Message struct {
	ID              string            `json:"id"`
	From            string            `json:"from"`
	To              string            `json:"to"`
	ToType          int               `json:"toType"`
	SessionID       int               `json:"sessionId,omitempty"`
	CreatedTime     json.Number       `json:"createdTime"`
	ContentType     int               `json:"contentType"`
	HasContent      bool              `json:"hasContent,omitempty"`
	ContentMetadata map[string]string `json:"contentMetadata"`

	Text     string           `json:"text,omitempty"`
	Location *MessageLocation `json:"location,omitempty"`

	Chunks []string `json:"chunks,omitempty"`

	RelatedMessageID          string `json:"relatedMessageId,omitempty"`
	MessageRelationType       int    `json:"messageRelationType,omitempty"`
	RelatedMessageServiceCode int    `json:"relatedMessageServiceCode,omitempty"`
}

// E2EEPublicKey represents the peer key returned by negotiateE2EEPublicKey
type E2EEPublicKey struct {
	KeyID        json.Number `json:"keyId"` // raw key id used in sender/receiver chunks
	PublicKey    string      `json:"publicKey"`
	E2EEVersion  int         `json:"e2eeVersion"`
	Expired      bool        `json:"expired"`
	CreatedTime  json.Number `json:"createdTime"`
	RenewalCount int         `json:"renewalCount"`
}

type RSAKeyInfo struct {
	KeyName    string `json:"keynm"`
	NValue     string `json:"nvalue"`
	EValue     string `json:"evalue"`
	SessionKey string `json:"sessionKey"`
}

type LoginResult struct {
	AuthToken           string              `json:"authToken"`
	Certificate         string              `json:"certificate"`
	Verifier            string              `json:"verifier"`
	PinCode             string              `json:"pinCode"`
	Pin                 string              `json:"-"` // Generated locally
	Type                int                 `json:"type"`
	Param               string              `json:"param"` // Sometimes used
	Message             string              `json:"message"`
	TokenV3IssueResult  *TokenV3IssueResult `json:"tokenV3IssueResult,omitempty"`
	Mid                 string              `json:"mid,omitempty"`
	LastPrimaryBindTime string              `json:"lastPrimaryBindTime,omitempty"`
	EncryptedKeyChain   string              `json:"encryptedKeyChain,omitempty"`
	E2EEPublicKey       string              `json:"publicKey,omitempty"`
	E2EEVersion         string              `json:"e2eeVersion,omitempty"`
	E2EEKeyID           string              `json:"keyId,omitempty"`
	NoE2EE              bool                `json:"-"` // True when login fell back to non-E2EE (LSOFF)
}

type TokenV3IssueResult struct {
	AccessToken             string                 `json:"accessToken"`
	RefreshToken            string                 `json:"refreshToken"`
	DurationUntilRefreshSec string                 `json:"durationUntilRefreshInSec"`
	RefreshApiRetryPolicy   *RefreshApiRetryPolicy `json:"refreshApiRetryPolicy,omitempty"`
	LoginSessionID          string                 `json:"loginSessionId"`
	TokenIssueTimeEpochSec  string                 `json:"tokenIssueTimeEpochSec"`
}

type QRCodeResponse struct {
	CallbackURL                string `json:"callbackUrl"`
	LongPollingMaxCount        int    `json:"longPollingMaxCount"`
	LongPollingIntervalSeconds int    `json:"longPollingIntervalSec"`
}

type RefreshApiRetryPolicy struct {
	InitialDelayInMillis string  `json:"initialDelayInMillis"`
	MaxDelayInMillis     string  `json:"maxDelayInMillis"`
	Multiplier           float64 `json:"multiplier"`
	JitterRate           float64 `json:"jitterRate"`
}

type LoginPollingResult struct {
	Result struct {
		Metadata struct {
			ErrorCode         string `json:"errorCode"`
			EncryptedKeyChain string `json:"encryptedKeyChain"`
			E2EEVersion       string `json:"e2eeVersion"`
			KeyID             string `json:"keyId"`
			PublicKey         string `json:"publicKey"`
			AuthToken         string `json:"authToken"`   // Sometimes here
			Certificate       string `json:"certificate"` // Sometimes here
			PinCode           string `json:"pinCode"`
		} `json:"metadata"`
	} `json:"result"`
	Timestamp string `json:"timestamp"`
	AuthPhase string `json:"authPhase"`
}

type EncryptedIdentityV3 struct {
	WrappedNonce  string `json:"wrappedNonce"`
	KDFParameter1 string `json:"kdfParameter1"`
	KDFParameter2 string `json:"kdfParameter2"`
}

type GetContactsV2Request struct {
	TargetUserMids []string `json:"targetUserMids"`
}

type ContactsResponse struct {
	Contacts map[string]ContactWrapper `json:"contacts"`
}

type ContactWrapper struct {
	Contact Contact `json:"contact"`
}

type Contact struct {
	Mid                   string `json:"mid"`
	DisplayName           string `json:"displayName"`
	DisplayNameOverridden string `json:"displayNameOverridden"`
	StatusMessage         string `json:"statusMessage"`
	PicturePath           string `json:"picturePath"`
}

// EffectiveDisplayName returns the custom name if set, otherwise the original display name.
func (c Contact) EffectiveDisplayName() string {
	if c.DisplayNameOverridden != "" {
		return c.DisplayNameOverridden
	}
	return c.DisplayName
}

type BuddyProfile struct {
	Mid         string `json:"buddyMid"`
	DisplayName string `json:"displayName"`
	PicturePath string `json:"picturePath"`
}

type E2EEGroupSharedKey struct {
	KeyVersion         int    `json:"keyVersion"`
	GroupKeyID         int    `json:"groupKeyId"`
	Creator            string `json:"creator"`
	CreatorKeyID       int    `json:"creatorKeyId"`
	Receiver           string `json:"receiver"`
	ReceiverKeyID      int    `json:"receiverKeyId"`
	EncryptedSharedKey string `json:"encryptedSharedKey"`
	AllowedTypes       []int  `json:"allowedTypes"`
	SpecVersion        int    `json:"specVersion"`
}

type RefreshAccessTokenRequest struct {
	RefreshToken string `json:"refreshToken"`
	RetryCount   int    `json:"retryCount"`
}

type GetAllChatMidsRequest struct {
	WithMemberChats  bool `json:"withMemberChats"`
	WithInvitedChats bool `json:"withInvitedChats"`
}

type GetAllChatMidsResponse struct {
	MemberChatMids  []string `json:"memberChatMids"`
	InvitedChatMids []string `json:"invitedChatMids"`
}

type GetChatsRequest struct {
	ChatMids     []string `json:"chatMids"`
	WithMembers  bool     `json:"withMembers"`
	WithInvitees bool     `json:"withInvitees"`
}

type GetChatsResponse struct {
	Chats []Chat `json:"chats"`
}

type Chat struct {
	ChatMid              string      `json:"chatMid"`
	CreatedTime          json.Number `json:"createdTime"`
	NotificationDisabled bool        `json:"notificationDisabled"`
	FavoriteTimestamp    json.Number `json:"favoriteTimestamp"`
	ChatName             string      `json:"chatName"`
	PicturePath          string      `json:"picturePath"`
	Extra                ChatExtra   `json:"extra"`
	Type                 int         `json:"type"` // 0=GROUP, 1=ROOM
}

type ChatExtra struct {
	GroupExtra *GroupExtra `json:"groupExtra,omitempty"`
	PeerExtra  *PeerExtra  `json:"peerExtra,omitempty"`
}

type GroupExtra struct {
	CreatorMid       string         `json:"creatorMid"`
	PreventedMids    FlexibleMidMap `json:"preventedMids"`
	InvitationTicket string         `json:"invitationTicket"`
	MemberMids       FlexibleMidMap `json:"memberMids"`
	InviteeMids      FlexibleMidMap `json:"inviteeMids"`
}

type PeerExtra struct {
}

type AcquireEncryptedAccessTokenResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data"` // Format: "expirySeconds\x1eToken"
}

type MessageBoxesOptions struct {
	ActiveOnly                     bool `json:"activeOnly"`
	UnreadOnly                     bool `json:"unreadOnly"`
	MessageBoxCountLimit           int  `json:"messageBoxCountLimit"`
	WithUnreadCount                bool `json:"withUnreadCount"`
	LastMessagesPerMessageBoxCount int  `json:"lastMessagesPerMessageBoxCount"`
}

type MessageBoxesResponse struct {
	MessageBoxes []MessageBox `json:"messageBoxes"`
	HasNext      bool         `json:"hasNext"`
}

type MessageBox struct {
	ID                     string            `json:"id"`
	MidType                int               `json:"midType"`
	LastDeliveredMessageID *MessageIDWrapper `json:"lastDeliveredMessageId"`
	LastSeenMessageID      string            `json:"lastSeenMessageId"`
	UnreadCount            json.Number       `json:"unreadCount"`
	LastMessages           []Message         `json:"lastMessages"`
}

type MessageIDWrapper struct {
	DeliveredTime json.Number `json:"deliveredTime"`
	MessageID     string      `json:"messageId"`
}

type PageInfoResponse struct {
	Result  PageInfoResult `json:"result"`
	Code    int            `json:"code"`
	Message string         `json:"message"`
}

type PageInfoResult struct {
	URL     string  `json:"url"`
	Domain  string  `json:"domain"`
	Title   string  `json:"title"`
	Summary string  `json:"summary"`
	Image   string  `json:"image"`
	Obs     ObsInfo `json:"obs"`
}

type ObsInfo struct {
	CDN string `json:"cdn"`
}

type MediaMessageFlowResponse struct {
	FlowMap        map[string]int `json:"flowMap"`
	CacheTTLMillis string         `json:"cacheTtlMillis"`
}
