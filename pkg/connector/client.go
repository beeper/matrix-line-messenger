package connector

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/highesttt/matrix-line-messenger/pkg/e2ee"
	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

type LineClient struct {
	UserLogin    *bridgev2.UserLogin
	AccessToken  string
	RefreshToken string
	Mid          string
	HTTPClient   *http.Client
	E2EE         *e2ee.Manager
	peerKeys     map[string]peerKeyInfo

	reqSeqMu    sync.Mutex
	sentReqSeqs map[int]time.Time

	// cacheMu protects peerKeys, blockedUsers, contactCache, mediaFlowCache,
	// noE2EEGroups, groupMemberCache, and generatedGroupNameCache.
	// Hold it only around map accesses; never across network calls.
	cacheMu                 sync.Mutex
	blockedUsers            map[string]bool      // mid -> true if the user has blocked this contact in LINE
	noE2EEGroups            map[string]time.Time // chatMid -> when group E2EE failure was cached
	contactCache            map[string]cachedContact
	mediaFlowCache          map[string]cachedMediaFlow
	groupMemberCache        map[string][]string // chatMid -> list of member MIDs from CreateGroup or getChatMemberMIDs
	generatedGroupNameCache map[string]bool     // chatMid -> true when Matrix name should be generated from member names
	reactionIconMXC         map[int]string      // predefinedReactionType -> cached MXC URI
	recentReactions         sync.Map            // "msgID\x00emoji" -> struct{} to dedup concurrent 139/140 events

	wg sync.WaitGroup
}

type cachedMediaFlow struct {
	flowMap  map[string]int
	cachedAt time.Time
	ttl      time.Duration
}

type peerKeyInfo struct {
	raw       int
	pub       string
	noE2EE    bool      // true if peer has Letter Sealing off
	checkedAt time.Time // when noE2EE was last verified
}

const contactCacheTTL = 1 * time.Hour

type cachedContact struct {
	line.Contact
	cachedAt time.Time
}

const defaultMediaFlowTTL = 6 * time.Hour

// shouldUseE2EEMediaFlow checks whether the server wants E2EE upload (flow 2)
// for the given chat and content type. Returns true for E2EE, false for plain.
// Falls back to true (E2EE) if the server call fails, to preserve existing behavior.
func (lc *LineClient) shouldUseE2EEMediaFlow(chatMid string, contentType int) bool {
	lc.cacheMu.Lock()
	if lc.mediaFlowCache == nil {
		lc.mediaFlowCache = make(map[string]cachedMediaFlow)
	}
	if cached, ok := lc.mediaFlowCache[chatMid]; ok && time.Since(cached.cachedAt) < cached.ttl {
		lc.cacheMu.Unlock()
		flow, exists := cached.flowMap[strconv.Itoa(contentType)]
		if exists {
			return flow == 2
		}
		return true
	}
	lc.cacheMu.Unlock()

	client := line.NewClient(lc.AccessToken)
	resp, err := client.DetermineMediaMessageFlow(chatMid)
	if err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Str("chat_mid", chatMid).
			Msg("Failed to determine media flow, defaulting to E2EE upload")
		return true
	}

	ttl := defaultMediaFlowTTL
	if resp.CacheTTLMillis != "" {
		if parsed, err := strconv.ParseInt(resp.CacheTTLMillis, 10, 64); err == nil && parsed > 0 {
			ttl = time.Duration(parsed) * time.Millisecond
		}
	}

	lc.cacheMu.Lock()
	lc.mediaFlowCache[chatMid] = cachedMediaFlow{
		flowMap:  resp.FlowMap,
		cachedAt: time.Now(),
		ttl:      ttl,
	}
	lc.cacheMu.Unlock()

	flow, exists := resp.FlowMap[strconv.Itoa(contentType)]
	if exists {
		return flow == 2
	}
	return true
}

func (lc *LineClient) isUserBlocked(mid string) bool {
	lc.cacheMu.Lock()
	defer lc.cacheMu.Unlock()
	return lc.blockedUsers[mid]
}

var _ bridgev2.NetworkAPI = (*LineClient)(nil)
var _ bridgev2.NetworkAPIWithUserID = (*LineClient)(nil)
var _ bridgev2.ReadReceiptHandlingNetworkAPI = (*LineClient)(nil)
var _ bridgev2.ReactionHandlingNetworkAPI = (*LineClient)(nil)

func (lc *LineClient) refreshAndSave(ctx context.Context) error {
	if lc.RefreshToken == "" {
		return fmt.Errorf("no refresh token available")
	}

	client := line.NewClient(lc.AccessToken)
	res, err := client.RefreshAccessToken(lc.RefreshToken)
	if err != nil {
		return fmt.Errorf("failed to refresh token: %w", err)
	}

	lc.AccessToken = res.AccessToken
	if res.RefreshToken != "" {
		lc.RefreshToken = res.RefreshToken
	}

	meta := lc.UserLogin.Metadata.(*UserLoginMetadata)
	meta.AccessToken = lc.AccessToken
	meta.RefreshToken = lc.RefreshToken
	err = lc.UserLogin.Save(ctx)
	if err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to save refreshed tokens to DB")
	} else {
		lc.UserLogin.Bridge.Log.Info().Msg("Tokens refreshed and saved")
	}

	return nil
}

func (lc *LineClient) isRefreshRequired(err error) bool {
	return strings.Contains(err.Error(), "\"code\":119") || strings.Contains(err.Error(), "Access token refresh required")
}

func (lc *LineClient) isLoggedOut(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "V3_TOKEN_CLIENT_LOGGED_OUT")
}

// recoverToken attempts to restore a valid session by refreshing, then re-logging in.
// Returns nil on success. On failure the caller should send StateBadCredentials.
func (lc *LineClient) recoverToken(ctx context.Context) error {
	if err := lc.refreshAndSave(ctx); err == nil {
		lc.UserLogin.Bridge.Log.Info().Msg("Token recovered via refresh")
		return nil
	}
	lc.UserLogin.Bridge.Log.Info().Msg("Refresh failed, attempting re-login with stored credentials...")
	return lc.tryLogin(ctx)
}

func (lc *LineClient) Connect(ctx context.Context) {
	lc.cacheMu.Lock()
	if lc.peerKeys == nil {
		lc.peerKeys = make(map[string]peerKeyInfo)
	}
	if lc.blockedUsers == nil {
		lc.blockedUsers = make(map[string]bool)
	}
	if lc.contactCache == nil {
		lc.contactCache = make(map[string]cachedContact)
	}
	if lc.groupMemberCache == nil {
		lc.groupMemberCache = make(map[string][]string)
	}
	lc.cacheMu.Unlock()
	lc.reqSeqMu.Lock()
	if lc.sentReqSeqs == nil {
		lc.sentReqSeqs = make(map[int]time.Time)
	}
	lc.reqSeqMu.Unlock()

	if lc.Mid == "" {
		if meta, ok := lc.UserLogin.Metadata.(*UserLoginMetadata); ok {
			lc.Mid = meta.Mid
		}
	}
	if lc.AccessToken == "" {
		if err := lc.tryLogin(ctx); err != nil {
			lc.UserLogin.BridgeState.Send(status.BridgeState{
				StateEvent: status.StateBadCredentials,
				Error:      "line-login-failed",
				Message:    err.Error(),
			})
			return
		}
	}

	// Verify the token is still valid before proceeding
	if err := lc.ensureValidToken(ctx); err != nil {
		lc.UserLogin.BridgeState.Send(status.BridgeState{
			StateEvent: status.StateBadCredentials,
			Error:      "line-token-expired",
			Message:    fmt.Sprintf("session expired and could not be restored: %v", err),
		})
		return
	}

	lc.UserLogin.Bridge.Log.Info().Int("token_len", len(lc.AccessToken)).Msg("LINE client connected; notifying bridge")
	lc.UserLogin.BridgeState.Send(status.BridgeState{
		StateEvent: status.StateConnected,
	})

	// Initialize E2EE manager and load keys
	mgr, err := e2ee.NewManager()
	if err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to init E2EE manager")
	} else {
		lc.E2EE = mgr
		if meta, ok := lc.UserLogin.Metadata.(*UserLoginMetadata); ok && len(meta.ExportedKeyMap) > 0 {
			if err := mgr.LoadMyKeyFromExportedMap(meta.ExportedKeyMap); err != nil {
				lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to load E2EE key from DB metadata")
			} else {
				lc.UserLogin.Bridge.Log.Info().Int("exported_keys", len(meta.ExportedKeyMap)).Msg("Loaded E2EE key from DB metadata")
			}
		}

		// Storage key is optional for runtime decrypt/encrypt; try it for file support
		client := line.NewClient(lc.AccessToken)
		ei3, err := client.GetEncryptedIdentityV3()
		if err != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to fetch EncryptedIdentityV3")
		} else {
			if err := mgr.InitStorage(ei3.WrappedNonce, ei3.KDFParameter1, ei3.KDFParameter2); err != nil {
				lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to init storage key")
			} else if data, err := mgr.LoadSecureDataFromFile(string(lc.UserLogin.ID)); err == nil {
				if err := mgr.LoadMyKeyFromSecureData(data); err != nil {
					lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to load E2EE key from secure data")
				}
			}
		}
	}

	// Fetch initial blocked contacts list before starting sync loops.
	blockedMIDs, err := func() ([]string, error) {
		client := line.NewClient(lc.AccessToken)
		return client.GetBlockedContactIds()
	}()
	if err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to fetch blocked contacts, continuing without block list")
	} else {
		lc.cacheMu.Lock()
		for _, mid := range blockedMIDs {
			lc.blockedUsers[mid] = true
		}
		lc.cacheMu.Unlock()
		lc.UserLogin.Bridge.Log.Info().Int("count", len(blockedMIDs)).Msg("Fetched blocked contacts")
	}

	lc.wg.Add(4)
	go lc.syncChats(ctx)
	go lc.syncDMChats(ctx)
	go lc.prefetchMessages(ctx)
	go lc.pollLoop(ctx)
}

func (lc *LineClient) tryLogin(ctx context.Context) error {
	var email, password, certificate string
	if meta, ok := lc.UserLogin.Metadata.(*UserLoginMetadata); ok {
		email = meta.Email
		password = meta.Password
		certificate = meta.Certificate
	}

	if email == "" || password == "" {
		return fmt.Errorf("no stored credentials available for re-login")
	}

	lc.UserLogin.Bridge.Log.Info().
		Str("email", email).
		Bool("has_certificate", certificate != "").
		Msg("Attempting to login with email/password...")
	client := line.NewClient("")
	res, err := client.Login(email, password, certificate)
	if err != nil {
		return fmt.Errorf("login failed: %w", err)
	}
	if res.AuthToken == "" {
		pin := res.Pin
		if res.PinCode != "" {
			pin = res.PinCode
		}
		if pin != "" {
			lc.UserLogin.Bridge.Log.Warn().Msg("PIN verification required — check your LINE mobile app to complete re-login")
			// Send the PIN via bridge state so the user sees it in their Matrix client
			lc.UserLogin.BridgeState.Send(status.BridgeState{
				StateEvent: status.StateConnecting,
				Error:      "line-pin-required",
				Message:    fmt.Sprintf("Enter this PIN on your LINE mobile app: %s", pin),
			})
		}
		if res.Verifier == "" {
			return fmt.Errorf("login requires interaction but no verifier returned")
		}

		lc.UserLogin.Bridge.Log.Info().Msg("Waiting for PIN verification on mobile device...")
		waitClient := line.NewClient("")
		waitRes, err := waitClient.WaitForLogin(res.Verifier, res.NoE2EE)
		if err != nil {
			return fmt.Errorf("PIN verification failed: %w", err)
		}
		if waitRes.AuthToken == "" {
			return fmt.Errorf("PIN verification completed but no auth token received")
		}
		// Replace res with the verified result
		res = waitRes
		client = waitClient
	}
	lc.AccessToken = client.AccessToken
	if res.TokenV3IssueResult != nil {
		if res.TokenV3IssueResult.AccessToken != "" {
			lc.AccessToken = res.TokenV3IssueResult.AccessToken
		}
		if res.TokenV3IssueResult.RefreshToken != "" {
			lc.RefreshToken = res.TokenV3IssueResult.RefreshToken
		}
	}
	if res.Mid != "" {
		lc.Mid = res.Mid
		if meta, ok := lc.UserLogin.Metadata.(*UserLoginMetadata); ok {
			meta.Mid = res.Mid
		}
	}

	// Save the new tokens and updated certificate to metadata
	if meta, ok := lc.UserLogin.Metadata.(*UserLoginMetadata); ok {
		meta.AccessToken = lc.AccessToken
		meta.RefreshToken = lc.RefreshToken
		if res.Certificate != "" {
			meta.Certificate = res.Certificate
		}
		if err := lc.UserLogin.Save(ctx); err != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to save new tokens to DB")
		}
	}

	lc.UserLogin.Bridge.Log.Info().Msg("Login successful!")
	return nil
}

func (lc *LineClient) ensureValidToken(ctx context.Context) error {
	client := line.NewClient(lc.AccessToken)
	_, err := client.GetProfile()
	if err == nil {
		return nil
	}

	if lc.isLoggedOut(err) {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("Session invalidated (logged out by another client), attempting recovery...")
		return lc.recoverToken(ctx)
	}

	if !lc.isRefreshRequired(err) {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("GetProfile failed with non-auth error, continuing anyway")
		return nil
	}

	lc.UserLogin.Bridge.Log.Info().Msg("Access token expired, attempting refresh...")
	if errRefresh := lc.refreshAndSave(ctx); errRefresh == nil {
		lc.UserLogin.Bridge.Log.Info().Msg("Token refreshed successfully")
		return nil
	} else {
		lc.UserLogin.Bridge.Log.Warn().Err(errRefresh).Msg("Token refresh failed")
	}

	lc.UserLogin.Bridge.Log.Info().Msg("Attempting re-login with stored credentials...")
	return lc.tryLogin(ctx)
}

func (lc *LineClient) Disconnect() {
	lc.wg.Wait()
}

func (lc *LineClient) IsLoggedIn() bool { return lc.AccessToken != "" }

func (lc *LineClient) GetUserID() networkid.UserID {
	return makeUserID(lc.Mid)
}

func (lc *LineClient) LogoutRemote(ctx context.Context) {}

func (lc *LineClient) PreHandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (bridgev2.MatrixReactionPreResponse, error) {
	return bridgev2.MatrixReactionPreResponse{}, bridgev2.ErrReactionsNotSupported
}

func (lc *LineClient) HandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*database.Reaction, error) {
	return nil, bridgev2.ErrReactionsNotSupported
}

func (lc *LineClient) HandleMatrixReactionRemove(ctx context.Context, msg *bridgev2.MatrixReactionRemove) error {
	return nil
}

func (lc *LineClient) midOrFallback() string {
	if lc.Mid != "" {
		return lc.Mid
	}
	return string(lc.UserLogin.ID)
}

func makeUserID(userID string) networkid.UserID { return networkid.UserID(userID) }

func makePortalID(userID string) networkid.PortalID { return networkid.PortalID(userID) }

func guessToType(mid string) ToType {
	if strings.HasPrefix(strings.ToLower(mid), "c") {
		return ToGroup
	}
	if strings.HasPrefix(strings.ToLower(mid), "r") {
		return ToRoom
	}
	return ToUser
}
