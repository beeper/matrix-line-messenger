package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.mau.fi/util/configupgrade"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/highesttt/matrix-line-messenger/pkg/e2ee"
	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

type LineConnector struct {
	br *bridgev2.Bridge
}

var _ bridgev2.NetworkConnector = (*LineConnector)(nil)

func (lc *LineConnector) Init(bridge *bridgev2.Bridge) {
	lc.br = bridge
}

func (lc *LineConnector) Start(ctx context.Context) error {
	_, ok := lc.br.Matrix.(bridgev2.MatrixConnectorWithServer)
	if !ok {
		return fmt.Errorf("matrix connector does not implement MatrixConnectorWithServer")
	}
	return nil
}

func (lc *LineConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 1
}

func (lc *LineConnector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{
		AggressiveUpdateInfo: true,
		Provisioning: bridgev2.ProvisioningCapabilities{
			ImagePackImport: false,
			ResolveIdentifier: bridgev2.ResolveIdentifierCapabilities{
				Search:      true,
				ContactList: true,
				CreateDM:    true,
			},
			GroupCreation: map[string]bridgev2.GroupTypeCapabilities{
				"group": {
					TypeDescription: "LINE Group",
					Name: bridgev2.GroupFieldCapability{
						Allowed:   true,
						MinLength: 1,
						MaxLength: 50,
					},
					Participants: bridgev2.GroupFieldCapability{
						Allowed:   true,
						MinLength: 1,
						MaxLength: 100,
					},
				},
			},
		},
	}
}

func (lc *LineConnector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{
		DisplayName:      "LINE",
		NetworkURL:       "https://line.me",
		NetworkIcon:      "",
		NetworkID:        "line",
		BeeperBridgeType: "github.com/highesttt/matrix-line-messenger",
		DefaultPort:      29322,
	}
}

func (lc *LineConnector) GetConfig() (example string, data any, upgrader configupgrade.Upgrader) {
	return "", nil, nil
}

func (lc *LineConnector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{
		Portal:   nil,
		Ghost:    nil,
		Message:  nil,
		Reaction: nil,
		UserLogin: func() any {
			return &UserLoginMetadata{}
		},
	}
}

type UserLoginMetadata struct {
	AccessToken       string            `json:"access_token"`
	RefreshToken      string            `json:"refresh_token,omitempty"`
	Email             string            `json:"email,omitempty"`
	Password          string            `json:"password,omitempty"`
	Certificate       string            `json:"certificate,omitempty"`
	Mid               string            `json:"mid,omitempty"`
	EncryptedKeyChain string            `json:"encrypted_key_chain,omitempty"`
	E2EEPublicKey     string            `json:"e2ee_public_key,omitempty"`
	E2EEVersion       string            `json:"e2ee_version,omitempty"`
	E2EEKeyID         string            `json:"e2ee_key_id,omitempty"`
	ExportedKeyMap    map[string]string `json:"exported_key_map,omitempty"`
}

func (lc *LineConnector) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) error {
	meta := login.Metadata.(*UserLoginMetadata)
	login.Client = &LineClient{
		UserLogin:    login,
		AccessToken:  meta.AccessToken,
		RefreshToken: meta.RefreshToken,
		Mid:          meta.Mid,
		HTTPClient:   &http.Client{Timeout: 10 * time.Second},
	}
	return nil
}

func (lc *LineConnector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{{
		Name:        "Login",
		Description: "Login with your LINE Email and Password",
		ID:          "dev.highest.matrix.line.email_login",
	}}
}

func (lc *LineConnector) CreateLogin(ctx context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	return &LineEmailLogin{User: user}, nil
}

type LineEmailLogin struct {
	User        *bridgev2.User
	Email       string
	Password    string
	Verifier    string
	AwaitingPIN bool
	NoE2EE      bool // True when login fell back to non-E2EE (LSOFF account)

	pollResult chan *line.LoginResult
	pollErr    chan error
	polling    bool
	mu         sync.Mutex
}

var _ bridgev2.LoginProcessUserInput = (*LineEmailLogin)(nil)
var _ bridgev2.LoginProcessDisplayAndWait = (*LineEmailLogin)(nil)

func (ll *LineEmailLogin) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeUserInput,
		StepID:       "dev.highest.matrix.line.enter_creds",
		Instructions: "Please enter your LINE email and password.",
		UserInputParams: &bridgev2.LoginUserInputParams{
			Fields: []bridgev2.LoginInputDataField{
				{
					Type: bridgev2.LoginInputFieldTypeUsername,
					ID:   "email",
					Name: "Email",
				},
				{
					Type: bridgev2.LoginInputFieldTypePassword,
					ID:   "password",
					Name: "Password",
				},
			},
		},
	}, nil
}

func (ll *LineEmailLogin) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	if ll.Verifier != "" {
		return ll.Wait(ctx)
	}

	if input["email"] != "" {
		ll.Email = input["email"]
		ll.Password = input["password"]
	}

	if ll.Email == "" || ll.Password == "" {
		return ll.loginErrorStep("Email and password are required"), nil
	}

	client := line.NewClient("")
	res, err := client.Login(ll.Email, ll.Password, "")
	if err != nil {
		reason := loginErrorReason(err)
		if reason == "" {
			reason = fmt.Sprintf("Login failed: %v", err)
		}
		return ll.loginErrorStep(reason), nil
	}

	return ll.handleLoginResponse(ctx, res)
}

func (ll *LineEmailLogin) loginErrorStep(message string) *bridgev2.LoginStep {
	instructions := fmt.Sprintf("Error when logging in: %s", message)
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeUserInput,
		StepID:       "dev.highest.matrix.line.enter_creds",
		Instructions: instructions,
		UserInputParams: &bridgev2.LoginUserInputParams{
			Fields: []bridgev2.LoginInputDataField{
				{
					Type: bridgev2.LoginInputFieldTypeUsername,
					ID:   "email",
					Name: "Email",
				},
				{
					Type: bridgev2.LoginInputFieldTypePassword,
					ID:   "password",
					Name: "Password",
				},
			},
		},
	}
}

func loginErrorReason(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	start := strings.Index(msg, "{")
	end := strings.LastIndex(msg, "}")
	if start == -1 || end == -1 || end <= start {
		return ""
	}
	var payload struct {
		Data struct {
			Reason string `json:"reason"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(msg[start:end+1]), &payload); err != nil {
		return ""
	}
	return payload.Data.Reason
}

func (ll *LineEmailLogin) Wait(ctx context.Context) (*bridgev2.LoginStep, error) {
	if ll.Verifier != "" {
		select {
		case res := <-ll.pollResult:
			if res.AuthToken != "" {
				return ll.finishLogin(ctx, res)
			}
			return nil, fmt.Errorf("verification failed: no auth token received")
		case err := <-ll.pollErr:
			return nil, fmt.Errorf("verification failed: %w", err)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if ll.AwaitingPIN {
		client := line.NewClient("")
		res, err := client.Login(ll.Email, ll.Password, "")
		if err != nil {
			return nil, fmt.Errorf("login failed: %w", err)
		}
		return ll.handleLoginResponse(ctx, res)
	}

	return nil, fmt.Errorf("no pending login continuation")
}

func (ll *LineEmailLogin) handleLoginResponse(ctx context.Context, res *line.LoginResult) (*bridgev2.LoginStep, error) {
	if res.AuthToken != "" {
		return ll.finishLogin(ctx, res)
	}

	if (res.Type == 3 || res.Type == 0) && res.Verifier != "" {
		ll.Verifier = res.Verifier
		ll.NoE2EE = res.NoE2EE
		ll.AwaitingPIN = false
		instructions := "Please open the LINE app on your mobile device to complete the login."
		pin := res.Pin
		if res.PinCode != "" {
			pin = res.PinCode
		}
		if pin != "" {
			instructions = fmt.Sprintf("Open LINE and enter this PIN on your mobile device: %s", pin)
		}

		// Start polling in background immediately so it's running while the user enters the PIN
		ll.mu.Lock()
		ll.polling = true
		ll.pollResult = make(chan *line.LoginResult, 1)
		ll.pollErr = make(chan error, 1)
		go func() {
			client := line.NewClient("")
			res, err := client.WaitForLogin(ll.Verifier, ll.NoE2EE)
			if err != nil {
				ll.pollErr <- err
			} else {
				ll.pollResult <- res
			}
		}()
		ll.mu.Unlock()

		return &bridgev2.LoginStep{
			Type:         bridgev2.LoginStepTypeDisplayAndWait,
			StepID:       "dev.highest.matrix.line.wait_verification",
			Instructions: instructions,
			DisplayAndWaitParams: &bridgev2.LoginDisplayAndWaitParams{
				Type: bridgev2.LoginDisplayTypeNothing,
			},
		}, nil
	}

	if res.Certificate != "" {
		ll.AwaitingPIN = true
		return &bridgev2.LoginStep{
			Type:         bridgev2.LoginStepTypeDisplayAndWait,
			StepID:       "dev.highest.matrix.line.enter_pin",
			Instructions: fmt.Sprintf("Please open the LINE app on your mobile device and enter this PIN code: **%s**\n\nAfter entering the code, click Continue below.", res.Certificate),
			DisplayAndWaitParams: &bridgev2.LoginDisplayAndWaitParams{
				Type: bridgev2.LoginDisplayTypeNothing,
			},
		}, nil
	}

	return nil, fmt.Errorf("login incomplete but no PIN found in response (Type: %d, Msg: %s)", res.Type, res.Message)
}

func (ll *LineEmailLogin) finishLogin(ctx context.Context, res *line.LoginResult) (*bridgev2.LoginStep, error) {
	if res == nil {
		return nil, fmt.Errorf("login result missing")
	}

	token := res.AuthToken
	refreshToken := ""
	if res.TokenV3IssueResult != nil {
		if token == "" {
			token = res.TokenV3IssueResult.AccessToken
		}
		refreshToken = res.TokenV3IssueResult.RefreshToken
	}
	if token == "" {
		return nil, fmt.Errorf("missing access token in login result")
	}

	client := line.NewClient(token)
	profile, err := client.GetProfile()
	if err != nil {
		return nil, fmt.Errorf("failed to verify token: %w", err)
	}

	displayName := profile.DisplayName
	if displayName == "" {
		displayName = "LINE User"
	}

	meta := &UserLoginMetadata{AccessToken: token, RefreshToken: refreshToken, Email: ll.Email, Password: ll.Password, Certificate: res.Certificate, Mid: res.Mid}

	ll.fetchLoginKeys(res, meta, client)

	detectedLineID := networkid.UserLoginID(profile.Mid)

	ul, err := ll.User.NewLogin(ctx, &database.UserLogin{
		ID:         detectedLineID,
		RemoteName: displayName,
		Metadata:   meta,
	}, &bridgev2.NewLoginParams{
		LoadUserLogin: func(ctx context.Context, login *bridgev2.UserLogin) error {
			login.Client = &LineClient{
				UserLogin:    login,
				AccessToken:  token,
				RefreshToken: refreshToken,
				HTTPClient:   &http.Client{Timeout: 10 * time.Second},
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create user login: %w", err)
	}

	go ul.Client.Connect(context.Background())
	ul.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})

	return &bridgev2.LoginStep{
		Type:           bridgev2.LoginStepTypeComplete,
		StepID:         "dev.highest.matrix.line.complete",
		Instructions:   "Successfully logged in",
		CompleteParams: &bridgev2.LoginCompleteParams{UserLoginID: ul.ID, UserLogin: ul},
	}, nil
}

func (ll *LineEmailLogin) fetchLoginKeys(res *line.LoginResult, meta *UserLoginMetadata, client *line.Client) {
	if res.EncryptedKeyChain == "" || res.E2EEPublicKey == "" {
		return
	}
	meta.EncryptedKeyChain = res.EncryptedKeyChain
	meta.E2EEPublicKey = res.E2EEPublicKey
	meta.E2EEVersion = res.E2EEVersion
	meta.E2EEKeyID = res.E2EEKeyID
	mgr, err := e2ee.NewManager()
	if err != nil {
		ll.User.Bridge.Log.Warn().Err(err).Msg("Login: failed to create E2EE manager")
		return
	}
	ei3, err := client.GetEncryptedIdentityV3()
	if err != nil {
		ll.User.Bridge.Log.Warn().Err(err).Msg("Login: failed to get EncryptedIdentityV3")
		return
	}
	if err := mgr.InitStorage(ei3.WrappedNonce, ei3.KDFParameter1, ei3.KDFParameter2); err != nil {
		ll.User.Bridge.Log.Warn().Err(err).Msg("Login: InitStorage failed")
		return
	}
	exported, err := mgr.InitFromLoginKeyChain(res.E2EEPublicKey, res.EncryptedKeyChain)
	if err != nil {
		ll.User.Bridge.Log.Warn().Err(err).Msg("Login: InitFromLoginKeyChain failed")
		return
	}
	meta.ExportedKeyMap = exported
	_ = mgr.SaveSecureDataToFile(string(ll.User.MXID), map[string]any{"exportedKeyMap": exported})
	ll.User.Bridge.Log.Info().Int("keys", len(exported)).Msg("Login: E2EE keys exported successfully")
}

func (ll *LineEmailLogin) Cancel() {}
