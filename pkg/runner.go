package line

import (
	"crypto/aes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/highesttt/matrix-line-messenger/pkg/ltsm"
)

type Runner struct {
	rt            *ltsm.Runtime
	token         string
	clientVersion string
	skPtr         uint32         // SecureKey from loadToken
	storageKey    uint32         // AesKey ptr (after StorageInit)
	loginCurveKey uint32         // Curve25519Key ptr (after GenerateE2EESecret)
	keyStore      map[int]uint32 // internal ID -> E2EEKey ptr
	channelStore  map[int]uint32 // internal ID -> E2EEChannel ptr
	nextID        int
	mu            sync.Mutex

	// Pure Go channels for encrypt/decrypt when raw key material is available.
	// Currently populated when a Go-generated key (with known private key) is
	// used to create a channel. WASM-unwrapped keys (from key chain) do NOT
	// have raw private keys because of SKB (white-box crypto).
	goKeys     map[int]*goKeyEntry   // internal ID -> raw key material
	goChannels map[int]*ltsm.Channel // internal ID -> pure Go channel
}

type SecretResult struct {
	Secret          string `json:"secret"`
	Pin             string `json:"pin"`
	PublicKeyHex    string `json:"publicKeyHex"`
	PublicKeyBase64 string `json:"publicKeyBase64"`
}

type UnwrappedKey struct {
	KeyID    int    `json:"keyId"`
	Exported string `json:"exported"`
	Version  int    `json:"version"`
	RawKeyID int    `json:"rawKeyId"`
}

// goKeyEntry holds raw Curve25519 key material for pure Go crypto.
type goKeyEntry struct {
	privKey []byte // raw 32-byte private key (nil if not available)
}

var (
	globalRunner *Runner
	runnerOnce   sync.Once
	runnerErr    error
)

func GetRunner() (*Runner, error) {
	runnerOnce.Do(func() {
		token := os.Getenv("SECURE_KEY")
		if token == "" {
			token = "wODdrvWqmdP4Zliay-iF3cz3KZcK0ekrial868apg06TXeCo7A1hIQO0ESElHg6D"
		}
		clientVersion := os.Getenv("CLIENT_VERSION")
		if clientVersion == "" {
			clientVersion = "3.7.2"
		}

		rt, err := ltsm.NewRuntime()
		if err != nil {
			runnerErr = fmt.Errorf("failed to initialize LTSM runtime: %w", err)
			return
		}

		skPtr, err := rt.SecureKeyLoadToken(token)
		if err != nil {
			rt.Close()
			runnerErr = fmt.Errorf("failed to load secure key: %w", err)
			return
		}

		globalRunner = &Runner{
			rt:            rt,
			token:         token,
			clientVersion: clientVersion,
			skPtr:         skPtr,
			keyStore:      make(map[int]uint32),
			channelStore:  make(map[int]uint32),
			nextID:        1,
			goKeys:        make(map[int]*goKeyEntry),
			goChannels:    make(map[int]*ltsm.Channel),
		}
	})
	return globalRunner, runnerErr
}

func (r *Runner) putKey(ptr uint32) int {
	id := r.nextID
	r.nextID++
	r.keyStore[id] = ptr
	return id
}

func (r *Runner) getKey(id int) (uint32, error) {
	ptr, ok := r.keyStore[id]
	if !ok {
		return 0, fmt.Errorf("unknown key: %d", id)
	}
	return ptr, nil
}

func (r *Runner) putChannel(ptr uint32) int {
	id := r.nextID
	r.nextID++
	r.channelStore[id] = ptr
	return id
}

func (r *Runner) getChannel(id int) (uint32, error) {
	ptr, ok := r.channelStore[id]
	if !ok {
		return 0, fmt.Errorf("unknown channel: %d", id)
	}
	return ptr, nil
}

func (r *Runner) GetSignature(reqPath, body, accessToken string) (string, error) {
	if reqPath == "" {
		reqPath = "/"
	} else if reqPath[0] != '/' {
		reqPath = "/" + reqPath
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	sig, err := r.rt.Sign(r.token, r.clientVersion, accessToken, reqPath, body)
	if err != nil {
		return "", err
	}
	if sig == "" {
		return "", fmt.Errorf("runner returned empty signature")
	}
	return sig, nil
}

// DebugExportDerivedSigningKey returns the derived SecureKey export blob used for
// signing. Disabled unless LTSM_ENABLE_DEBUG_EXPORT=1 is set.
func (r *Runner) DebugExportDerivedSigningKey(accessToken string) (string, error) {
	if os.Getenv("LTSM_ENABLE_DEBUG_EXPORT") != "1" {
		return "", fmt.Errorf("runner error: debug export disabled")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	cvHash := sha256.Sum256([]byte(r.clientVersion))
	atHash := sha256.Sum256([]byte(accessToken))

	dkPtr, err := r.rt.SecureKeyDeriveKey(r.skPtr, cvHash[:], atHash[:])
	if err != nil {
		return "", err
	}

	// Mark the derived key as exportable by setting the C++ flag at ptr+16
	r.rt.MarkSecureKeyExportable(dkPtr)

	exported, err := r.rt.SecureKeyExportKey(dkPtr)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(exported), nil
}

// StorageInit initializes the storage key using getEncryptedIdentityV3 response fields.
func (r *Runner) StorageInit(wrappedNonce, kdf1, kdf2 string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	nonceBytes, err := base64.StdEncoding.DecodeString(wrappedNonce)
	if err != nil {
		return fmt.Errorf("invalid wrappedNonce: %w", err)
	}
	kdf1Bytes, err := base64.StdEncoding.DecodeString(kdf1)
	if err != nil {
		return fmt.Errorf("invalid kdfParameter1: %w", err)
	}
	kdf2Bytes, err := base64.StdEncoding.DecodeString(kdf2)
	if err != nil {
		return fmt.Errorf("invalid kdfParameter2: %w", err)
	}

	unwrappedPtr, err := r.rt.SecureKeyUnwrapV3(nonceBytes, kdf1Bytes, kdf2Bytes)
	if err != nil {
		return err
	}

	aesPtr, err := r.rt.AesKeyNew(unwrappedPtr)
	if err != nil {
		return err
	}

	r.storageKey = aesPtr
	return nil
}

// StorageDecrypt decrypts lcs_secure blobs with the initialized storage key.
func (r *Runner) StorageDecrypt(ciphertext string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.storageKey == 0 {
		return "", fmt.Errorf("storage key not initialized")
	}

	ctBytes, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("invalid ciphertext: %w", err)
	}

	ptBytes, err := r.rt.AesKeyDecrypt(r.storageKey, ctBytes)
	if err != nil {
		return "", err
	}

	return string(ptBytes), nil
}

// StorageEncrypt encrypts plaintext with the initialized storage key.
func (r *Runner) StorageEncrypt(plaintext string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.storageKey == 0 {
		return "", fmt.Errorf("storage key not initialized")
	}

	ctBytes, err := r.rt.AesKeyEncrypt(r.storageKey, []byte(plaintext))
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(ctBytes), nil
}

// LoginUnwrapKeyChain unwraps the encrypted key chain from LF1 using the login curve key.
func (r *Runner) LoginUnwrapKeyChain(serverPubB64, encryptedKeyChainB64 string) ([]UnwrappedKey, error) {
	normalizedServerPub, err := normalizeServerPublicKeyB64(serverPubB64)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.loginCurveKey == 0 {
		return nil, fmt.Errorf("login key not initialized")
	}

	serverPubBytes, err := base64.StdEncoding.DecodeString(normalizedServerPub)
	if err != nil {
		return nil, fmt.Errorf("invalid server public key: %w", err)
	}

	chanPtr, err := r.rt.Curve25519KeyCreateChannel(r.loginCurveKey, serverPubBytes)
	if err != nil {
		return nil, err
	}

	encKeyChainBytes, err := base64.StdEncoding.DecodeString(encryptedKeyChainB64)
	if err != nil {
		return nil, fmt.Errorf("invalid encrypted key chain: %w", err)
	}

	kcPtr, err := r.rt.E2EEChannelUnwrapKeyChain(chanPtr, encKeyChainBytes)
	if err != nil {
		return nil, err
	}

	size, err := r.rt.E2EEKeychainSize(kcPtr)
	if err != nil {
		return nil, err
	}

	keys := make([]UnwrappedKey, 0, size)
	for i := 0; i < size; i++ {
		keyPtr, err := r.rt.E2EEKeychainGet(kcPtr, i)
		if err != nil {
			return nil, fmt.Errorf("failed to get key %d from keychain: %w", i, err)
		}

		id := r.putKey(keyPtr)

		exported, err := r.rt.E2EEKeyExportKey(keyPtr)
		if err != nil {
			return nil, fmt.Errorf("failed to export key %d: %w", i, err)
		}

		version, err := r.rt.E2EEKeyGetVersion(keyPtr)
		if err != nil {
			return nil, fmt.Errorf("failed to get version for key %d: %w", i, err)
		}

		rawKeyID, err := r.rt.E2EEKeyGetKeyId(keyPtr)
		if err != nil {
			return nil, fmt.Errorf("failed to get key ID for key %d: %w", i, err)
		}

		keys = append(keys, UnwrappedKey{
			KeyID:    id,
			Exported: base64.StdEncoding.EncodeToString(exported),
			Version:  version,
			RawKeyID: rawKeyID,
		})
	}

	return keys, nil
}

// KeyLoad loads a base64 E2EE key and returns an internal key ID.
func (r *Runner) KeyLoad(b64Key string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	keyBytes, err := base64.StdEncoding.DecodeString(b64Key)
	if err != nil {
		return 0, fmt.Errorf("invalid key: %w", err)
	}

	keyPtr, err := r.rt.E2EEKeyLoadKey(keyBytes)
	if err != nil {
		return 0, err
	}

	return r.putKey(keyPtr), nil
}

// KeyGetID returns the raw key ID for a loaded key.
func (r *Runner) KeyGetID(keyID int) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	keyPtr, err := r.getKey(keyID)
	if err != nil {
		return 0, err
	}

	return r.rt.E2EEKeyGetKeyId(keyPtr)
}

// KeyGetPublic returns the base64 public key for a loaded key.
func (r *Runner) KeyGetPublic(keyID int) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	keyPtr, err := r.getKey(keyID)
	if err != nil {
		return "", err
	}

	pubBytes, err := r.rt.E2EEKeyGetPublicKey(keyPtr)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(pubBytes), nil
}

// ChannelCreate creates a channel with our key and peer public key.
// If a raw Go private key is available for the key, also creates a pure Go
// channel for V1/V2 encrypt/decrypt without WASM overhead.
func (r *Runner) ChannelCreate(keyID int, peerPublicB64 string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	keyPtr, err := r.getKey(keyID)
	if err != nil {
		return 0, err
	}

	normalized, err := NormalizePeerPublicKeyB64(peerPublicB64)
	if err != nil {
		return 0, fmt.Errorf("invalid peer public key: %w", err)
	}
	peerPubBytes, _ := base64.StdEncoding.DecodeString(normalized)

	chanPtr, err := r.rt.E2EEKeyCreateChannel(keyPtr, peerPubBytes)
	if err != nil {
		return 0, err
	}

	id := r.putChannel(chanPtr)

	// If we have raw key material, also create a pure Go channel
	if goKey, ok := r.goKeys[keyID]; ok && goKey.privKey != nil {
		goChan, err := ltsm.NewChannel(goKey.privKey, peerPubBytes)
		if err == nil {
			r.goChannels[id] = goChan
		}
	}

	return id, nil
}

// ChannelUnwrapGroupSharedKey unwraps the group shared key using the channel.
func (r *Runner) ChannelUnwrapGroupSharedKey(channelID int, encryptedSharedKeyB64 string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	chanPtr, err := r.getChannel(channelID)
	if err != nil {
		return 0, err
	}

	encBytes, err := base64.StdEncoding.DecodeString(encryptedSharedKeyB64)
	if err != nil {
		return 0, fmt.Errorf("invalid encrypted shared key: %w", err)
	}

	keyPtr, err := r.rt.E2EEChannelUnwrapGroupSharedKey(chanPtr, encBytes)
	if err != nil {
		return 0, err
	}

	return r.putKey(keyPtr), nil
}

// ChannelEncryptV1 encrypts plaintext with channel V1 (AES-256-CBC + MAC).
// Uses pure Go crypto when a Go channel is available.
func (r *Runner) ChannelEncryptV1(channelID int, plaintext string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Prefer pure Go channel
	if goChan, ok := r.goChannels[channelID]; ok {
		ctBytes, err := goChan.EncryptV1([]byte(plaintext))
		if err != nil {
			return "", fmt.Errorf("pure Go EncryptV1 failed: %w", err)
		}
		return base64.StdEncoding.EncodeToString(ctBytes), nil
	}

	// Fall back to transpiled module
	chanPtr, err := r.getChannel(channelID)
	if err != nil {
		return "", err
	}

	ctBytes, err := r.rt.E2EEChannelEncryptV1(chanPtr, []byte(plaintext))
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(ctBytes), nil
}

// ChannelEncryptV2 encrypts plaintext with channel v2.
// Uses pure Go crypto when a Go channel is available (faster, and V2 works
// correctly in Go whereas it fails in the WASM bridge due to SKB issues).
func (r *Runner) ChannelEncryptV2(channelID int, to, from string, senderKeyID, receiverKeyID, contentType, seq int, plaintext string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Prefer pure Go channel (V2 is broken in WASM due to SKB AAD failure)
	if goChan, ok := r.goChannels[channelID]; ok {
		ctBytes, err := goChan.EncryptV2(to, from, senderKeyID, receiverKeyID, contentType, int64(seq), []byte(plaintext))
		if err != nil {
			return "", fmt.Errorf("pure Go EncryptV2 failed: %w", err)
		}
		return base64.StdEncoding.EncodeToString(ctBytes), nil
	}

	// Fall back to transpiled module
	chanPtr, err := r.getChannel(channelID)
	if err != nil {
		return "", err
	}

	ctBytes, err := r.rt.E2EEChannelEncryptV2(chanPtr,
		to, from, senderKeyID, receiverKeyID, contentType, int64(seq), []byte(plaintext))
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(ctBytes), nil
}

// ChannelDecryptV1 decrypts ciphertext with channel v1 (ios).
// Uses pure Go crypto when a Go channel is available.
func (r *Runner) ChannelDecryptV1(channelID, senderKeyID, receiverKeyID int, ciphertext string) (string, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ctBytes, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", "", fmt.Errorf("invalid ciphertext: %w", err)
	}

	// Prefer pure Go channel
	if goChan, ok := r.goChannels[channelID]; ok {
		ptBytes, err := goChan.DecryptV1(ctBytes)
		if err != nil {
			return "", "", fmt.Errorf("pure Go DecryptV1 failed: %w", err)
		}
		return string(ptBytes), base64.StdEncoding.EncodeToString(ptBytes), nil
	}

	// Fall back to transpiled module
	chanPtr, err := r.getChannel(channelID)
	if err != nil {
		return "", "", err
	}

	ptBytes, err := r.rt.E2EEChannelDecryptV1(chanPtr, ctBytes)
	if err != nil {
		return "", "", err
	}

	return string(ptBytes), base64.StdEncoding.EncodeToString(ptBytes), nil
}

// ChannelDecryptV2 decrypts ciphertext with channel v2.
// Uses pure Go crypto when a Go channel is available (V2 is broken in WASM).
func (r *Runner) ChannelDecryptV2(channelID int, to, from string, senderKeyID, receiverKeyID, contentType int, ciphertext string) (string, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ctBytes, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", "", fmt.Errorf("invalid ciphertext: %w", err)
	}

	// Prefer pure Go channel (V2 is broken in WASM due to SKB AAD failure)
	if goChan, ok := r.goChannels[channelID]; ok {
		ptBytes, err := goChan.DecryptV2(to, from, senderKeyID, receiverKeyID, contentType, ctBytes)
		if err != nil {
			return "", "", fmt.Errorf("pure Go DecryptV2 failed: %w", err)
		}
		return string(ptBytes), base64.StdEncoding.EncodeToString(ptBytes), nil
	}

	// Fall back to transpiled module
	chanPtr, err := r.getChannel(channelID)
	if err != nil {
		return "", "", err
	}

	ptBytes, err := r.rt.E2EEChannelDecryptV2(chanPtr,
		to, from, senderKeyID, receiverKeyID, contentType, ctBytes)
	if err != nil {
		return "", "", err
	}

	return string(ptBytes), base64.StdEncoding.EncodeToString(ptBytes), nil
}

// GenerateE2EESecret generates a login secret with PIN and public key.
// The Curve25519Key is used because GenerateConfirmHash and
// LoginUnwrapKeyChain require the SKB-wrapped key for ECDH.
func (r *Runner) GenerateE2EESecret() (*SecretResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ckPtr, err := r.rt.Curve25519KeyNew(r.skPtr)
	if err != nil {
		return nil, err
	}
	r.loginCurveKey = ckPtr

	pubBytes, err := r.rt.Curve25519KeyGetPublicKey(ckPtr)
	if err != nil {
		return nil, fmt.Errorf("failed to get public key: %w", err)
	}

	pin, err := generateLoginPIN()
	if err != nil {
		return nil, err
	}

	secret, err := buildLoginSecret(pin, pubBytes)
	if err != nil {
		return nil, err
	}

	return &SecretResult{
		Secret:          secret,
		Pin:             pin,
		PublicKeyHex:    hex.EncodeToString(pubBytes),
		PublicKeyBase64: base64.StdEncoding.EncodeToString(pubBytes),
	}, nil
}

// GenerateConfirmHash derives the hash key chain for confirmE2EELogin.
// Must be called after GenerateE2EESecret.
func (r *Runner) GenerateConfirmHash(serverPublicKeyB64, encryptedKeyChainB64 string) (string, error) {
	normalizedServerPub, err := normalizeServerPublicKeyB64(serverPublicKeyB64)
	if err != nil {
		return "", err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.loginCurveKey == 0 {
		return "", fmt.Errorf("login key not initialized")
	}

	serverPubBytes, err := base64.StdEncoding.DecodeString(normalizedServerPub)
	if err != nil {
		return "", fmt.Errorf("invalid server public key: %w", err)
	}

	chanPtr, err := r.rt.Curve25519KeyCreateChannel(r.loginCurveKey, serverPubBytes)
	if err != nil {
		return "", err
	}

	encKeyChainBytes, err := base64.StdEncoding.DecodeString(encryptedKeyChainB64)
	if err != nil {
		return "", fmt.Errorf("invalid encrypted key chain: %w", err)
	}

	hashBytes, err := r.rt.E2EEChannelGenerateConfirmHash(chanPtr, encKeyChainBytes)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(hashBytes), nil
}

// --- Pure Go helpers (no WASM dependency) ---

func generateLoginPIN() (string, error) {
	const limit uint32 = 4_294_000_000 // floor(2^32 / 1e6) * 1e6 to avoid modulo bias
	var nbuf [4]byte
	for {
		if _, err := rand.Read(nbuf[:]); err != nil {
			return "", fmt.Errorf("failed to generate pin: %w", err)
		}
		n := binary.BigEndian.Uint32(nbuf[:])
		if n < limit {
			return fmt.Sprintf("%06d", n%1_000_000), nil
		}
	}
}

func normalizeServerPublicKeyB64(serverPubB64 string) (string, error) {
	serverRaw, err := base64.StdEncoding.DecodeString(serverPubB64)
	if err != nil {
		return "", fmt.Errorf("invalid server public key: %w", err)
	}
	if len(serverRaw) < 32 {
		return "", fmt.Errorf("invalid server public key length: %d", len(serverRaw))
	}
	if len(serverRaw) > 32 {
		serverRaw = serverRaw[len(serverRaw)-32:]
	}
	return base64.StdEncoding.EncodeToString(serverRaw), nil
}

// NormalizePeerPublicKeyB64 accepts a peer public key in any common encoding
// (standard base64, base64url, unpadded variants, or hex) and returns a
// standard-base64-encoded 32-byte Curve25519 public key.
func NormalizePeerPublicKeyB64(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty peer public key")
	}

	// Try each base64 variant in order.
	encodings := []struct {
		name string
		enc  *base64.Encoding
	}{
		{"StdEncoding", base64.StdEncoding},
		{"URLEncoding", base64.URLEncoding},
		{"RawStdEncoding", base64.RawStdEncoding},
		{"RawURLEncoding", base64.RawURLEncoding},
	}

	var decoded []byte
	for _, e := range encodings {
		if b, err := e.enc.DecodeString(raw); err == nil && len(b) > 0 {
			decoded = b
			break
		}
	}

	// Fallback: try hex decoding.
	if decoded == nil {
		if b, err := hex.DecodeString(raw); err == nil && len(b) > 0 {
			decoded = b
		}
	}

	if decoded == nil {
		preview := raw
		if len(preview) > 8 {
			preview = preview[:8]
		}
		return "", fmt.Errorf("cannot decode peer public key (starts with %q): tried base64 std/url/raw, hex", preview)
	}

	if len(decoded) < 32 {
		return "", fmt.Errorf("peer public key too short: got %d bytes, need 32", len(decoded))
	}
	// If longer than 32 bytes (e.g. type-prefix byte), take the last 32.
	if len(decoded) > 32 {
		decoded = decoded[len(decoded)-32:]
	}

	return base64.StdEncoding.EncodeToString(decoded), nil
}

func buildLoginSecret(pin string, publicKey []byte) (string, error) {
	if len(publicKey) != 32 {
		return "", fmt.Errorf("invalid login public key length: got %d, want 32", len(publicKey))
	}
	aesKey := sha256.Sum256([]byte(pin))
	block, err := aes.NewCipher(aesKey[:])
	if err != nil {
		return "", fmt.Errorf("failed to create aes cipher: %w", err)
	}
	secret := make([]byte, 32)
	block.Encrypt(secret[:16], publicKey[:16])
	block.Encrypt(secret[16:], publicKey[16:])
	return base64.StdEncoding.EncodeToString(secret), nil
}
