package e2ee

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	gen "github.com/highesttt/matrix-line-messenger/pkg"
	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

/*
This wraps the JS runner to perform storage init, key/channel handling, and e2ee encrypt/decrypt.
*/
type Manager struct {
	runner *gen.Runner

	mu            sync.Mutex
	myKeyID       int
	myRawKeyID    int
	myKeyB64      string
	myPublicB64   string
	peerPublic    map[int]string // raw key id -> public key b64
	keyByRawID    map[int]int    // raw key id -> runner key id
	channelByPair map[string]int // privRaw/peerRaw -> channelId
	sequence      map[string]int // portal/chat id -> seq

	// groupKeys maps chatMid -> groupKeyId -> e2eeKeyId (unwrapped group private key, runner ID)
	groupKeys map[string]map[int]int
	// groupSessionChannels maps chatMid -> groupKeyId -> senderKeyId -> channelId (runner Channel ID)
	groupSessionChannels map[string]map[int]map[int]int

	latestGroupKey map[string]int // chatMid -> latest groupKeyId
}

func NewManager() (*Manager, error) {
	r, err := gen.GetRunner()
	if err != nil {
		return nil, err
	}
	return &Manager{
		runner:               r,
		peerPublic:           make(map[int]string),
		keyByRawID:           make(map[int]int),
		channelByPair:        make(map[string]int),
		sequence:             make(map[string]int),
		groupKeys:            make(map[string]map[int]int),
		groupSessionChannels: make(map[string]map[int]map[int]int),
		latestGroupKey:       make(map[string]int),
	}, nil
}

// returns both the raw key id and the runner key ids
func (m *Manager) MyKeyIDs() (rawID int, keyID int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.myKeyID == 0 || m.myRawKeyID == 0 {
		return 0, 0, fmt.Errorf("my key not loaded")
	}
	return m.myRawKeyID, m.myKeyID, nil
}

// MyPublicKey returns the bridge user's own latest LINE key ID and base64 public key.
// Used to include the caller's own entry when registering a group key.
func (m *Manager) MyPublicKey() (rawID int, pubB64 string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.myRawKeyID == 0 || m.myPublicB64 == "" {
		return 0, "", fmt.Errorf("my key not loaded")
	}
	return m.myRawKeyID, m.myPublicB64, nil
}

func (m *Manager) InitStorage(wrappedNonce, kdf1, kdf2 string) error {
	return m.runner.StorageInit(wrappedNonce, kdf1, kdf2)
}

func (m *Manager) LoadMyKey(b64Key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	keyID, err := m.runner.KeyLoad(b64Key)
	if err != nil {
		return err
	}
	rawID, err := m.runner.KeyGetID(keyID)
	if err != nil {
		return err
	}
	pub, err := m.runner.KeyGetPublic(keyID)
	if err != nil {
		return err
	}
	m.myKeyID = keyID
	m.myRawKeyID = rawID
	m.myKeyB64 = b64Key
	m.myPublicB64 = pub
	m.keyByRawID[rawID] = keyID
	m.peerPublic[rawID] = pub
	return nil
}

func (m *Manager) LoadMyKeyFromSecureData(data map[string]any) error {
	rawMap, ok := data["exportedKeyMap"].(map[string]any)
	if !ok || len(rawMap) == 0 {
		return fmt.Errorf("exportedKeyMap missing in secure data")
	}
	ids := make([]int, 0, len(rawMap))
	for k := range rawMap {
		if n, err := strconv.Atoi(k); err == nil {
			ids = append(ids, n)
		}
	}
	if len(ids) == 0 {
		return fmt.Errorf("no numeric key ids in exportedKeyMap")
	}
	// Ascending: highest keyID is loaded LAST so it wins as myKey, while all
	// older device keys are still registered in keyByRawID/peerPublic for
	// decrypting echoes of messages sent from another of the user's devices.
	sort.Ints(ids)
	var loadedAny bool
	var lastErr error
	for _, id := range ids {
		b64, ok := rawMap[strconv.Itoa(id)].(string)
		if !ok {
			continue
		}
		if err := m.LoadMyKey(b64); err == nil {
			loadedAny = true
		} else {
			lastErr = err
		}
	}
	if !loadedAny {
		if lastErr != nil {
			return fmt.Errorf("failed to load any exported key: %w", lastErr)
		}
		return fmt.Errorf("failed to load any exported key")
	}
	return nil
}

func (m *Manager) LoadMyKeyFromExportedMap(exported map[string]string) error {
	if len(exported) == 0 {
		return fmt.Errorf("no exported keys provided")
	}
	raw := make(map[string]any, len(exported))
	for k, v := range exported {
		raw[k] = v
	}
	return m.LoadMyKeyFromSecureData(map[string]any{"exportedKeyMap": raw})
}

func (m *Manager) DecryptSecureBlob(b64Cipher string) (map[string]any, error) {
	pt, err := m.runner.StorageDecrypt(b64Cipher)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(pt), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (m *Manager) EncryptSecureBlob(data map[string]any) (string, error) {
	buf, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return m.runner.StorageEncrypt(string(buf))
}

func (m *Manager) LoadSecureDataFromFile(id string) (map[string]any, error) {
	path := filepath.Join("data", fmt.Sprintf("lcs_secure_%s.json", id))
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return m.DecryptSecureBlob(strings.TrimSpace(string(b)))
}

func (m *Manager) SaveSecureDataToFile(id string, data map[string]any) error {
	ct, err := m.EncryptSecureBlob(data)
	if err != nil {
		return err
	}
	path := filepath.Join("data", fmt.Sprintf("lcs_secure_%s.json", id))
	return os.WriteFile(path, []byte(ct), 0o600)
}

func (m *Manager) InitFromLoginKeyChain(serverPubB64, encryptedKeyChainB64 string) (map[string]string, error) {
	keys, err := m.runner.LoginUnwrapKeyChain(serverPubB64, encryptedKeyChainB64)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("no keys unwrapped")
	}
	// Sort keys by raw id desc and pick the latest for self
	sort.Slice(keys, func(i, j int) bool { return keys[i].RawKeyID > keys[j].RawKeyID })
	exportedMap := make(map[string]string)
	for _, k := range keys {
		exportedMap[strconv.Itoa(k.RawKeyID)] = k.Exported
		m.RegisterUnwrappedKey(k.RawKeyID, k.KeyID)
		pub, err := m.runner.KeyGetPublic(k.KeyID)
		if err == nil {
			m.RegisterPeerPublicKey(k.RawKeyID, pub)
		}
	}
	if err := m.LoadMyKey(keys[0].Exported); err != nil {
		return nil, err
	}
	return exportedMap, nil
}

func (m *Manager) EncryptMessageV2(chatID, from string, myKeyID int, peerPubKeyB64 string, senderKeyID, receiverKeyID, contentType int, plaintext string) ([]string, error) {
	// Standard text message payload
	payload, err := json.Marshal(map[string]string{"text": plaintext})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}
	return m.EncryptMessageV2Raw(chatID, from, myKeyID, peerPubKeyB64, senderKeyID, receiverKeyID, contentType, payload)
}

// EncryptMessageV2Raw encrypts a raw JSON payload (bytes) instead of forcing a text object.
// This is required for media messages.
// Falls back to V1 encryption if V2 fails (some clients like iOS still use V1).
func (m *Manager) EncryptMessageV2Raw(chatID, from string, myKeyID int, peerPubKeyB64 string, senderKeyID, receiverKeyID, contentType int, payloadJSON []byte) ([]string, error) {
	chanID, seq, err := m.ensureChannelForEncrypt(chatID, myKeyID, senderKeyID, receiverKeyID, peerPubKeyB64)
	if err != nil {
		return nil, err
	}

	ctB64, err := m.runner.ChannelEncryptV2(chanID, chatID, from, senderKeyID, receiverKeyID, contentType, seq, string(payloadJSON))
	if err != nil {
		// Fall back to V1 encryption (some clients like iOS still use V1).
		return m.encryptV1Chunks(chanID, senderKeyID, receiverKeyID, payloadJSON)
	}
	ctBytes, err := base64.StdEncoding.DecodeString(ctB64)
	if err != nil {
		return nil, err
	}
	// Split into chunks: [first16, middle, last12, senderKeyId(4), receiverKeyId(4)]
	if len(ctBytes) < 28 {
		return nil, fmt.Errorf("ciphertext too short")
	}
	// output = [Header (16)] [Tag (12)] [Body (...)]
	// chunks sent: [Header] [Body] [Tag]
	first := ctBytes[:16]
	tag := ctBytes[16:28]
	body := ctBytes[28:]

	senderBuf := make([]byte, 4)
	receiverBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(senderBuf, uint32(senderKeyID))
	binary.BigEndian.PutUint32(receiverBuf, uint32(receiverKeyID))
	toB64 := func(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
	// [first, body, tag]
	chunks := []string{toB64(first), toB64(body), toB64(tag), toB64(senderBuf), toB64(receiverBuf)}
	return chunks, nil
}

// encryptV1Chunks encrypts payload with V1 and splits into the wire chunk format.
// V1 ciphertext = salt(8) || encrypted || MAC(16).
// Wire chunks for V1: [salt(8), encrypted, MAC(16), senderKeyID(4), receiverKeyID(4)]
func (m *Manager) encryptV1Chunks(chanID, senderKeyID, receiverKeyID int, payloadJSON []byte) ([]string, error) {
	ctB64, err := m.runner.ChannelEncryptV1(chanID, string(payloadJSON))
	if err != nil {
		return nil, fmt.Errorf("V1 encrypt failed: %w", err)
	}
	ctBytes, err := base64.StdEncoding.DecodeString(ctB64)
	if err != nil {
		return nil, err
	}
	// V1 format: salt(8) || ciphertext || MAC(16)
	if len(ctBytes) < 24+16 { // 8 salt + 16 min block + 16 MAC
		return nil, fmt.Errorf("V1 ciphertext too short")
	}

	salt := ctBytes[:8]
	body := ctBytes[8 : len(ctBytes)-16]
	mac := ctBytes[len(ctBytes)-16:]

	senderBuf := make([]byte, 4)
	receiverBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(senderBuf, uint32(senderKeyID))
	binary.BigEndian.PutUint32(receiverBuf, uint32(receiverKeyID))
	toB64 := func(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
	chunks := []string{toB64(salt), toB64(body), toB64(mac), toB64(senderBuf), toB64(receiverBuf)}
	return chunks, nil
}

func (m *Manager) ensureChannelForEncrypt(chatID string, myKeyID, senderKeyID, receiverKeyID int, peerPubKeyB64 string) (int, int, error) {
	m.mu.Lock()
	myRaw := m.myRawKeyID
	myRunnerKey := myKeyID
	if myRunnerKey == 0 {
		myRunnerKey = m.myKeyID
	}
	peerPub := m.peerPublic
	keyByRaw := m.keyByRawID
	seqMap := m.sequence
	m.mu.Unlock()

	var privRaw, pubRaw, privKeyID int
	if senderKeyID == myRaw {
		privRaw, pubRaw, privKeyID = senderKeyID, receiverKeyID, myRunnerKey
	} else if receiverKeyID == myRaw {
		privRaw, pubRaw, privKeyID = receiverKeyID, senderKeyID, myRunnerKey
	} else if id, ok := keyByRaw[senderKeyID]; ok {
		privRaw, pubRaw, privKeyID = senderKeyID, receiverKeyID, id
	} else if id, ok := keyByRaw[receiverKeyID]; ok {
		privRaw, pubRaw, privKeyID = receiverKeyID, senderKeyID, id
	} else {
		return 0, 0, fmt.Errorf("no matching private key for senderKeyID=%d receiverKeyID=%d", senderKeyID, receiverKeyID)
	}

	pubKey := peerPubKeyB64
	if pubKey == "" {
		m.mu.Lock()
		pubKey = peerPub[pubRaw]
		m.mu.Unlock()
	}
	if pubKey == "" {
		return 0, 0, fmt.Errorf("missing peer public key for raw id %d", pubRaw)
	}

	m.RegisterPeerPublicKey(pubRaw, pubKey)

	pairKey := fmt.Sprintf("%d|%d", privRaw, pubRaw)
	m.mu.Lock()
	chanID, ok := m.channelByPair[pairKey]
	m.mu.Unlock()
	if !ok {
		created, err := m.runner.ChannelCreate(privKeyID, pubKey)
		if err != nil {
			return 0, 0, err
		}
		chanID = created
		m.mu.Lock()
		m.channelByPair[pairKey] = chanID
		m.mu.Unlock()
	}

	m.mu.Lock()
	seq := seqMap[chatID] + 1 // start at 1 like extension?
	seqMap[chatID] = seq
	m.mu.Unlock()

	return chanID, seq, nil
}

func (m *Manager) DecryptMessageV2(msg *line.Message) (string, error) {
	if len(msg.Chunks) < 5 {
		return "", fmt.Errorf("not enough chunks")
	}
	m.mu.Lock()
	myRaw := m.myRawKeyID
	m.mu.Unlock()
	if myRaw == 0 {
		return "", fmt.Errorf("my key not loaded")
	}
	// Extract sender/receiver key ids from last two chunks
	senderKeyID, err := DecodeKeyID(msg.Chunks[len(msg.Chunks)-2])
	if err != nil {
		return "", err
	}
	receiverKeyID, err := DecodeKeyID(msg.Chunks[len(msg.Chunks)-1])
	if err != nil {
		return "", err
	}
	cipher, err := assembleCipher(msg.Chunks)
	if err != nil {
		return "", err
	}
	chanID, err := m.channelFromKeyIDs(senderKeyID, receiverKeyID)
	if err != nil {
		return "", err
	}

	ver := 2
	if vStr, ok := msg.ContentMetadata["e2eeVersion"]; ok && vStr == "1" {
		ver = 1
	}

	if ver == 1 {
		cipher, err := assembleCipherV1(msg.Chunks)
		if err != nil {
			return "", err
		}
		pt, _, err := m.runner.ChannelDecryptV1(chanID, senderKeyID, receiverKeyID, base64.StdEncoding.EncodeToString(cipher))
		if err != nil {
			return "", err
		}
		return pt, nil
	}

	pt, _, err := m.runner.ChannelDecryptV2(chanID, msg.To, msg.From, senderKeyID, receiverKeyID, msg.ContentType, base64.StdEncoding.EncodeToString(cipher))
	if err != nil {
		// V2 decrypt failed, try V1 as fallback (some clients like iOS still use V1)
		cipherV1, err2 := assembleCipherV1(msg.Chunks)
		if err2 != nil {
			return "", err // return original V2 error
		}
		ptV1, _, err2 := m.runner.ChannelDecryptV1(chanID, senderKeyID, receiverKeyID, base64.StdEncoding.EncodeToString(cipherV1))
		if err2 != nil {
			return "", fmt.Errorf("V2 decrypt failed: %w; V1 fallback also failed: %v", err, err2)
		}
		return ptV1, nil
	}
	return pt, nil
}

func assembleCipherV1(chunks []string) ([]byte, error) {
	if len(chunks) < 3 {
		return nil, fmt.Errorf("not enough chunks")
	}
	decode := func(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(strings.TrimSpace(s)) }
	b0, err := decode(chunks[0])
	if err != nil {
		return nil, err
	}
	b1, err := decode(chunks[1])
	if err != nil {
		return nil, err
	}
	b2, err := decode(chunks[2])
	if err != nil {
		return nil, err
	}
	return append(append(b0, b1...), b2...), nil
}

func (m *Manager) UnwrapGroupSharedKey(chatMid string, sharedKey *line.E2EEGroupSharedKey) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	creatorRawKeyID := sharedKey.CreatorKeyID
	creatorPubKeyB64, ok := m.peerPublic[creatorRawKeyID]
	if !ok {
		return 0, fmt.Errorf("missing creator public key for %d", creatorRawKeyID)
	}

	receiverRawKeyID := sharedKey.ReceiverKeyID
	myRunnerKey := 0
	if receiverRawKeyID == m.myRawKeyID {
		myRunnerKey = m.myKeyID
	} else if id, ok := m.keyByRawID[receiverRawKeyID]; ok {
		myRunnerKey = id
	}

	if myRunnerKey == 0 {
		return 0, fmt.Errorf("missing my private key for %d", receiverRawKeyID)
	}

	chanID, err := m.runner.ChannelCreate(myRunnerKey, creatorPubKeyB64)
	if err != nil {
		return 0, fmt.Errorf("failed to create channel with creator: %w", err)
	}

	unwrappedKeyID, err := m.runner.ChannelUnwrapGroupSharedKey(chanID, sharedKey.EncryptedSharedKey)
	if err != nil {
		return 0, fmt.Errorf("failed to unwrap group key: %w", err)
	}

	if _, ok := m.groupKeys[chatMid]; !ok {
		m.groupKeys[chatMid] = make(map[int]int)
	}
	m.groupKeys[chatMid][sharedKey.GroupKeyID] = unwrappedKeyID

	m.latestGroupKey[chatMid] = sharedKey.GroupKeyID

	return unwrappedKeyID, nil
}

// GenerateGroupKey generates a new Curve25519 key in the WASM module for use as a group key.
// Returns the key ID for use with WrapGroupKeyForMember.
func (m *Manager) GenerateGroupKey() (int, error) {
	return m.runner.KeyGenerate()
}

// WrapGroupKeyForMember wraps a WASM-generated group key for a specific group member.
// keyID must reference an E2EEKey obtained from GenerateGroupKey.
// Returns the base64-encoded wrapped key, ready for registerE2EEGroupKey.
func (m *Manager) WrapGroupKeyForMember(memberPubKeyB64 string, keyID int) (string, error) {
	m.mu.Lock()
	myKeyID := m.myKeyID
	m.mu.Unlock()

	if myKeyID == 0 {
		return "", fmt.Errorf("my E2EE key not loaded")
	}

	normalized, err := gen.NormalizePeerPublicKeyB64(memberPubKeyB64)
	if err != nil {
		return "", fmt.Errorf("invalid member public key: %w", err)
	}

	chanID, err := m.runner.ChannelCreate(myKeyID, normalized)
	if err != nil {
		return "", fmt.Errorf("failed to create ECDH channel: %w", err)
	}

	encryptedKey, err := m.runner.ChannelWrapGroupSharedKey(chanID, keyID)
	if err != nil {
		return "", fmt.Errorf("failed to wrap group key: %w", err)
	}

	return encryptedKey, nil
}

func (m *Manager) DecryptGroupMessage(msg *line.Message, chatMid string) (string, int, error) {
	// Group chunks: [..., groupKeyID(4), ???]
	// JS: vF365(t, n, r); n=myKeyID, r=groupKeyID.
	// chunks = [first, body, tag, senderKeyID, groupKeyID]
	if len(msg.Chunks) < 5 {
		return "", 0, fmt.Errorf("not enough chunks")
	}

	senderKeyID, err := DecodeKeyID(msg.Chunks[len(msg.Chunks)-2])
	if err != nil {
		return "", 0, fmt.Errorf("failed to decode sender key id: %w", err)
	}

	groupKeyID, err := DecodeKeyID(msg.Chunks[len(msg.Chunks)-1])
	if err != nil {
		return "", 0, fmt.Errorf("failed to decode group key id: %w", err)
	}

	m.mu.Lock()
	keyMap, ok := m.groupKeys[chatMid]
	var unwrappedKeyID int
	if ok {
		unwrappedKeyID = keyMap[groupKeyID]
	}
	m.mu.Unlock()

	if unwrappedKeyID == 0 {
		return "", groupKeyID, fmt.Errorf("group key %d not found", groupKeyID)
	}

	chanID, err := m.ensureGroupChannel(chatMid, groupKeyID, unwrappedKeyID, senderKeyID)
	if err != nil {
		return "", groupKeyID, err
	}

	cipher, err := assembleCipher(msg.Chunks)
	if err != nil {
		return "", groupKeyID, err
	}

	pt, _, err := m.runner.ChannelDecryptV2(chanID, msg.To, msg.From, senderKeyID, groupKeyID, msg.ContentType, base64.StdEncoding.EncodeToString(cipher))
	if err != nil {
		// V2 decrypt failed, try V1 as fallback (some clients like iOS still use V1)
		cipherV1, err2 := assembleCipherV1(msg.Chunks)
		if err2 != nil {
			return "", groupKeyID, err // return original V2 error
		}
		ptV1, _, err2 := m.runner.ChannelDecryptV1(chanID, senderKeyID, groupKeyID, base64.StdEncoding.EncodeToString(cipherV1))
		if err2 != nil {
			return "", groupKeyID, fmt.Errorf("V2 decrypt failed: %w; V1 fallback also failed: %v", err, err2)
		}
		return ptV1, groupKeyID, nil
	}

	return pt, groupKeyID, nil
}

func (m *Manager) ensureGroupChannel(chatMid string, groupKeyID, unwrappedKeyID, senderKeyID int) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.groupSessionChannels[chatMid] == nil {
		m.groupSessionChannels[chatMid] = make(map[int]map[int]int)
	}
	if m.groupSessionChannels[chatMid][groupKeyID] == nil {
		m.groupSessionChannels[chatMid][groupKeyID] = make(map[int]int)
	}
	if id, ok := m.groupSessionChannels[chatMid][groupKeyID][senderKeyID]; ok {
		return id, nil
	}

	senderPubB64, ok := m.peerPublic[senderKeyID]
	if !ok {
		return 0, fmt.Errorf("missing public key for sender %d", senderKeyID)
	}

	chanID, err := m.runner.ChannelCreate(unwrappedKeyID, senderPubB64)
	if err != nil {
		return 0, fmt.Errorf("failed to create group channel: %w", err)
	}

	m.groupSessionChannels[chatMid][groupKeyID][senderKeyID] = chanID
	return chanID, nil
}

func (m *Manager) EncryptGroupMessage(chatMid, fromMid string, plaintext string) ([]string, error) {
	payload, err := json.Marshal(map[string]string{"text": plaintext})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}
	return m.EncryptGroupMessageRaw(chatMid, fromMid, 0, payload)
}

// group messages are encrypted differently compared to 1:1 messages.
// Falls back to V1 if V2 fails (some clients like iOS still use V1).
func (m *Manager) EncryptGroupMessageRaw(chatMid, fromMid string, contentType int, payload []byte) ([]string, error) {
	m.mu.Lock()
	groupKeyID, ok := m.latestGroupKey[chatMid]
	var unwrappedKeyID int
	if ok {
		unwrappedKeyID = m.groupKeys[chatMid][groupKeyID]
	}
	myKeyID := m.myRawKeyID
	seq := m.sequence[chatMid] + 1
	m.sequence[chatMid] = seq
	m.mu.Unlock()

	if !ok || unwrappedKeyID == 0 {
		return nil, fmt.Errorf("e2ee key not loaded for group %s", chatMid)
	}

	chanID, err := m.ensureGroupChannel(chatMid, groupKeyID, unwrappedKeyID, myKeyID)
	if err != nil {
		return nil, err
	}

	ctB64, err := m.runner.ChannelEncryptV2(chanID, chatMid, fromMid, myKeyID, groupKeyID, contentType, seq, string(payload))
	if err != nil {
		// V2 failed, fall back to V1 (some clients like iOS still use V1)
		return m.encryptV1Chunks(chanID, myKeyID, groupKeyID, payload)
	}
	ctBytes, err := base64.StdEncoding.DecodeString(ctB64)
	if err != nil {
		return nil, err
	}

	if len(ctBytes) < 28 {
		return nil, fmt.Errorf("ciphertext too short")
	}

	first := ctBytes[:16]
	tag := ctBytes[16:28]
	body := ctBytes[28:]

	senderBuf := make([]byte, 4)
	receiverBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(senderBuf, uint32(myKeyID))      // Sender is my Key ID
	binary.BigEndian.PutUint32(receiverBuf, uint32(groupKeyID)) // Receiver is Group Key ID

	toB64 := func(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
	// [first, body, tag, senderKeyID, groupKeyID]
	chunks := []string{toB64(first), toB64(body), toB64(tag), toB64(senderBuf), toB64(receiverBuf)}
	return chunks, nil
}

func (m *Manager) HasPeerPublicKey(rawKeyID int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.peerPublic[rawKeyID]
	return ok
}

// IsMyKey returns true if the given raw key ID is one of the user's own
// device keys — i.e., a private key we have via the shared keychain. Used
// to detect echoes of messages sent from another of the user's devices,
// since myRawKeyID only points to the latest one.
func (m *Manager) IsMyKey(rawKeyID int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.keyByRawID[rawKeyID]
	return ok
}

func (m *Manager) RegisterPeerPublicKey(rawKeyID int, pubB64 string) {
	normalized, err := gen.NormalizePeerPublicKeyB64(pubB64)
	if err != nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peerPublic[rawKeyID] = normalized
}

func (m *Manager) RegisterUnwrappedKey(rawKeyID, runnerKeyID int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keyByRawID[rawKeyID] = runnerKeyID
}

func (m *Manager) channelFromKeyIDs(senderKeyID, receiverKeyID int) (int, error) {
	m.mu.Lock()
	myRaw := m.myRawKeyID
	myKeyID := m.myKeyID
	peerPub := m.peerPublic
	keyByRaw := m.keyByRawID
	m.mu.Unlock()

	// Determine which side is the users's
	var privRaw, pubRaw, privKeyID int
	if senderKeyID == myRaw {
		privRaw, pubRaw = senderKeyID, receiverKeyID
		privKeyID = myKeyID
	} else if receiverKeyID == myRaw {
		privRaw, pubRaw = receiverKeyID, senderKeyID
		privKeyID = myKeyID
	} else if id, ok := keyByRaw[senderKeyID]; ok {
		privRaw, pubRaw = senderKeyID, receiverKeyID
		privKeyID = id
	} else if id, ok := keyByRaw[receiverKeyID]; ok {
		privRaw, pubRaw = receiverKeyID, senderKeyID
		privKeyID = id
	} else {
		return 0, fmt.Errorf("no matching private key for senderKeyID=%d receiverKeyID=%d", senderKeyID, receiverKeyID)
	}

	pubB64, ok := peerPub[pubRaw]
	if !ok {
		return 0, fmt.Errorf("missing peer public key for raw id %d", pubRaw)
	}

	pairKey := fmt.Sprintf("%d|%d", privRaw, pubRaw)
	m.mu.Lock()
	if id, ok := m.channelByPair[pairKey]; ok {
		m.mu.Unlock()
		return id, nil
	}
	m.mu.Unlock()

	chanID, err := m.runner.ChannelCreate(privKeyID, pubB64)
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	m.channelByPair[pairKey] = chanID
	m.mu.Unlock()
	return chanID, nil
}

func DecodeKeyID(b64 string) (int, error) {
	b, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return 0, err
	}
	if len(b) != 4 {
		return 0, fmt.Errorf("invalid key id length")
	}
	return int(binary.BigEndian.Uint32(b)), nil
}

func assembleCipher(chunks []string) ([]byte, error) {
	if len(chunks) < 3 {
		return nil, fmt.Errorf("not enough chunks")
	}
	decode := func(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(strings.TrimSpace(s)) }
	b0, err := decode(chunks[0])
	if err != nil {
		return nil, err
	}
	b1, err := decode(chunks[1])
	if err != nil {
		return nil, err
	}
	b2, err := decode(chunks[2])
	if err != nil {
		return nil, err
	}
	// Decrypted blob is [first, tag, middle], while chunks are [first, middle, tag]
	return append(append(b0, b2...), b1...), nil
}
