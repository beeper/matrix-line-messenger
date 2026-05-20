// runtime.go provides a high-level API for the transpiled LTSM module,
// mirroring the wasm.Runtime interface so callers can switch seamlessly.
package ltsm

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

// Runtime wraps the transpiled Module and embind Imports to provide
// the same high-level API as wasm.Runtime.
type Runtime struct {
	mod *Module
	imp *Imports
}

// NewRuntime creates and initializes a new transpiled LTSM runtime.
func NewRuntime() (*Runtime, error) {
	imp := NewImports()
	mod := NewModule(imp)
	imp.SetModule(mod)

	// Phase 1: __wasm_call_ctors
	mod.fP()
	// Phase 2: embind type/class registrations
	mod.fT()

	return &Runtime{mod: mod, imp: imp}, nil
}

// ModuleMem returns the linear memory buffer for direct access.
func (rt *Runtime) ModuleMem() []byte {
	return rt.mod.mem
}

// Close releases resources. No-op for the transpiled module.
func (rt *Runtime) Close() {}

// --- SecureKey ---

func (rt *Runtime) SecureKeyLoadToken(token string) (uint32, error) {
	strPtr := rt.imp.writeStdString(token)
	ptr, err := rt.imp.CallStatic("SecureKey", "loadToken", strPtr)
	if err != nil {
		return 0, fmt.Errorf("ltsm: SecureKey.loadToken failed: %w", err)
	}
	if ptr == 0 {
		return 0, fmt.Errorf("ltsm: SecureKey.loadToken returned null")
	}
	return ptr, nil
}

func (rt *Runtime) SecureKeyDeriveKey(skPtr uint32, cvHash, atHash []byte) (uint32, error) {
	cvHandle := rt.imp.WriteEmvalBytes(cvHash)
	atHandle := rt.imp.WriteEmvalBytes(atHash)
	ptr, err := rt.imp.CallMethod("SecureKey", "deriveKey", skPtr, cvHandle, atHandle)
	if err != nil {
		return 0, fmt.Errorf("ltsm: SecureKey.deriveKey failed: %w", err)
	}
	if ptr == 0 {
		return 0, fmt.Errorf("ltsm: SecureKey.deriveKey returned null")
	}
	return ptr, nil
}

func (rt *Runtime) SecureKeyUnwrapV3(nonce, kdf1, kdf2 []byte) (uint32, error) {
	ptr, err := rt.imp.CallStatic("SecureKey", "unwrapKeyFromEncryptedIdentityV3Response",
		rt.imp.WriteEmvalBytes(nonce), rt.imp.WriteEmvalBytes(kdf1), rt.imp.WriteEmvalBytes(kdf2))
	if err != nil {
		return 0, fmt.Errorf("ltsm: SecureKey.unwrapV3 failed: %w", err)
	}
	if ptr == 0 {
		return 0, fmt.Errorf("ltsm: SecureKey.unwrapV3 returned null")
	}
	return ptr, nil
}

func (rt *Runtime) SecureKeyLoadKey(keyBytes []byte) (uint32, error) {
	ptr, err := rt.imp.CallStatic("SecureKey", "loadKey", rt.imp.WriteEmvalBytes(keyBytes))
	if err != nil {
		return 0, fmt.Errorf("ltsm: SecureKey.loadKey failed: %w", err)
	}
	if ptr == 0 {
		return 0, fmt.Errorf("ltsm: SecureKey.loadKey returned null")
	}
	return ptr, nil
}

func (rt *Runtime) SecureKeyExportKey(skPtr uint32) ([]byte, error) {
	rt.imp.MarkSecureKeyExportable(skPtr)
	handle, err := rt.imp.CallMethod("SecureKey", "exportKey", skPtr)
	if err != nil {
		return nil, fmt.Errorf("ltsm: SecureKey.exportKey failed: %w", err)
	}
	data, err := rt.imp.ReadEmvalBytes(handle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: failed to read exportKey result: %w", err)
	}
	rt.imp.emval.DecRef(handle)
	return data, nil
}

func (rt *Runtime) MarkSecureKeyExportable(ptr uint32) {
	rt.imp.MarkSecureKeyExportable(ptr)
}

// --- Hmac ---

func (rt *Runtime) HmacNew(dkPtr uint32) (uint32, error) {
	ptr, err := rt.imp.Construct("Hmac", dkPtr)
	if err != nil {
		return 0, fmt.Errorf("ltsm: Hmac.new failed: %w", err)
	}
	if ptr == 0 {
		return 0, fmt.Errorf("ltsm: Hmac.new returned null")
	}
	return ptr, nil
}

func (rt *Runtime) HmacDigest(hmacPtr uint32, data []byte) ([]byte, error) {
	handle, err := rt.imp.CallMethod("Hmac", "digest", hmacPtr, rt.imp.WriteEmvalBytes(data))
	if err != nil {
		return nil, fmt.Errorf("ltsm: Hmac.digest failed: %w", err)
	}
	result, err := rt.imp.ReadEmvalBytes(handle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: failed to read digest result: %w", err)
	}
	rt.imp.emval.DecRef(handle)
	return result, nil
}

// --- AesKey ---

func (rt *Runtime) AesKeyNew(skPtr uint32) (uint32, error) {
	ptr, err := rt.imp.Construct("AesKey", skPtr)
	if err != nil {
		return 0, fmt.Errorf("ltsm: AesKey.new failed: %w", err)
	}
	if ptr == 0 {
		return 0, fmt.Errorf("ltsm: AesKey.new returned null")
	}
	return ptr, nil
}

func (rt *Runtime) AesKeyLoadKey(keyBytes []byte) (uint32, error) {
	ptr, err := rt.imp.CallStatic("AesKey", "loadKey", rt.imp.WriteEmvalBytes(keyBytes))
	if err != nil {
		return 0, fmt.Errorf("ltsm: AesKey.loadKey failed: %w", err)
	}
	if ptr == 0 {
		return 0, fmt.Errorf("ltsm: AesKey.loadKey returned null")
	}
	return ptr, nil
}

func (rt *Runtime) AesKeyEncrypt(aesPtr uint32, plaintext []byte) ([]byte, error) {
	handle, err := rt.imp.CallMethod("AesKey", "encrypt", aesPtr, rt.imp.WriteEmvalBytes(plaintext))
	if err != nil {
		return nil, fmt.Errorf("ltsm: AesKey.encrypt failed: %w", err)
	}
	data, err := rt.imp.ReadEmvalBytes(handle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: failed to read encrypt result: %w", err)
	}
	rt.imp.emval.DecRef(handle)
	return data, nil
}

func (rt *Runtime) AesKeyDecrypt(aesPtr uint32, ciphertext []byte) ([]byte, error) {
	handle, err := rt.imp.CallMethod("AesKey", "decrypt", aesPtr, rt.imp.WriteEmvalBytes(ciphertext))
	if err != nil {
		return nil, fmt.Errorf("ltsm: AesKey.decrypt failed: %w", err)
	}
	data, err := rt.imp.ReadEmvalBytes(handle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: failed to read decrypt result: %w", err)
	}
	rt.imp.emval.DecRef(handle)
	return data, nil
}

// --- Curve25519Key ---

func (rt *Runtime) Curve25519KeyNew(skPtr uint32) (uint32, error) {
	ptr, err := rt.imp.Construct("Curve25519Key", skPtr)
	if err != nil {
		return 0, fmt.Errorf("ltsm: Curve25519Key.new failed: %w", err)
	}
	if ptr == 0 {
		return 0, fmt.Errorf("ltsm: Curve25519Key.new returned null")
	}
	return ptr, nil
}

func (rt *Runtime) Curve25519KeyGetPublicKey(keyPtr uint32) ([]byte, error) {
	handle, err := rt.imp.CallMethod("Curve25519Key", "getPublicKey", keyPtr)
	if err != nil {
		return nil, fmt.Errorf("ltsm: Curve25519Key.getPublicKey failed: %w", err)
	}
	data, err := rt.imp.ReadEmvalBytes(handle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: failed to read getPublicKey result: %w", err)
	}
	rt.imp.emval.DecRef(handle)
	return data, nil
}

func (rt *Runtime) Curve25519KeyCreateChannel(keyPtr uint32, peerPub []byte) (uint32, error) {
	ptr, err := rt.imp.CallMethod("Curve25519Key", "createChannel", keyPtr, rt.imp.WriteEmvalBytes(peerPub))
	if err != nil {
		return 0, fmt.Errorf("ltsm: Curve25519Key.createChannel failed: %w", err)
	}
	if ptr == 0 {
		return 0, fmt.Errorf("ltsm: Curve25519Key.createChannel returned null")
	}
	return ptr, nil
}

func (rt *Runtime) Curve25519KeyGenerate() (uint32, error) {
	ptr, err := rt.imp.CallStatic("Curve25519Key", "generate", 1)
	if err != nil {
		return 0, fmt.Errorf("ltsm: Curve25519Key.generate failed: %w", err)
	}
	if ptr == 0 {
		return 0, fmt.Errorf("ltsm: Curve25519Key.generate returned nil")
	}
	return ptr, nil
}

// --- E2EEKey ---

func (rt *Runtime) E2EEKeyLoadKey(keyBytes []byte) (uint32, error) {
	ptr, err := rt.imp.CallStatic("E2EEKey", "loadKey", rt.imp.WriteEmvalBytes(keyBytes))
	if err != nil {
		return 0, fmt.Errorf("ltsm: E2EEKey.loadKey failed: %w", err)
	}
	if ptr == 0 {
		return 0, fmt.Errorf("ltsm: E2EEKey.loadKey returned null")
	}
	return ptr, nil
}

func (rt *Runtime) E2EEKeyExportKey(keyPtr uint32) ([]byte, error) {
	handle, err := rt.imp.CallMethod("E2EEKey", "exportKey", keyPtr)
	if err != nil {
		return nil, fmt.Errorf("ltsm: E2EEKey.exportKey failed: %w", err)
	}
	data, err := rt.imp.ReadEmvalBytes(handle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: failed to read E2EEKey.exportKey result: %w", err)
	}
	rt.imp.emval.DecRef(handle)
	return data, nil
}

func (rt *Runtime) E2EEKeyGetVersion(keyPtr uint32) (int, error) {
	result, err := rt.imp.CallMethod("E2EEKey", "getVersion", keyPtr)
	if err != nil {
		return 0, fmt.Errorf("ltsm: E2EEKey.getVersion failed: %w", err)
	}
	return int(int32(result)), nil
}

func (rt *Runtime) E2EEKeyGetKeyId(keyPtr uint32) (int, error) {
	result, err := rt.imp.CallMethod("E2EEKey", "getKeyId", keyPtr)
	if err != nil {
		return 0, fmt.Errorf("ltsm: E2EEKey.getKeyId failed: %w", err)
	}
	return int(int32(result)), nil
}

func (rt *Runtime) E2EEKeyGetPublicKey(keyPtr uint32) ([]byte, error) {
	return rt.Curve25519KeyGetPublicKey(keyPtr)
}

func (rt *Runtime) E2EEKeyCreateChannel(keyPtr uint32, peerPub []byte) (uint32, error) {
	return rt.Curve25519KeyCreateChannel(keyPtr, peerPub)
}

// --- E2EEChannel ---

func (rt *Runtime) E2EEChannelUnwrapKeyChain(chanPtr uint32, encKeyChain []byte) (uint32, error) {
	ptr, err := rt.imp.CallMethod("E2EEChannel", "unwrapE2EEKeyChain", chanPtr, rt.imp.WriteEmvalBytes(encKeyChain))
	if err != nil {
		return 0, fmt.Errorf("ltsm: E2EEChannel.unwrapE2EEKeyChain failed: %w", err)
	}
	if ptr == 0 {
		return 0, fmt.Errorf("ltsm: E2EEChannel.unwrapE2EEKeyChain returned null")
	}
	return ptr, nil
}

func (rt *Runtime) E2EEChannelUnwrapGroupSharedKey(chanPtr uint32, encKey []byte) (uint32, error) {
	ptr, err := rt.imp.CallMethod("E2EEChannel", "unwrapGroupSharedKey", chanPtr, rt.imp.WriteEmvalBytes(encKey))
	if err != nil {
		return 0, fmt.Errorf("ltsm: E2EEChannel.unwrapGroupSharedKey failed: %w", err)
	}
	if ptr == 0 {
		return 0, fmt.Errorf("ltsm: E2EEChannel.unwrapGroupSharedKey returned null")
	}
	return ptr, nil
}

func (rt *Runtime) E2EEChannelWrapGroupSharedKey(chanPtr uint32, keyHandle uint32) ([]byte, error) {
	handle, err := rt.imp.CallMethod("E2EEChannel", "wrapGroupSharedKey", chanPtr, keyHandle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: E2EEChannel.wrapGroupSharedKey failed: %w", err)
	}
	data, err := rt.imp.ReadEmvalBytes(handle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: failed to read wrapGroupSharedKey result: %w", err)
	}
	rt.imp.emval.DecRef(handle)
	return data, nil
}

func (rt *Runtime) E2EEChannelGenerateConfirmHash(chanPtr uint32, encKeyChain []byte) ([]byte, error) {
	handle, err := rt.imp.CallMethod("E2EEChannel", "generateHashKeyChainToConfirmE2EE", chanPtr, rt.imp.WriteEmvalBytes(encKeyChain))
	if err != nil {
		return nil, fmt.Errorf("ltsm: E2EEChannel.generateConfirmHash failed: %w", err)
	}
	data, err := rt.imp.ReadEmvalBytes(handle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: failed to read confirm hash result: %w", err)
	}
	rt.imp.emval.DecRef(handle)
	return data, nil
}

func (rt *Runtime) E2EEChannelEncryptV1(chanPtr uint32, plaintext []byte) ([]byte, error) {
	handle, err := rt.imp.CallMethod("E2EEChannel", "encryptV1", chanPtr, rt.imp.WriteEmvalBytes(plaintext))
	if err != nil {
		return nil, fmt.Errorf("ltsm: E2EEChannel.encryptV1 failed: %w", err)
	}
	data, err := rt.imp.ReadEmvalBytes(handle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: failed to read encryptV1 result: %w", err)
	}
	rt.imp.emval.DecRef(handle)
	return data, nil
}

func (rt *Runtime) E2EEChannelEncryptV2(chanPtr uint32,
	to, from string, senderKeyID, receiverKeyID, contentType int, seq int64, plaintext []byte) ([]byte, error) {
	toPtr := rt.imp.writeStdString(to)
	fromPtr := rt.imp.writeStdString(from)
	ptHandle := rt.imp.WriteEmvalBytes(plaintext)
	handle, err := rt.imp.CallMethod("E2EEChannel", "encryptV2", chanPtr,
		toPtr, fromPtr,
		uint32(senderKeyID), uint32(receiverKeyID), uint32(contentType),
		uint32(seq), ptHandle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: E2EEChannel.encryptV2 failed: %w", err)
	}
	data, err := rt.imp.ReadEmvalBytes(handle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: failed to read encryptV2 result: %w", err)
	}
	rt.imp.emval.DecRef(handle)
	return data, nil
}

func (rt *Runtime) E2EEChannelDecryptV1(chanPtr uint32, ciphertext []byte) ([]byte, error) {
	handle, err := rt.imp.CallMethod("E2EEChannel", "decryptV1", chanPtr, rt.imp.WriteEmvalBytes(ciphertext))
	if err != nil {
		return nil, fmt.Errorf("ltsm: E2EEChannel.decryptV1 failed: %w", err)
	}
	data, err := rt.imp.ReadEmvalBytes(handle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: failed to read decryptV1 result: %w", err)
	}
	rt.imp.emval.DecRef(handle)
	return data, nil
}

func (rt *Runtime) E2EEChannelDecryptV2(chanPtr uint32,
	to, from string, senderKeyID, receiverKeyID, contentType int, ciphertext []byte) ([]byte, error) {
	toPtr := rt.imp.writeStdString(to)
	fromPtr := rt.imp.writeStdString(from)
	ctHandle := rt.imp.WriteEmvalBytes(ciphertext)
	handle, err := rt.imp.CallMethod("E2EEChannel", "decryptV2", chanPtr,
		toPtr, fromPtr,
		uint32(senderKeyID), uint32(receiverKeyID), uint32(contentType),
		ctHandle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: E2EEChannel.decryptV2 failed: %w", err)
	}
	data, err := rt.imp.ReadEmvalBytes(handle)
	if err != nil {
		return nil, fmt.Errorf("ltsm: failed to read decryptV2 result: %w", err)
	}
	rt.imp.emval.DecRef(handle)
	return data, nil
}

// --- E2EEKeychain ---

func (rt *Runtime) E2EEKeychainSize(kcPtr uint32) (int, error) {
	result, err := rt.imp.CallMethod("E2EEKeychain", "size", kcPtr)
	if err != nil {
		return 0, fmt.Errorf("ltsm: E2EEKeychain.size failed: %w", err)
	}
	return int(result), nil
}

func (rt *Runtime) E2EEKeychainGet(kcPtr uint32, index int) (uint32, error) {
	result, err := rt.imp.CallMethod("E2EEKeychain", "get", kcPtr, uint32(index))
	if err != nil {
		return 0, fmt.Errorf("ltsm: E2EEKeychain.get failed: %w", err)
	}
	// The get method may return an emval handle wrapping the pointer
	val := rt.imp.emval.ToValue(result)
	if ptr, ok := val.(uint32); ok {
		return ptr, nil
	}
	return result, nil
}

// --- High-level operations ---

// Sign performs the full signing operation:
// loadToken → deriveKey → Hmac.new → Hmac.digest → base64 signature
func (rt *Runtime) Sign(token, clientVersion, accessToken, reqPath, body string) (string, error) {
	skPtr, err := rt.SecureKeyLoadToken(token)
	if err != nil {
		return "", err
	}

	cvHash := sha256.Sum256([]byte(clientVersion))
	atHash := sha256.Sum256([]byte(accessToken))

	dkPtr, err := rt.SecureKeyDeriveKey(skPtr, cvHash[:], atHash[:])
	if err != nil {
		return "", err
	}

	hmacPtr, err := rt.HmacNew(dkPtr)
	if err != nil {
		return "", err
	}

	dataToSign := []byte(reqPath + body)
	sigBytes, err := rt.HmacDigest(hmacPtr, dataToSign)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(sigBytes), nil
}
