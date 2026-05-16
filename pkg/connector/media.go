package connector

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"io"
	"os"

	ffmpeg "github.com/u2takey/ffmpeg-go"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/image/draw"
)

// AES-256-CTR
// LINE's E2EE file format: [encrypted_data][32-byte HMAC]
// The keyMaterial is derived using HKDF to get encKey (32), macKey (32), and nonce (12 bytes)
func (lc *LineClient) decryptImageData(encryptedData []byte, keyMaterialB64 string) ([]byte, error) {
	keyMaterial, err := base64.StdEncoding.DecodeString(keyMaterialB64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode key material: %w", err)
	}

	// Derive keys using HKDF (SHA-256, no salt, info="FileEncryption")
	// Derives 76 bytes: encKey(32) + macKey(32) + nonce(12)
	kdf := hkdf.New(sha256.New, keyMaterial, nil, []byte("FileEncryption"))
	derived := make([]byte, 76)
	if _, err := io.ReadFull(kdf, derived); err != nil {
		return nil, fmt.Errorf("failed to derive keys: %w", err)
	}

	encKey := derived[0:32]
	// macKey := derived[32:64] // for HMAC verification
	nonce := derived[64:76]

	// Create 16-byte counter: nonce(12 bytes) + zero counter(4 bytes)
	counter := make([]byte, 16)
	copy(counter, nonce)
	// Last 4 bytes remain zero

	if len(encryptedData) < 32 {
		return nil, fmt.Errorf("encrypted data too short (< 32 bytes for HMAC)")
	}
	encryptedData = encryptedData[:len(encryptedData)-32]

	block, err := aes.NewCipher(encKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	stream := cipher.NewCTR(block, counter)

	decrypted := make([]byte, len(encryptedData))
	stream.XORKeyStream(decrypted, encryptedData)

	return decrypted, nil
}

// AES-256-CTR
func (lc *LineClient) encryptFileData(plainData []byte) ([]byte, string, error) {
	keyMaterial := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, keyMaterial); err != nil {
		return nil, "", fmt.Errorf("failed to generate key material: %w", err)
	}

	// Derive keys using HKDF (SHA-256, no salt, info="FileEncryption")
	// Derives 76 bytes: encKey(32) + macKey(32) + nonce(12)
	kdf := hkdf.New(sha256.New, keyMaterial, nil, []byte("FileEncryption"))
	derived := make([]byte, 76)
	if _, err := io.ReadFull(kdf, derived); err != nil {
		return nil, "", fmt.Errorf("failed to derive keys: %w", err)
	}

	encKey := derived[0:32]
	macKey := derived[32:64]
	nonce := derived[64:76]

	// Create 16-byte counter: nonce(12 bytes) + zero counter(4 bytes)
	counter := make([]byte, 16)
	copy(counter, nonce)

	block, err := aes.NewCipher(encKey)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create AES cipher: %w", err)
	}

	stream := cipher.NewCTR(block, counter)

	encrypted := make([]byte, len(plainData))
	stream.XORKeyStream(encrypted, plainData)

	h := hmac.New(sha256.New, macKey)
	h.Write(encrypted)
	hmacSum := h.Sum(nil)

	// LINE E2EE file format: [encrypted_data][32-byte HMAC]
	result := append(encrypted, hmacSum...)

	keyMaterialB64 := base64.StdEncoding.EncodeToString(keyMaterial)

	return result, keyMaterialB64, nil
}

// encryptVideoData encrypts video data with HMAC computed on chunk hashes
func (lc *LineClient) encryptVideoData(plainData []byte) ([]byte, string, error) {
	keyMaterial := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, keyMaterial); err != nil {
		return nil, "", fmt.Errorf("failed to generate key material: %w", err)
	}

	kdf := hkdf.New(sha256.New, keyMaterial, nil, []byte("FileEncryption"))
	derived := make([]byte, 76)
	if _, err := io.ReadFull(kdf, derived); err != nil {
		return nil, "", fmt.Errorf("failed to derive keys: %w", err)
	}

	encKey := derived[0:32]
	macKey := derived[32:64]
	nonce := derived[64:76]

	counter := make([]byte, 16)
	copy(counter, nonce)

	block, err := aes.NewCipher(encKey)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create AES cipher: %w", err)
	}

	stream := cipher.NewCTR(block, counter)

	encrypted := make([]byte, len(plainData))
	stream.XORKeyStream(encrypted, plainData)

	// For videos: compute HMAC on chunk hashes
	chunkHashes := generateChunkHashes(encrypted)
	h := hmac.New(sha256.New, macKey)
	h.Write(chunkHashes)
	hmacSum := h.Sum(nil)

	result := append(encrypted, hmacSum...)
	keyMaterialB64 := base64.StdEncoding.EncodeToString(keyMaterial)

	return result, keyMaterialB64, nil
}

func generateThumbnail(imageData []byte) ([]byte, int, int, error) {
	img, _, err := image.Decode(bytes.NewReader(imageData))
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to decode image: %w", err)
	}

	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()

	maxDim := 1280
	newWidth := width
	newHeight := height

	if width > maxDim || height > maxDim {
		if width > height {
			newWidth = maxDim
			newHeight = (height * maxDim) / width
		} else {
			newHeight = maxDim
			newWidth = (width * maxDim) / height
		}
	}

	var thumbnail image.Image
	if newWidth != width || newHeight != height {
		thumbnail = image.NewRGBA(image.Rect(0, 0, newWidth, newHeight))
		draw.CatmullRom.Scale(thumbnail.(draw.Image), thumbnail.Bounds(), img, bounds, draw.Over, nil)
	} else {
		thumbnail = img
	}

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, thumbnail, &jpeg.Options{Quality: 60}); err != nil {
		return nil, 0, 0, fmt.Errorf("failed to encode thumbnail: %w", err)
	}

	return buf.Bytes(), newWidth, newHeight, nil
}

// encryptThumbnail encrypts thumbnail data using the same key material as the parent media.
// Returns the encrypted thumbnail with HMAC appended, matching LINE's E2EE thumbnail format.
func encryptThumbnail(thumbnailData []byte, keyMaterialB64 string) ([]byte, error) {
	keyMaterial, err := base64.StdEncoding.DecodeString(keyMaterialB64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode key material: %w", err)
	}

	kdf := hkdf.New(sha256.New, keyMaterial, nil, []byte("FileEncryption"))
	derived := make([]byte, 76)
	if _, err := io.ReadFull(kdf, derived); err != nil {
		return nil, fmt.Errorf("failed to derive keys: %w", err)
	}

	encKey := derived[0:32]
	macKey := derived[32:64]
	nonce := derived[64:76]

	counter := make([]byte, 16)
	copy(counter, nonce)

	block, err := aes.NewCipher(encKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}
	stream := cipher.NewCTR(block, counter)

	encrypted := make([]byte, len(thumbnailData))
	stream.XORKeyStream(encrypted, thumbnailData)

	h := hmac.New(sha256.New, macKey)
	h.Write(encrypted)
	return append(encrypted, h.Sum(nil)...), nil
}

func isAnimatedGif(data []byte) bool {
	// GIF header: "GIF89a" or "GIF87a"
	if len(data) < 6 {
		return false
	}

	if string(data[0:3]) != "GIF" {
		return false
	}

	// Count image descriptors (0x2C) which indicate frames
	frameCount := 0
	for i := 0; i < len(data)-1; i++ {
		if data[i] == 0x2C { // Image descriptor separator
			frameCount++
			if frameCount > 1 {
				return true
			}
		}
	}

	return false
}

// convertVideoToGIF converts video data (mp4/webm) to an animated GIF using ffmpeg.
func convertVideoToGIF(videoData []byte) ([]byte, error) {
	tmpVideoFile, err := os.CreateTemp("", "video-*.mp4")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp video file: %w", err)
	}
	defer os.Remove(tmpVideoFile.Name())

	if _, err := tmpVideoFile.Write(videoData); err != nil {
		tmpVideoFile.Close()
		return nil, fmt.Errorf("failed to write video data: %w", err)
	}
	tmpVideoFile.Close()

	tmpGIFFile, err := os.CreateTemp("", "gif-*.gif")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp GIF file: %w", err)
	}
	defer os.Remove(tmpGIFFile.Name())
	tmpGIFFile.Close()

	err = ffmpeg.Input(tmpVideoFile.Name()).
		Output(tmpGIFFile.Name(), ffmpeg.KwArgs{
			"vf": "fps=15,scale=320:-1:flags=lanczos",
		}).
		OverWriteOutput().
		Silent(true).
		Run()
	if err != nil {
		return nil, fmt.Errorf("ffmpeg video-to-gif failed: %w", err)
	}

	gifData, err := os.ReadFile(tmpGIFFile.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to read GIF output: %w", err)
	}

	return gifData, nil
}

// generates the first frame of a video and resizes it to fit within 384x384
func extractVideoThumbnail(videoData []byte) ([]byte, int, int, error) {
	tmpVideoFile, err := os.CreateTemp("", "video-*.mp4")
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to create temp video file: %w", err)
	}
	defer os.Remove(tmpVideoFile.Name())

	if _, err := tmpVideoFile.Write(videoData); err != nil {
		return nil, 0, 0, fmt.Errorf("failed to write video data: %w", err)
	}
	tmpVideoFile.Close()

	tmpThumbFile, err := os.CreateTemp("", "thumb-*.jpg")
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to create temp thumb file: %w", err)
	}
	defer os.Remove(tmpThumbFile.Name())
	tmpThumbFile.Close()

	err = ffmpeg.Input(tmpVideoFile.Name()).
		Filter("scale", ffmpeg.Args{"384:384:force_original_aspect_ratio=decrease"}).
		Output(tmpThumbFile.Name(), ffmpeg.KwArgs{
			"vframes": 1,
			"q:v":     5,
		}).
		OverWriteOutput().
		Silent(true).
		Run()

	if err != nil {
		return nil, 0, 0, fmt.Errorf("ffmpeg failed: %w", err)
	}

	thumbData, err := os.ReadFile(tmpThumbFile.Name())
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to read thumbnail: %w", err)
	}

	img, _, err := image.Decode(bytes.NewReader(thumbData))
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to decode thumbnail: %w", err)
	}

	bounds := img.Bounds()
	return thumbData, bounds.Dx(), bounds.Dy(), nil
}

// generateChunkHashes generates SHA-256 hashes for 128KB chunks of encrypted video data
// This is required by LINE for video integrity verification
func generateChunkHashes(encryptedData []byte) []byte {
	const chunkSize = 131072 // 128KB chunks
	var allHashes []byte

	for i := 0; i < len(encryptedData); i += chunkSize {
		end := i + chunkSize
		if end > len(encryptedData) {
			end = len(encryptedData)
		}

		chunk := encryptedData[i:end]
		hash := sha256.Sum256(chunk)
		allHashes = append(allHashes, hash[:]...)
	}

	return allHashes
}
