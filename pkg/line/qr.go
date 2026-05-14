package line

import (
	"fmt"
	"net/url"
)

// QRCodeCallbackURLWithE2EESecret returns the Chrome-style QR callback URL.
// The QR service returns a bare callbackUrl, but the extension appends the
// login Curve25519 public key so mobile can encrypt the login keychain.
func QRCodeCallbackURLWithE2EESecret(callbackURL, publicKeyBase64 string) (string, error) {
	parsed, err := url.Parse(callbackURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse QR callback URL: %w", err)
	}
	query := parsed.Query()
	query.Set("secret", publicKeyBase64)
	query.Set("e2eeVersion", "1")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
