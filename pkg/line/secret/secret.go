package secret

import (
	"fmt"

	gen "github.com/highesttt/matrix-line-messenger/pkg"
)

type SecretResult struct {
	Secret          string `json:"secret"`
	Pin             string `json:"pin"`
	PublicKeyHex    string `json:"publicKeyHex"`
	PublicKeyBase64 string `json:"publicKeyBase64"`
	LoginKeyID      int    `json:"loginKeyId"`
}

// GenerateSecret creates the LINE login E2EE secret and returns the matching
// local login key ID for the follow-up keychain unwrap.
func GenerateSecret() (*SecretResult, error) {
	runner, err := gen.GetRunner()
	if err != nil {
		return nil, fmt.Errorf("failed to get runner: %w", err)
	}

	res, err := runner.GenerateE2EESecret()
	if err != nil {
		return nil, fmt.Errorf("failed to generate secret: %w", err)
	}

	return &SecretResult{
		Secret:          res.Secret,
		Pin:             res.Pin,
		PublicKeyHex:    res.PublicKeyHex,
		PublicKeyBase64: res.PublicKeyBase64,
		LoginKeyID:      res.LoginKeyID,
	}, nil
}
