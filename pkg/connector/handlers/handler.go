package handlers

import (
	"context"
	"net/http"
	"strings"

	"github.com/rs/zerolog"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// Handler provides dependencies needed by content type conversion functions.
type Handler struct {
	Log        zerolog.Logger
	HTTPClient *http.Client

	// RecoverToken attempts to restore a valid session by refreshing or re-logging in.
	RecoverToken      func(ctx context.Context) error
	IsRefreshRequired func(err error) bool
	IsLoggedOut       func(err error) bool

	// NewClient creates a new LINE API client with the current access token.
	NewClient func() *line.Client

	// DecryptMedia decrypts E2EE encrypted media data using the given key material.
	DecryptMedia func(data []byte, keyMaterial string) ([]byte, error)
}

// tryRecoverClient attempts token recovery on auth errors and returns a fresh client.
// Returns (newClient, true) on success, (nil, false) if recovery was not needed or failed.
func (h *Handler) tryRecoverClient(ctx context.Context, err error) (*line.Client, bool) {
	if err == nil {
		return nil, false
	}
	isOBSAuthFailure := strings.Contains(err.Error(), "OBS download failed (401)")
	if isOBSAuthFailure {
		line.ClearEncryptedAccessTokenCache()
		return h.NewClient(), true
	}
	if !strings.Contains(err.Error(), "401") && !h.IsRefreshRequired(err) && !h.IsLoggedOut(err) {
		return nil, false
	}
	if errRecover := h.RecoverToken(ctx); errRecover != nil {
		h.Log.Warn().Err(errRecover).Msg("Failed to recover token for media download")
		return nil, false
	}
	return h.NewClient(), true
}
