package line

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNoUsableE2EEPublicKey = errors.New("no usable E2EE public key")
	ErrNoUsableE2EEGroupKey  = errors.New("no usable E2EE group key")
	ErrGroupKeyNotFound      = errors.New("group key not found")
)

// IsNoUsableE2EEPublicKey returns true when a peer has Letter Sealing disabled
// (negotiateE2EEPublicKey returns empty allowedTypes / specVersion -1, or no key data).
func IsNoUsableE2EEPublicKey(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNoUsableE2EEPublicKey) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "missing fields (pub=false keyID=-1") ||
		strings.Contains(msg, "missing fields (pub=false keyID=0") ||
		(strings.Contains(msg, "\"allowedTypes\":[]") && strings.Contains(msg, "\"specVersion\":-1"))
}

// IsGroupKeyNotFound returns true when the error is specifically code 5 "not found"
// from getE2EEGroupSharedKey / getLastE2EEGroupSharedKey — meaning no group key has been
// registered yet, but E2EE is supported. Callers should attempt to register a key.
// Matches both the processed error (ErrGroupKeyNotFound) and the raw HTTP 400 error
// from callRPC which contains the TalkException JSON payload.
func IsGroupKeyNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrGroupKeyNotFound) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "group key not found: not found") {
		return true
	}
	// Match raw API error: HTTP 400 with TalkException code 5 "not found"
	return strings.Contains(msg, "\"code\":10051") &&
		strings.Contains(msg, "talkexception") &&
		(strings.Contains(msg, "\"code\":5,") || strings.Contains(msg, "\"code\":5}"))
}

// IsNoUsableE2EEGroupKey returns true when a group has no shared E2EE key
// (at least one member has Letter Sealing disabled).
func IsNoUsableE2EEGroupKey(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNoUsableE2EEGroupKey) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "no group key found") ||
		strings.Contains(msg, "no group shared key returned") {
		return true
	}
	// Detect TalkException codes in raw API error strings (HTTP 400 with code 10051).
	// Code 98 = member has LS off; Code 1 = auth failed.
	// NOTE: Code 5 "not found" is handled by IsGroupKeyNotFound (auto-register), NOT here.
	if strings.Contains(msg, "\"code\":10051") && strings.Contains(msg, "talkexception") {
		if strings.Contains(msg, "\"code\":98,") || strings.Contains(msg, "\"code\":98}") ||
			strings.Contains(msg, "\"code\":1,") || strings.Contains(msg, "\"code\":1}") {
			return true
		}
	}
	return false
}

type talkExceptionData struct {
	Name    string `json:"name"`
	Message string `json:"message"`
	Code    int    `json:"code"`
	Reason  string `json:"reason"`
}

// IsGroupKeyNotRegisteredError returns true when SendMessage returns code 99
// "group key is not registered". This means a group key must be registered before
// sending any message (even plain text) to this group.
func IsGroupKeyNotRegisteredError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "\"code\":99,") &&
		strings.Contains(msg, "group key is not registered")
}

// IsNotAMemberError returns true when the API reports the user is not a member of a chat.
func IsNotAMemberError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "\"code\":10051") &&
		strings.Contains(msg, "talkexception") &&
		strings.Contains(msg, "\"code\":10,") &&
		strings.Contains(msg, "\"not a member\"")
}

func isNoUsableE2EEGroupKeyTalkException(message string, data talkExceptionData) bool {
	if !strings.EqualFold(message, "RESPONSE_ERROR") || !strings.EqualFold(data.Name, "TalkException") {
		return false
	}
	// Error 5 "not found" = no group shared key exists
	// Error 98 "member settings off" = at least one member has LS disabled
	return (data.Code == 5 && strings.EqualFold(data.Reason, "not found")) ||
		(data.Code == 98 && strings.Contains(strings.ToLower(data.Reason), "member settings off"))
}

func parseTalkExceptionData(raw json.RawMessage) talkExceptionData {
	var data talkExceptionData
	_ = json.Unmarshal(raw, &data)
	return data
}

func parseE2EEGroupKeyError(method, message string, rawData json.RawMessage) error {
	talk := parseTalkExceptionData(rawData)
	if isNoUsableE2EEGroupKeyTalkException(message, talk) {
		if talk.Code == 5 {
			return fmt.Errorf("%w: %s", ErrGroupKeyNotFound, talk.Reason)
		}
		return fmt.Errorf("%w: %s", ErrNoUsableE2EEGroupKey, talk.Reason)
	}
	return fmt.Errorf("%s failed: %s", method, message)
}
