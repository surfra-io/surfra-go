package surfra

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// WebhookTolerance is how far a callback's timestamp may be from the verifier's clock, either way.
const WebhookTolerance = 300 * time.Second

// WebhookSignatureHeader carries the callback signature: t=<unix seconds>,v1=<hex HMAC-SHA256>[,v1=...].
const WebhookSignatureHeader = "X-Surfra-Signature"

// webhookMaxBody caps the body WebhookHandler reads.
const webhookMaxBody = 8 << 20

var (
	// ErrWebhookSignature: no v1 signature matches any of the secrets.
	ErrWebhookSignature = errors.New("surfra: callback signature does not match")
	// ErrWebhookExpired: the timestamp is more than the tolerance away from now.
	ErrWebhookExpired = errors.New("surfra: callback timestamp is outside the tolerance")
	// ErrWebhookMalformed: the signature header (or the signed body) cannot be parsed.
	ErrWebhookMalformed = errors.New("surfra: callback signature header is malformed")
)

// CallbackStage is one stage of the task a callback reports.
type CallbackStage struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	App      string   `json:"app"`
	Status   string   `json:"status"`
	Attempt  int      `json:"attempt"`
	Progress *float64 `json:"progress"`
	Error    *string  `json:"error"`
}

// CallbackTask is the task a callback reports.
type CallbackTask struct {
	ID              string          `json:"id"`
	Status          string          `json:"status"`
	Progress        *float64        `json:"progress"`
	Metadata        map[string]any  `json:"metadata"`
	CreatedAt       string          `json:"created_at"`
	FinishedAt      *string         `json:"finished_at"`
	Stages          []CallbackStage `json:"stages"`
	Output          any             `json:"output"`
	OutputTruncated bool            `json:"output_truncated,omitempty"`
}

// Callback is a verified callback body.
type Callback struct {
	Type string       `json:"type"`
	Seq  int64        `json:"seq"`
	Task CallbackTask `json:"task"`
}

// VerifyWebhook checks the X-Surfra-Signature header of a callback against body (the exact raw bytes)
// and any of secrets, using the current time and WebhookTolerance, and returns the parsed body.
func VerifyWebhook(body []byte, header http.Header, secrets ...string) (*Callback, error) {
	return VerifyWebhookAt(body, header.Get(WebhookSignatureHeader), time.Now(), WebhookTolerance, secrets...)
}

// VerifyWebhookAt is VerifyWebhook with an explicit signature header value, clock and tolerance.
// Errors wrap ErrWebhookMalformed, ErrWebhookSignature or ErrWebhookExpired.
func VerifyWebhookAt(body []byte, signature string, now time.Time, tolerance time.Duration, secrets ...string) (*Callback, error) {
	stamp, sigs, err := parseSignature(signature)
	if err != nil {
		return nil, err
	}
	signed := make([]byte, 0, len(stamp)+1+len(body))
	signed = append(append(append(signed, stamp...), '.'), body...)
	matched := false
	for _, secret := range secrets {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(signed)
		want := mac.Sum(nil)
		for _, sig := range sigs {
			if hmac.Equal(want, sig) {
				matched = true
			}
		}
	}
	if !matched {
		return nil, ErrWebhookSignature
	}
	t, _ := strconv.ParseInt(stamp, 10, 64)
	skew := now.Sub(time.Unix(t, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > tolerance {
		return nil, fmt.Errorf("%w: timestamp %d is %s away", ErrWebhookExpired, t, skew)
	}
	var cb Callback
	if err := json.Unmarshal(body, &cb); err != nil {
		return nil, fmt.Errorf("%w: body: %v", ErrWebhookMalformed, err)
	}
	return &cb, nil
}

// parseSignature returns the t value as signed and the decoded v1 signatures. Unknown schemes are ignored.
func parseSignature(header string) (string, [][]byte, error) {
	header = strings.TrimSpace(header)
	if header == "" {
		return "", nil, fmt.Errorf("%w: empty", ErrWebhookMalformed)
	}
	stamp := ""
	var sigs [][]byte
	for _, part := range strings.Split(header, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || key == "" {
			return "", nil, fmt.Errorf("%w: part %q", ErrWebhookMalformed, part)
		}
		switch key {
		case "t":
			if stamp != "" {
				return "", nil, fmt.Errorf("%w: more than one t", ErrWebhookMalformed)
			}
			if _, err := strconv.ParseInt(value, 10, 64); err != nil || value == "" || value[0] == '+' || value[0] == '-' {
				return "", nil, fmt.Errorf("%w: t %q", ErrWebhookMalformed, value)
			}
			stamp = value
		case "v1":
			sig, err := hex.DecodeString(value)
			if err != nil || len(sig) != sha256.Size {
				return "", nil, fmt.Errorf("%w: v1 %q", ErrWebhookMalformed, value)
			}
			sigs = append(sigs, sig)
		}
	}
	if stamp == "" {
		return "", nil, fmt.Errorf("%w: no t", ErrWebhookMalformed)
	}
	if len(sigs) == 0 {
		return "", nil, fmt.Errorf("%w: no v1", ErrWebhookMalformed)
	}
	return stamp, sigs, nil
}

// WebhookHandler reads a callback (up to 8 MiB), verifies it against secrets and calls handle with the
// parsed body; r.Body is replaced with the raw bytes. A bad signature gets 401, an oversized body 413.
func WebhookHandler(secrets []string, handle func(w http.ResponseWriter, r *http.Request, cb *Callback)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, webhookMaxBody))
		if err != nil {
			var tooBig *http.MaxBytesError
			if errors.As(err, &tooBig) {
				http.Error(w, "callback body too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "cannot read callback body", http.StatusBadRequest)
			return
		}
		cb, err := VerifyWebhook(body, r.Header, secrets...)
		if err != nil {
			http.Error(w, "invalid callback signature", http.StatusUnauthorized)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		handle(w, r, cb)
	})
}
