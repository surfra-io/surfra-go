package surfra

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func sign(secret string, t int64, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.%s", t, body)
	return "t=" + strconv.FormatInt(t, 10) + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}

func TestVerifyWebhookErrors(t *testing.T) {
	body := `{"type":"task.updated","seq":7,"task":{"id":"tx_1","status":"running"}}`
	now := time.Unix(1761415391, 0)
	good := sign("s", now.Unix(), body)
	for name, c := range map[string]struct {
		sig, body string
		want      error
	}{
		"malformed":   {"garbage", body, ErrWebhookMalformed},
		"bad hex":     {"t=1761415391,v1=zz", body, ErrWebhookMalformed},
		"wrong sig":   {sign("other", now.Unix(), body), body, ErrWebhookSignature},
		"expired":     {sign("s", now.Unix()-301, body), body, ErrWebhookExpired},
		"body is bad": {sign("s", now.Unix(), "nope"), "nope", ErrWebhookMalformed},
	} {
		if _, err := VerifyWebhookAt([]byte(c.body), c.sig, now, WebhookTolerance, "s"); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", name, err, c.want)
		}
	}
	cb, err := VerifyWebhookAt([]byte(body), good, now, WebhookTolerance, "old", "s")
	if err != nil || cb.Seq != 7 || cb.Task.ID != "tx_1" {
		t.Fatalf("valid: %v %+v", err, cb)
	}
}

func TestWebhookHandler(t *testing.T) {
	body := `{"type":"task.updated","seq":7,"task":{"id":"tx_1","status":"succeeded"}}`
	called := 0
	server := httptest.NewServer(WebhookHandler([]string{"whsec_a"}, func(w http.ResponseWriter, r *http.Request, cb *Callback) {
		called++
		raw, _ := io.ReadAll(r.Body)
		if cb.Seq != 7 || string(raw) != body {
			t.Errorf("handler got seq %d body %q", cb.Seq, raw)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	post := func(sig, payload string) int {
		req, _ := http.NewRequest(http.MethodPost, server.URL, strings.NewReader(payload))
		req.Header.Set("X-Surfra-Signature", sig)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	sig := sign("whsec_a", time.Now().Unix(), body)
	if code := post(sig, body); code != http.StatusNoContent || called != 1 {
		t.Fatalf("valid: status %d, called %d", code, called)
	}
	if code := post(sig, strings.Replace(body, "succeeded", "failed", 1)); code != http.StatusUnauthorized || called != 1 {
		t.Fatalf("tampered: status %d, called %d", code, called)
	}
	if code := post("", body); code != http.StatusUnauthorized || called != 1 {
		t.Fatalf("unsigned: status %d, called %d", code, called)
	}
}
