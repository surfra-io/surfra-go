package surfra

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The retry cases of the shared contract (surfra-sdk-tests/CASES.md, "Retries"), against the fake server's
// injected faults (POST /_test/faults).

func refusal(method, path string, status int, kind string, retryAfter *int) map[string]any {
	errBody := map[string]any{"type": kind, "message": strings.ToLower(kind)}
	fault := map[string]any{"method": method, "path": path, "status": status, "body": map[string]any{"detail": strings.ToLower(kind), "error": errBody}}
	if retryAfter != nil {
		errBody["retry_after"] = *retryAfter
		fault["headers"] = map[string]string{"Retry-After": strconv.Itoa(*retryAfter)}
	}
	return fault
}

func seconds(n int) *int { return &n }

func (f *fake) faults(items ...map[string]any) {
	f.post("/_test/faults", map[string]any{"faults": items})
}

func (f *fake) count(method, prefix string) int {
	n := 0
	for _, item := range f.requests() {
		if item.Method == method && strings.HasPrefix(item.Path, prefix) {
			n++
		}
	}
	return n
}

func asError(t *testing.T, err error) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("want *surfra.Error, got %T %v", err, err)
	}
	return e
}

func TestRetryConformance(t *testing.T) {
	ctx := context.Background()
	echo := func(t *testing.T, out any, want string) {
		t.Helper()
		if got, _ := json.Marshal(out); string(got) != want {
			t.Fatalf("output %s, want %s", got, want)
		}
	}
	cases := map[string]func(t *testing.T){
		"r01_rate_limited_waits_retry_after_then_succeeds": func(t *testing.T) {
			f := start(t)
			f.faults(refusal("POST", "/v1/transactions", 429, "RATE_LIMITED", seconds(1)))
			started := time.Now()
			out, err := NewClientWith(f.settings()).Run(ctx, "embed", map[string]any{"n": 1})
			check(t, err)
			echo(t, out, `{"app":"embed","echo":{"n":1}}`)
			if time.Since(started) < 900*time.Millisecond || f.count("POST", "/v1/transactions") != 2 {
				t.Fatalf("waited %v, %d submits", time.Since(started), f.count("POST", "/v1/transactions"))
			}
		},
		"r02_queue_full_is_retried": func(t *testing.T) {
			f := start(t)
			f.faults(refusal("POST", "/v1/transactions", 503, "QUEUE_FULL", seconds(0)))
			out, err := NewClientWith(f.settings()).Run(ctx, "embed", map[string]any{})
			check(t, err)
			echo(t, out, `{"app":"embed","echo":{}}`)
			if n := f.count("POST", "/v1/transactions"); n != 2 {
				t.Fatalf("%d submits", n)
			}
		},
		"r03_server_busy_on_a_get_is_retried": func(t *testing.T) {
			f := start(t)
			call, err := NewClientWith(f.settings()).Enqueue(ctx, "embed", map[string]any{"n": 3})
			check(t, err)
			f.faults(refusal("GET", "/v1/transactions/", 503, "SERVER_BUSY", seconds(0)))
			out, err := call.Result(ctx)
			check(t, err)
			echo(t, out, `{"app":"embed","echo":{"n":3}}`)
		},
		"r04_app_paused_is_not_retried": func(t *testing.T) {
			f := start(t)
			f.faults(refusal("POST", "/v1/transactions", 503, "APP_PAUSED", nil))
			_, err := NewClientWith(f.settings()).Enqueue(ctx, "embed", map[string]any{})
			e := asError(t, err)
			if e.Status != 503 || e.Type != "APP_PAUSED" || e.Attempts != 1 || f.count("POST", "/v1/transactions") != 1 {
				t.Fatalf("%+v", e)
			}
		},
		"r05_defcon_active_is_not_retried": func(t *testing.T) {
			f := start(t)
			f.faults(refusal("POST", "/v1/transactions", 503, "DEFCON_ACTIVE", nil))
			_, err := NewClientWith(f.settings()).Enqueue(ctx, "embed", map[string]any{})
			if e := asError(t, err); e.Type != "DEFCON_ACTIVE" || f.count("POST", "/v1/transactions") != 1 {
				t.Fatalf("%+v", e)
			}
		},
		"r13_http_calls_limit_is_not_retried": func(t *testing.T) {
			f := start(t)
			f.faults(refusal("POST", "/v1/transactions", 402, "HTTP_CALLS_LIMIT", nil))
			_, err := NewClientWith(f.settings()).Enqueue(ctx, "embed", map[string]any{})
			e := asError(t, err)
			if e.Status != 402 || e.Type != "HTTP_CALLS_LIMIT" || e.Attempts != 1 || e.Message == "" || f.count("POST", "/v1/transactions") != 1 {
				t.Fatalf("%+v", e)
			}
		},
		"r06_a_plain_502_is_retried_on_get_only": func(t *testing.T) {
			f := start(t)
			client := NewClientWith(f.settings())
			call, err := client.Enqueue(ctx, "embed", map[string]any{})
			check(t, err)
			f.faults(map[string]any{"method": "GET", "path": "/v1/transactions/", "status": 502, "raw": "<html>bad gateway</html>"})
			out, err := call.Result(ctx)
			check(t, err)
			echo(t, out, `{"app":"embed","echo":{}}`)
			f.faults(map[string]any{"method": "POST", "path": "/v1/transactions", "status": 502, "raw": "<html>bad gateway</html>"})
			_, err = client.Enqueue(ctx, "embed", map[string]any{})
			if e := asError(t, err); e.Status != 502 || e.Type != "" || e.OutcomeUnknown || f.count("POST", "/v1/transactions") != 2 {
				t.Fatalf("%+v", e)
			}
		},
		"r07_a_plain_504_on_a_submit_is_an_unknown_outcome": func(t *testing.T) {
			f := start(t)
			f.faults(map[string]any{"method": "POST", "path": "/v1/transactions", "status": 504, "raw": "upstream timed out"})
			_, err := NewClientWith(f.settings()).Enqueue(ctx, "embed", map[string]any{})
			e := asError(t, err)
			if !errors.Is(err, ErrOutcomeUnknown) || e.Status != 504 || !strings.Contains(err.Error(), "may be running") || f.count("POST", "/v1/transactions") != 1 {
				t.Fatalf("%v %+v", err, e)
			}
		},
		"r08_a_dropped_connection_is_retried_on_get_only": func(t *testing.T) {
			f := start(t)
			client := NewClientWith(f.settings())
			call, err := client.Enqueue(ctx, "embed", map[string]any{})
			check(t, err)
			f.faults(map[string]any{"method": "GET", "path": "/v1/transactions/", "drop": true})
			out, err := call.Result(ctx)
			check(t, err)
			echo(t, out, `{"app":"embed","echo":{}}`)
			f.faults(map[string]any{"method": "POST", "path": "/v1/transactions", "drop": true})
			_, err = client.Enqueue(ctx, "embed", map[string]any{})
			if !errors.Is(err, ErrOutcomeUnknown) || !strings.Contains(err.Error(), "may be running") || f.count("POST", "/v1/transactions") != 2 {
				t.Fatalf("%v", err)
			}
		},
		"r09_a_400_is_not_retried": func(t *testing.T) {
			f := start(t)
			f.faults(map[string]any{"method": "POST", "path": "/v1/transactions", "status": 400, "body": map[string]any{"detail": "stages must be a list"}})
			_, err := NewClientWith(f.settings()).Enqueue(ctx, "embed", map[string]any{})
			if e := asError(t, err); e.Status != 400 || e.Attempts != 1 || f.count("POST", "/v1/transactions") != 1 {
				t.Fatalf("%+v", e)
			}
		},
		"r10_a_refused_connection_is_retried": func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			check(t, err)
			addr := listener.Addr().String()
			listener.Close()
			var mu sync.Mutex
			var attempts []int
			policy := RetryPolicy{MaxRetries: Retries(2), OnRetry: func(event RetryEvent) {
				mu.Lock()
				attempts = append(attempts, event.Attempt)
				mu.Unlock()
			}}
			client := NewClientWith(Settings{Master: "http://" + addr, EnvAPIKey: "sfk_test", Retry: policy})
			_, err = client.Enqueue(ctx, "embed", map[string]any{})
			e := asError(t, err)
			if e.Attempts != 3 || e.OutcomeUnknown || len(attempts) != 2 || attempts[0] != 1 || attempts[1] != 2 {
				t.Fatalf("%+v %v", e, attempts)
			}
		},
		"r11_retries_stop_after_max_retries": func(t *testing.T) {
			f := start(t)
			fault := refusal("POST", "/v1/transactions", 503, "SERVER_BUSY", seconds(0))
			fault["times"] = 10
			f.faults(fault)
			_, err := NewClientWith(f.settings()).WithRetry(RetryPolicy{MaxRetries: Retries(2)}).Enqueue(ctx, "embed", map[string]any{})
			if e := asError(t, err); e.Type != "SERVER_BUSY" || e.Attempts != 3 || f.count("POST", "/v1/transactions") != 3 {
				t.Fatalf("%+v", e)
			}
		},
		"r12_a_wait_past_the_deadline_is_not_taken": func(t *testing.T) {
			f := start(t)
			f.faults(refusal("POST", "/v1/transactions", 429, "RATE_LIMITED", seconds(30)))
			started := time.Now()
			_, err := NewClientWith(f.settings()).Enqueue(WithRetryPolicy(ctx, RetryPolicy{Deadline: 2 * time.Second}), "embed", map[string]any{})
			e := asError(t, err)
			if time.Since(started) > 1500*time.Millisecond || e.Type != "RATE_LIMITED" || e.RetryAfter != 30*time.Second || e.Attempts != 1 {
				t.Fatalf("%v %+v", time.Since(started), e)
			}
		},
	}
	for _, name := range keys(cases) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cases[name](t)
		})
	}
}
