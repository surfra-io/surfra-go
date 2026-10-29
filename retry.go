package surfra

// One retry loop for every control-plane and volume request, as in the shared SDK contract
// (surfra-sdk-tests/CASES.md, "Retries").

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	defaultMaxRetries    = 7
	defaultRetryDeadline = 3 * time.Minute
	backoffBase          = 500 * time.Millisecond
	backoffCap           = 30 * time.Second
)

// RetryPolicy limits how Surfra requests are retried. The zero value means Surfra's defaults: up to 7
// retries (8 attempts) within 3 minutes of the first attempt, reported only through slog at Debug level.
//
// Set it per client with Settings.Retry or Client.WithRetry, and per call with WithRetryPolicy. A per-call
// policy overrides the client's field by field: fields left unset fall back to the client's, then to the
// defaults.
type RetryPolicy struct {
	// MaxRetries is how many times a request may be repeated. nil means the default, 7; Retries(0) turns
	// retrying off.
	MaxRetries *int
	// Deadline bounds the whole request, retries and waits included, from the first attempt. 0 means the
	// default, 3 minutes; a negative value means no limit other than ctx. A wait that would end past the
	// deadline (or past ctx's deadline) is not taken: the last error is returned at once.
	Deadline time.Duration
	// OnRetry, when set, is called before each wait. Nothing else is reported, except a slog Debug record.
	OnRetry func(RetryEvent)
}

// Retries is a MaxRetries value: RetryPolicy{MaxRetries: surfra.Retries(0)} never retries.
func Retries(n int) *int { return &n }

// NoRetries is a policy that sends every request once.
func NoRetries() RetryPolicy { return RetryPolicy{MaxRetries: Retries(0)} }

// RetryEvent describes one retry: the attempt that failed, why, and how long the SDK waits before the next.
type RetryEvent struct {
	Attempt int           // the attempt that failed, from 1
	Method  string        // the request's HTTP method
	URL     string        // the request's URL, without its query
	Status  int           // the HTTP status, or 0 for a connection error
	Type    string        // Surfra's error.type, such as QUEUE_FULL, or ""
	Err     error         // the failure: an *Error
	Wait    time.Duration // the wait before the next attempt
	Reason  string        // a short description, such as "503 QUEUE_FULL" or "connection refused"
}

type retryKey struct{}

// WithRetryPolicy is ctx carrying a retry policy for every Surfra call made with it, overriding the
// client's policy field by field.
func WithRetryPolicy(ctx context.Context, p RetryPolicy) context.Context {
	return context.WithValue(ctx, retryKey{}, p)
}

type limits struct {
	maxRetries int
	deadline   time.Duration
	onRetry    func(RetryEvent)
}

func resolve(ctx context.Context, client RetryPolicy) limits {
	l := limits{maxRetries: defaultMaxRetries, deadline: defaultRetryDeadline}
	apply := func(p RetryPolicy) {
		if p.MaxRetries != nil {
			l.maxRetries = max(0, *p.MaxRetries)
		}
		if p.Deadline != 0 {
			l.deadline = p.Deadline
		}
		if p.OnRetry != nil {
			l.onRetry = p.OnRetry
		}
	}
	apply(client)
	if p, ok := ctx.Value(retryKey{}).(RetryPolicy); ok {
		apply(p)
	}
	return l
}

// ErrOutcomeUnknown matches (with errors.Is) a submission that got a 504 or lost its connection after it
// was sent: the work may be running. Check it, or submit with Enqueue and poll Result.
var ErrOutcomeUnknown = errors.New("surfra: outcome unknown, the submission may be running")

// IsRetryable reports whether err is a failure Surfra or the network said may be repeated, returned
// because the attempts or the deadline ran out.
func IsRetryable(err error) bool {
	var apiErr *Error
	return errors.As(err, &apiErr) && apiErr.retryable
}

// verdict is how a failed attempt may be retried.
type verdict int

const (
	never    verdict = iota
	anyKind          // safe to repeat with any method
	readOnly         // safe to repeat for GET and HEAD only
)

func idempotent(method string) bool { return method == http.MethodGet || method == http.MethodHead }

// classifyStatus sorts an error answer by the contract's table.
func classifyStatus(e *Error) verdict {
	switch e.Type {
	case "RATE_LIMITED", "QUEUE_FULL", "SERVER_BUSY":
		return anyKind
	case "APP_PAUSED", "DEFCON_ACTIVE", "HTTP_CALLS_LIMIT":
		return never
	}
	switch {
	case e.Status == http.StatusTooManyRequests:
		return anyKind
	case e.Type == "" && (e.Status == http.StatusBadGateway || e.Status == http.StatusServiceUnavailable || e.Status == http.StatusGatewayTimeout):
		return readOnly
	}
	return never
}

// classifyTransport sorts a connection error: one that never reached the server may be repeated with any
// method; one after the request was sent, only for GET and HEAD.
func classifyTransport(err error) verdict {
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return anyKind
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return anyKind
	}
	var netErr net.Error
	inner := errors.Unwrap(err) // past *url.Error, which is itself a net.Error
	if inner == nil {
		inner = err
	}
	if errors.Is(inner, io.EOF) || errors.Is(inner, io.ErrUnexpectedEOF) || errors.Is(inner, syscall.ECONNRESET) ||
		errors.Is(inner, syscall.EPIPE) || errors.As(inner, &op) || (errors.As(inner, &netErr) && netErr.Timeout()) {
		return readOnly
	}
	return never
}

// retryAfter is the wait Surfra asked for: the Retry-After header in seconds, else error.retry_after.
func retryAfter(h http.Header, body float64, hasBody bool) (time.Duration, bool) {
	if value := strings.TrimSpace(h.Get("Retry-After")); value != "" {
		if seconds, err := strconv.ParseFloat(value, 64); err == nil {
			return time.Duration(max(0, seconds) * float64(time.Second)), true
		}
		if at, err := http.ParseTime(value); err == nil {
			return max(0, time.Until(at)), true
		}
	}
	if hasBody {
		return time.Duration(max(0, body) * float64(time.Second)), true
	}
	return 0, false
}

// backoff is full jitter: a random wait from 0 to min(30 s, 0.5 s × 2^n) before retry n+1.
func backoff(n int) time.Duration {
	ceiling := backoffCap
	if n < 16 {
		ceiling = min(backoffCap, backoffBase<<n)
	}
	return time.Duration(rand.Int64N(int64(ceiling) + 1))
}

// errorFrom reads an error answer: {"error": "..."} (volumes), {"detail": "..."} (control plane), or
// Surfra's typed {"detail", "error": {"type", "message", "retry_after"}}.
func errorFrom(resp *http.Response) *Error {
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	e := &Error{Status: resp.StatusCode, Detail: strings.TrimSpace(string(data)), Attempts: 1}
	var body struct {
		Error  json.RawMessage `json:"error"`
		Detail *string         `json:"detail"`
	}
	var after float64
	var hasAfter bool
	if json.Unmarshal(data, &body) == nil {
		var text string
		var typed struct {
			Type       string   `json:"type"`
			Message    string   `json:"message"`
			RetryAfter *float64 `json:"retry_after"`
		}
		switch {
		case len(body.Error) > 0 && json.Unmarshal(body.Error, &text) == nil:
			e.Detail = text
		case len(body.Error) > 0 && json.Unmarshal(body.Error, &typed) == nil:
			e.Type, e.Message = typed.Type, typed.Message
			if typed.RetryAfter != nil {
				after, hasAfter = *typed.RetryAfter, true
			}
			if body.Detail != nil {
				e.Detail = *body.Detail
			} else if typed.Message != "" {
				e.Detail = typed.Message
			}
		case body.Detail != nil:
			e.Detail = *body.Detail
		}
	}
	if e.Detail == "" {
		e.Detail = http.StatusText(resp.StatusCode)
	}
	if e.Message == "" {
		e.Message = e.Detail
	}
	e.RetryAfter, e.hasRetryAfter = retryAfter(resp.Header, after, hasAfter)
	return e
}

// submission is a POST that starts work (a transaction or a job): if it is lost after sending, the work
// may be running, so it is never repeated and its error reports the outcome as unknown.
func submission(r request) bool {
	if r.method != http.MethodPost || !r.control {
		return false
	}
	path := r.url
	if i := strings.Index(path, "://"); i >= 0 {
		if j := strings.Index(path[i+3:], "/"); j >= 0 {
			path = path[i+3+j:]
		}
	}
	return strings.HasSuffix(path, "/v1/transactions") || strings.HasSuffix(path, "/v1/jobs")
}

// send makes the request, retrying as the contract says. It returns the response for a success (status
// below 300), and *Error for an error answer or a connection error (carrying the attempts and whether the
// outcome is unknown). Credential and ctx errors are returned as they are.
func send(ctx context.Context, client *http.Client, s Settings, r request) (*http.Response, error) {
	credential := s.Credential
	if r.control {
		credential = s.ControlCredential
	}
	target := r.url
	if len(r.query) > 0 {
		target += "?" + r.query.Encode()
	}
	body := r.body
	if r.json != nil {
		var err error
		if body, err = json.Marshal(r.json); err != nil {
			return nil, err
		}
	}
	l := resolve(ctx, s.Retry)
	first := time.Now()
	submits := submission(r)
	for attempt := 1; ; attempt++ {
		token, err := credential()
		if err != nil {
			return nil, err
		}
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body) // a fresh reader per attempt; also sets GetBody
		}
		req, err := http.NewRequestWithContext(ctx, r.method, target, reader)
		if err != nil {
			return nil, err
		}
		for key, values := range r.header {
			req.Header[key] = values
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Surfra-Client", "surfra-go/"+Version)
		if r.json != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if body != nil {
			req.ContentLength = int64(len(body))
		}

		var failure *Error
		var kind verdict
		resp, err := client.Do(req)
		switch {
		case err != nil && ctx.Err() != nil:
			return nil, err
		case err != nil:
			kind = classifyTransport(err)
			failure = &Error{Message: err.Error(), Detail: err.Error(), Err: err}
			if kind == readOnly && submits {
				failure.OutcomeUnknown = true
			}
		case resp.StatusCode < 300:
			return resp, nil
		default:
			failure = errorFrom(resp)
			resp.Body.Close()
			kind = classifyStatus(failure)
			if submits && failure.Status == http.StatusGatewayTimeout && failure.Type == "" {
				failure.OutcomeUnknown = true
			}
		}
		failure.Attempts = attempt
		if kind == never || (kind == readOnly && !idempotent(r.method)) {
			return nil, failure
		}
		failure.retryable = true
		if attempt > l.maxRetries {
			return nil, failure
		}
		wait := backoff(attempt - 1)
		if failure.hasRetryAfter {
			wait = failure.RetryAfter
		}
		resume := time.Now().Add(wait)
		if l.deadline > 0 && resume.After(first.Add(l.deadline)) {
			return nil, failure
		}
		if end, ok := ctx.Deadline(); ok && resume.After(end) {
			return nil, failure
		}
		event := RetryEvent{Attempt: attempt, Method: r.method, URL: r.url, Status: failure.Status, Type: failure.Type, Err: failure, Wait: wait, Reason: failure.reason()}
		slog.DebugContext(ctx, "surfra: retrying", "attempt", attempt, "method", r.method, "url", r.url, "reason", event.Reason, "wait", wait)
		if l.onRetry != nil {
			l.onRetry(event)
		}
		if err := sleep(ctx, wait); err != nil {
			return nil, errors.Join(err, failure)
		}
	}
}
