package surfra

// Tests for goroutine safety, an injected HTTP client, ctx cancellation, callback_url and cached
// credential files. They use a local httptest server, so they run without surfra-sdk-tests.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// control is a tiny control plane: transactions and jobs finish after `after` polls each.
type control struct {
	mu      sync.Mutex
	after   int
	polls   map[string]int
	bodies  []map[string]any
	running bool // never finish
}

func (c *control) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	status := func(id string) string {
		c.polls[id]++
		if c.running || c.polls[id] <= c.after {
			return "running"
		}
		return "succeeded"
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/transactions":
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		c.bodies = append(c.bodies, body)
		json.NewEncoder(w).Encode(map[string]any{"id": "tx_1", "status": "queued", "stages": body["stages"]})
	case strings.HasPrefix(r.URL.Path, "/v1/transactions/"):
		json.NewEncoder(w).Encode(map[string]any{"id": "tx_1", "status": status("tx_1"),
			"stages": []any{map[string]any{"name": "call", "output": map[string]any{"ok": true}}}})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
		json.NewEncoder(w).Encode(map[string]any{"id": "job_1", "status": "queued"})
	case strings.HasPrefix(r.URL.Path, "/v1/jobs/"):
		json.NewEncoder(w).Encode(map[string]any{"id": "job_1", "status": status("job_1")})
	default:
		http.NotFound(w, r)
	}
}

func newControl(t *testing.T, after int) (*control, *httptest.Server, Settings) {
	t.Helper()
	c := &control{after: after, polls: map[string]int{}}
	server := httptest.NewServer(c)
	t.Cleanup(server.Close)
	return c, server, Settings{Master: server.URL, VolumesURL: server.URL, Cache: t.TempDir(), PartBytes: part, EnvAPIKey: "sfk_test"}
}

func TestSharedCallAndJobAcrossGoroutines(t *testing.T) {
	ctx := context.Background()
	_, _, s := newControl(t, 40)
	client := NewClientWith(s)
	call, err := client.Enqueue(ctx, "embed", map[string]any{"x": 1})
	check(t, err)
	job, err := client.SubmitJob(ctx, JobSpec{Entrypoint: "true"})
	check(t, err)
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for i := 0; i < 16; i++ {
		wg.Add(4)
		go func() {
			defer wg.Done()
			out, err := call.Result(ctx)
			if err == nil && out.(map[string]any)["ok"] != true {
				err = errors.New("wrong output")
			}
			errs <- err
		}()
		go func() {
			defer wg.Done()
			for k := 0; k < 5; k++ {
				if _, err := call.Get(ctx); err != nil {
					errs <- err
					return
				}
				_ = call.LastView()["status"]
			}
		}()
		go func() {
			defer wg.Done()
			_, err := job.Wait(ctx, time.Millisecond)
			errs <- err
		}()
		go func() {
			defer wg.Done()
			for k := 0; k < 5; k++ {
				if _, err := job.Get(ctx); err != nil {
					errs <- err
					return
				}
				_ = job.Status()
				_ = job.LastView()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		check(t, err)
	}
	if call.LastView()["status"] != "succeeded" || job.Status() != "succeeded" {
		t.Fatalf("call %v, job %v", call.LastView()["status"], job.Status())
	}
}

type countingTransport struct{ n atomic.Int64 }

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.n.Add(1)
	return http.DefaultTransport.RoundTrip(r)
}

func TestInjectedHTTPClientIsUsed(t *testing.T) {
	ctx := context.Background()
	_, _, s := newControl(t, 0)
	transport := &countingTransport{}
	h := &http.Client{Transport: transport}

	_, err := NewClientWith(s).WithHTTPClient(h).Run(ctx, "embed", nil)
	check(t, err)
	if transport.n.Load() == 0 {
		t.Fatal("WithHTTPClient: the injected client sent nothing")
	}

	before := transport.n.Load()
	with := s
	with.HTTPClient = h
	_, err = NewAppWith("embed", with).Run(ctx, nil)
	check(t, err)
	if transport.n.Load() == before {
		t.Fatal("Settings.HTTPClient: the injected client sent nothing")
	}

	// Volumes: the control-plane call and the volumes call both go through the injected client.
	before = transport.n.Load()
	v := NewVolumeWith("weights", s).WithHTTPClient(h)
	_, _ = v.Info(ctx)
	_, _ = v.ReadFile(ctx, "a.txt")
	if got := transport.n.Load() - before; got != 2 {
		t.Fatalf("volume sent %d requests through the injected client, want 2", got)
	}
	if v.stream != nil {
		t.Fatal("an injected client must not be swapped for the stream client")
	}
}

func TestOpenUsesCtxDeadlineInsteadOfTotalTimeout(t *testing.T) {
	v := NewVolumeWith("weights", Settings{})
	if v.http.Timeout != 30*time.Minute || v.stream == nil || v.stream.Timeout != 0 {
		t.Fatalf("default volume clients: %v, %v", v.http.Timeout, v.stream)
	}
}

func TestCancelMidWaitReturnsPromptly(t *testing.T) {
	c, _, s := newControl(t, 0)
	c.running = true
	client := NewClientWith(s)
	call, err := client.Enqueue(context.Background(), "embed", nil)
	check(t, err)
	job, err := client.SubmitJob(context.Background(), JobSpec{Entrypoint: "true"})
	check(t, err)

	for name, wait := range map[string]func(context.Context) error{
		"call.Result": func(ctx context.Context) error { _, err := call.Result(ctx); return err },
		"job.Wait":    func(ctx context.Context) error { _, err := job.Wait(ctx, time.Hour); return err },
	} {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(150*time.Millisecond, cancel)
		started := time.Now()
		err := wait(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: %v, want context.Canceled", name, err)
		}
		if took := time.Since(started); took > 2*time.Second {
			t.Fatalf("%s took %v after cancel", name, took)
		}
	}
}

func TestCallbackURLIsSent(t *testing.T) {
	ctx := context.Background()
	c, _, s := newControl(t, 0)
	client := NewClientWith(s)
	_, err := client.EnqueueWith(ctx, "embed", nil, EnqueueOptions{CallbackURL: "https://example.com/hook"})
	check(t, err)
	_, err = client.SubmitWith(ctx, []map[string]any{}, SubmitOptions{Metadata: map[string]any{"k": "v"}, CallbackURL: "https://example.com/two"})
	check(t, err)
	_, err = NewAppWith("embed", s).EnqueueWith(ctx, nil, EnqueueOptions{CallbackURL: "https://example.com/three"})
	check(t, err)
	_, err = client.Enqueue(ctx, "embed", nil)
	check(t, err)
	c.mu.Lock()
	defer c.mu.Unlock()
	want := []any{"https://example.com/hook", "https://example.com/two", "https://example.com/three", nil}
	for i, body := range c.bodies {
		if body["callback_url"] != want[i] {
			t.Fatalf("request %d callback_url = %v, want %v", i, body["callback_url"], want[i])
		}
	}
	if c.bodies[1]["metadata"].(map[string]any)["k"] != "v" {
		t.Fatal("metadata lost")
	}
	if _, ok := c.bodies[3]["callback_url"]; ok {
		t.Fatal("Enqueue without a callback must not send callback_url")
	}
}

func TestCredentialFileIsCachedAndRereadOnChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	check(t, os.WriteFile(path, []byte("tok-one\n"), 0o600))
	s := Settings{TokenFile: path}
	got, err := s.Credential()
	check(t, err)
	if got != "tok-one" {
		t.Fatal("first read")
	}
	// Same mtime and size: served from the cache even though the bytes differ on disk.
	info, _ := os.Stat(path)
	check(t, os.WriteFile(path, []byte("tok-two\n"), 0o600))
	check(t, os.Chtimes(path, info.ModTime(), info.ModTime()))
	if got, _ := s.Credential(); got != "tok-one" {
		t.Fatalf("expected the cached value, got a re-read")
	}
	// A new mtime: read again.
	later := info.ModTime().Add(2 * time.Second)
	check(t, os.Chtimes(path, later, later))
	if got, _ := s.Credential(); got != "tok-two" {
		t.Fatal("expected a re-read after the file changed")
	}
	// Removed: falls through to the next credential.
	check(t, os.Remove(path))
	s.EnvAPIKey = "sfk_env"
	if got, _ := s.Credential(); got != "sfk_env" {
		t.Fatal("expected fallback after the token file was removed")
	}
}
