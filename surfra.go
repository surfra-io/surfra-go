// Package surfra is the Surfra SDK for Go: volumes, apps and jobs on serverless GPUs across your own clouds.
//
//	dir, err := surfra.NewVolume("weights").Path("minilm/").Local(ctx)   // a folder, synced to local disk
//	out, err := surfra.NewClient().Run(ctx, "embed", map[string]any{"texts": []string{"hello"}})
//
// Credentials come from the environment, in Surfra's order: the volumes token Surfra gives apps and jobs
// (SURFRA_VOLUMES_TOKEN_FILE), SURFRA_API_KEY, SURFRA_REPLICA_TOKEN, then the session saved by
// `surfra login`. Inside a Surfra app or job, SURFRA_VOLUMES_URL is a local proxy that adds it itself.
package surfra

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultMaster  = "https://api.surfra.io"
	defaultVolumes = "https://volumes.surfra.io"
	partBytes      = 64 << 20
)

// Settings say where Surfra is and which credential to send. SettingsFromEnv reads the same variables
// as every Surfra SDK; set fields directly to override them.
type Settings struct {
	APIKey       string // sent before anything else when set
	Master       string // SURFRA_MASTER, default https://api.surfra.io
	VolumesURL   string // SURFRA_VOLUMES_URL, default https://volumes.surfra.io
	Cache        string // SURFRA_VOLUME_CACHE, default <temp dir>/surfra
	PartBytes    int64  // SURFRA_VOLUMES_PART_BYTES, default 64 MiB: larger files upload in parts
	TokenFile    string // SURFRA_VOLUMES_TOKEN_FILE, re-read whenever the file changes
	EnvAPIKey    string // SURFRA_API_KEY
	ReplicaToken string // SURFRA_REPLICA_TOKEN
	LoginFile    string // the session `surfra login` saved, ~/.surfra/config.json

	// HTTPClient, when set, sends every request instead of the SDK's own client (60 s timeout for the
	// control plane, 30 min for volumes). Use it for proxies, custom TLS, tracing or your own timeouts.
	// It is shared, so it must be safe for concurrent use, as *http.Client is.
	HTTPClient *http.Client
}

func env(name string) string { return strings.TrimSpace(os.Getenv(name)) }

func or(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// SettingsFromEnv reads SURFRA_* variables, falling back to Surfra's defaults.
func SettingsFromEnv() Settings {
	s := Settings{
		Master:       strings.TrimRight(or(env("SURFRA_MASTER"), defaultMaster), "/"),
		VolumesURL:   strings.TrimRight(or(env("SURFRA_VOLUMES_URL"), defaultVolumes), "/"),
		Cache:        or(env("SURFRA_VOLUME_CACHE"), filepath.Join(os.TempDir(), "surfra")),
		PartBytes:    partBytes,
		TokenFile:    env("SURFRA_VOLUMES_TOKEN_FILE"),
		EnvAPIKey:    env("SURFRA_API_KEY"),
		ReplicaToken: env("SURFRA_REPLICA_TOKEN"),
	}
	if n, err := strconv.ParseInt(env("SURFRA_VOLUMES_PART_BYTES"), 10, 64); err == nil && n > 0 {
		s.PartBytes = n
	}
	if home, err := os.UserHomeDir(); err == nil {
		s.LoginFile = filepath.Join(home, ".surfra", "config.json")
	}
	return s
}

// ErrNoCredential means no credential was found anywhere.
var ErrNoCredential = errors.New("no credential: set SURFRA_API_KEY or run `surfra login`")

// Credential is the credential to send, in Surfra's order.
func (s Settings) Credential() (string, error) {
	if s.APIKey != "" {
		return s.APIKey, nil
	}
	if s.TokenFile != "" {
		if data, err := readCached(s.TokenFile); err == nil && strings.TrimSpace(string(data)) != "" {
			return strings.TrimSpace(string(data)), nil
		}
	}
	if s.EnvAPIKey != "" {
		return s.EnvAPIKey, nil
	}
	if s.ReplicaToken != "" {
		return s.ReplicaToken, nil
	}
	if s.LoginFile != "" {
		if data, err := readCached(s.LoginFile); err == nil {
			var saved struct {
				Token string `json:"token"`
			}
			if json.Unmarshal(data, &saved) == nil && saved.Token != "" {
				return saved.Token, nil
			}
		}
	}
	return "", ErrNoCredential
}

type cachedFile struct {
	modTime time.Time
	size    int64
	data    []byte
}

// files caches credential files by path, so a request costs a stat rather than a read.
var files sync.Map // path -> cachedFile

// readCached returns the file's contents, reading it again only when its modification time or size
// changed. It follows symlinks, so a rotated token behind a swapped link is picked up.
func readCached(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		files.Delete(path)
		return nil, err
	}
	if hit, ok := files.Load(path); ok {
		c := hit.(cachedFile)
		if c.modTime.Equal(info.ModTime()) && c.size == info.Size() {
			return c.data, nil
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		files.Delete(path)
		return nil, err
	}
	files.Store(path, cachedFile{modTime: info.ModTime(), size: info.Size(), data: data})
	return data, nil
}

// ControlCredential is the credential for control-plane calls: an explicit key, SURFRA_API_KEY, then the
// saved login. Never the volumes token, which only the volumes service accepts.
func (s Settings) ControlCredential() (string, error) {
	s.TokenFile, s.ReplicaToken = "", ""
	return s.Credential()
}

// Error is Surfra refusing a request: Status is the HTTP code, Detail the reason it gave.
type Error struct {
	Status int
	Detail string
}

func (e *Error) Error() string { return fmt.Sprintf("%d: %s", e.Status, e.Detail) }

// IsNotFound reports whether err is Surfra answering 404.
func IsNotFound(err error) bool {
	var apiErr *Error
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound
}

func errorFrom(resp *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	detail := strings.TrimSpace(string(data))
	var body struct {
		Error  *string `json:"error"`
		Detail *string `json:"detail"`
	}
	if json.Unmarshal(data, &body) == nil {
		if body.Error != nil {
			detail = *body.Error
		} else if body.Detail != nil {
			detail = *body.Detail
		}
	}
	if detail == "" {
		detail = http.StatusText(resp.StatusCode)
	}
	return &Error{Status: resp.StatusCode, Detail: detail}
}

type request struct {
	control bool // a control-plane call: never sends the volumes token
	method  string
	url     string
	body    []byte
	json    any
	query   url.Values
	header  http.Header
}

func send(ctx context.Context, client *http.Client, s Settings, r request) (*http.Response, error) {
	credential := s.Credential
	if r.control {
		credential = s.ControlCredential
	}
	token, err := credential()
	if err != nil {
		return nil, err
	}
	target := r.url
	if len(r.query) > 0 {
		target += "?" + r.query.Encode()
	}
	var reader io.Reader
	body := r.body
	if r.json != nil {
		body, err = json.Marshal(r.json)
		if err != nil {
			return nil, err
		}
	}
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, r.method, target, reader)
	if err != nil {
		return nil, err
	}
	for key, values := range r.header {
		req.Header[key] = values
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if r.json != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if body != nil {
		req.ContentLength = int64(len(body))
	}
	return client.Do(req)
}

// do sends a request and turns a non-success answer into *Error.
func do(ctx context.Context, client *http.Client, s Settings, r request) (*http.Response, error) {
	resp, err := send(ctx, client, s, r)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, errorFrom(resp)
	}
	return resp, nil
}

func decode(resp *http.Response, dest any) error {
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(dest)
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
