package surfra

// The shared SDK cases (surfra-sdk-tests/CASES.md), each against its own copy of the fake server.
// Set SURFRA_SDK_TESTS to the suite's folder; a sibling checkout is found on its own. Skipped when absent.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

const part = 1024

type fake struct {
	t     *testing.T
	url   string
	cache string
}

func suite() string {
	if dir := os.Getenv("SURFRA_SDK_TESTS"); dir != "" {
		return dir
	}
	return filepath.Join("..", "surfra-sdk-tests")
}

func start(t *testing.T) *fake {
	t.Helper()
	server := filepath.Join(suite(), "server.py")
	if _, err := os.Stat(server); err != nil {
		t.Skip("surfra-sdk-tests is not checked out")
	}
	python := os.Getenv("SURFRA_SDK_PYTHON")
	if python == "" {
		python = "python3"
	}
	cmd := exec.Command(python, server, "--port", "0")
	cmd.Env = append(os.Environ(), fmt.Sprintf("SURFRA_VOLUMES_PART_BYTES=%d", part))
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return &fake{t: t, url: strings.TrimSpace(strings.TrimPrefix(line, "listening on ")), cache: t.TempDir()}
}

func (f *fake) settings() Settings {
	return Settings{Master: f.url, VolumesURL: f.url, Cache: f.cache, PartBytes: part, EnvAPIKey: "sfk_test"}
}

func (f *fake) post(path string, body any) {
	data, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, f.url+path, bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer sfk_test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	resp.Body.Close()
}

func (f *fake) volume(name string) *Volume {
	f.post("/v1/volumes", map[string]string{"name": name})
	return NewVolumeWith(name, f.settings())
}

func (f *fake) seed(volume, path, body string) {
	f.post("/_test/files", map[string]string{"volume": volume, "path": path, "body": body})
}

type state struct {
	Files     map[string]map[string]struct{ ETag, Body string } `json:"files"`
	Downloads int                                               `json:"downloads"`
	Log       []struct{ Method, Path, Auth string }             `json:"log"`
	Jobs      map[string]map[string]any                         `json:"jobs"`
}

func (f *fake) state() state {
	resp, err := http.Get(f.url + "/_test/state")
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	var s state
	json.NewDecoder(resp.Body).Decode(&s)
	return s
}

func (f *fake) requests() []struct{ Method, Path, Auth string } {
	var out []struct{ Method, Path, Auth string }
	for _, item := range f.state().Log {
		if !strings.HasPrefix(item.Path, "/_test/") {
			out = append(out, item)
		}
	}
	return out
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestConformance(t *testing.T) {
	ctx := context.Background()
	cases := map[string]func(t *testing.T){
		"v01_small_upload_round_trips": func(t *testing.T) {
			f := start(t)
			weights := f.volume("weights")
			_, err := weights.WriteFile(ctx, "a.txt", []byte("hello"))
			check(t, err)
			data, err := weights.ReadFile(ctx, "a.txt")
			check(t, err)
			if string(data) != "hello" {
				t.Fatalf("read %q", data)
			}
			for _, item := range f.requests() {
				if strings.HasSuffix(item.Path, "/uploads") {
					t.Fatal("a small file used multipart")
				}
			}
		},
		"v02_large_upload_goes_in_parts": func(t *testing.T) {
			f := start(t)
			data := make([]byte, part*3+17)
			for i := range data {
				data[i] = byte(i * 7 % 251)
			}
			local := filepath.Join(t.TempDir(), "big.bin")
			check(t, os.WriteFile(local, data, 0o644))
			weights := f.volume("weights")
			_, err := weights.PutFile(ctx, local, "big.bin")
			check(t, err)
			back, err := weights.ReadFile(ctx, "big.bin")
			check(t, err)
			if !bytes.Equal(back, data) {
				t.Fatal("multipart round trip changed the bytes")
			}
			used := false
			for _, item := range f.requests() {
				used = used || strings.HasSuffix(item.Path, "/uploads")
			}
			if !used {
				t.Fatal("a large file did not use multipart")
			}
		},
		"v03_put_directory_keeps_relative_paths": func(t *testing.T) {
			f := start(t)
			dir := t.TempDir()
			check(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o755))
			check(t, os.WriteFile(filepath.Join(dir, "a"), []byte("a"), 0o644))
			check(t, os.WriteFile(filepath.Join(dir, "sub", "b"), []byte("b"), 0o644))
			n, err := f.volume("weights").PutDirectory(ctx, dir, "minilm/")
			check(t, err)
			if got := keys(f.state().Files["weights"]); n != 2 || strings.Join(got, ",") != "minilm/a,minilm/sub/b" {
				t.Fatalf("uploaded %d: %v", n, got)
			}
		},
		"v04_ls_lists_one_level_or_everything": func(t *testing.T) {
			f := start(t)
			weights := f.volume("weights")
			for _, path := range []string{"m/a", "m/sub/b", "top"} {
				f.seed("weights", path, "x")
			}
			top, err := weights.Ls(ctx, "m/", false)
			check(t, err)
			all, err := weights.Ls(ctx, "", true)
			check(t, err)
			var topPaths, allPaths []string
			for _, e := range top {
				topPaths = append(topPaths, e.Path)
			}
			for _, e := range all {
				allPaths = append(allPaths, e.Path)
			}
			sort.Strings(allPaths)
			if strings.Join(topPaths, ",") != "m/a,m/sub/" || strings.Join(allPaths, ",") != "m/a,m/sub/b,top" {
				t.Fatalf("top %v all %v", topPaths, allPaths)
			}
		},
		"v05_local_downloads_once": func(t *testing.T) {
			f := start(t)
			weights := f.volume("weights")
			f.seed("weights", "m/a", "aaa")
			f.seed("weights", "m/b", "bbb")
			dir, err := weights.Path("m/").Local(ctx)
			check(t, err)
			a, _ := os.ReadFile(filepath.Join(dir, "a"))
			b, _ := os.ReadFile(filepath.Join(dir, "b"))
			if string(a) != "aaa" || string(b) != "bbb" {
				t.Fatalf("local copy %q %q", a, b)
			}
			before := len(f.requests())
			_, err = weights.Path("m/").Local(ctx)
			check(t, err)
			if len(f.requests()) != before {
				t.Fatal("a second Local made requests")
			}
		},
		"v06_a_new_object_fetches_only_changes": func(t *testing.T) {
			f := start(t)
			f.volume("weights")
			f.seed("weights", "m/a", "aaa")
			f.seed("weights", "m/b", "bbb")
			_, err := NewVolumeWith("weights", f.settings()).Path("m/").Local(ctx)
			check(t, err)
			f.seed("weights", "m/a", "AAAA")
			f.post("/v1/weights/delete", map[string]any{"paths": []string{"m/b"}})
			downloads := f.state().Downloads
			dir, err := NewVolumeWith("weights", f.settings()).Path("m/").Local(ctx)
			check(t, err)
			a, _ := os.ReadFile(filepath.Join(dir, "a"))
			_, gone := os.Stat(filepath.Join(dir, "b"))
			if f.state().Downloads != downloads+1 || string(a) != "AAAA" || !errors.Is(gone, os.ErrNotExist) {
				t.Fatalf("downloads %d->%d, a=%q", downloads, f.state().Downloads, a)
			}
		},
		"v07_commit_uploads_changes_and_deletes": func(t *testing.T) {
			f := start(t)
			ckpts := f.volume("ckpts")
			for _, path := range []string{"r/a", "r/b", "r/c"} {
				f.seed("ckpts", path, path)
			}
			dir, err := ckpts.Path("r/").LocalWith(ctx, ReadWrite, false)
			check(t, err)
			check(t, os.WriteFile(filepath.Join(dir, "a"), []byte("changed!"), 0o644))
			check(t, os.WriteFile(filepath.Join(dir, "d"), []byte("new"), 0o644))
			check(t, os.Remove(filepath.Join(dir, "c")))
			done, err := ckpts.Commit(ctx)
			check(t, err)
			files := f.state().Files["ckpts"]
			if done != (Committed{Uploaded: 2, Deleted: 1}) || strings.Join(keys(files), ",") != "r/a,r/b,r/d" || files["r/a"].Body != "changed!" {
				t.Fatalf("commit %+v files %v", done, keys(files))
			}
			again, err := ckpts.Commit(ctx)
			check(t, err)
			if again != (Committed{}) {
				t.Fatalf("second commit %+v", again)
			}
		},
		"v08_open_write_uploads_on_close_only": func(t *testing.T) {
			f := start(t)
			results := f.volume("results")
			_, err := results.WriteFile(ctx, "runs/1.jsonl", []byte("one\n"))
			check(t, err)
			// A write whose data fails before it is sent uploads nothing.
			produce := func() ([]byte, error) { return nil, errors.New("boom") }
			if data, err := produce(); err == nil {
				results.WriteFile(ctx, "runs/2.jsonl", data)
			}
			if got := keys(f.state().Files["results"]); strings.Join(got, ",") != "runs/1.jsonl" {
				t.Fatalf("files %v", got)
			}
		},
		"v09_rm_file_and_folder": func(t *testing.T) {
			f := start(t)
			weights := f.volume("weights")
			for _, path := range []string{"a", "old/x", "old/y"} {
				f.seed("weights", path, "x")
			}
			check(t, weights.Rm(ctx, "a"))
			n, err := weights.RmRecursive(ctx, "old/")
			check(t, err)
			if n != 2 || len(f.state().Files["weights"]) != 0 {
				t.Fatalf("deleted %d, left %v", n, keys(f.state().Files["weights"]))
			}
		},
		"v10_missing_file_raises_404": func(t *testing.T) {
			f := start(t)
			_, err := f.volume("weights").ReadFile(ctx, "nope")
			var apiErr *Error
			if !errors.As(err, &apiErr) || apiErr.Status != 404 || !strings.Contains(apiErr.Detail, "no such file") {
				t.Fatalf("got %v", err)
			}
		},
		"v11_token_file_wins_over_the_api_key": func(t *testing.T) {
			f := start(t)
			f.volume("weights")
			token := filepath.Join(t.TempDir(), "token")
			check(t, os.WriteFile(token, []byte("aaa.bbb.ccc\n"), 0o600))
			s := f.settings()
			s.TokenFile = token
			_, err := NewVolumeWith("weights", s).Ls(ctx, "", false)
			check(t, err)
			sent := f.requests()
			if sent[len(sent)-1].Auth != "Bearer aaa.bbb.ccc" {
				t.Fatalf("sent %q", sent[len(sent)-1].Auth)
			}
		},
		"v12_no_credential_says_what_to_set": func(t *testing.T) {
			f := start(t)
			s := f.settings()
			s.EnvAPIKey = ""
			_, err := NewVolumeWith("weights", s).Ls(ctx, "", false)
			if !errors.Is(err, ErrNoCredential) || !strings.Contains(err.Error(), "SURFRA_API_KEY") {
				t.Fatalf("got %v", err)
			}
		},
		"v13_writing_to_a_new_volume_creates_it": func(t *testing.T) {
			f := start(t)
			_, err := NewVolumeWith("fresh", f.settings()).WriteFile(ctx, "a.txt", []byte("x"))
			check(t, err)
			if f.state().Files["fresh"]["a.txt"].Body != "x" {
				t.Fatal("the new volume does not hold the file")
			}
		},
		"v14_another_sdks_state_file_is_honored": func(t *testing.T) {
			f := start(t)
			f.volume("weights")
			f.seed("weights", "m/a", "aaa")
			local := filepath.Join(f.cache, "vol-weights", "m", "a")
			check(t, os.MkdirAll(filepath.Dir(local), 0o755))
			check(t, os.WriteFile(local, []byte("aaa"), 0o644))
			info, _ := os.Stat(local)
			etag := f.state().Files["weights"]["m/a"].ETag
			stateFile := filepath.Join(f.cache, ".state", "vol-weights.json")
			check(t, os.MkdirAll(filepath.Dir(stateFile), 0o755))
			body := fmt.Sprintf(`{"m/a": {"etag": %q, "size": 3, "mtime_ns": %d}}`, etag, info.ModTime().UnixNano())
			check(t, os.WriteFile(stateFile, []byte(body), 0o644))
			_, err := NewVolumeWith("weights", f.settings()).Path("m/").Local(ctx)
			check(t, err)
			if f.state().Downloads != 0 {
				t.Fatalf("downloaded %d files the state already had", f.state().Downloads)
			}
		},
		"a01_run_returns_the_output": func(t *testing.T) {
			f := start(t)
			out, err := NewClientWith(f.settings()).Run(ctx, "embed", map[string]any{"texts": []string{"hi"}})
			check(t, err)
			got, _ := json.Marshal(out)
			if string(got) != `{"app":"embed","echo":{"texts":["hi"]}}` {
				t.Fatalf("output %s", got)
			}
		},
		"a02_enqueue_then_result": func(t *testing.T) {
			f := start(t)
			client := NewClientWith(f.settings())
			call, err := client.Enqueue(ctx, "embed", map[string]any{"n": 1})
			check(t, err)
			out, err := call.Result(ctx)
			check(t, err)
			got, _ := json.Marshal(out)
			if string(got) != `{"app":"embed","echo":{"n":1}}` {
				t.Fatalf("output %s", got)
			}
			failing, err := client.Enqueue(ctx, "fails", nil)
			check(t, err)
			if _, err := failing.Result(ctx); err == nil || !strings.Contains(err.Error(), "ValueError") {
				t.Fatalf("a failed call returned %v", err)
			}
		},
		"v15_write_and_read_helpers": func(t *testing.T) {
			f := start(t)
			weights := f.volume("weights")
			_, err := weights.WriteFile(ctx, "bytes.bin", []byte{0, 1, 'r', 'a', 'w'})
			check(t, err)
			back, err := weights.ReadFile(ctx, "bytes.bin")
			check(t, err)
			if !bytes.Equal(back, []byte{0, 1, 'r', 'a', 'w'}) {
				t.Fatalf("read %v", back)
			}
		},
		"c01_control_plane_calls_skip_the_volumes_token": func(t *testing.T) {
			f := start(t)
			token := filepath.Join(t.TempDir(), "token")
			check(t, os.WriteFile(token, []byte("aaa.bbb.ccc"), 0o600))
			s := f.settings()
			s.TokenFile = token
			weights := NewVolumeWith("weights", s)
			_, err := weights.Create(ctx)
			check(t, err)
			_, err = weights.Ls(ctx, "", false)
			check(t, err)
			_, err = NewClientWith(s).Run(ctx, "embed", nil)
			check(t, err)
			sent := map[string]string{}
			for _, item := range f.requests() {
				sent[item.Path] = item.Auth
			}
			if sent["/v1/volumes"] != "Bearer sfk_test" || sent["/v1/transactions"] != "Bearer sfk_test" || sent["/v1/weights/list"] != "Bearer aaa.bbb.ccc" {
				t.Fatalf("sent %v", sent)
			}
		},
		"a03_app_deploy_sends_the_spec": func(t *testing.T) {
			f := start(t)
			saved, err := NewAppWith("embed", f.settings()).Deploy(ctx, AppSpec{Image: "ghcr.io/acme/embed:1", Entrypoint: "app.py", Autoscale: map[string]any{"max_replicas": 2}, Runtime: map[string]any{"engine": "python"}})
			check(t, err)
			autoscale, _ := json.Marshal(saved["autoscale"])
			if saved["name"] != "embed" || !strings.HasPrefix(fmt.Sprint(saved["id"]), "sfa_") || saved["image"] != "ghcr.io/acme/embed:1" || string(autoscale) != `{"max_replicas":2}` {
				t.Fatalf("saved %v", saved)
			}
			found := false
			for _, item := range f.requests() {
				found = found || (item.Method == "PUT" && item.Path == "/v1/apps/embed")
			}
			if !found {
				t.Fatal("no PUT /v1/apps/embed")
			}
		},
		"a04_app_run_and_volumes": func(t *testing.T) {
			f := start(t)
			app := NewAppWith("embed", f.settings())
			out, err := app.Run(ctx, map[string]any{"x": 1})
			check(t, err)
			if got, _ := json.Marshal(out); string(got) != `{"app":"embed","echo":{"x":1}}` {
				t.Fatalf("output %s", got)
			}
			_, err = app.SetVolumes(ctx, []string{"weights:r"})
			check(t, err)
			grants, err := app.Volumes(ctx)
			check(t, err)
			if len(grants) != 1 || grants[0] != (VolumeGrant{Volume: "weights", Mode: "r"}) {
				t.Fatalf("volumes %v", grants)
			}
		},
		"a05_a_rate_limited_submit_waits_and_succeeds": func(t *testing.T) {
			f := start(t)
			out, err := NewClientWith(f.settings()).Run(ctx, "busy", map[string]any{"n": 1})
			check(t, err)
			if got, _ := json.Marshal(out); string(got) != `{"app":"busy","echo":{"n":1}}` {
				t.Fatalf("output %s", got)
			}
			posts := 0
			for _, item := range f.requests() {
				if item.Path == "/v1/transactions" {
					posts++
				}
			}
			if posts != 2 {
				t.Fatalf("posted %d times", posts)
			}
		},
		"w01_workflow_run_returns_each_stage": func(t *testing.T) {
			f := start(t)
			out, err := NewWorkflowWith("pipeline", f.settings()).Run(ctx, map[string]any{"q": "hi"})
			check(t, err)
			if got, _ := json.Marshal(out); string(got) != `{"a":{"app":"embed","echo":{"q":"hi"}},"b":{"app":"rerank","echo":{}}}` {
				t.Fatalf("output %s", got)
			}
		},
		"w02_workflow_deploy_saves_stages": func(t *testing.T) {
			f := start(t)
			flow := NewWorkflowWith("solo", f.settings())
			_, err := flow.Deploy(ctx, []Stage{{Name: "only", App: "embed"}})
			check(t, err)
			out, err := flow.Run(ctx, map[string]any{"q": "hi"})
			check(t, err)
			if got, _ := json.Marshal(out); string(got) != `{"app":"embed","echo":{"q":"hi"}}` {
				t.Fatalf("output %s", got)
			}
		},
		"j02_job_logs_oldest_first": func(t *testing.T) {
			f := start(t)
			job, err := NewClientWith(f.settings()).SubmitJob(ctx, JobSpec{Entrypoint: "echo hi", Image: "img", Providers: []map[string]any{{"provider": "local"}}})
			check(t, err)
			lines, err := job.Logs(ctx, "")
			check(t, err)
			var got []string
			for _, line := range lines {
				got = append(got, line.M)
			}
			if strings.Join(got, ",") != "line 0,line 1,line 2" {
				t.Fatalf("logs %v", got)
			}
		},
		"j01_job_with_volumes_finishes": func(t *testing.T) {
			f := start(t)
			job, err := NewClientWith(f.settings()).SubmitJob(ctx, JobSpec{Entrypoint: "python train.py", Image: "img", Providers: []map[string]any{{"provider": "local"}}, Volumes: []string{NewVolume("weights").Grant(Read)}})
			check(t, err)
			if got, _ := json.Marshal(f.state().Jobs[job.ID]["volumes"]); string(got) != `["weights:r"]` {
				t.Fatalf("volumes sent %s", got)
			}
			waiting, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			view, err := job.Wait(waiting, 50*time.Millisecond)
			check(t, err)
			if view["status"] != "succeeded" || view["exit_code"] != float64(0) {
				t.Fatalf("job ended %v", view)
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
