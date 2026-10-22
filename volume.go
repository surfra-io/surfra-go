package surfra

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	downloads = 8
	uploads   = 4
)

// Mode is what code may do with a volume, or how a folder is synced.
type Mode string

const (
	Read      Mode = "r"  // list and download
	Write     Mode = "w"  // upload and delete, without reading
	ReadWrite Mode = "rw" // both
)

// Entry is one file in a volume. Folders from a non-recursive listing end in "/" and have size 0.
type Entry struct {
	Path     string `json:"path"`
	Size     int64  `json:"size"`
	ETag     string `json:"etag"`
	Modified string `json:"modified"`
}

// Committed is what Commit did.
type Committed struct {
	Uploaded int
	Deleted  int
}

type known struct {
	ETag    string `json:"etag"`
	Size    int64  `json:"size"`
	MtimeNs int64  `json:"mtime_ns"`
}

// Volume is a volume by name. No request is made until it is used. It is safe for concurrent use.
type Volume struct {
	Name     string
	settings Settings
	http     *http.Client
	mu       sync.Mutex
	synced   map[string]Mode
}

// NewVolume is a volume using credentials and URLs from the environment.
func NewVolume(name string) *Volume { return NewVolumeWith(name, SettingsFromEnv()) }

// NewVolumeWith is a volume with explicit settings.
func NewVolumeWith(name string, s Settings) *Volume {
	return &Volume{Name: name, settings: s, http: &http.Client{Timeout: 30 * time.Minute}, synced: map[string]Mode{}}
}

// Grant is a grant such as "weights:r", for jobs and apps.
func (v *Volume) Grant(mode Mode) string { return v.Name + ":" + string(mode) }

func encodePath(path string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func isFolder(path string) bool { return path == "" || strings.HasSuffix(path, "/") }

func (v *Volume) url(route string) string {
	return v.settings.VolumesURL + "/v1/" + url.PathEscape(v.Name) + "/" + route
}

// call talks to the volumes service. With create, a write to a volume that does not exist yet creates it first.
func (v *Volume) call(ctx context.Context, r request, create bool) (*http.Response, error) {
	resp, err := send(ctx, v.http, v.settings, r)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		failure := errorFrom(resp)
		resp.Body.Close()
		var apiErr *Error
		if create && resp.StatusCode == http.StatusNotFound && asError(failure, &apiErr) && strings.Contains(apiErr.Detail, "no volume") {
			if _, err := v.Create(ctx); err != nil {
				return nil, err
			}
			return do(ctx, v.http, v.settings, r)
		}
		return nil, failure
	}
	return resp, nil
}

func asError(err error, dest **Error) bool {
	found, ok := err.(*Error)
	if ok {
		*dest = found
	}
	return ok
}

func (v *Volume) control(ctx context.Context, method, route string, body any) (map[string]any, error) {
	resp, err := do(ctx, v.http, v.settings, request{method: method, url: v.settings.Master + route, json: body})
	if err != nil {
		return nil, err
	}
	var out map[string]any
	return out, decode(resp, &out)
}

// Create creates the volume. Writing to a volume that does not exist yet does this for you.
func (v *Volume) Create(ctx context.Context) (map[string]any, error) {
	return v.control(ctx, http.MethodPost, "/v1/volumes", map[string]string{"name": v.Name})
}

// Info is the volume's size, file count and what uses it.
func (v *Volume) Info(ctx context.Context) (map[string]any, error) {
	return v.control(ctx, http.MethodGet, "/v1/volumes/"+url.PathEscape(v.Name), nil)
}

// Delete deletes the whole volume and every file in it. Needs an admin.
func (v *Volume) Delete(ctx context.Context) (map[string]any, error) {
	return v.control(ctx, http.MethodDelete, "/v1/volumes/"+url.PathEscape(v.Name), nil)
}

// ListVolumes is every volume in the organization.
func ListVolumes(ctx context.Context, s Settings) ([]map[string]any, error) {
	body, err := NewVolumeWith("_", s).control(ctx, http.MethodGet, "/v1/volumes", nil)
	if err != nil {
		return nil, err
	}
	raw, _ := json.Marshal(body["volumes"])
	var out []map[string]any
	return out, json.Unmarshal(raw, &out)
}

// Ls lists files under prefix. Without recursive, folders show as entries ending in "/".
func (v *Volume) Ls(ctx context.Context, prefix string, recursive bool) ([]Entry, error) {
	var out []Entry
	cursor := ""
	for {
		query := url.Values{"prefix": {prefix}, "recursive": {map[bool]string{true: "1", false: "0"}[recursive]}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		resp, err := v.call(ctx, request{method: http.MethodGet, url: v.url("list"), query: query}, false)
		if err != nil {
			return nil, err
		}
		var page struct {
			Files   []Entry  `json:"files"`
			Folders []string `json:"folders"`
			Cursor  *string  `json:"cursor"`
		}
		if err := decode(resp, &page); err != nil {
			return nil, err
		}
		out = append(out, page.Files...)
		if !recursive {
			for _, folder := range page.Folders {
				out = append(out, Entry{Path: folder})
			}
		}
		if page.Cursor == nil || *page.Cursor == "" {
			return out, nil
		}
		cursor = *page.Cursor
	}
}

// Rm deletes one file.
func (v *Volume) Rm(ctx context.Context, path string) error {
	resp, err := v.call(ctx, request{method: http.MethodDelete, url: v.url("files/" + encodePath(path))}, false)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// RmRecursive deletes everything under a folder and returns how many files were deleted.
func (v *Volume) RmRecursive(ctx context.Context, folder string) (int, error) {
	prefix := folder
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	total := 0
	for {
		resp, err := v.call(ctx, request{method: http.MethodPost, url: v.url("delete"), json: map[string]string{"prefix": prefix}}, false)
		if err != nil {
			return total, err
		}
		var result struct {
			Deleted int  `json:"deleted"`
			More    bool `json:"more"`
		}
		if err := decode(resp, &result); err != nil {
			return total, err
		}
		total += result.Deleted
		if !result.More {
			return total, nil
		}
	}
}

// ReadFile downloads one file into memory.
func (v *Volume) ReadFile(ctx context.Context, path string) ([]byte, error) {
	resp, err := v.call(ctx, request{method: http.MethodGet, url: v.url("files/" + encodePath(path))}, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// Open streams one file. Close the reader when done.
func (v *Volume) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	resp, err := v.call(ctx, request{method: http.MethodGet, url: v.url("files/" + encodePath(path))}, false)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

type stored struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
	ETag string `json:"etag"`
}

// WriteFile uploads bytes as one file. More than the part size goes up in parts.
func (v *Volume) WriteFile(ctx context.Context, path string, data []byte) (Entry, error) {
	if int64(len(data)) > v.settings.PartBytes {
		return v.multipart(ctx, path, int64(len(data)), func(start, end int64) ([]byte, error) { return data[start:end], nil })
	}
	return v.put(ctx, path, data)
}

// PutFile uploads one local file. Files over the part size go up in parts, read a part at a time.
func (v *Volume) PutFile(ctx context.Context, local, remote string) (Entry, error) {
	info, err := os.Stat(local)
	if err != nil {
		return Entry{}, err
	}
	if info.Size() <= v.settings.PartBytes {
		data, err := os.ReadFile(local)
		if err != nil {
			return Entry{}, err
		}
		return v.put(ctx, remote, data)
	}
	file, err := os.Open(local)
	if err != nil {
		return Entry{}, err
	}
	defer file.Close()
	return v.multipart(ctx, remote, info.Size(), func(start, end int64) ([]byte, error) {
		chunk := make([]byte, end-start)
		_, err := file.ReadAt(chunk, start)
		if err == io.EOF {
			err = nil
		}
		return chunk, err
	})
}

func (v *Volume) put(ctx context.Context, remote string, data []byte) (Entry, error) {
	resp, err := v.call(ctx, request{method: http.MethodPut, url: v.url("files/" + encodePath(remote)), body: data}, true)
	if err != nil {
		return Entry{}, err
	}
	var s stored
	if err := decode(resp, &s); err != nil {
		return Entry{}, err
	}
	return Entry{Path: s.Path, Size: s.Size, ETag: s.ETag}, nil
}

func (v *Volume) multipart(ctx context.Context, remote string, size int64, slice func(start, end int64) ([]byte, error)) (Entry, error) {
	resp, err := v.call(ctx, request{method: http.MethodPost, url: v.url("uploads"), json: map[string]any{"path": remote, "size": size}}, true)
	if err != nil {
		return Entry{}, err
	}
	var started struct {
		UploadID string `json:"upload_id"`
		PartSize int64  `json:"part_size"`
	}
	if err := decode(resp, &started); err != nil {
		return Entry{}, err
	}
	if started.PartSize <= 0 {
		started.PartSize = v.settings.PartBytes
	}
	route := "uploads/" + url.PathEscape(started.UploadID)
	count := (size + started.PartSize - 1) / started.PartSize
	type part struct {
		Part int64  `json:"part"`
		ETag string `json:"etag"`
	}
	parts := make([]part, count)
	errs := make(chan error, count)
	next := make(chan int64, count)
	for n := int64(1); n <= count; n++ {
		next <- n
	}
	close(next)
	var wg sync.WaitGroup
	for w := 0; w < uploads && int64(w) < count; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for number := range next {
				start := (number - 1) * started.PartSize
				chunk, err := slice(start, min(start+started.PartSize, size))
				if err != nil {
					errs <- err
					return
				}
				resp, err := v.call(ctx, request{method: http.MethodPut, url: v.url(route + "/" + strconv.FormatInt(number, 10)), body: chunk, query: url.Values{"path": {remote}}}, false)
				if err != nil {
					errs <- err
					return
				}
				var sent part
				if err := decode(resp, &sent); err != nil {
					errs <- err
					return
				}
				parts[number-1] = part{Part: number, ETag: sent.ETag}
			}
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		if resp, abortErr := v.call(context.Background(), request{method: http.MethodDelete, url: v.url(route), query: url.Values{"path": {remote}}}, false); abortErr == nil {
			resp.Body.Close()
		}
		return Entry{}, err
	}
	resp, err = v.call(ctx, request{method: http.MethodPost, url: v.url(route + "/complete"), json: map[string]any{"path": remote, "parts": parts}}, false)
	if err != nil {
		return Entry{}, err
	}
	var s stored
	if err := decode(resp, &s); err != nil {
		return Entry{}, err
	}
	return Entry{Path: s.Path, Size: s.Size, ETag: s.ETag}, nil
}

// PutDirectory uploads a folder's files under remote and returns how many were uploaded.
func (v *Volume) PutDirectory(ctx context.Context, local, remote string) (int, error) {
	prefix := remote
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	files, err := walk(local)
	if err != nil {
		return 0, err
	}
	err = pool(len(files), uploads, func(i int) error {
		rel, _ := filepath.Rel(local, files[i])
		_, err := v.PutFile(ctx, files[i], prefix+filepath.ToSlash(rel))
		return err
	})
	return len(files), err
}

func (v *Volume) downloadTo(ctx context.Context, remote, target string) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	body, err := v.Open(ctx, remote)
	if err != nil {
		return err
	}
	defer body.Close()
	temp, err := os.CreateTemp(filepath.Dir(target), ".surfra-")
	if err != nil {
		return err
	}
	if _, err := io.Copy(temp, body); err != nil {
		temp.Close()
		os.Remove(temp.Name())
		return err
	}
	if err := temp.Close(); err != nil {
		os.Remove(temp.Name())
		return err
	}
	return os.Rename(temp.Name(), target)
}

func (v *Volume) head(ctx context.Context, path string) ([]Entry, error) {
	resp, err := send(ctx, v.http, v.settings, request{method: http.MethodHead, url: v.url("files/" + encodePath(path))})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode >= 300 {
		return nil, errorFrom(resp)
	}
	return []Entry{{Path: path, Size: resp.ContentLength, ETag: resp.Header.Get("ETag"), Modified: resp.Header.Get("Last-Modified")}}, nil
}

func (v *Volume) root() string { return filepath.Join(v.settings.Cache, "vol-"+v.Name) }

func (v *Volume) statePath() string {
	return filepath.Join(v.settings.Cache, ".state", "vol-"+v.Name+".json")
}

func (v *Volume) loadState() map[string]known {
	state := map[string]known{}
	if data, err := os.ReadFile(v.statePath()); err == nil {
		_ = json.Unmarshal(data, &state)
	}
	return state
}

func (v *Volume) saveState(state map[string]known) error {
	path := v.statePath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, _ := json.Marshal(state)
	if err := os.WriteFile(path+".tmp", data, 0o644); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

func mtimeNs(path string) (int64, int64, bool) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return 0, 0, false
	}
	return info.Size(), info.ModTime().UnixNano(), true
}

// VolumePath is a file or folder inside a volume. A trailing slash means a folder.
type VolumePath struct {
	volume *Volume
	path   string
}

// Path names a file or folder inside the volume.
func (v *Volume) Path(path string) VolumePath {
	return VolumePath{volume: v, path: strings.TrimLeft(path, "/")}
}

// Local downloads this piece into the local cache, read-only, and returns its real path.
func (p VolumePath) Local(ctx context.Context) (string, error) {
	return p.volume.sync(ctx, p.path, Read, false)
}

// LocalWith syncs with a mode; with refresh it looks again even if this volume already synced it.
func (p VolumePath) LocalWith(ctx context.Context, mode Mode, refresh bool) (string, error) {
	return p.volume.sync(ctx, p.path, mode, refresh)
}

// Download is a plain copy into dest, outside the sync cache.
func (p VolumePath) Download(ctx context.Context, dest string) (string, error) {
	if isFolder(p.path) {
		entries, err := p.volume.Ls(ctx, p.path, true)
		if err != nil {
			return "", err
		}
		for _, entry := range entries {
			if err := p.volume.downloadTo(ctx, entry.Path, filepath.Join(dest, entry.Path[len(p.path):])); err != nil {
				return "", err
			}
		}
		return dest, nil
	}
	target := dest
	if info, err := os.Stat(dest); err == nil && info.IsDir() {
		target = filepath.Join(dest, filepath.Base(p.path))
	}
	return dest, p.volume.downloadTo(ctx, p.path, target)
}

func (v *Volume) sync(ctx context.Context, path string, mode Mode, refresh bool) (string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	root := v.root()
	target := filepath.Join(root, filepath.FromSlash(path))
	if previous, ok := v.synced[path]; ok && !refresh {
		if previous != mode {
			v.synced[path] = ReadWrite
		}
		return target, nil
	}
	folder := isFolder(path)
	if mode == Write {
		// Write-only: nothing to download, only a place to write.
		dir := target
		if !folder {
			dir = filepath.Dir(target)
		}
		v.synced[path] = mode
		return target, os.MkdirAll(dir, 0o755)
	}
	state := v.loadState()
	var listed []Entry
	var err error
	if folder {
		listed, err = v.Ls(ctx, path, true)
	} else {
		listed, err = v.head(ctx, path)
	}
	if err != nil {
		return "", err
	}
	remote := map[string]Entry{}
	var changed []Entry
	for _, entry := range listed {
		remote[entry.Path] = entry
		k, seen := state[entry.Path]
		size, mtime, exists := mtimeNs(filepath.Join(root, filepath.FromSlash(entry.Path)))
		if !seen || k.ETag != entry.ETag || !exists || size != entry.Size || mtime != k.MtimeNs {
			changed = append(changed, entry)
		}
	}
	if err := pool(len(changed), downloads, func(i int) error {
		return v.downloadTo(ctx, changed[i].Path, filepath.Join(root, filepath.FromSlash(changed[i].Path)))
	}); err != nil {
		return "", err
	}
	for _, entry := range changed {
		_, mtime, _ := mtimeNs(filepath.Join(root, filepath.FromSlash(entry.Path)))
		state[entry.Path] = known{ETag: entry.ETag, Size: entry.Size, MtimeNs: mtime}
	}
	for key := range state {
		inside := key == path
		if folder {
			inside = strings.HasPrefix(key, path)
		}
		if _, ok := remote[key]; inside && !ok {
			os.Remove(filepath.Join(root, filepath.FromSlash(key)))
			delete(state, key)
		}
	}
	if err := v.saveState(state); err != nil {
		return "", err
	}
	if folder {
		if err := os.MkdirAll(target, 0o755); err != nil {
			return "", err
		}
	}
	v.synced[path] = mode
	return target, nil
}

// Commit uploads what changed in every folder this volume synced with Write or ReadWrite.
func (v *Volume) Commit(ctx context.Context) (Committed, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	state := v.loadState()
	root := v.root()
	var done Committed
	paths := make([]string, 0, len(v.synced))
	for path := range v.synced {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		mode := v.synced[path]
		if mode == Read {
			continue
		}
		folder := isFolder(path)
		base := filepath.Join(root, filepath.FromSlash(path))
		var found []string
		if folder {
			found, _ = walk(base)
		} else if _, _, ok := mtimeNs(base); ok {
			found = []string{base}
		}
		seen := map[string]bool{}
		for _, file := range found {
			if strings.HasPrefix(filepath.Base(file), ".surfra-") {
				continue
			}
			rel, _ := filepath.Rel(root, file)
			remote := filepath.ToSlash(rel)
			seen[remote] = true
			size, mtime, _ := mtimeNs(file)
			if k, ok := state[remote]; ok && k.Size == size && k.MtimeNs == mtime {
				continue
			}
			entry, err := v.PutFile(ctx, file, remote)
			if err != nil {
				return done, err
			}
			state[remote] = known{ETag: entry.ETag, Size: entry.Size, MtimeNs: mtime}
			done.Uploaded++
		}
		if mode == ReadWrite {
			for key := range state {
				inside := key == path
				if folder {
					inside = strings.HasPrefix(key, path)
				}
				if !inside || seen[key] {
					continue
				}
				if err := v.Rm(ctx, key); err != nil && !IsNotFound(err) {
					return done, err
				}
				delete(state, key)
				done.Deleted++
			}
		}
	}
	return done, v.saveState(state)
}

func walk(dir string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if path == dir && os.IsNotExist(err) {
				return filepath.SkipDir
			}
			return err
		}
		if entry.Type().IsRegular() {
			out = append(out, path)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

// pool runs work(0..n-1) with at most size at once and returns the first error.
func pool(n, size int, work func(i int) error) error {
	if n == 0 {
		return nil
	}
	next := make(chan int, n)
	for i := 0; i < n; i++ {
		next <- i
	}
	close(next)
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for w := 0; w < size && w < n; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				if err := work(i); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return fmt.Errorf("%w", err)
	}
	return nil
}
