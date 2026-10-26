package surfra

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Client is the control plane: call apps and run jobs. It holds no mutable state, so one Client is safe
// to share across goroutines.
type Client struct {
	settings Settings
	http     *http.Client
}

// NewClient is a client using credentials and URLs from the environment.
func NewClient() *Client { return NewClientWith(SettingsFromEnv()) }

// NewClientWith is a client with explicit settings.
func NewClientWith(s Settings) *Client {
	h := s.HTTPClient
	if h == nil {
		h = &http.Client{Timeout: 60 * time.Second}
	}
	return &Client{settings: s, http: h}
}

// WithHTTPClient is a copy of this client that sends every request with h, such as one with a proxy,
// custom TLS or tracing. Per-request deadlines still come from ctx. The original client is unchanged.
func (c *Client) WithHTTPClient(h *http.Client) *Client {
	s := c.settings
	s.HTTPClient = h
	return NewClientWith(s)
}

func (c *Client) call(ctx context.Context, method, route string, body any, dest any) error {
	resp, err := do(ctx, c.http, c.settings, request{control: true, method: method, url: c.settings.Master + route, json: body})
	if err != nil {
		return err
	}
	return decode(resp, dest)
}

// Call is one call to an app. It is safe to share across goroutines: Get, Result and LastView may run at
// once. View is the last view fetched; it is kept for compatibility and is written under the Call's lock,
// so read it directly only when no other goroutine is using the Call, and use LastView otherwise.
// Views are replaced, never modified, so a map returned by LastView or Get stays as it was.
type Call struct {
	ID     string
	View   map[string]any
	mu     sync.RWMutex
	client *Client
}

// LastView is the last view fetched, without a request. Safe to call from any goroutine.
func (call *Call) LastView() map[string]any {
	call.mu.RLock()
	defer call.mu.RUnlock()
	return call.View
}

func (call *Call) setView(view map[string]any) {
	call.mu.Lock()
	call.View = view
	call.mu.Unlock()
}

// NewCall tracks a call from the view Submit returned.
func NewCall(c *Client, view map[string]any) *Call {
	id, _ := view["id"].(string)
	return &Call{ID: id, View: view, client: c}
}

// EnqueueOptions are optional settings for EnqueueWith.
type EnqueueOptions struct {
	// CallbackURL is sent as callback_url: Surfra POSTs the task's latest state there each time it changes.
	CallbackURL string
}

// Enqueue queues one call to an app and returns it before the app finishes.
func (c *Client) Enqueue(ctx context.Context, app string, input any) (*Call, error) {
	return c.EnqueueWith(ctx, app, input, EnqueueOptions{})
}

// EnqueueWith is Enqueue with options, such as a callback URL.
func (c *Client) EnqueueWith(ctx context.Context, app string, input any, opts EnqueueOptions) (*Call, error) {
	if input == nil {
		input = map[string]any{}
	}
	view, err := c.SubmitWith(ctx, []map[string]any{{"name": "call", "app_name": app, "depends_on": []string{}, "input": input}}, SubmitOptions{CallbackURL: opts.CallbackURL})
	if err != nil {
		return nil, err
	}
	id, _ := view["id"].(string)
	return &Call{ID: id, View: view, client: c}, nil
}

const rateRetries = 5

// SubmitOptions are optional settings for SubmitWith.
type SubmitOptions struct {
	Metadata map[string]any
	// CallbackURL is sent as callback_url: Surfra POSTs the task's latest state there each time it changes.
	CallbackURL string
}

// Submit starts a transaction. Over the organization's request rate, it waits as Surfra asks, up to five times.
func (c *Client) Submit(ctx context.Context, stages []map[string]any, metadata map[string]any) (map[string]any, error) {
	return c.SubmitWith(ctx, stages, SubmitOptions{Metadata: metadata})
}

// SubmitWith is Submit with options, such as a callback URL.
func (c *Client) SubmitWith(ctx context.Context, stages []map[string]any, opts SubmitOptions) (map[string]any, error) {
	metadata := opts.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}
	body := map[string]any{"stages": stages, "metadata": metadata}
	if opts.CallbackURL != "" {
		body["callback_url"] = opts.CallbackURL
	}
	for attempt := 0; ; attempt++ {
		resp, err := send(ctx, c.http, c.settings, request{control: true, method: http.MethodPost, url: c.settings.Master + "/v1/transactions", json: body})
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < rateRetries {
			resp.Body.Close()
			wait, err := strconv.ParseFloat(resp.Header.Get("Retry-After"), 64)
			if err != nil {
				wait = 1
			}
			if err := sleep(ctx, time.Duration(max(1, min(60, wait))*float64(time.Second))); err != nil {
				return nil, err
			}
			continue
		}
		if resp.StatusCode >= 300 {
			defer resp.Body.Close()
			return nil, errorFrom(resp)
		}
		var view map[string]any
		return view, decode(resp, &view)
	}
}

// App is a deployed app by name. No request is made until it is used.
type App struct {
	Name   string
	client *Client
}

// NewApp is an app using credentials and URLs from the environment.
func NewApp(name string) *App { return &App{Name: name, client: NewClient()} }

// NewAppWith is an app with explicit settings.
func NewAppWith(name string, s Settings) *App { return &App{Name: name, client: NewClientWith(s)} }

// AppSpec is what an app runs. Runtime is {"engine": "python"} or {"engine": "http", "port": 8000}.
type AppSpec struct {
	Image      string            `json:"image"`
	Entrypoint string            `json:"entrypoint"`
	Env        map[string]string `json:"env"`
	Autoscale  map[string]any    `json:"autoscale"` // machine rows, replicas and scaling, as in the docs' autoscale reference
	Runtime    map[string]any    `json:"runtime,omitempty"`
	Registry   map[string]any    `json:"registry,omitempty"`
}

func (a *App) route(rest string) string { return "/v1/apps/" + url.PathEscape(a.Name) + rest }

// Deploy creates the app, or updates the app with this name, and returns the saved app with its sfa_ id.
func (a *App) Deploy(ctx context.Context, spec AppSpec) (map[string]any, error) {
	if spec.Env == nil {
		spec.Env = map[string]string{}
	}
	if spec.Autoscale == nil {
		spec.Autoscale = map[string]any{}
	}
	var saved map[string]any
	return saved, a.client.call(ctx, http.MethodPut, a.route(""), spec, &saved)
}

// Info is the saved app: image, entrypoint, runtime and autoscale.
func (a *App) Info(ctx context.Context) (map[string]any, error) {
	var saved map[string]any
	return saved, a.client.call(ctx, http.MethodGet, a.route(""), nil, &saved)
}

// Enqueue queues one request and returns it before the app finishes.
func (a *App) Enqueue(ctx context.Context, input any) (*Call, error) {
	return a.client.Enqueue(ctx, a.Name, input)
}

// EnqueueWith is Enqueue with options, such as a callback URL.
func (a *App) EnqueueWith(ctx context.Context, input any, opts EnqueueOptions) (*Call, error) {
	return a.client.EnqueueWith(ctx, a.Name, input, opts)
}

// Run calls the app and waits for its output. Bound the wait with ctx.
func (a *App) Run(ctx context.Context, input any) (any, error) {
	return a.client.Run(ctx, a.Name, input)
}

// VolumeGrant is one volume an app may reach.
type VolumeGrant struct {
	Volume string `json:"volume"`
	Mode   string `json:"mode"`
}

// Volumes lists the volumes this app's replicas may reach.
func (a *App) Volumes(ctx context.Context) ([]VolumeGrant, error) {
	var out struct {
		Volumes []VolumeGrant `json:"volumes"`
	}
	return out.Volumes, a.client.call(ctx, http.MethodGet, a.route("/volumes"), nil, &out)
}

// SetVolumes replaces the app's volumes, such as "weights:r" and "outputs:rw".
func (a *App) SetVolumes(ctx context.Context, volumes []string) ([]VolumeGrant, error) {
	var out struct {
		Volumes []VolumeGrant `json:"volumes"`
	}
	return out.Volumes, a.client.call(ctx, http.MethodPut, a.route("/volumes"), map[string]any{"volumes": volumes}, &out)
}

// Workflow is a saved workflow by name.
type Workflow struct {
	Name   string
	client *Client
}

// NewWorkflow is a workflow using credentials and URLs from the environment.
func NewWorkflow(name string) *Workflow { return &Workflow{Name: name, client: NewClient()} }

// NewWorkflowWith is a workflow with explicit settings.
func NewWorkflowWith(name string, s Settings) *Workflow {
	return &Workflow{Name: name, client: NewClientWith(s)}
}

// Stage is one step of a workflow: the app it calls and the stages it waits for.
type Stage struct {
	Name    string   `json:"name"`
	App     string   `json:"app"`
	Depends []string `json:"depends"`
}

// Deploy saves the stages.
func (w *Workflow) Deploy(ctx context.Context, stages []Stage) (map[string]any, error) {
	for i := range stages {
		if stages[i].Depends == nil {
			stages[i].Depends = []string{}
		}
	}
	var saved map[string]any
	return saved, w.client.call(ctx, http.MethodPut, "/v1/workflows/"+url.PathEscape(w.Name), map[string]any{"stages": stages}, &saved)
}

// Enqueue starts one run. Stages with no dependencies receive input.
func (w *Workflow) Enqueue(ctx context.Context, input any) (*Call, error) {
	if input == nil {
		input = map[string]any{}
	}
	var saved struct {
		Stages []struct {
			Name    string   `json:"name"`
			App     string   `json:"app"`
			Depends []string `json:"depends"`
		} `json:"stages"`
	}
	if err := w.client.call(ctx, http.MethodGet, "/v1/workflows/"+url.PathEscape(w.Name), nil, &saved); err != nil {
		return nil, err
	}
	stages := make([]map[string]any, 0, len(saved.Stages))
	for _, row := range saved.Stages {
		stageInput := input
		if len(row.Depends) > 0 {
			stageInput = map[string]any{}
		}
		depends := row.Depends
		if depends == nil {
			depends = []string{}
		}
		stages = append(stages, map[string]any{"name": row.Name, "app_name": row.App, "depends_on": depends, "input": stageInput})
	}
	view, err := w.client.Submit(ctx, stages, map[string]any{"workflow": w.Name})
	if err != nil {
		return nil, err
	}
	id, _ := view["id"].(string)
	return &Call{ID: id, View: view, client: w.client}, nil
}

// Run starts one run and waits. It returns {stage name: output}, or the output for a single stage.
func (w *Workflow) Run(ctx context.Context, input any) (any, error) {
	call, err := w.Enqueue(ctx, input)
	if err != nil {
		return nil, err
	}
	return call.Result(ctx)
}

// Run calls an app and waits for its output. Bound the wait with ctx.
func (c *Client) Run(ctx context.Context, app string, input any) (any, error) {
	call, err := c.Enqueue(ctx, app, input)
	if err != nil {
		return nil, err
	}
	return call.Result(ctx)
}

// Get refreshes the call's view.
func (call *Call) Get(ctx context.Context) (map[string]any, error) {
	var view map[string]any
	if err := call.client.call(ctx, http.MethodGet, "/v1/transactions/"+url.PathEscape(call.ID), nil, &view); err != nil {
		return nil, err
	}
	call.setView(view)
	return view, nil
}

// Result waits for the call and returns its output. A failed call returns each stage's error.
func (call *Call) Result(ctx context.Context) (any, error) {
	wait := 100 * time.Millisecond
	var view map[string]any
	for {
		var err error
		view, err = call.Get(ctx)
		if err != nil {
			return nil, err
		}
		status, _ := view["status"].(string)
		if status == "succeeded" || status == "failed" {
			break
		}
		if err := sleep(ctx, wait); err != nil {
			return nil, fmt.Errorf("call %s is still %s: %w", call.ID, status, err)
		}
		wait = min(wait*2, 2*time.Second)
	}
	stages, _ := view["stages"].([]any)
	if view["status"] != "succeeded" {
		var reasons []string
		for _, raw := range stages {
			stage, _ := raw.(map[string]any)
			reason := "failed"
			if output, ok := stage["output"].(map[string]any); ok {
				if text, ok := output["error"].(string); ok {
					reason = text
				}
			}
			reasons = append(reasons, fmt.Sprintf("%v: %s", stage["name"], reason))
		}
		return nil, fmt.Errorf("%s", strings.Join(reasons, "; "))
	}
	if len(stages) == 1 {
		return stages[0].(map[string]any)["output"], nil
	}
	out := map[string]any{}
	for _, raw := range stages {
		stage := raw.(map[string]any)
		out[fmt.Sprint(stage["name"])] = stage["output"]
	}
	return out, nil
}

// JobSpec is what to run. Unset fields are left to the job spec, if any.
type JobSpec struct {
	Entrypoint  string            `json:"entrypoint,omitempty"`
	Image       string            `json:"image,omitempty"`
	Spec        string            `json:"spec,omitempty"`
	Args        []string          `json:"args,omitempty"`
	Providers   []map[string]any  `json:"providers,omitempty"` // machine rows in order, such as {"provider": "runpod", "gpu": "rtx4090"}
	Env         map[string]string `json:"env,omitempty"`
	Volumes     []string          `json:"volumes,omitempty"` // such as "weights:r"; missing volumes are created
	Name        string            `json:"name,omitempty"`
	DeadlineSec int               `json:"deadline_sec,omitempty"`
}

// Job is one job. It is safe to share across goroutines: Get, Wait, Stop, Status and LastView may run at
// once. View is the last view fetched; it is kept for compatibility and is written under the Job's lock,
// so read it directly only when no other goroutine is using the Job, and use LastView otherwise.
type Job struct {
	ID     string
	View   map[string]any
	mu     sync.RWMutex
	client *Client
}

// LastView is the last view fetched, without a request. Safe to call from any goroutine.
func (j *Job) LastView() map[string]any {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.View
}

func (j *Job) setView(view map[string]any) {
	j.mu.Lock()
	j.View = view
	j.mu.Unlock()
}

// SubmitJob runs a command once on one machine.
func (c *Client) SubmitJob(ctx context.Context, spec JobSpec) (*Job, error) {
	var view map[string]any
	if err := c.call(ctx, http.MethodPost, "/v1/jobs", spec, &view); err != nil {
		return nil, err
	}
	id, _ := view["id"].(string)
	return &Job{ID: id, View: view, client: c}, nil
}

// Job looks up a job by id.
func (c *Client) Job(ctx context.Context, id string) (*Job, error) {
	job := &Job{ID: id, client: c}
	_, err := job.Get(ctx)
	return job, err
}

// Status is the job's last known status.
func (j *Job) Status() string {
	status, _ := j.LastView()["status"].(string)
	return status
}

// Get refreshes the job's view.
func (j *Job) Get(ctx context.Context) (map[string]any, error) {
	var view map[string]any
	if err := j.client.call(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(j.ID), nil, &view); err != nil {
		return nil, err
	}
	j.setView(view)
	return view, nil
}

// Stop asks Surfra to stop the job.
func (j *Job) Stop(ctx context.Context) (map[string]any, error) {
	var view map[string]any
	if err := j.client.call(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(j.ID)+"/stop", map[string]any{}, &view); err != nil {
		return nil, err
	}
	j.setView(view)
	return view, nil
}

// LogLine is one line of a job's output. S is out, err, or sys for Surfra's own lines.
type LogLine struct {
	T string `json:"t"`
	S string `json:"s"`
	M string `json:"m"`
}

// Logs returns the job's lines after `after` (a timestamp from an earlier line), oldest first.
func (j *Job) Logs(ctx context.Context, after string) ([]LogLine, error) {
	var out []LogLine
	for {
		query := url.Values{"limit": {"1000"}, "order": {"asc"}}
		if after != "" {
			query.Set("after", after)
		}
		var page struct {
			Lines []LogLine `json:"lines"`
		}
		if err := j.client.call(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(j.ID)+"/logs?"+query.Encode(), nil, &page); err != nil {
			return out, err
		}
		out = append(out, page.Lines...)
		if len(page.Lines) > 0 {
			after = page.Lines[len(page.Lines)-1].T
		}
		if len(page.Lines) < 1000 {
			return out, nil
		}
	}
}

// Wait polls until the job ends: succeeded, failed, stopped or lost. Bound the wait with ctx.
func (j *Job) Wait(ctx context.Context, interval time.Duration) (map[string]any, error) {
	for {
		view, err := j.Get(ctx)
		if err != nil {
			return nil, err
		}
		status, _ := view["status"].(string)
		switch status {
		case "succeeded", "failed", "stopped", "lost":
			return view, nil
		}
		if err := sleep(ctx, interval); err != nil {
			return nil, fmt.Errorf("job %s is still %s: %w", j.ID, status, err)
		}
	}
}
