package surfra

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is the control plane: call apps and run jobs.
type Client struct {
	settings Settings
	http     *http.Client
}

// NewClient is a client using credentials and URLs from the environment.
func NewClient() *Client { return NewClientWith(SettingsFromEnv()) }

// NewClientWith is a client with explicit settings.
func NewClientWith(s Settings) *Client {
	return &Client{settings: s, http: &http.Client{Timeout: 60 * time.Second}}
}

func (c *Client) call(ctx context.Context, method, route string, body any, dest any) error {
	resp, err := do(ctx, c.http, c.settings, request{method: method, url: c.settings.Master + route, json: body})
	if err != nil {
		return err
	}
	return decode(resp, dest)
}

// Call is one call to an app.
type Call struct {
	ID     string
	View   map[string]any
	client *Client
}

// Enqueue queues one call to an app and returns it before the app finishes.
func (c *Client) Enqueue(ctx context.Context, app string, input any) (*Call, error) {
	if input == nil {
		input = map[string]any{}
	}
	body := map[string]any{"stages": []map[string]any{{"name": "call", "app_name": app, "depends_on": []string{}, "input": input}}, "metadata": map[string]any{}}
	var view map[string]any
	if err := c.call(ctx, http.MethodPost, "/v1/transactions", body, &view); err != nil {
		return nil, err
	}
	id, _ := view["id"].(string)
	return &Call{ID: id, View: view, client: c}, nil
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
	call.View = view
	return view, nil
}

// Result waits for the call and returns its output. A failed call returns each stage's error.
func (call *Call) Result(ctx context.Context) (any, error) {
	wait := 100 * time.Millisecond
	for {
		view, err := call.Get(ctx)
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
	stages, _ := call.View["stages"].([]any)
	if call.View["status"] != "succeeded" {
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

// Job is one job.
type Job struct {
	ID     string
	View   map[string]any
	client *Client
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
	status, _ := j.View["status"].(string)
	return status
}

// Get refreshes the job's view.
func (j *Job) Get(ctx context.Context) (map[string]any, error) {
	var view map[string]any
	if err := j.client.call(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(j.ID), nil, &view); err != nil {
		return nil, err
	}
	j.View = view
	return view, nil
}

// Stop asks Surfra to stop the job.
func (j *Job) Stop(ctx context.Context) (map[string]any, error) {
	var view map[string]any
	if err := j.client.call(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(j.ID)+"/stop", map[string]any{}, &view); err != nil {
		return nil, err
	}
	j.View = view
	return view, nil
}

// Wait polls until the job ends: succeeded, failed, stopped or lost. Bound the wait with ctx.
func (j *Job) Wait(ctx context.Context, interval time.Duration) (map[string]any, error) {
	for {
		view, err := j.Get(ctx)
		if err != nil {
			return nil, err
		}
		switch j.Status() {
		case "succeeded", "failed", "stopped", "lost":
			return view, nil
		}
		if err := sleep(ctx, interval); err != nil {
			return nil, fmt.Errorf("job %s is still %s: %w", j.ID, j.Status(), err)
		}
	}
}
