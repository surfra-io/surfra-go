# surfra-go

The Surfra SDK for Go: volumes, apps and jobs on serverless GPUs across your own clouds. Standard library only.

```bash
go get github.com/surfra-io/surfra-go
```

## Volumes

```go
import "github.com/surfra-io/surfra-go"

weights := surfra.NewVolume("weights")

// A folder of the volume, synced to local disk. Only changed files download.
dir, err := weights.Path("minilm/").Local(ctx)

// One file in, one file out.
_, err = weights.WriteFile(ctx, "notes/run-1.txt", []byte("done"))
data, err := weights.ReadFile(ctx, "notes/run-1.txt")

// Change a folder in place, then upload what changed.
ckpts := surfra.NewVolume("ckpts")
runDir, err := ckpts.Path("run-7/").LocalWith(ctx, surfra.ReadWrite, false)
// ...write files into runDir...
_, err = ckpts.Commit(ctx)
```

## Apps and jobs

```go
client := surfra.NewClient()
out, err := client.Run(ctx, "embed", map[string]any{"texts": []string{"hello"}})

job, err := client.SubmitJob(ctx, surfra.JobSpec{
	Entrypoint: "python train.py --epochs 3",
	Image:      "ghcr.io/acme/train:3",
	Providers:  []map[string]any{{"provider": "runpod", "gpu": "rtx4090"}},
	Volumes:    []string{"weights:r", "ckpts:rw"},
})
view, err := job.Wait(ctx, 10*time.Second)
```

Bound waits with the context, such as `context.WithTimeout`.

To be told when a call changes, pass a callback URL. Surfra POSTs the task's latest state there each time it changes.

```go
call, err := client.EnqueueWith(ctx, "embed", input, surfra.EnqueueOptions{CallbackURL: "https://example.com/hook"})
```

### Verify callbacks

Each callback carries `X-Surfra-Signature`. `WebhookHandler` reads the body, checks the signature against
your signing secrets (pass the old and the new one while rotating) and answers 401 when it does not match:

```go
secrets := []string{os.Getenv("SURFRA_WEBHOOK_SECRET")}
http.Handle("/hook", surfra.WebhookHandler(secrets, func(w http.ResponseWriter, r *http.Request, cb *surfra.Callback) {
	log.Printf("%s seq %d: task %s is %s", cb.Type, cb.Seq, cb.Task.ID, cb.Task.Status)
	w.WriteHeader(http.StatusNoContent)
}))
```

Elsewhere, call `surfra.VerifyWebhook(body, r.Header, secrets...)` with the exact raw body bytes.

## Goroutines and HTTP

Every call is a blocking call that takes a `ctx`; run calls concurrently with `go`. `Client` and `Volume`
are safe to share. `Call` and `Job` are too: `Get`, `Result`, `Wait` and `Stop` may run from many
goroutines at once. Read the last view with `LastView()` rather than the `View` field when the handle is
shared.

To use your own `*http.Client` (proxy, TLS, tracing, timeouts), set `Settings.HTTPClient` or:

```go
client := surfra.NewClient().WithHTTPClient(httpClient)
weights := surfra.NewVolume("weights").WithHTTPClient(httpClient)
```

The SDK's own client caps a control-plane request at 60 s and a volume request at 30 min. A volume stream
(`Open`, and the downloads `Local` and `Download` make) under a ctx with a deadline is bounded by that
deadline instead, so set one for very large files.

## Errors and retries

When Surfra can't take work right now, it refuses with a typed error and queues nothing, so every
request retries it for you, after the `Retry-After` Surfra sends:

| Status | `(*surfra.Error).Type` | Retried? |
| --- | --- | --- |
| 429 | `RATE_LIMITED`: your organization's per-minute limit | yes |
| 503 | `QUEUE_FULL`: the app's queue is as long as its machines can clear soon | yes |
| 503 | `SERVER_BUSY`: Surfra is short of database connections | yes |
| 503 | `APP_PAUSED`, `DEFCON_ACTIVE`: the app is paused, or the kill switch is on | no |
| 402 | `HTTP_CALLS_LIMIT`: this month's included HTTP calls are used up and the organization has no card on file. Add one in Settings, or wait for the month to turn | no |
| 502/503/504 with no Surfra error | `""` | only reads (`GET`) |
| a connection that never got through | `""` | yes |
| other 4xx | | no |

**Limits:** at most 7 retries (8 attempts) within 3 minutes of the first try, and never past `ctx`'s
deadline. Without `Retry-After`, the waits back off exponentially with jitter (0.5 s base, 30 s cap).
Change the limits per client, or per call through the context:

```go
client := surfra.NewClient().WithRetry(surfra.RetryPolicy{
	MaxRetries: surfra.Retries(3),
	Deadline:   30 * time.Second,
	OnRetry:    func(e surfra.RetryEvent) { log.Printf("surfra: %s, retrying in %v", e.Reason, e.Wait) },
})
ctx = surfra.WithRetryPolicy(ctx, surfra.NoRetries())   // calls with this ctx are never retried
```

`Volume.WithRetry` works the same way, and `slog` gets a Debug record for every retry.

**The final error:** `*surfra.Error` carries `Status`, `Type`, `Message`, `Attempts` and `RetryAfter`, and
`Err` for a connection error (`Status` is 0 then).

**When the outcome is unknown:** a submission (a transaction or a job) that got a 504, or lost its
connection after it was sent, is never retried, because it may already be running.
`errors.Is(err, surfra.ErrOutcomeUnknown)` is true for it.
- Check for the work before submitting again.
- For long work, prefer `Enqueue` then `Result`, or a callback URL, over one long call.

## Credentials

Inside a Surfra app or job nothing needs setting: `SURFRA_VOLUMES_URL` is a local proxy that adds the
credential. Elsewhere, set `SURFRA_API_KEY`, or build `surfra.Settings` yourself and use `NewVolumeWith`
and `NewClientWith`.

The order is the same in every Surfra SDK: an explicit key, `SURFRA_VOLUMES_TOKEN_FILE`, `SURFRA_API_KEY`,
`SURFRA_REPLICA_TOKEN`, then the session `surfra login` saved.

## Tests

The tests are the shared [Surfra SDK cases](https://github.com/surfra-io/surfra-sdk-tests), run against its
fake server. Check it out next to this repository, or set `SURFRA_SDK_TESTS`, then `go test ./...`.
