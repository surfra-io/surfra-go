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

To be told when a call finishes, pass a callback URL. Surfra stores it with the call; delivery is planned.

```go
call, err := client.EnqueueWith(ctx, "embed", input, surfra.EnqueueOptions{CallbackURL: "https://example.com/hook"})
```

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

## Credentials

Inside a Surfra app or job nothing needs setting: `SURFRA_VOLUMES_URL` is a local proxy that adds the
credential. Elsewhere, set `SURFRA_API_KEY`, or build `surfra.Settings` yourself and use `NewVolumeWith`
and `NewClientWith`.

The order is the same in every Surfra SDK: an explicit key, `SURFRA_VOLUMES_TOKEN_FILE`, `SURFRA_API_KEY`,
`SURFRA_REPLICA_TOKEN`, then the session `surfra login` saved.

## Tests

The tests are the shared [Surfra SDK cases](https://github.com/surfra-io/surfra-sdk-tests), run against its
fake server. Check it out next to this repository, or set `SURFRA_SDK_TESTS`, then `go test ./...`.
