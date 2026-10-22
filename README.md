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

## Credentials

Inside a Surfra app or job nothing needs setting: `SURFRA_VOLUMES_URL` is a local proxy that adds the
credential. Elsewhere, set `SURFRA_API_KEY`, or build `surfra.Settings` yourself and use `NewVolumeWith`
and `NewClientWith`.

The order is the same in every Surfra SDK: an explicit key, `SURFRA_VOLUMES_TOKEN_FILE`, `SURFRA_API_KEY`,
`SURFRA_REPLICA_TOKEN`, then the session `surfra login` saved.

## Tests

The tests are the shared [Surfra SDK cases](https://github.com/surfra-io/surfra-sdk-tests), run against its
fake server. Check it out next to this repository, or set `SURFRA_SDK_TESTS`, then `go test ./...`.
