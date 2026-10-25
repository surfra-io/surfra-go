# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions are git tags (`vX.Y.Z`).

## [v0.2.0] - 2026-10-25

### Added
- `Settings.HTTPClient`, `Client.WithHTTPClient` and `Volume.WithHTTPClient` to send requests with your own `*http.Client`.
- `Call.LastView` and `Job.LastView`: the last fetched view, safe from any goroutine.
- `Client.SubmitWith`, `Client.EnqueueWith` and `App.EnqueueWith` with `CallbackURL` (sent as `callback_url`; stored, delivery is planned).

### Changed
- `Call` and `Job` are safe to share across goroutines: `View` is written under a lock.
- Credential files (`SURFRA_VOLUMES_TOKEN_FILE`, the saved login) are cached and re-read only when their mtime or size changes.
- With the SDK's own client, `Volume.Open` (and the downloads behind `Local` and `Download`) under a ctx with a deadline is bounded by that deadline instead of the 30-minute cap.

## [v0.1.0]

- First release: volumes, apps, workflows and jobs, standard library only.
