# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions are git tags (`vX.Y.Z`).

## [Unreleased]

### Added
- Every request sends `Surfra-Client: surfra-go/<version>`, so the control plane can trace errors to a release.
- Overload retries for every request:
  - **Retried for every request:** 429 `RATE_LIMITED`, 503 `QUEUE_FULL` and 503 `SERVER_BUSY`, after `Retry-After`, plus connections that never got through.
  - **Retried only for `GET`/`HEAD`:** 502/503/504 without a Surfra error body, and connections lost after sending.
  - **Never retried:** `APP_PAUSED`, `DEFCON_ACTIVE`, other 4xx, and submissions that got a 504 or lost their connection after sending.
- `RetryPolicy` (`MaxRetries` 7, `Deadline` 3 min, `OnRetry`), `Settings.Retry`, `Client.WithRetry`, `Volume.WithRetry`, and `WithRetryPolicy(ctx, p)` for one call. `Retries(n)` and `NoRetries()` build them.
- `Error` gains `Type`, `Message`, `Attempts`, `RetryAfter`, `OutcomeUnknown` and `Err`. `ErrOutcomeUnknown` works with `errors.Is`, and `IsRetryable(err)` is added.
- 402 `HTTP_CALLS_LIMIT` (the organization used its included HTTP calls this month and has no card on file) is named in the docs and never retried; conformance case r13.

### Changed
- The submit-only 429 retry (5 tries, 1 to 60 s) is replaced by the policy above.

## [v0.2.1] - 2026-10-26

### Added
- `VerifyWebhook`, `VerifyWebhookAt` and `WebhookHandler` verify a callback's `X-Surfra-Signature` (HMAC-SHA256, 300 s tolerance, several secrets during rotation) and return the parsed `Callback`.

## [v0.2.0] - 2026-10-25

### Added
- `Settings.HTTPClient`, `Client.WithHTTPClient` and `Volume.WithHTTPClient` to send requests with your own `*http.Client`.
- `Call.LastView` and `Job.LastView`: the last fetched view, safe from any goroutine.
- `Client.SubmitWith`, `Client.EnqueueWith` and `App.EnqueueWith` with `CallbackURL` (sent as `callback_url`: Surfra POSTs the task's latest state there each time it changes).

### Changed
- `Call` and `Job` are safe to share across goroutines: `View` is written under a lock.
- Credential files (`SURFRA_VOLUMES_TOKEN_FILE`, the saved login) are cached and re-read only when their mtime or size changes.
- With the SDK's own client, `Volume.Open` (and the downloads behind `Local` and `Download`) under a ctx with a deadline is bounded by that deadline instead of the 30-minute cap.

## [v0.1.0]

- First release: volumes, apps, workflows and jobs, standard library only.
