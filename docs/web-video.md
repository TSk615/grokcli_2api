# Web text-to-video

Use `POST /v1/videos/generations` with a Grok gateway API key:

```json
{
  "model": "Web/grok-imagine-video",
  "prompt": "A blue ball rolling gently across a white table.",
  "resolution": "480p",
  "duration": 6,
  "aspect_ratio": "1:1"
}
```

Resolution defaults to `480p`, duration to 6 seconds. Basic/free accounts can
request 480p; their duration is capped at 6 seconds. `720p` remains selectable,
but availability depends on upstream account permissions and quota. A 429 does
not establish whether a resolution is supported.

Enable `GROK2API_WEB_ENABLED=1` and configure the private signature pair described
in [Web REST signatures](web-image-edit-statsig.md). Web video does not depend on
the Console provider or Console media switches. Generation and video download
use the same account-bound Resin transport when Resin is enabled.

The synchronous response contains `status: "completed"`, `url`, `bytes` and
`content_type`. The URL points to `/v1/media/videos/<filename>` on this gateway.
Allow several minutes for generation. This is a custom video endpoint, not a
chat-completions request. NewAPI requires separate compatible video routing;
listing a model alone does not ensure this endpoint is forwarded.

This route currently generates video from text. It does not yet accept an input
image for image-to-video generation. Existing text-to-image and image-edit
protocols are unchanged.
