# Web text, frame, loop, and reference video

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

Without an image the route uses `textToVideo`. Image modes use the current Grok
Web protocol on the same account-bound session:

- `mode=first_frame`: one `first_frame`; uses `imageToVideo.inputAssets`.
- `mode=last_frame`: one `last_frame`; uses `referenceToVideo.lastFrameAsset`.
- `mode=loop`: one or two images; pins the first and last asset, reusing one
  image for both pins when only one is supplied.
- `mode=reference`: one to nine repeated `image[]` files; one image uses
  `imageToVideo.useFirstFrame=false`, while multiple images use
  `referenceToVideo.inputAssets`.

Upload errors never fall back to text-to-video.

Use a PNG, JPEG or WebP image (maximum 20 MiB):

- Multipart file fields: `first_frame`, `last_frame`, repeated `image[]`, or the
  legacy single-image aliases `image`, `input_reference`, and `image_reference`.
- JSON: one of those fields containing a `data:image/...;base64,...` URL;
  `{ "url": "data:..." }` is also accepted.
- Reference mode accepts at most 9 images. Remote HTTP image URLs are rejected;
  URLs are not fetched from the server's network, avoiding SSRF and credential
  forwarding risks.

`aspect_ratio` selects `1:1` (default), `16:9`, `9:16`, `4:3`, `3:4`, `3:2` or
`2:3`. The default does not infer the uploaded image's ratio. The text,
first-frame, last-frame, loop, single-reference, and two-reference modes have
been live-verified at 480p/6s.
Quality and ratio are upstream presets, not promises of exact pixels: a 480p,
16:9 image-to-video test returned 736x400; the earlier 1:1 test returned 560x560.

```powershell
curl.exe "https://YOUR_GROK_GATEWAY/v1/videos/generations" -H "Authorization: Bearer YOUR_GROK_KEY" -F "model=Web/grok-imagine-video" -F "prompt=Animate the objects gently" -F "image=@C:\Pictures\source.png" -F "resolution=480p" -F "duration=6" -F "aspect_ratio=16:9"
```

The response also includes `mode`, `resolution`, `duration` and `aspect_ratio`.
Existing text-to-image and image-edit wire protocols are unchanged; image upload
encoding is shared between editing and video generation.

For NewAPI, upload `integrations/newapi/grok-web-video.js` manually and follow
the adjacent README. Merely listing the model in an OpenAI/Sora channel is not
sufficient. The plugin translates `/v1/videos` to this route.
