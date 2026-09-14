# Web image editing: local request signatures

Web image editing can sign REST requests locally with a matched browser
seed and SVG fingerprint (HEX). The pair is deployment data, not an account
password. Treat it as private configuration and keep it out of Git and logs.

Place a JSON object with `seed` and `hex` fields in
`GROK2API_DATA_DIR/web-statsig.json`. The seed must be Base64 encoding of 48 bytes;
HEX must contain hexadecimal digits. Set file permissions to restrict access to
the application's runtime user. The existing `data/` ignore rule excludes this
file when using the default data directory. Protect it explicitly if configuring
another directory.

Alternatively, set both `GROK2API_WEB_STATSIG_SEED` and
`GROK2API_WEB_STATSIG_HEX` in the private process environment. Environment values
take precedence. File configuration is read per request, allowing pair rotation
without restarting the application. Invalid or incomplete pairs fail with a
sanitized configuration error. If no local pair is configured, the existing
external signer remains the fallback.

Each request signs its HTTP method, path, current timestamp and fingerprint,
then packs the seed and truncated SHA-256 digest into the 70-byte wire format.
The pair can be reused, but signatures are generated afresh. A future Grok
frontend change may require a new pair.

Image-edit and Web video REST requests use this signer. The Imagine
WebSocket text-to-image protocol is unchanged. Existing Resin account binding
continues to handle upload, generation and asset download.

Protocol reference reviewed:
[aurora-develop/grok2api, pure.go](https://github.com/aurora-develop/grok2api/blob/aa640954ee65229f6403027d36f337bfc05f5dd7/internal/grok/statsig/pure.go).
No captured pair from that repository is included here.
