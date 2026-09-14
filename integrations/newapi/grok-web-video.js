/*
 * NewAPI v1.0.0-rc.33 Task Plugin API v1; single-file, no imports/secrets.
 * Upload manually. Create a dedicated "Task Plugin" channel, select this
 * plugin, set Base URL to your Grok gateway origin (no /v1), and its API key.
 * Remove Web/grok-imagine-video from old OpenAI/Sora channels to avoid routing
 * it there. Configure this model's price and your token's group/model access.
 * POST /v1/videos waits for upstream generation; allow >=300s where possible.
 * GET /v1/videos/:id and /:id/content are owned/authenticated by NewAPI.
 * Default 480p/6s/1:1. size selects aspect ratio, NOT guaranteed pixels.
 * File upload or image data URL supported; remote image URLs intentionally
 * rejected. Existing backend must include imageToVideo support.
 */
export const meta = {
  apiVersion: 1,
  key: "grok_web_video",
  name: "Grok Web Video",
  icon: "text:GW",
  version: "1.0.0",
  author: {name: "Rainflow"},
  description: {
    en: "Grok Web text-to-video and image-to-video",
    zh: "Grok Web 文生视频与图生视频",
  },
  models: ["Web/grok-imagine-video"],
  protocols: ["openai_video"],
  fetchMode: "per_task",
  usageSchema: {
    seconds: {type: "number", unit: "second", description: {en: "Video generation unit price", zh: "视频生成单价"}},
    resolution: {enum: ["480p", "720p"], description: {en: "Output video resolution", zh: "输出视频分辨率"}},
  },
};

const RATIOS = ["1:1", "16:9", "9:16", "4:3", "3:4", "3:2", "2:3"];
const IMAGE_FIELDS = ["image", "input_reference", "image_reference"];
const MAX_IMAGE = 20 * 1024 * 1024;
function text(value) { return typeof value === "string" ? value.trim() : ""; }
function object(value) { return !!value && typeof value === "object" && !Array.isArray(value); }
function base(ctx) {
  const value = text(ctx.baseUrl).replace(/\/+$/, "").replace(/\/v1$/, "");
  if (!/^https?:\/\/[^\s/?#@]+(?::\d+)?$/.test(value)) throw new Error("Channel Base URL must be the Grok gateway origin without a path");
  return value;
}
function authorization(ctx) {
  const value = ctx.apiKey ? "Bearer " + ctx.apiKey : "";
  if (typeof value !== "string" || !value || /[\r\n]/.test(value)) throw new Error("Channel API key is not configured");
  return value;
}
function normalize(ctx) {
  const body = ctx.body || {};
  let req;
  if (body.kind === "json") {
    if (!object(body.value)) throw new Error("JSON object required");
    req = body.value;
  } else if (body.kind === "multipart" || body.kind === "form") {
    req = {};
    for (const key of Object.keys(body.fields || {})) {
      const values = body.fields[key];
      if (!Array.isArray(values) || values.length !== 1) throw new Error("Each form field must be provided once");
      if (key === "__proto__" || key === "constructor" || key === "prototype") throw new Error("Invalid form field");
      req[key] = values[0];
    }
  } else throw new Error("JSON or multipart body required");
  if (!text(ctx.model)) throw new Error("model is required");
  const prompt = text(req.prompt);
  if (!prompt) throw new Error("prompt is required");
  if (req.seconds !== undefined && req.duration !== undefined && Number(req.seconds) !== Number(req.duration)) throw new Error("seconds and duration conflict");
  const requested = req.seconds === undefined ? (req.duration === undefined ? 6 : req.duration) : req.seconds;
  const seconds = Number(requested);
  if (!Number.isInteger(seconds) || seconds < 1 || seconds > 15) throw new Error("seconds must be an integer between 1 and 15; Basic accounts are capped at 6");
  let resolution = text(req.resolution).toLowerCase();
  const quality = text(req.quality).toLowerCase();
  if (quality && quality !== "standard" && quality !== "high") throw new Error("quality must be standard or high");
  const qualityResolution = quality === "high" ? "720p" : "480p";
  if (quality && resolution && resolution !== qualityResolution) throw new Error("quality and resolution conflict");
  resolution = resolution || qualityResolution;
  if (resolution !== "480p" && resolution !== "720p") throw new Error("resolution must be 480p or 720p");
  let ratio = text(req.aspect_ratio);
  const size = text(req.size).toLowerCase();
  if (size) {
    const match = /^(\d{1,5})x(\d{1,5})$/.exec(size);
    if (!match || Number(match[1]) < 1 || Number(match[2]) < 1) throw new Error("size must be WIDTHxHEIGHT");
    const w = Number(match[1]), h = Number(match[2]);
    let derived = "";
    for (const candidate of RATIOS) {
      const parts = candidate.split(":");
      if (w * Number(parts[1]) === h * Number(parts[0])) derived = candidate;
    }
    if (!derived) throw new Error("size must use a supported aspect ratio; specify aspect_ratio instead");
    if (ratio && ratio !== derived) throw new Error("size and aspect_ratio conflict");
    ratio = derived;
  }
  ratio = ratio || "1:1";
  if (RATIOS.indexOf(ratio) < 0) throw new Error("Unsupported aspect_ratio");
  const normalized = {prompt, duration: Math.min(seconds, 6), resolution, aspect_ratio: ratio};
  const supplied = IMAGE_FIELDS.filter(function (key) { return req[key] !== undefined; });
  const files = body.files || [];
  if (supplied.length + files.length > 1) throw new Error("Provide only one input image");
  if (req.images !== undefined || req.image_url !== undefined || req.image_bytes !== undefined) throw new Error("Use image or input_reference for one input image");
  if (files.length) {
    const file = files[0];
    if (IMAGE_FIELDS.indexOf(file.field) < 0) throw new Error("Unsupported image upload field");
    if (file.size <= 0 || file.size > MAX_IMAGE) throw new Error("Image must contain 1 byte to 20 MiB");
    // Opaque upload reference resolved by NewAPI, not by JavaScript.
    normalized.image = {__fileRef: file.ref, encoding: "dataUrl", maxBytes: MAX_IMAGE};
  } else if (supplied.length) {
    let image = req[supplied[0]];
    if (object(image)) image = image.url || image.image_url;
    if (typeof image !== "string" || !/^data:image\/(png|jpeg|webp);base64,[A-Za-z0-9+/]+={0,2}$/.test(image)) throw new Error("Use a PNG/JPEG/WebP data URL or upload a file; remote image URLs are not supported");
    if (image.length > Math.ceil(MAX_IMAGE / 3) * 4 + 64) throw new Error("Image exceeds 20 MiB");
    normalized.image = image;
  }
  return {kind: "submit", model: ctx.model, action: normalized.image ? "image_to_video" : "text_to_video", requestBody: normalized};
}

export function buildSubmitRequest(ctx) {
  const req = ctx.requestBody || {};
  const body = {model: ctx.upstreamModel || ctx.model, prompt: req.prompt, duration: req.duration, resolution: req.resolution, aspect_ratio: req.aspect_ratio};
  if (req.image !== undefined) body.image = req.image;
  return {url: base(ctx) + "/v1/videos/generations", method: "POST", headers: {Authorization: authorization(ctx), "Content-Type": "application/json"}, body};
}
function mediaPath(value) {
  const match = /^(?:https?:\/\/[^/?#\s]+)?(\/v1\/media\/videos\/[a-f0-9]{64}\.(?:mp4|webm))$/.exec(text(value));
  if (!match) throw new Error("Upstream returned an invalid video media path");
  return match[1];
}
export function parseSubmitResponse(ctx, response) {
  const body = response.body || {};
  if (response.statusCode < 200 || response.statusCode >= 300) throw new Error("Grok video request failed (HTTP " + response.statusCode + ")");
  if (body.status !== "completed" || !/^[a-f0-9]{32}$/.test(text(body.id))) throw new Error("Grok did not return a completed video");
  const path = mediaPath(body.url);
  const req = ctx.requestBody || {};
  const duration = Number(body.duration || req.duration);
  if (!Number.isInteger(duration) || duration < 1 || duration > 6) throw new Error("Unexpected completed video duration");
  const resolution = body.resolution || req.resolution;
  if (resolution !== "480p" && resolution !== "720p") throw new Error("Unexpected completed video resolution");
  const data = {status: "completed", mediaPath: path, duration, resolution, aspect_ratio: req.aspect_ratio, mode: ctx.action};
  // The upstream is synchronous. Persist a completed task; NEVER submit again
  // in a polling hook or pretend there is an upstream async task endpoint.
  return {taskId: body.id, taskData: data, immediate: {status: "SUCCESS", progress: "100%"}};
}
export function extractUsage(ctx) {
  const req = ctx.requestBody || {};
  return {seconds: req.duration, resolution: req.resolution};
}
export function extractUsageOnComplete(ctx, result, data) {
  return result.status === "SUCCESS" ? {seconds: data.duration, resolution: data.resolution} : {};
}
export function buildQueryRequest(ctx) { throw new Error("This synchronous provider has no pending jobs to poll"); }
export function parseTaskResult(ctx, body) { return {status: "UNKNOWN", reason: "Unexpected polling of synchronous video provider"}; }
export function listArtifacts(task) {
  return task.status === "SUCCESS" ? [{key: "video", type: "video", mimeType: "video/mp4"}] : [];
}
export function buildContentRequest(ctx) {
  if (ctx.artifactKey !== "video") throw new Error("Video artifact not found");
  const path = mediaPath((ctx.data || {}).mediaPath);
  const method = ctx.clientRequest.method;
  if (method !== "GET" && method !== "HEAD") throw new Error("Unsupported content request method");
  // The backend serves GET only. NewAPI owns the client's HEAD response and
  // filters forwarded headers; do not forward browser cookies to the upstream.
  return {url: base(ctx) + path, method: "GET", headers: {Authorization: authorization(ctx)}};
}
export const protocols = {
  openai_video: {
    decodeRequest: normalize,
    render: function (ctx, task) {
      const data = task.data || {};
      return {id: task.task_id, object: "video", model: (task.properties || {}).origin_model_name || "Web/grok-imagine-video", status: task.status === "SUCCESS" ? "completed" : "failed", seconds: String(data.duration || 6), created_at: Number(task.created_at || 0)};
    },
  },
};
