import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {test} from 'node:test';
const source = await readFile(new URL('../../integrations/newapi/grok-web-video.js', import.meta.url), 'utf8');
const plugin = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
const MODEL = 'Web/grok-imagine-video';
const decode = (value) => plugin.protocols.openai_video.decodeRequest({model: MODEL, body: {kind: 'json', value}});
const context = (req) => ({model: MODEL, upstreamModel: MODEL, baseUrl: 'https://gateway.example', apiKey: 'fixture-key', action: req.action, requestBody: req.requestBody});

test('manifest targets dedicated plugin channels, not Sora/OpenAI types', () => {
  assert.equal(plugin.meta.apiVersion, 1);
  assert.equal(plugin.meta.channelTypes, undefined);
  assert.deepEqual(plugin.meta.models, [MODEL]);
  assert.deepEqual(plugin.meta.protocols, ['openai_video']);
  assert.ok(Buffer.byteLength(source) < 1048576);
});
test('default parameters and correct upstream path', () => {
  const intent = decode({prompt: 'test'});
  assert.equal(intent.action, 'text_to_video');
  assert.deepEqual(intent.requestBody, {prompt: 'test', duration: 6, resolution: '480p', aspect_ratio: '1:1'});
  const req = plugin.buildSubmitRequest(context(intent));
  assert.equal(req.url, 'https://gateway.example/v1/videos/generations');
  assert.equal(req.headers.Authorization, 'Bearer fixture-key');
  assert.equal(req.body.model, MODEL);
});
test('size chooses ratio, not quality; portrait and landscape', () => {
  assert.equal(decode({prompt: 'x', size: '1280x720'}).requestBody.aspect_ratio, '16:9');
  assert.equal(decode({prompt: 'x', size: '720x1280'}).requestBody.aspect_ratio, '9:16');
  assert.equal(decode({prompt: 'x', size: '1280x720'}).requestBody.resolution, '480p');
  assert.equal(decode({prompt: 'x', seconds: 12}).requestBody.duration, 6);
  assert.equal(decode({prompt: 'x', quality: 'high'}).requestBody.resolution, '720p');
});
test('data URL is passed as image; no silent text fallback', () => {
  const image = 'data:image/png;base64,aGVsbG8=';
  const intent = decode({prompt: 'x', input_reference: image});
  assert.equal(intent.action, 'image_to_video');
  assert.equal(plugin.buildSubmitRequest(context(intent)).body.image, image);
  for (const input of [null, '', 'https://127.0.0.1/', {__fileRef: 'request_file:x'}]) assert.throws(() => decode({prompt: 'x', image: input}));
});
test('multipart uses the host-owned upload reference', () => {
  const intent = plugin.protocols.openai_video.decodeRequest({model: MODEL, body: {kind: 'multipart', fields: {prompt: ['animate'], aspect_ratio: ['9:16']}, files: [{ref: 'request_file:input_reference', field: 'input_reference', size: 100, mimeType: 'image/png'}]}});
  assert.equal(intent.action, 'image_to_video');
  assert.deepEqual(intent.requestBody.image, {__fileRef: 'request_file:input_reference', encoding: 'dataUrl', maxBytes: 20971520});
});
test('invalid parameters and conflicting inputs are rejected', () => {
  for (const extra of [{seconds: 0}, {seconds: 1.5}, {seconds: 16}, {seconds: 6, duration: 4}, {resolution: '1080p'}, {resolution: '480p', quality: 'high'}, {aspect_ratio: 'auto'}, {size: '720x1280', aspect_ratio: '16:9'}, {image: 'x', input_reference: 'y'}, {image_url: 'x'}, {images: []}]) assert.throws(() => decode({prompt: 'x', ...extra}));
});
test('synchronous success is persisted as terminal; no re-submission polling', () => {
  const ctx = context(decode({prompt: 'x'}));
  const path = '/v1/media/videos/' + 'a'.repeat(64) + '.mp4';
  const parsed = plugin.parseSubmitResponse(ctx, {statusCode: 200, body: {id: 'b'.repeat(32), status: 'completed', url: 'https://upstream.example' + path, duration: 6, resolution: '480p'}});
  assert.equal(parsed.immediate.status, 'SUCCESS');
  assert.equal(parsed.taskData.mediaPath, path);
  assert.equal(JSON.stringify(parsed).includes('fixture-key'), false);
  assert.deepEqual(plugin.extractUsageOnComplete({}, parsed.immediate, parsed.taskData), {seconds: 6, resolution: '480p'});
  assert.throws(() => plugin.buildQueryRequest({}));
  const content = plugin.buildContentRequest({...ctx, artifactKey: 'video', data: parsed.taskData, clientRequest: {method: 'GET'}});
  assert.equal(content.url, 'https://gateway.example' + path);
  assert.equal(plugin.listArtifacts({status: 'SUCCESS'}).length, 1);
  assert.throws(() => plugin.buildContentRequest({...ctx, artifactKey: 'video', data: {mediaPath: '/../../private'}, clientRequest: {method: 'GET'}}));
});
test('failed/incomplete upstream response is never marked completed', () => {
  const ctx = context(decode({prompt: 'x'}));
  assert.throws(() => plugin.parseSubmitResponse(ctx, {statusCode: 502, body: {error: 'private-detail'}}), /HTTP 502/);
  assert.throws(() => plugin.parseSubmitResponse(ctx, {statusCode: 200, body: {id: 'a'.repeat(32), status: 'queued'}}));
});
