import json
import unittest
from email import policy
from email.parser import BytesParser
from unittest.mock import AsyncMock, patch

import httpx

from grok2api.providers.web.auth import WebCredential
from grok2api.providers.web.gateway import (
    GrokWebGateway, WebImageEditError, _extract_final_edit_urls,
)


class WebImageEditTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        signer = patch('grok2api.providers.web.statsig.generate', return_value=None)
        signer.start()
        self.addCleanup(signer.stop)

    async def test_upload_bytes_and_final_image(self):
        calls = []

        def handle(request):
            calls.append(request)
            if request.url.path.endswith('/direct'):
                parsed = BytesParser(policy=policy.default).parsebytes(
                    ('Content-Type: ' + request.headers['content-type'] + '\r\n\r\n').encode()
                    + request.content
                )
                parts = {p.get_param('name', header='content-disposition'): p for p in parsed.iter_parts()}
                self.assertEqual(parts['file'].get_payload(decode=True), b'image-fixture')
                self.assertEqual(parts['file_source'].get_payload(decode=True), b'IMAGINE_SELF_UPLOAD_FILE_SOURCE')
                return httpx.Response(200, json={'fileMetadata': {'fileMetadataId': 'asset-fixture'}})
            body = json.loads(request.content)
            self.assertEqual(body['mediaGenInput']['imageToImage']['inputAssets'], ['asset-fixture'])
            frames = [
                {'result': {'streamingImageGenerationResponse': {'imageUrl': 'preview.jpg', 'progress': 30}}},
                {'result': {'streamingImageGenerationResponse': {'imageUrl': 'final.jpg', 'progress': 100, 'moderated': False}}},
            ]
            return httpx.Response(200, content=''.join(json.dumps(f) for f in frames))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            gateway = GrokWebGateway(client)
            gateway._statsig._optional_signed_statsig = AsyncMock(return_value='test-proof')
            images = await gateway.edit_image({'prompt': 'edit'}, WebCredential(sso='fixture', sso_rw='fixture'), image_bytes=b'image-fixture')
        self.assertEqual([i.url for i in images], ['https://assets.grok.com/final.jpg'])
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0].headers['cookie'], calls[1].headers['cookie'])
        self.assertEqual(calls[1].headers['x-statsig-id'], 'test-proof')
        self.assertNotIn('x-kl-kfa-ajax-request', calls[1].headers)

    def test_moderated_and_error_events_are_rejected(self):
        for event in ({'moderated': True, 'progress': 100, 'imageUrl': 'final.jpg'},):
            with self.assertRaises(WebImageEditError):
                _extract_final_edit_urls(json.dumps({'result': {'streamingImageGenerationResponse': event}}).encode())
        with self.assertRaises(WebImageEditError):
            _extract_final_edit_urls(b'{"error":{"message":"secret-fixture"}}')

    def test_sse_and_generated_urls(self):
        raw = b'data: {"result":{"modelResponse":{"generatedImageUrls":["final.jpg"]}}}\n\ndata: [DONE]\n'
        self.assertEqual(_extract_final_edit_urls(raw), ['https://assets.grok.com/final.jpg'])

    async def test_bad_upload_metadata_stops_before_generation(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={'uploadId': 'not-a-file-metadata-id'})
        )) as client:
            gateway = GrokWebGateway(client)
            gateway._statsig._optional_signed_statsig = AsyncMock(return_value='')
            with self.assertRaisesRegex(RuntimeError, 'no asset'):
                await gateway.edit_image({'prompt': 'edit'}, WebCredential(sso='fixture', sso_rw='fixture'), image_bytes=b'image')

    async def test_terminal_upload_error_is_not_treated_as_success(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={'uploadId': 'job-id', 'terminalError': {'message': 'private-detail'}})
        )) as client:
            gateway = GrokWebGateway(client)
            gateway._statsig._optional_signed_statsig = AsyncMock(return_value='')
            with self.assertRaises(WebImageEditError) as raised:
                await gateway.edit_image({'prompt': 'edit'}, WebCredential(sso='fixture', sso_rw='fixture'), image_bytes=b'image')
            self.assertEqual(raised.exception.stage, 'upload_terminal_error')
            self.assertNotIn('private-detail', str(raised.exception))
