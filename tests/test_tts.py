"""Tests for the custom Kokoro HTTP TTS plugin (livekit_agent.tts)."""
from __future__ import annotations

import aiohttp
import numpy as np
import pytest
from aiohttp import web
from livekit import rtc

from livekit_agent.tts import KokoroHTTPTTS

SAMPLE_RATE = 44100
NUM_CHANNELS = 1


class MockTTSServer:
    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.pcm = (np.arange(8820, dtype=np.int16) % 1000).tobytes()  # 200ms @ 44.1kHz
        self.status = 200
        self.app = web.Application()
        self.app.router.add_post("/v1/tts/stream", self._handler)
        self._runner: web.AppRunner | None = None
        self.url: str | None = None

    async def _handler(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.requests.append(body)
        if self.status == 429:
            return web.json_response({"success": False}, status=429)
        if self.status == 503:
            return web.json_response({"success": False}, status=503)
        resp = web.Response(
            body=self.pcm,
            headers={
                "X-Sample-Rate": str(SAMPLE_RATE),
                "X-Channels": str(NUM_CHANNELS),
                "X-Sample-Format": "s16le",
            },
        )
        resp.content_type = "application/octet-stream"
        return resp

    async def start(self) -> None:
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()


@pytest.fixture()
async def mock_tts():
    server = MockTTSServer()
    await server.start()
    yield server
    await server.stop()


@pytest.fixture()
async def session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest.mark.asyncio
async def test_synthesize_streams_pcm(mock_tts: MockTTSServer, session: aiohttp.ClientSession):
    tts = KokoroHTTPTTS(url=mock_tts.url, http_session=session)
    frames = [ev.frame async for ev in tts.synthesize("नमस्ते")]

    assert len(frames) >= 1
    combined = rtc.combine_audio_frames(frames)
    assert combined.sample_rate == SAMPLE_RATE
    assert combined.num_channels == NUM_CHANNELS
    # The emitter may append a small trailing pad frame; the actual audio
    # delivered must match the server payload.
    assert bytes(combined.data)[: len(mock_tts.pcm)] == mock_tts.pcm

    body = mock_tts.requests[0]
    assert body["text"] == "नमस्ते"
    assert body["sample_rate"] == SAMPLE_RATE
    assert body["language"] == "hi"
    assert "request_id" in body


@pytest.mark.asyncio
async def test_synthesize_collect(mock_tts: MockTTSServer, session: aiohttp.ClientSession):
    tts = KokoroHTTPTTS(url=mock_tts.url, http_session=session)
    frame = await tts.synthesize("नमस्ते").collect()
    assert isinstance(frame, rtc.AudioFrame)
    assert bytes(frame.data)[: len(mock_tts.pcm)] == mock_tts.pcm


@pytest.mark.asyncio
async def test_synthesize_overload_error(mock_tts: MockTTSServer, session: aiohttp.ClientSession):
    mock_tts.status = 429
    tts = KokoroHTTPTTS(url=mock_tts.url, http_session=session)

    from livekit.agents import APIStatusError

    with pytest.raises(APIStatusError):
        await tts.synthesize("नमस्ते").collect()
