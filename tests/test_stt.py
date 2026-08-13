"""Tests for the custom faster-whisper HTTP STT plugin (livekit_agent.stt)."""
from __future__ import annotations

import aiohttp
import numpy as np
import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import stt

from livekit_agent.stt import WhisperHTTPSTT, _to_mono_pcm16

SAMPLE_RATE = 48000


def make_frame(sample_rate: int = SAMPLE_RATE, num_channels: int = 1, samples: int = 480) -> rtc.AudioFrame:
    values = np.arange(samples, dtype=np.int16)
    if num_channels > 1:
        values = np.repeat(values, num_channels)
    return rtc.AudioFrame(
        data=values.tobytes(),
        sample_rate=sample_rate,
        num_channels=num_channels,
        samples_per_channel=values.size // num_channels,
    )


class MockSTTServer:
    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.response: dict | None = None
        self.status: int = 200
        self.app = web.Application()
        self.app.router.add_post("/transcribe-pcm", self._handler)
        self._runner: web.AppRunner | None = None
        self.url: str | None = None

    async def _handler(self, request: web.Request) -> web.Response:
        body = await request.read()
        self.requests.append(
            {
                "headers": dict(request.headers),
                "query": dict(request.query),
                "body": body,
            }
        )
        if self.status == 429:
            return web.json_response({"success": False, "message": "busy"}, status=429)
        if self.status == 500:
            return web.json_response({"success": False, "message": "boom"}, status=500)
        if self.response is None:
            self.response = {
                "success": True,
                "data": {"transcript": "गेहूं में पीला रोग लग गया है", "language": "hi", "confidence": 0.92},
            }
        return web.json_response(self.response)

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
async def mock_stt():
    server = MockSTTServer()
    await server.start()
    yield server
    await server.stop()


@pytest.fixture()
async def session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest.mark.asyncio
async def test_recognize_success(mock_stt: MockSTTServer, session: aiohttp.ClientSession):
    plugin = WhisperHTTPSTT(url=mock_stt.url, http_session=session)
    event = await plugin.recognize(make_frame(), language="hi")

    assert event.type == stt.SpeechEventType.FINAL_TRANSCRIPT
    assert len(event.alternatives) == 1
    alt = event.alternatives[0]
    assert "गेहूं" in alt.text
    assert alt.language == "hi"
    assert alt.confidence == pytest.approx(0.92)

    assert mock_stt.requests[0]["query"]["language"] == "hi"


@pytest.mark.asyncio
async def test_recognize_sends_mono_pcm16_and_sample_rate(mock_stt: MockSTTServer, session: aiohttp.ClientSession):
    plugin = WhisperHTTPSTT(url=mock_stt.url, http_session=session)
    frame = make_frame(sample_rate=48000, num_channels=1, samples=480)

    await plugin.recognize(frame)

    req = mock_stt.requests[0]
    assert req["headers"]["X-Sample-Rate"] == "48000"
    assert req["headers"]["Content-Type"] == "application/octet-stream"
    assert req["body"] == bytes(frame.data)


@pytest.mark.asyncio
async def test_recognize_empty_transcript_yields_no_alternatives(mock_stt: MockSTTServer, session: aiohttp.ClientSession):
    mock_stt.response = {
        "success": True,
        "message": "ok",
        "data": {"transcript": "", "language": "hi", "confidence": 0.0},
    }
    plugin = WhisperHTTPSTT(url=mock_stt.url, http_session=session)
    event = await plugin.recognize(make_frame())
    assert event.alternatives == []


@pytest.mark.asyncio
async def test_recognize_downmix_stereo_to_mono(mock_stt: MockSTTServer, session: aiohttp.ClientSession):
    plugin = WhisperHTTPSTT(url=mock_stt.url, http_session=session)
    frame = make_frame(num_channels=2, samples=480)

    await plugin.recognize(frame)

    pcm = mock_stt.requests[0]["body"]
    mono = np.frombuffer(pcm, dtype=np.int16)
    assert mono.ndim == 1
    assert mono.size == 480
    assert mock_stt.requests[0]["headers"]["X-Sample-Rate"] == str(SAMPLE_RATE)


@pytest.mark.asyncio
async def test_recognize_429_exhausts_retries(mock_stt: MockSTTServer, session: aiohttp.ClientSession):
    mock_stt.status = 429
    plugin = WhisperHTTPSTT(url=mock_stt.url, http_session=session)

    from livekit.agents import APIStatusError, APIConnectOptions

    with pytest.raises(APIStatusError) as excinfo:
        await plugin.recognize(make_frame(), conn_options=APIConnectOptions(max_retry=0))
    assert excinfo.value.status_code == 429
