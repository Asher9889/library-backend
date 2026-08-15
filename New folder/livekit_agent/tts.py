"""Custom TTS plugin backed by the self-hosted Kokoro HTTP server.

The server exposes:

    POST /v1/tts/stream
      body: {text, language, voice, speed, sample_rate, request_id}
      -> 200 application/octet-stream (raw mono s16le PCM, chunked)

Response headers: X-Sample-Rate, X-Channels, X-Sample-Format.
"""
from __future__ import annotations

import asyncio
import uuid

import aiohttp
from livekit.agents import (
    APIConnectOptions,
    APIError,
    APITimeoutError,
    APIStatusError,
    tts,
    utils,
)
from livekit.agents.types import DEFAULT_API_CONNECT_OPTIONS

from .config import settings

DEFAULT_SAMPLE_RATE = 44100
NUM_CHANNELS = 1


class KokoroHTTPTTS(tts.TTS):
    """TTS that streams each sentence from the Kokoro server as raw PCM."""

    def __init__(
        self,
        *,
        url: str | None = None,
        language: str | None = None,
        voice: str | None = None,
        speed: float | None = None,
        sample_rate: int | None = None,
        http_session: aiohttp.ClientSession | None = None,
    ) -> None:
        super().__init__(
            capabilities=tts.TTSCapabilities(streaming=False),
            sample_rate=sample_rate or DEFAULT_SAMPLE_RATE,
            num_channels=NUM_CHANNELS,
        )
        self._url = (url or settings.tts_url).rstrip("/")
        self._language = language or settings.tts_language
        self._voice = voice or settings.tts_voice
        self._speed = speed or settings.tts_speed
        self._session = http_session

    @property
    def model(self) -> str:
        return "Kokoro-82M"

    @property
    def provider(self) -> str:
        return "Kokoro-http"

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = utils.http_context.http_session()
        return self._session

    def synthesize(
        self,
        text: str,
        *,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> tts.ChunkedStream:
        return ChunkedStream(tts=self, input_text=text, conn_options=conn_options)


class ChunkedStream(tts.ChunkedStream):
    def __init__(
        self,
        *,
        tts: KokoroHTTPTTS,
        input_text: str,
        conn_options: APIConnectOptions,
    ) -> None:
        super().__init__(tts=tts, input_text=input_text, conn_options=conn_options)
        self._tts: KokoroHTTPTTS = tts

    async def _run(self, output_emitter: tts.AudioEmitter) -> None:
        request_id = uuid.uuid4().hex[:16]
        sample_rate = self._tts._sample_rate

        body = {
            "text": self._input_text,
            "language": self._tts._language,
            "voice": self._tts._voice,
            "speed": self._tts._speed,
            "sample_rate": sample_rate,
            "request_id": request_id,
        }

        try:
            async with self._tts._ensure_session().post(
                f"{self._tts._url}/v1/tts/stream",
                json=body,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=60, sock_connect=self._conn_options.timeout),
            ) as resp:
                if resp.status in (429, 503):
                    raise APIStatusError(
                        message="Kokoro TTS not ready / busy",
                        status_code=resp.status,
                        retryable=True,
                    )
                resp.raise_for_status()

                if not resp.content_type.startswith("audio") and "octet-stream" not in resp.content_type:
                    error_text = await resp.text()
                    raise APIError(message="Kokoro returned non-audio data", body=error_text)

                output_emitter.initialize(
                    request_id=request_id,
                    sample_rate=sample_rate,
                    num_channels=NUM_CHANNELS,
                    mime_type="audio/pcm",
                )

                async for data, _ in resp.content.iter_chunks():
                    if data:
                        output_emitter.push(data)

                output_emitter.flush()
        except asyncio.TimeoutError:
            raise APITimeoutError() from None
        except aiohttp.ClientResponseError as e:
            raise APIStatusError(
                message=e.message,
                status_code=e.status,
                retryable=e.status in (429, 500, 503),
            ) from None
