"""Custom STT plugin backed by a self-hosted faster-whisper HTTP server.

The server exposes the contract the voice agent expects:

    POST /transcribe-pcm
      body:   raw mono s16le PCM
      header: X-Sample-Rate  (default 48000)
      query:  language (optional), request_id (optional)

    -> 200 {success, message, data: {transcript, language, confidence}}
    -> 200 with data.transcript == "" when there was silence / no speech
    -> 429 when the inference queue is full (retryable)
    -> 500 on transcription failure (retryable)
"""
from __future__ import annotations

import asyncio
import time
import uuid

import aiohttp
import numpy as np
from livekit.agents import (
    APIConnectOptions,
    APITimeoutError,
    APIStatusError,
    stt,
    utils,
)
from livekit.agents.types import DEFAULT_API_CONNECT_OPTIONS, NOT_GIVEN, NotGivenOr
from livekit.agents.utils import is_given

from .config import settings

DEFAULT_SAMPLE_RATE = 48000
DEFAULT_NUM_CHANNELS = 1


def _to_mono_pcm16(buffer: utils.audio.AudioBuffer) -> tuple[bytes, int]:
    """Flatten the incoming frames into mono s16le PCM + the native sample rate."""
    frame = utils.audio.combine_frames(buffer)
    sample_rate = frame.sample_rate
    num_channels = frame.num_channels
    raw = frame.data
    if isinstance(raw, memoryview):
        raw = raw.tobytes()

    if num_channels <= 1:
        return bytes(raw), sample_rate

    samples = np.frombuffer(raw, dtype=np.int16)
    samples = samples.reshape(-1, num_channels)
    mono = samples.mean(axis=1).astype(np.int16)
    return mono.tobytes(), sample_rate


class WhisperHTTPSTT(stt.STT):
    """Non-streaming STT that posts each VAD-segmented utterance to faster-whisper."""

    def __init__(
        self,
        *,
        url: str | None = None,
        http_session: aiohttp.ClientSession | None = None,
        sample_rate: int | None = None,
    ) -> None:
        super().__init__(
            capabilities=stt.STTCapabilities(
                streaming=False,
                interim_results=False,
            )
        )
        self._url = (url or settings.stt_url).rstrip("/")
        self._session = http_session
        self._sample_rate = sample_rate

    @property
    def model(self) -> str:
        return "faster-whisper"

    @property
    def provider(self) -> str:
        return "whisper-http"

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = utils.http_context.http_session()
        return self._session

    async def _recognize_impl(
        self,
        buffer: utils.audio.AudioBuffer,
        *,
        language: NotGivenOr[str] = NOT_GIVEN,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> stt.SpeechEvent:
        pcm, frame_sample_rate = _to_mono_pcm16(buffer)
        request_id = uuid.uuid4().hex[:16]
        started = time.perf_counter()

        params = {"request_id": request_id}
        if is_given(language):
            params["language"] = language

        headers = {
            "Content-Type": "application/octet-stream",
            "X-Sample-Rate": str(self._sample_rate or frame_sample_rate),
        }

        try:
            async with self._ensure_session().post(
                f"{self._url}/transcribe-pcm",
                data=pcm,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=conn_options.timeout),
            ) as resp:
                if resp.status == 429:
                    raise APIStatusError(
                        message="whisper STT queue full",
                        status_code=429,
                        retryable=True,
                    )
                resp.raise_for_status()
                payload = await resp.json(content_type=None)
        except asyncio.TimeoutError:
            raise APITimeoutError() from None
        except aiohttp.ClientResponseError as e:
            raise APIStatusError(
                message=e.message,
                status_code=e.status,
                body=e.response is not None,
                retryable=e.status in (429, 500, 503),
            ) from None

        data = (payload or {}).get("data") or {}
        transcript = str(data.get("transcript") or "").strip()
        detected_language = str(data.get("language") or "en")
        confidence = float(data.get("confidence") or 0.0)

        if not transcript:
            return stt.SpeechEvent(
                type=stt.SpeechEventType.FINAL_TRANSCRIPT,
                request_id=request_id,
                alternatives=[],
            )

        return stt.SpeechEvent(
            type=stt.SpeechEventType.FINAL_TRANSCRIPT,
            request_id=request_id,
            alternatives=[
                stt.SpeechData(
                    language=detected_language,
                    text=transcript,
                    confidence=confidence,
                    start_time=0.0,
                    end_time=time.perf_counter() - started,
                )
            ],
        )
