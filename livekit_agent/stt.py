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
import logging
import struct
import time
import uuid
from pathlib import Path

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

_logger = logging.getLogger("soul.stt")

_DUMP_DIR = Path(__file__).resolve().parent.parent / "voice-tmp"
if settings.debug_dump_audio:
    _DUMP_DIR.mkdir(parents=True, exist_ok=True)

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


def _dump_wav(pcm: bytes, sample_rate: int, request_id: str) -> None:
    """Write raw mono s16le PCM to a WAV file under voice-tmp/."""
    filename = _DUMP_DIR / f"{request_id}.wav"
    num_samples = len(pcm) // 2  # s16le = 2 bytes per sample
    data_size = num_samples * 2
    with open(filename, "wb") as f:
        # RIFF header
        f.write(b"RIFF")
        f.write(struct.pack("<I", 36 + data_size))
        f.write(b"WAVE")
        # fmt sub-chunk
        f.write(b"fmt ")
        f.write(struct.pack("<I", 16))          # sub-chunk size
        f.write(struct.pack("<H", 1))           # PCM format
        f.write(struct.pack("<H", 1))           # mono
        f.write(struct.pack("<I", sample_rate))  # sample rate
        f.write(struct.pack("<I", sample_rate * 2))  # byte rate (mono s16le)
        f.write(struct.pack("<H", 2))           # block align
        f.write(struct.pack("<H", 16))          # bits per sample
        # data sub-chunk
        f.write(b"data")
        f.write(struct.pack("<I", data_size))
        f.write(pcm)
    _logger.info("dumped STT audio -> %s (%d Hz, %d bytes)", filename.name, sample_rate, len(pcm))


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
        sample_rate = self._sample_rate or frame_sample_rate
        started = time.perf_counter()

        # ---- debug: dump raw audio to voice-tmp/ ----
        if settings.debug_dump_audio:
            try:
                _dump_wav(pcm, sample_rate, request_id)
            except Exception:
                _logger.debug("failed to dump audio for %s", request_id, exc_info=True)

        params = {"request_id": request_id}
        if is_given(language):
            params["language"] = language

        headers = {
            "Content-Type": "application/octet-stream",
            "X-Sample-Rate": str(sample_rate),
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
                body=e.message,
                retryable=e.status in (429, 500, 503),
            ) from None

        data = (payload or {}).get("data") or {}
        transcript = str(data.get("transcript") or "").strip()
        detected_language = str(data.get("language") or "en")
        confidence = float(data.get("confidence") or 0.0)

        _logger.info(
            "STT RESULT %s latency_ms=%d language=%s confidence=%.3f transcript=%r",
            request_id,
            round((time.perf_counter() - started) * 1000),
            detected_language,
            confidence,
            transcript,
        )

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
