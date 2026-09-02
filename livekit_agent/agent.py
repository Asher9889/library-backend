"""LiveKit voice agent worker for the SOUL library assistant.

Runs as a worker process that registers with the LiveKit server under
``LIVEKIT_AGENT_NAME``. When the backend dispatches the agent (either via the
token's room configuration or the explicit dispatch endpoint), this worker
spawns a voice session in the client's room.

Pipeline (pure pass-through — no LLM in the agent):
  VAD  -> silero (local, free)
  STT  -> self-hosted faster-whisper server  (livekit_agent.stt.WhisperHTTPSTT)
  TTS  -> self-hosted Kokoro server          (livekit_agent.tts.KokoroHTTPTTS)

Every final user transcript is sent directly to the Python backend
``/ask`` endpoint (which has its own LLM + memory). The backend's answer
is spoken back to the user via TTS. The agent never generates responses
itself.

Lifecycle logging:
  Every phase of the agent lifecycle is logged to the ``soul.livekit`` logger
  at INFO level (spawn -> connect -> session -> reply -> end). Set
  ``LIVEKIT_AGENT_LOGS=0`` in the environment/.env to silence these INFO logs;
  errors are always logged.

Start:
    python run_agent.py dev      # development mode
    python run_agent.py start    # production mode
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass

import httpx

from livekit import agents, rtc
from livekit.agents import Agent, AgentServer, AgentSession, room_io, inference
from livekit.plugins import silero

from .config import settings
from .stt import WhisperHTTPSTT
from .tts import KokoroHTTPTTS

logger = logging.getLogger("soul.livekit")

# Master switch for lifecycle INFO logs. Errors are always logged regardless.
_LOGS_ENABLED = settings.agent_logs
if not _LOGS_ENABLED:
    logger.setLevel(logging.WARNING)


@dataclass
class SessionData:
    thread_id: str = ""
    mem_cd: str = ""
    user_name: str = ""


_session_started_at: float = 0.0

_last_user_transcript: str = ""
_current_turn: int = 0


def _log(msg: str, **extra) -> None:
    """Emit a lifecycle log line. Suppressed entirely when logs are disabled."""
    if not _LOGS_ENABLED:
        return
    if extra:
        logger.info(
            "%s %s", msg, json.dumps(extra, ensure_ascii=False, default=str)
        )
    else:
        logger.info(msg)


class VoicePassThrough(Agent):
    """Minimal agent — VAD only, no LLM, no tools."""

    def __init__(self) -> None:
        super().__init__(
            instructions="Pass-through voice link to the library agent backend.",
            vad=inference.VAD(
                model="silero",
                min_speech_duration=0.9,
                min_silence_duration=1.0,
                activation_threshold=0.7,
                prefix_padding_duration=0.5,
            ),
        )


server = AgentServer()


async def _call_backend(
    transcript: str,
    session_data: SessionData,
    turn_number: int,
    session: AgentSession,
) -> None:
    """POST the user's transcript to /ask and speak the backend's answer."""
    global _current_turn

    url = f"{settings.library_agent_url}/ask"
    thread = session_data.thread_id or "voice-session"
    user_mem_cd = session_data.mem_cd
    payload = {"question": transcript, "thread_id": thread, "mem_cd": user_mem_cd}

    _log(
        "BACKEND REQUEST DISPATCHED",
        event="backend_dispatched",
        turn=turn_number,
        transcript=transcript,
        endpoint=url,
        thread_id=thread,
        mem_cd=user_mem_cd,
    )

    started = time.time()

    try:
        async with httpx.AsyncClient(timeout=settings.library_agent_timeout) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()

        # ---- check if a newer turn started while we were waiting ----
        if turn_number != _current_turn:
            _log(
                "BACKEND CALL DISCARDED (STALE TURN)",
                event="stale_turn_discarded",
                turn=turn_number,
                current_turn=_current_turn,
                transcript=transcript,
                latency_ms=round((time.time() - started) * 1000),
            )
            return

        answer = data.get("answer") or ""
        debug_error = data.get("debug_error")

        _log(
            "BACKEND RESPONSE RECEIVED",
            event="backend_response_received",
            turn=turn_number,
            http_status=resp.status_code,
            answer=answer,
            answer_chars=len(answer),
            debug_error=debug_error,
            latency_ms=round((time.time() - started) * 1000),
        )

        if not answer or debug_error:
            answer = (
                "I couldn't get that information from the library system. "
                "Please try again."
            )

    except Exception as e:
        if turn_number != _current_turn:
            _log(
                "BACKEND CALL DISCARDED (STALE TURN)",
                event="stale_turn_discarded",
                turn=turn_number,
                current_turn=_current_turn,
                transcript=transcript,
                error=str(e),
                latency_ms=round((time.time() - started) * 1000),
            )
            return

        _log(
            "BACKEND CALL FAILED",
            event="backend_call_failed",
            turn=turn_number,
            error=str(e),
            latency_ms=round((time.time() - started) * 1000),
        )
        answer = (
            "I could not reach the library system right now. "
            "Please try again in a moment."
        )

    _log(
        "AGENT SPEAKING (TTS)",
        event="agent_speaking",
        turn=turn_number,
        answer=answer,
    )
    await session.say(answer)


def _on_user_input_transcribed(ev, session: AgentSession, session_data: SessionData) -> None:
    global _last_user_transcript, _current_turn

    transcript = ev.transcript or ""

    _log(
        "STEP1 USER SPOKE (STT OUTPUT)",
        event="user_input_transcribed",
        is_final=ev.is_final,
        transcript=transcript,
    )

    if not ev.is_final or not transcript.strip():
        return

    _last_user_transcript = transcript.strip()
    _current_turn += 1
    turn = _current_turn

    _log(
        "STEP2 TRANSCRIPT READY (DISPATCHING TO BACKEND)",
        event="transcript_dispatching",
        turn=turn,
        transcript=_last_user_transcript,
    )

    asyncio.ensure_future(_call_backend(_last_user_transcript, session_data, turn, session))


@server.rtc_session(agent_name=settings.agent_name)
async def library_assistant_session(ctx: agents.JobContext) -> None:
    global _session_started_at, _current_turn
    _session_started_at = time.time()
    _current_turn = 0
    job = ctx.job
    _log(
        "AGENT SPAWNED",
        event="spawned",
        job_id=job.id,
        dispatch_id=getattr(job, "dispatch_id", "") or "",
        room=ctx.room.name,
        agent_name=settings.agent_name,
    )

    # ---- job metadata (dispatch payload from the backend) ----
    meta: dict = {}
    if job.metadata:
        try:
            meta = json.loads(job.metadata)
        except json.JSONDecodeError:
            logger.warning("unparseable job metadata: %s", job.metadata)

    session_data = SessionData(
        thread_id=str(meta.get("thread_id") or ""),
        mem_cd=str(meta.get("user_id") or ""),
        user_name=str(meta.get("user_name") or "").strip(),
    )

    _log(
        "AGENT USER NAME RESOLVED",
        event="user_name",
        job_id=job.id,
        raw_user_name=meta.get("user_name"),
        resolved_user_name=session_data.user_name,
        has_name=bool(session_data.user_name),
    )

    _log(
        "AGENT JOB METADATA",
        event="job_metadata",
        job_id=job.id,
        metadata=meta,
    )

    # ---- participant lifecycle (the human caller) ----
    @ctx.room.on("participant_connected")
    def _on_participant_connected(participant: rtc.RemoteParticipant) -> None:
        _log(
            "PARTICIPANT CONNECTED",
            event="participant_connected",
            job_id=job.id,
            room=ctx.room.name,
            identity=participant.identity,
        )

    @ctx.room.on("participant_disconnected")
    def _on_participant_disconnected(participant: rtc.RemoteParticipant) -> None:
        _log(
            "PARTICIPANT DISCONNECTED",
            event="participant_disconnected",
            job_id=job.id,
            room=ctx.room.name,
            identity=participant.identity,
        )

    for identity in ctx.room.remote_participants:
        _log(
            "PARTICIPANT CONNECTED (already in room)",
            event="participant_connected",
            job_id=job.id,
            room=ctx.room.name,
            identity=identity,
        )

    @ctx.room.on("disconnected")
    def _on_room_disconnected(*args) -> None:
        _log(
            "AGENT SESSION ENDED",
            event="session_ended",
            job_id=job.id,
            room=ctx.room.name,
            duration_s=round(time.time() - _session_started_at, 2),
        )

    _log(
        "AGENT SESSION PIPELINE CONFIGURED",
        event="session_configured",
        job_id=job.id,
        room=ctx.room.name,
        vad="silero",
        stt=f"whisper-http({settings.stt_url})",
        llm="NONE (pure pass-through to /ask)",
        tts=f"kokoro-http({settings.tts_url})",
        backend_url=f"{settings.library_agent_url}/ask",
    )

    session = AgentSession(
        vad=silero.VAD.load(),
        stt=WhisperHTTPSTT(),
        llm=None,
        tts=KokoroHTTPTTS(),
        userdata=session_data,
        turn_handling={
            "turn_detection": "vad",
            "endpointing": {
                "mode": "dynamic",
                "min_delay": 0.7,
                "max_delay": 2.5,
            },
            "interruption": {
                "enabled": True,
                "mode": "adaptive",
            },
            "preemptive_generation": {
                "enabled": False,
            },
        },
    )

    # ---- pipeline diagnostics (show which stage runs/fails per turn) ----
    session.on("agent_state_changed", lambda ev: _log(
        "AGENT STATE", event="agent_state_changed",
        old=ev.old_state, new=ev.new_state,
    ))
    session.on("user_state_changed", lambda ev: _log(
        "USER STATE", event="user_state_changed",
        old=ev.old_state, new=ev.new_state,
    ))

    session.on(
        "user_input_transcribed",
        lambda ev: _on_user_input_transcribed(ev, session, session_data),
    )
    session.on("error", lambda ev: logger.error(
        "SESSION ERROR %s: %s (source=%s)",
        type(ev.error).__name__, ev.error, ev.source,
    ))

    audio_input_options = room_io.AudioInputOptions(sample_rate=48000)

    await session.start(
        room=ctx.room,
        agent=VoicePassThrough(),
        room_options=room_io.RoomOptions(audio_input=audio_input_options),
    )

    _log(
        "AGENT SESSION STARTED (LISTENING)",
        event="session_started",
        job_id=job.id,
        room=ctx.room.name,
    )

    _log(
        "AGENT PLAYING FIXED GREETING",
        event="greeting",
        job_id=job.id,
        room=ctx.room.name,
    )

    if session_data.user_name:
        greeting = (
            f"नमस्ते {session_data.user_name}! मैं सोल लाइब्रेरी असिस्टेंट हूँ। "
            "आप लाइब्रेरी के बारे में क्या जानना चाहेंगे?"
        )
    else:
        greeting = (
            "नमस्ते! मैं सोल लाइब्रेरी असिस्टेंट हूँ। "
            "आप लाइब्रेरी के बारे में क्या जानना चाहेंगे?"
        )
    _log(
        "AGENT GREETING TEXT",
        event="greeting_text",
        job_id=job.id,
        room=ctx.room.name,
        user_name=session_data.user_name,
        greeting=greeting,
    )
    await session.say(greeting)

    _log(
        "AGENT GREETING COMPLETE",
        event="greeting_complete",
        job_id=job.id,
        room=ctx.room.name,
        latency_s=round(time.time() - _session_started_at, 2),
    )
