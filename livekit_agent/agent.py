"""LiveKit voice agent worker for the SOUL library assistant.

Runs as a worker process that registers with the LiveKit server under
``LIVEKIT_AGENT_NAME``. When the backend dispatches the agent (either via the
token's room configuration or the explicit dispatch endpoint), this worker
spawns a voice session in the client's room.

Pipeline:
  VAD  -> silero (local, free)
  STT  -> self-hosted faster-whisper server  (livekit_agent.stt.WhisperHTTPSTT)
  LLM  -> Groq llama-3.3-70b (tool-calls the library text-to-SQL backend)
  TTS  -> self-hosted Kokoro server          (livekit_agent.tts.KokoroHTTPTTS)

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

import httpx

from livekit import agents, rtc
from livekit.agents import Agent, AgentServer, AgentSession, function_tool, room_io
from livekit.plugins import groq, silero

try:
    from livekit.plugins import dtln
except ImportError:
    dtln = None

from .config import settings
from .stt import WhisperHTTPSTT
from .tts import KokoroHTTPTTS

logger = logging.getLogger("soul.livekit")

# Master switch for lifecycle INFO logs. Errors are always logged regardless.
_LOGS_ENABLED = settings.agent_logs
if not _LOGS_ENABLED:
    logger.setLevel(logging.WARNING)

# Thread id for the current voice session. Set from the dispatch metadata when a
# session starts so follow-up questions share the same /ask conversation memory.
_session_thread_id: str = ""
_session_started_at: float = 0.0


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


@function_tool
async def query_library(question: str, thread_id: str = "") -> str:
    """Query the SOUL 3.0 library database and return the answer.

    Handles any library-related question in Hindi, English or Hinglish: book
    search, available titles, issued/returned books, members, overdue books,
    department-wise counts, reports, etc. The answer is a fully formatted,
    human-readable text you must then relay to the user in your own words,
    speaking style, converted from markdown (tables -> spoken sentences).

    Args:
        question: The user's library question exactly as they asked it.
        thread_id: Optional conversation id so follow-up questions keep context.
    """
    url = f"{settings.library_agent_url}/ask"
    thread = thread_id or _session_thread_id or "voice-session"
    payload = {"question": question, "thread_id": thread}
    _log(
        "tool query_library called",
        question=question,
        thread_id=thread,
        endpoint=url,
    )
    started = time.time()
    try:
        async with httpx.AsyncClient(timeout=settings.library_agent_timeout) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.warning(
            "tool query_library FAILED",
            extra={"question": question, "error": str(e)},
        )
        return (
            "I could not reach the library system right now. "
            "Please try again in a moment."
        )

    answer = data.get("answer") or ""
    if not answer or data.get("debug_error"):
        _log(
            "tool query_library returned empty/error",
            question=question,
            debug_error=data.get("debug_error"),
        )
        return (
            "I couldn't fetch that information from the library database. "
            "Please rephrase the question and try again."
        )
    _log(
        "tool query_library succeeded",
        question=question,
        answer_chars=len(answer),
        latency_ms=round((time.time() - started) * 1000),
    )
    return answer


class LibraryAssistant(Agent):
    def __init__(self) -> None:
        super().__init__(
            instructions=(
                "You are the SOUL Library voice assistant. You speak in "
                "the same language the user uses (Hindi, English or Hinglish). "
                "Use the query_library tool for ANY question about books, "
                "members, issues, returns, dues, fines or library reports. "
                "Rules: keep replies short and conversational - never read "
                "tables, never say serial numbers, never mention markdown, "
                "files, SQL or downloads. Convert numbers to a spoken form. "
                "If the tool result is a table, summarize it in 1-3 sentences "
                "mentioning the key counts. If the tool result is empty, say "
                "no records were found."
            ),
            tools=[query_library],
        )


server = AgentServer()


@server.rtc_session(agent_name=settings.agent_name)
async def library_assistant_session(ctx: agents.JobContext) -> None:
    global _session_thread_id, _session_started_at
    _session_started_at = time.time()
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
    _session_thread_id = str(meta.get("thread_id") or "")
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
        llm=f"groq({settings.groq_model})",
        tts=f"kokoro-http({settings.tts_url})",
    )

    session = AgentSession(
        vad=silero.VAD.load(),
        stt=WhisperHTTPSTT(),
        llm=groq.LLM(model=settings.groq_model),
        tts=KokoroHTTPTTS(),
        turn_handling={
            "turn_detection": "vad",
            "endpointing": {"min_delay": 0.6, "max_delay": 2.5},
            "interruption": {"enabled": False},
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
    session.on("user_input_transcribed", lambda ev: _log(
        "USER TRANSCRIBED", event="user_input_transcribed",
        transcript=ev.transcript, is_final=ev.is_final,
    ))
    session.on("function_tools_executed", lambda ev: _log(
        "TOOLS EXECUTED", event="function_tools_executed",
        calls=[c.name for c in ev.function_calls],
    ))
    session.on("error", lambda ev: logger.error(
        "SESSION ERROR %s: %s (source=%s)",
        type(ev.error).__name__, ev.error, ev.source,
    ))

    audio_input_options = room_io.AudioInputOptions(sample_rate=48000)
    if dtln is not None:
        audio_input_options.noise_cancellation = dtln.noise_suppression()
    else:
        logger.warning("dtln not installed; running without noise cancellation")

    await session.start(
        room=ctx.room,
        agent=LibraryAssistant(),
        room_options=room_io.RoomOptions(audio_input=audio_input_options),
    )
    
    _log(
        "AGENT SESSION STARTED (LISTENING)",
        event="session_started",
        job_id=job.id,
        room=ctx.room.name,
    )

    _log(
        "AGENT GENERATING GREETING",
        event="greeting",
        job_id=job.id,
        room=ctx.room.name,
    )
    
    await session.generate_reply(
        instructions=(
            "Start by greeting the user warmly as the SOUL Library assistant in Hindi language, "
            "and ask what they would like to know about the library."
        )
    )

    _log(
        "AGENT GREETING COMPLETE",
        event="greeting_complete",
        job_id=job.id,
        room=ctx.room.name,
        latency_s=round(time.time() - _session_started_at, 2),
    )
