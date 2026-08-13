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

Start:
    python run_agent.py dev      # development mode
    python run_agent.py start    # production mode
"""
from __future__ import annotations

import asyncio
import json
import logging

import httpx

from livekit import agents, rtc
from livekit.agents import Agent, AgentServer, AgentSession, function_tool, room_io
from livekit.plugins import groq, silero

from .config import settings
from .stt import WhisperHTTPSTT
from .tts import KokoroHTTPTTS

logger = logging.getLogger("soul.livekit")

# Thread id for the current voice session. Set from the dispatch metadata when a
# session starts so follow-up questions share the same /ask conversation memory.
_session_thread_id: str = ""


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
    try:
        async with httpx.AsyncClient(timeout=settings.library_agent_timeout) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.warning("library agent tool failed: %s", e)
        return (
            "I could not reach the library system right now. "
            "Please try again in a moment."
        )

    answer = data.get("answer") or ""
    if not answer or data.get("debug_error"):
        return (
            "I couldn't fetch that information from the library database. "
            "Please rephrase the question and try again."
        )
    return answer


class LibraryAssistant(Agent):
    def __init__(self) -> None:
        super().__init__(
            instructions=(
                "You are the SOUL 3.0 Library voice assistant. You speak in "
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
    logger.info("dispatching %s into room %s", settings.agent_name, ctx.room.name)
    global _session_thread_id
    if ctx.job.metadata:
        try:
            meta = json.loads(ctx.job.metadata)
        except json.JSONDecodeError:
            meta = {}
        logger.info("job metadata: %s", meta)
        _session_thread_id = meta.get("thread_id") or ""

    session = AgentSession(
        vad=silero.VAD.load(),
        stt=WhisperHTTPSTT(),
        llm=groq.LLM(model=settings.groq_model),
        tts=KokoroHTTPTTS(),
        turn_handling={
            "turn_detection": "vad",
            "endpointing": {"min_delay": 0.6, "max_delay": 2.5},
            "interruption": {"enabled": True},
        },
    )

    await session.start(
        room=ctx.room,
        agent=LibraryAssistant(),
        room_options=room_io.RoomOptions(
            audio_input=room_io.AudioInputOptions(sample_rate=48000),
        ),
    )

    await session.generate_reply(
        instructions=(
            "Start by greeting the user warmly as the SOUL Library assistant "
            "and ask what they would like to know about the library."
        )
    )
