"""Tests for the voice agent's backend pass-through (livekit_agent.agent)."""
from __future__ import annotations

import pytest
from aiohttp import web

import livekit_agent.agent as agent_mod


class MockLibraryAPI:
    def __init__(self) -> None:
        self.app = web.Application()
        self.app.router.add_post("/ask", self._ask)
        self._runner: web.AppRunner | None = None
        self.url: str | None = None
        self.request_bodies: list[dict] = []
        self.response: dict = {
            "question": "q",
            "resolved_question": "q",
            "sql_query": "SELECT 1",
            "answer": "There are 42 books in the library.",
            "attempts": 1,
            "debug_error": None,
        }

    async def _ask(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.request_bodies.append(body)
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


class FakeSession:
    """Minimal stand-in for AgentSession — records what was spoken via say()."""

    def __init__(self) -> None:
        self.spoken: list[str] = []

    async def say(self, text: str) -> object:
        self.spoken.append(text)
        return None


@pytest.fixture()
async def mock_library(monkeypatch):
    server = MockLibraryAPI()
    await server.start()
    monkeypatch.setattr(agent_mod.settings, "library_agent_url", server.url)
    yield server
    await server.stop()


def _session_data(thread_id: str = "voice-1", mem_cd: str = "") -> agent_mod.SessionData:
    return agent_mod.SessionData(thread_id=thread_id, mem_cd=mem_cd)

async def _call(transcript: str, *, session_data=None, turn: int = 1):
    session = FakeSession()
    sd = session_data or _session_data()
    agent_mod._current_turn = turn
    await agent_mod._call_backend(transcript, sd, turn, session)
    return session


@pytest.mark.asyncio
async def test_call_backend_posts_and_speaks(mock_library: MockLibraryAPI):
    session = await _call("kitni books hain?")
    assert session.spoken == ["There are 42 books in the library."]
    assert mock_library.request_bodies[0]["question"] == "kitni books hain?"
    assert mock_library.request_bodies[0]["thread_id"] == "voice-1"


@pytest.mark.asyncio
async def test_call_backend_uses_session_thread_id(mock_library: MockLibraryAPI):
    await _call("kitni books hain?", session_data=_session_data(thread_id="sess-42"))
    assert mock_library.request_bodies[0]["thread_id"] == "sess-42"


@pytest.mark.asyncio
async def test_call_backend_default_thread_id(mock_library: MockLibraryAPI):
    await _call("hello", session_data=_session_data(thread_id=""))
    assert mock_library.request_bodies[0]["thread_id"] == "voice-session"


@pytest.mark.asyncio
async def test_call_backend_sends_mem_cd(mock_library: MockLibraryAPI):
    await _call("hello", session_data=_session_data(thread_id="t", mem_cd="M-1"))
    assert mock_library.request_bodies[0]["mem_cd"] == "M-1"


@pytest.mark.asyncio
async def test_call_backend_handles_empty_answer(mock_library: MockLibraryAPI):
    mock_library.response["answer"] = ""
    session = await _call("kuch bhi")
    assert "couldn" in session.spoken[0]


@pytest.mark.asyncio
async def test_call_backend_handles_debug_error(mock_library: MockLibraryAPI):
    mock_library.response["answer"] = "something"
    mock_library.response["debug_error"] = "bad column"
    session = await _call("kuch bhi")
    assert "couldn" in session.spoken[0]


@pytest.mark.asyncio
async def test_call_backend_handles_unreachable_backend(monkeypatch):
    monkeypatch.setattr(agent_mod.settings, "library_agent_url", "http://127.0.0.1:1")
    session = await _call("kuch bhi")
    assert "could not reach" in session.spoken[0]


@pytest.mark.asyncio
async def test_stale_turn_does_not_speak(mock_library: MockLibraryAPI):
    session = FakeSession()
    sd = _session_data()
    agent_mod._current_turn = 5
    await agent_mod._call_backend("stale?", sd, 3, session)
    assert session.spoken == []