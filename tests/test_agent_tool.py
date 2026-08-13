"""Tests for the voice agent's library query tool (livekit_agent.agent)."""
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


@pytest.fixture()
async def mock_library(monkeypatch):
    server = MockLibraryAPI()
    await server.start()
    monkeypatch.setattr(agent_mod.settings, "library_agent_url", server.url)
    yield server
    await server.stop()


@pytest.mark.asyncio
async def test_query_library_returns_answer(mock_library: MockLibraryAPI):
    answer = await agent_mod.query_library("kitni books hain?", thread_id="voice-1")
    assert answer == "There are 42 books in the library."
    assert mock_library.request_bodies[0]["question"] == "kitni books hain?"
    assert mock_library.request_bodies[0]["thread_id"] == "voice-1"


@pytest.mark.asyncio
async def test_query_library_uses_session_thread_id(mock_library: MockLibraryAPI, monkeypatch):
    monkeypatch.setattr(agent_mod, "_session_thread_id", "sess-42")
    await agent_mod.query_library("kitni books hain?")
    assert mock_library.request_bodies[0]["thread_id"] == "sess-42"


@pytest.mark.asyncio
async def test_query_library_posts_to_ask_endpoint(mock_library: MockLibraryAPI, monkeypatch):
    monkeypatch.setattr(agent_mod, "_session_thread_id", "")
    await agent_mod.query_library("hello")
    assert mock_library.request_bodies[0]["thread_id"] == "voice-session"


@pytest.mark.asyncio
async def test_query_library_handles_empty_answer(mock_library: MockLibraryAPI):
    mock_library.response["answer"] = ""
    answer = await agent_mod.query_library("kuch bhi")
    assert "couldn" in answer


@pytest.mark.asyncio
async def test_query_library_handles_debug_error(mock_library: MockLibraryAPI):
    mock_library.response["answer"] = "something"
    mock_library.response["debug_error"] = "bad column"
    answer = await agent_mod.query_library("kuch bhi")
    assert "couldn" in answer


@pytest.mark.asyncio
async def test_query_library_handles_unreachable_backend(monkeypatch):
    monkeypatch.setattr(agent_mod.settings, "library_agent_url", "http://127.0.0.1:1")
    answer = await agent_mod.query_library("kuch bhi")
    assert "could not reach" in answer


@pytest.mark.asyncio
async def test_assistant_exposes_library_tool():
    assistant = agent_mod.LibraryAssistant()
    tool_names = [t.info.name for t in (assistant.tools or [])]
    assert "query_library" in tool_names
