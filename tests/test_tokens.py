"""Tests for LiveKit access-token + dispatch helpers (livekit_agent.tokens)."""
from __future__ import annotations

import base64
import json

from livekit_agent.config import settings
from livekit_agent.tokens import create_join_token, generate_room_name


def _decode_payload(token: str) -> dict:
    raw = token.split(".")[1]
    raw += "=" * (-len(raw) % 4)
    return json.loads(base64.urlsafe_b64decode(raw))


def test_create_join_token_carries_dispatch():
    room = generate_room_name()
    token = create_join_token(
        room=room,
        identity="tester",
        dispatch_metadata={"user_id": "u-1", "thread_id": "t-1"},
    )
    payload = _decode_payload(token)

    assert payload["sub"] == "tester"
    assert payload["video"]["room"] == room
    assert payload["video"]["roomJoin"] is True
    assert payload["video"]["canPublish"] is True
    assert payload["video"]["canSubscribe"] is True

    agents = payload["roomConfig"]["agents"]
    assert agents[0]["agentName"] == settings.agent_name
    assert json.loads(agents[0]["metadata"])["thread_id"] == "t-1"


def test_create_join_token_without_dispatch_has_no_room_config():
    token = create_join_token(room="soul-x", identity="tester")
    payload = _decode_payload(token)
    assert "roomConfig" not in payload


def test_token_expires_after_ttl():
    token = create_join_token(room="soul-x", identity="tester", ttl_seconds=300)
    payload = _decode_payload(token)
    assert payload["exp"] - payload["nbf"] == 300
