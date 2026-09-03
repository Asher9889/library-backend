"""LiveKit voice session management endpoints.

Handles session creation, token generation, agent dispatch, and
config queries for the LiveKit voice agent integration.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from livekit_agent.config import settings as lk_settings
from livekit_agent.tokens import (
    create_join_token,
    dispatch_agent,
    generate_room_name,
)

router = APIRouter(prefix="/api/livekit", tags=["livekit"])


# ── Request / response models ──────────────────────────────────────────────


class LiveKitSessionRequest(BaseModel):
    room: Optional[str] = None
    identity: Optional[str] = None
    user_id: Optional[str] = None
    user_name: Optional[str] = None
    thread_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    dispatch: bool = True


class LiveKitDispatchRequest(BaseModel):
    room: str
    user_id: Optional[str] = None
    user_name: Optional[str] = None
    thread_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class LiveKitResponse(BaseModel):
    url: str
    token: str
    room: str
    identity: str
    agent_name: str
    ttl_seconds: int
    expires_at: str
    dispatch: str


class LiveKitDispatchResponse(BaseModel):
    room: str
    agent_name: str
    dispatch_id: Optional[str] = None
    dispatch: str


# ── Helpers ────────────────────────────────────────────────────────────────


def _build_session_response(
    room: str,
    identity: str,
    dispatch_method: str,
    dispatch_metadata: Optional[Dict[str, Any]],
) -> LiveKitResponse:
    token = create_join_token(
        room=room,
        identity=identity,
        name=identity,
        dispatch_metadata=dispatch_metadata,
        ttl_seconds=lk_settings.token_ttl_seconds,
    )
    expires = datetime.now(timezone.utc) + timedelta(
        seconds=lk_settings.token_ttl_seconds,
    )
    return LiveKitResponse(
        url=lk_settings.livekit_url,
        token=token,
        room=room,
        identity=identity,
        agent_name=lk_settings.agent_name,
        ttl_seconds=lk_settings.token_ttl_seconds,
        expires_at=expires.isoformat(),
        dispatch=dispatch_method,
    )


def _build_dispatch_metadata(
    request_user_id: Optional[str],
    request_user_name: Optional[str],
    request_thread_id: Optional[str],
    request_metadata: Optional[Dict[str, Any]],
    room: str,
    identity: str,
) -> Dict[str, Any]:
    dispatch_metadata = dict(request_metadata or {})
    if request_user_id:
        dispatch_metadata["user_id"] = request_user_id
    if request_user_name:
        dispatch_metadata["user_name"] = request_user_name
    if request_thread_id:
        dispatch_metadata["thread_id"] = request_thread_id
    dispatch_metadata["room"] = room
    dispatch_metadata["identity"] = identity
    dispatch_metadata.setdefault("session_id", uuid.uuid4().hex[:16])
    return dispatch_metadata


# ── Endpoints ──────────────────────────────────────────────────────────────


@router.get("/config")
def livekit_config():
    return {
        "url": lk_settings.livekit_url,
        "agent_name": lk_settings.agent_name,
        "token_ttl_seconds": lk_settings.token_ttl_seconds,
        "tts_language": lk_settings.tts_language,
    }


@router.post("/session", response_model=LiveKitResponse)
def livekit_session(request: LiveKitSessionRequest):
    room = request.room or generate_room_name("soul")
    identity = request.identity or f"user-{uuid.uuid4().hex[:10]}"

    dispatch_method = "none"
    if request.dispatch:
        try:
            dispatch_metadata = _build_dispatch_metadata(
                request.user_id,
                request.user_name,
                request.thread_id,
                request.metadata,
                room,
                identity,
            )
            dispatch_method = "token"
            return _build_session_response(
                room, identity, dispatch_method, dispatch_metadata,
            )
        except Exception as e:
            print(f"[LIVEKIT] Could not attach token dispatch: {e}")
            raise HTTPException(
                status_code=500, detail=f"LiveKit dispatch failed: {e}",
            )

    return _build_session_response(room, identity, dispatch_method, None)


@router.post("/token", response_model=LiveKitResponse)
def livekit_token(request: LiveKitSessionRequest):
    room = request.room or generate_room_name("soul")
    identity = request.identity or f"user-{uuid.uuid4().hex[:10]}"
    return _build_session_response(room, identity, "none", None)


@router.post("/dispatch", response_model=LiveKitDispatchResponse)
async def livekit_dispatch(request: LiveKitDispatchRequest):
    dispatch_metadata = dict(request.metadata or {})
    if request.user_id:
        dispatch_metadata["user_id"] = request.user_id
    if request.user_name:
        dispatch_metadata["user_name"] = request.user_name
    if request.thread_id:
        dispatch_metadata["thread_id"] = request.thread_id

    try:
        dispatch_id = await dispatch_agent(request.room, dispatch_metadata)
        return LiveKitDispatchResponse(
            room=request.room,
            agent_name=lk_settings.agent_name,
            dispatch_id=dispatch_id,
            dispatch="api",
        )
    except Exception as e:
        print(
            f"[LIVEKIT] AgentDispatchService unavailable ({e}); "
            "using token dispatch instead."
        )
        return LiveKitDispatchResponse(
            room=request.room,
            agent_name=lk_settings.agent_name,
            dispatch_id=None,
            dispatch="token",
        )
