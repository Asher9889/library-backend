"""Helpers for issuing LiveKit access tokens and explicit agent dispatch."""
from __future__ import annotations

import datetime
import json
import uuid
from typing import Any

from livekit import api

from .config import settings


def _ws_to_https(url: str) -> str:
    if url.startswith("wss://"):
        return "https://" + url[len("wss://"):]
    if url.startswith("ws://"):
        return "http://" + url[len("ws://"):]
    return url


def generate_room_name(prefix: str = "soul") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def create_join_token(
    *,
    room: str,
    identity: str,
    name: str | None = None,
    dispatch_metadata: dict[str, Any] | None = None,
    ttl_seconds: int | None = None,
) -> str:
    """Build a JWT that lets a web client join ``room``.

    When ``dispatch_metadata`` is provided the token also carries the agent
    dispatch inside its room configuration, so the agent is spawned the moment
    the first participant connects and the room is created.
    """
    ttl = datetime.timedelta(seconds=ttl_seconds or settings.token_ttl_seconds)

    token = (
        api.AccessToken(settings.livekit_api_key, settings.livekit_api_secret)
        .with_identity(identity)
        .with_name(name or identity)
        .with_grants(
            api.VideoGrants(
                room_join=True,
                room=room,
                can_publish=True,
                can_subscribe=True,
                can_publish_data=True,
            )
        )
        .with_ttl(ttl)
    )

    if dispatch_metadata is not None:
        token = token.with_room_config(
            api.RoomConfiguration(
                agents=[
                    api.RoomAgentDispatch(
                        agent_name=settings.agent_name,
                        metadata=json.dumps(dispatch_metadata),
                    )
                ],
            )
        )

    return token.to_jwt()


async def dispatch_agent(
    room: str,
    metadata: dict[str, Any] | None = None,
) -> str:
    """Explicitly dispatch the agent to ``room`` via the server API.

    Returns the dispatch id. Requires the LiveKit HTTP API to be reachable at
    ``LIVEKIT_API_URL`` (or the ws url translated to https/http).
    """
    url = settings.livekit_api_url or _ws_to_https(settings.livekit_url)
    async with api.LiveKitAPI(
        url=url,
        api_key=settings.livekit_api_key,
        api_secret=settings.livekit_api_secret,
    ) as lkapi:
        dispatch = await lkapi.agent_dispatch.create_dispatch(
            api.CreateAgentDispatchRequest(
                agent_name=settings.agent_name,
                room=room,
                metadata=json.dumps(metadata or {}),
                deployment=settings.agent_deployment,
            )
        )
    return dispatch.id
