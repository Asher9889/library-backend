"""Small shared utilities used across route modules."""

from __future__ import annotations

import json


def sse(event_type: str, **payload) -> str:
    """Format a Server-Sent Event line."""
    return f"data: {json.dumps({'type': event_type, **payload}, ensure_ascii=False, default=str)}\n\n"


def is_llm_connection_error(err: Exception) -> bool:
    """Detect whether an exception looks like a connectivity / timeout issue
    with the local LLM (Ollama), rather than a real application bug."""
    msg = str(err).lower()
    needles = (
        "connection", "connect", "refused", "timed out", "timeout",
        "max retries exceeded", "failed to establish", "unreachable",
        "temporary failure", "econnrefused",
    )
    return any(n in msg for n in needles)
