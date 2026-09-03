"""Application-wide configuration sourced from environment variables.

Every module that needs a setting imports it from here, so there is
exactly one place to change defaults or env-var names.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

# ── Ollama / LLM ──────────────────────────────────────────────────────────
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://127.0.0.1:11434/v1")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen2.5:7b")
OLLAMA_API_KEY = os.environ.get("OLLAMA_API_KEY", "ollama")

# ── Database (SQL Server / ODBC) ──────────────────────────────────────────
MAX_ROWS = 100
MAX_RETRIES = 5
MAX_HISTORY_TURNS = 6
RESOLVER_CONTEXT_TURNS = 4
