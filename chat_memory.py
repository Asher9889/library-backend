"""
chat_memory.py
==================================================================
Long-term, per-member, date-wise chat history for the SOUL 3.0
Library AI Assistant, backed by SQLite3.

WHY THIS EXISTS
----------------
`MemorySaver` (langgraph) already gives you SHORT-TERM memory: it keeps
`chat_history` inside the graph's checkpoint so `resolve_followup_node` can
resolve "uska", "unka", etc. But that memory:
  - lives in RAM (MemorySaver), so it's wiped on every restart/deploy
  - is keyed by `thread_id`, not by the logged-in member
  - is trimmed to the last MAX_HISTORY_TURNS turns

This module is LONG-TERM memory: every finished turn (question + answer) is
written to disk, permanently, tagged with the member's `mem_cd` and the
calendar date (in IST) it happened on. A member can then log in any day and
pull up "what did I ask on 24 Aug", or just scroll all their past chats.

DESIGN NOTES
------------
* SQLite in WAL mode + one connection per thread (via threading.local)
  handles FastAPI's many-short-read / occasional-write workload safely.
  A module-level lock serializes writes as extra insurance against
  "database is locked" under bursty concurrent traffic.
* Every row stores both a UTC timestamp (created_at_utc, unambiguous,
  good for sorting/debugging) and an IST calendar date/time
  (chat_date / chat_time, what the member actually experienced) because
  "date-wise" grouping should follow the member's local day boundary,
  not UTC's.
* `mem_cd` is always `.strip()`-ed before storage/lookup, mirroring the
  char-column trailing-space issue you already handle in the main SQL DB.
* Zero dependency on the rest of app.py — importable and unit-testable
  on its own.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger("chat_memory")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

IST = ZoneInfo("Asia/Kolkata")

# Configurable via env var so prod/dev/docker can point at different files
# without editing code. Defaults to a file next to this module.
DB_PATH = os.environ.get(
    "CHAT_MEMORY_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "chat_memory.sqlite3"),
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS chat_messages (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id          TEXT    NOT NULL UNIQUE,
    mem_cd              TEXT    NOT NULL,
    thread_id           TEXT    NOT NULL,
    question             TEXT    NOT NULL,
    resolved_question   TEXT,
    answer              TEXT    NOT NULL,
    sql_query           TEXT,
    report_id           TEXT,
    had_chart           INTEGER NOT NULL DEFAULT 0,
    created_at_utc      TEXT    NOT NULL,   -- ISO-8601, e.g. 2026-08-26T10:15:03.123456+00:00
    chat_date           TEXT    NOT NULL,   -- 'YYYY-MM-DD' in Asia/Kolkata
    chat_time           TEXT    NOT NULL    -- 'HH:MM:SS' in Asia/Kolkata
);

-- Fast "give me all dates for this member" / "give me this member+date" lookups
CREATE INDEX IF NOT EXISTS idx_chat_mem_date  ON chat_messages (mem_cd, chat_date);
CREATE INDEX IF NOT EXISTS idx_chat_mem       ON chat_messages (mem_cd);
CREATE INDEX IF NOT EXISTS idx_chat_thread    ON chat_messages (thread_id);
"""

_write_lock = threading.Lock()
_local = threading.local()


def _get_conn() -> sqlite3.Connection:
    """One connection per thread (FastAPI/uvicorn workers reuse threads
    from a pool for sync endpoints), created lazily and cached."""
    conn = getattr(_local, "conn", None)
    if conn is not None:
        return conn

    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    # WAL = readers don't block the writer and vice versa; much better
    # concurrency than the default rollback journal for a web server.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")  # wait up to 30s instead of raising "locked"
    conn.execute("PRAGMA foreign_keys=ON;")
    _local.conn = conn
    return conn


def init_db() -> None:
    """Create the schema if it doesn't exist yet. Call once at app startup."""
    conn = _get_conn()
    with _write_lock:
        conn.executescript(_SCHEMA)
        conn.commit()
    logger.info(f"[CHAT_MEMORY] DB ready at {DB_PATH}")


def _now_parts() -> tuple[str, str, str]:
    """Returns (created_at_utc_iso, chat_date_ist, chat_time_ist)."""
    now_utc = datetime.now(ZoneInfo("UTC"))
    now_ist = now_utc.astimezone(IST)
    return (
        now_utc.isoformat(),
        now_ist.strftime("%Y-%m-%d"),
        now_ist.strftime("%H:%M:%S"),
    )


def _clean_mem_cd(mem_cd: str) -> str:
    return (mem_cd or "").strip()


# ------------------------------------------------------------------
# WRITE
# ------------------------------------------------------------------
def save_turn(
    mem_cd: str,
    thread_id: str,
    question: str,
    answer: str,
    resolved_question: Optional[str] = None,
    sql_query: Optional[str] = None,
    report_id: Optional[str] = None,
    had_chart: bool = False,
) -> Optional[str]:
    """
    Persist one finished Q&A turn. Returns the generated message_id, or
    None if the turn was skipped (e.g. no mem_cd — nothing to attach
    long-term history to, since it must be viewable per logged-in person).

    This is intentionally best-effort: a failure here must NEVER break the
    user-facing /ask response, so callers should wrap this in try/except
    (or use `save_turn_safe` below).
    """
    mem_cd = _clean_mem_cd(mem_cd)
    if not mem_cd:
        logger.debug("[CHAT_MEMORY] Skipping save: no mem_cd (anonymous/guest turn).")
        return None
    if not question or not answer:
        logger.debug("[CHAT_MEMORY] Skipping save: empty question/answer.")
        return None

    message_id = str(uuid.uuid4())
    created_at_utc, chat_date, chat_time = _now_parts()

    conn = _get_conn()
    with _write_lock:
        conn.execute(
            """
            INSERT INTO chat_messages
                (message_id, mem_cd, thread_id, question, resolved_question,
                 answer, sql_query, report_id, had_chart,
                 created_at_utc, chat_date, chat_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                mem_cd,
                thread_id or "",
                question,
                resolved_question,
                answer,
                sql_query,
                report_id,
                1 if had_chart else 0,
                created_at_utc,
                chat_date,
                chat_time,
            ),
        )
        conn.commit()

    return message_id


def save_turn_safe(**kwargs) -> Optional[str]:
    """Same as save_turn but swallows all exceptions and logs instead —
    use this from request-handling code so a DB hiccup never 500s the
    chat response the member is actively waiting on."""
    try:
        return save_turn(**kwargs)
    except Exception as e:
        logger.error(f"[CHAT_MEMORY] save_turn failed (non-fatal): {e}")
        return None


# ------------------------------------------------------------------
# READ
# ------------------------------------------------------------------
def get_chat_dates(mem_cd: str) -> List[Dict[str, Any]]:
    """List every date this member has chatted on, most recent first,
    with a message count and first/last time — good for a sidebar like
    'Aug 26 (12 messages)', 'Aug 24 (3 messages)', ..."""
    mem_cd = _clean_mem_cd(mem_cd)
    if not mem_cd:
        return []

    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT
            chat_date,
            COUNT(*)      AS message_count,
            MIN(chat_time) AS first_time,
            MAX(chat_time) AS last_time
        FROM chat_messages
        WHERE mem_cd = ?
        GROUP BY chat_date
        ORDER BY chat_date DESC
        """,
        (mem_cd,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_messages_for_date(mem_cd: str, chat_date: str) -> List[Dict[str, Any]]:
    """All messages for one member on one calendar date (YYYY-MM-DD, IST),
    oldest first (natural reading order for a conversation)."""
    mem_cd = _clean_mem_cd(mem_cd)
    if not mem_cd or not chat_date:
        return []

    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT message_id, thread_id, question, resolved_question, answer,
               sql_query, report_id, had_chart, created_at_utc, chat_date, chat_time
        FROM chat_messages
        WHERE mem_cd = ? AND chat_date = ?
        ORDER BY id ASC
        """,
        (mem_cd, chat_date),
    ).fetchall()
    return [dict(r) for r in rows]


def get_all_messages(mem_cd: str, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    """Paginated full history for a member, newest first. Use for an
    infinite-scroll / 'load more' history panel."""
    mem_cd = _clean_mem_cd(mem_cd)
    if not mem_cd:
        return []

    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT message_id, thread_id, question, resolved_question, answer,
               sql_query, report_id, had_chart, created_at_utc, chat_date, chat_time
        FROM chat_messages
        WHERE mem_cd = ?
        ORDER BY id DESC
        LIMIT ? OFFSET ?
        """,
        (mem_cd, limit, offset),
    ).fetchall()
    return [dict(r) for r in rows]


def get_thread_messages(thread_id: str) -> List[Dict[str, Any]]:
    """All messages belonging to one conversation thread — mainly for
    debugging/support, since a member normally browses by mem_cd+date."""
    if not thread_id:
        return []
    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT message_id, mem_cd, thread_id, question, resolved_question, answer,
               sql_query, report_id, had_chart, created_at_utc, chat_date, chat_time
        FROM chat_messages
        WHERE thread_id = ?
        ORDER BY id ASC
        """,
        (thread_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def search_messages(mem_cd: str, keyword: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Simple substring search across a member's own question+answer text.
    Good enough for 'find that chat where I asked about overdue books'
    without pulling in a full-text-search engine."""
    mem_cd = _clean_mem_cd(mem_cd)
    keyword = (keyword or "").strip()
    if not mem_cd or not keyword:
        return []

    like = f"%{keyword}%"
    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT message_id, thread_id, question, resolved_question, answer,
               sql_query, report_id, had_chart, created_at_utc, chat_date, chat_time
        FROM chat_messages
        WHERE mem_cd = ? AND (question LIKE ? OR answer LIKE ?)
        ORDER BY id DESC
        LIMIT ?
        """,
        (mem_cd, like, like, limit),
    ).fetchall()
    return [dict(r) for r in rows]


# ------------------------------------------------------------------
# DELETE / MAINTENANCE
# ------------------------------------------------------------------
def delete_member_history(mem_cd: str) -> int:
    """Wipe ALL history for one member (e.g. 'clear my chat history'
    button, or GDPR-style data deletion request). Returns rows deleted."""
    mem_cd = _clean_mem_cd(mem_cd)
    if not mem_cd:
        return 0
    conn = _get_conn()
    with _write_lock:
        cur = conn.execute("DELETE FROM chat_messages WHERE mem_cd = ?", (mem_cd,))
        conn.commit()
    return cur.rowcount


def delete_messages_before(mem_cd: str, before_date: str) -> int:
    """Delete a member's messages strictly before a given YYYY-MM-DD date.
    Useful for a retention policy (e.g. keep only last 180 days)."""
    mem_cd = _clean_mem_cd(mem_cd)
    if not mem_cd or not before_date:
        return 0
    conn = _get_conn()
    with _write_lock:
        cur = conn.execute(
            "DELETE FROM chat_messages WHERE mem_cd = ? AND chat_date < ?",
            (mem_cd, before_date),
        )
        conn.commit()
    return cur.rowcount


def get_member_count() -> int:
    """Total distinct members with saved history — handy for a quick
    admin/health check."""
    conn = _get_conn()
    row = conn.execute("SELECT COUNT(DISTINCT mem_cd) AS c FROM chat_messages").fetchone()
    return row["c"] if row else 0