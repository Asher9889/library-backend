"""SOUL 3.0 Library AI Assistant — application entry point.

This barrel file creates the FastAPI app, registers middleware and
routers, and defines the ``startup`` lifecycle hook.  All business
logic lives in the sibling modules (``database``, ``sql_agent``,
``auth_routes``, ``ask_routes``, etc.).
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import OLLAMA_BASE_URL, OLLAMA_MODEL
from database import execute_query
from face_engine import FaceEmbeddingCache
from sql_agent import llm
from langchain_core.messages import HumanMessage

import chat_memory

# ── FastAPI app ────────────────────────────────────────────────────────────

app = FastAPI(title="SOUL30 Library Text-to-SQL API", version="3.4")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Register routers ───────────────────────────────────────────────────────

from auth_routes import router as auth_router
from ask_routes import router as ask_router
from debug_routes import router as debug_router
from livekit_routes import router as livekit_router
from report_routes import router as report_router

app.include_router(auth_router)
app.include_router(ask_router)
app.include_router(debug_router)
app.include_router(livekit_router)
app.include_router(report_router)


# ── Root & health ──────────────────────────────────────────────────────────


@app.get("/")
def read_root():
    return {
        "status": (
            f"SOUL30 Library Text-to-SQL API v3.4 "
            f"(Ollama/{OLLAMA_MODEL}, per-node LLM instances, "
            f"follow-up memory + long-term chat history) is running perfectly!"
        )
    }


# ── Startup lifecycle ──────────────────────────────────────────────────────


def _ensure_photo_column():
    try:
        check = execute_query(
            "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'm_memberFace' AND COLUMN_NAME = 'photo_base64'",
            limit=False,
        )
        if not check:
            execute_query(
                "ALTER TABLE m_memberFace ADD photo_base64 NVARCHAR(MAX) NULL",
                limit=False,
            )
            print("[MIGRATION] Added photo_base64 column to m_memberFace.")
    except Exception as e:
        print(f"[MIGRATION] Could not verify/add photo_base64 column: {e}")


@app.on_event("startup")
async def startup_event():
    _ensure_photo_column()
    FaceEmbeddingCache.refresh_embeddings()
    chat_memory.init_db()
    print("[SYSTEM] Face Cache loaded into RAM.")
    print("[SYSTEM] Long-term chat memory (SQLite) ready.")
    print(f"[SYSTEM] Local LLM: Ollama @ {OLLAMA_BASE_URL} | model={OLLAMA_MODEL}")
    try:
        llm.invoke(
            [HumanMessage(content="ping")],
            config={"metadata": {"node_name": "startup_ping"}},
        )
        print("[SYSTEM] Ollama LLM reachable")
    except Exception as e:
        print(
            f"[SYSTEM WARNING] Ollama LLM NOT reachable at startup "
            f"({OLLAMA_BASE_URL}): {e}"
        )
        print("[SYSTEM WARNING] Make sure 'ollama serve' is running and the model is pulled:")
        print(f"[SYSTEM WARNING]   ollama pull {OLLAMA_MODEL}")
