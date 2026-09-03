"""Ask endpoints: ``/ask``, ``/ask/v2``, plus ancillary helpers.

These are the primary query endpoints that run the LangGraph
text-to-SQL agent and return the answer (sync or SSE-streamed).
Also includes ``/welcome``, ``/health/llm``, and the memory
reset / chat-history endpoints.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from langchain_core.messages import HumanMessage
from pydantic import BaseModel

import chat_memory
from config import OLLAMA_BASE_URL, OLLAMA_MODEL
from database import execute_query
from helpers import is_llm_connection_error, sse
from reports import get_report, store_report
from sql_agent import (
    MAX_HISTORY_TURNS,
    build_generate_answer_messages,
    check_status_node,
    library_agent,
    library_agent_stream,
    llm_answer,
    token_tracker,
)

router = APIRouter(tags=["ask"])


class QueryRequest(BaseModel):
    question: str
    thread_id: str
    mem_cd: Optional[str] = None


class QueryResponse(BaseModel):
    question: str
    resolved_question: str
    sql_query: str
    answer: str
    chart_base64: Optional[str] = None
    attempts: int
    debug_error: Optional[str] = None
    report_available: bool = False
    report_id: Optional[str] = None
    image_data: Optional[List[Dict[str, Any]]] = None


class ChatDateSummary(BaseModel):
    chat_date: str
    message_count: int
    first_time: str
    last_time: str


class ChatMessageOut(BaseModel):
    message_id: str
    thread_id: str
    question: str
    resolved_question: Optional[str] = None
    answer: str
    sql_query: Optional[str] = None
    report_id: Optional[str] = None
    had_chart: bool
    chat_date: str
    chat_time: str
    created_at_utc: str


def _row_to_message_out(row: dict) -> ChatMessageOut:
    return ChatMessageOut(
        message_id=row["message_id"],
        thread_id=row["thread_id"],
        question=row["question"],
        resolved_question=row.get("resolved_question"),
        answer=row["answer"],
        sql_query=row.get("sql_query"),
        report_id=row.get("report_id"),
        had_chart=bool(row["had_chart"]),
        chat_date=row["chat_date"],
        chat_time=row["chat_time"],
        created_at_utc=row["created_at_utc"],
    )


def _check_access_control(mem_cd: str, executed_sql: str) -> Optional[QueryResponse]:
    """Return a blocked response if mem_cd is missing from a sensitive query."""
    if not mem_cd:
        return None
    sql_lower = executed_sql.lower()
    if mem_cd.lower() not in sql_lower:
        sensitive_tables = [
            "m_member", "t_issue", "t_receive", "t_reserve", "t_memfine",
            "vmember", "vcmpltissuedetails",
        ]
        if any(table in sql_lower for table in sensitive_tables):
            return QueryResponse(
                question="",
                resolved_question="",
                sql_query="",
                answer=(
                    "🔒 Aap sirf apna data dekh sakte hain. "
                    "Yeh query access nahi ki ja sakti."
                ),
                attempts=0,
                debug_error="Blocked: Missing mem_cd filter",
                report_available=False,
                report_id=None,
            )
    return None


def _save_report_if_data(report_data, user_question: str, executed_sql: str) -> tuple[bool, Optional[str]]:
    report_available = False 
    report_id = None
    if report_data and isinstance(report_data, list) and len(report_data) > 0:
        try:
            report_id = store_report(
                data=report_data, question=user_question, sql=executed_sql,
            )
            report_available = True
        except Exception as e:
            print(f"[REPORT_STORE] Failed to store report: {e}")
            report_available = False
    return report_available, report_id


def _append_chat_history(thread_id: str, question: str, answer: str, base_state: Dict[str, Any]) -> None:
    history: List[Dict[str, str]] = base_state.get("chat_history", []) or []
    history = history + [{"question": question, "answer": answer}]
    if len(history) > MAX_HISTORY_TURNS:
        history = history[-MAX_HISTORY_TURNS:]
    library_agent.update_state(
        config={"configurable": {"thread_id": thread_id}},
        values={"chat_history": history},
    )


def _initial_state(user_question: str, mem_cd: str) -> dict:
    return {
        "question": user_question,
        "corrected_question": "",
        "mem_cd": mem_cd,
        "resolved_question": "",
        "attempts": 0,
        "error": "",
        "previous_sql": "",
        "error_history": [],
        "schema_hint": "",
        "chart_base64": None,
        "report_data": None,
        "asr_corrections": None,
    }


# ── GET /welcome ───────────────────────────────────────────────────────────


@router.get("/welcome")
def welcome_api():
    return {
        "message": "Welcome to the SOUL 3.0 Library AI Assistant!",
        "sample_questions": [
            "Library me total kitni physical books hain?"
        ],
    }


# ── GET /health/llm ───────────────────────────────────────────────────────


@router.get("/health/llm")
def health_llm():
    try:
        resp = llm_answer.invoke(
            [HumanMessage(content="ping")],
            config={"metadata": {"node_name": "health_check"}},
        )
        return {
            "status": "ok",
            "provider": "ollama",
            "base_url": OLLAMA_BASE_URL,
            "model": OLLAMA_MODEL,
            "sample_reply": (resp.content or "")[:80],
        }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Local Ollama LLM unreachable at {OLLAMA_BASE_URL} "
                   f"(model={OLLAMA_MODEL}): {e}",
        )


# ── POST /ask (sync) ──────────────────────────────────────────────────────


@router.post("/ask", response_model=QueryResponse)
def ask_library_agent(request: QueryRequest):
    user_question = request.question
    mem_cd = (request.mem_cd or "").strip()

    print(f"\n[API] mem_cd={mem_cd} Q={user_question}\n")

    token_tracker.reset()

    try:
        final_state = library_agent.invoke(
            _initial_state(user_question, mem_cd),
            config={
                "configurable": {"thread_id": request.thread_id},
                "callbacks": [token_tracker],
            },
        )
    except Exception as e:
        if is_llm_connection_error(e):
            print(f"[API] LOCAL LLM (Ollama) UNREACHABLE: {e}")
            return QueryResponse(
                question=user_question,
                resolved_question=user_question,
                sql_query="",
                answer=(
                    "स्थानीय AI मॉडल (Ollama) से कनेक्ट नहीं हो पाया। कृपया "
                    "सुनिश्चित करें कि Ollama server "
                    f"({OLLAMA_BASE_URL}) चालू है और मॉडल '{OLLAMA_MODEL}' "
                    "pull किया हुआ है। "
                    "Could not connect to the local Ollama LLM server — "
                    "please make sure it's running."
                ),
                attempts=1,
                debug_error=f"Ollama connection error: {e}",
            )
        print(f"[API] UNEXPECTED ERROR: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=f"Library agent failed: {e}")

    print(token_tracker.summary())

    final_answer = final_state.get("query_result", "Agent failed to generate answer.")
    executed_sql = (
        final_state.get("sql_query", "")
        .replace("```sql", "")
        .replace("```", "")
        .strip()
    )
    db_error = final_state.get("error", None) or None
    chart_b64 = final_state.get("chart_base64", None)
    resolved_q = final_state.get("resolved_question", "") or user_question

    blocked = _check_access_control(mem_cd, executed_sql)
    if blocked is not None:
        chat_memory.save_turn_safe(
            mem_cd=mem_cd,
            thread_id=request.thread_id,
            question=user_question,
            answer=blocked.answer,
            resolved_question=None,
            sql_query=None,
            report_id=None,
            had_chart=False,
        )
        blocked.question = user_question
        blocked.resolved_question = ""
        blocked.attempts = final_state.get("attempts", 0)
        return blocked

    report_available, report_id = _save_report_if_data(
        final_state.get("report_data"), user_question, executed_sql,
    )

    chat_memory.save_turn_safe(
        mem_cd=mem_cd,
        thread_id=request.thread_id,
        question=user_question,
        answer=final_answer,
        resolved_question=resolved_q,
        sql_query=executed_sql,
        report_id=report_id,
        had_chart=bool(chart_b64),
    )

    return QueryResponse(
        question=user_question,
        resolved_question=resolved_q,
        sql_query=executed_sql,
        answer=final_answer,
        chart_base64=chart_b64,
        attempts=final_state.get("attempts", 0),
        debug_error=db_error,
        report_available=report_available,
        report_id=report_id,
    )


# ── POST /ask/v2 (SSE streaming) ──────────────────────────────────────────


async def _ask_v2_events(request: QueryRequest):
    user_question = request.question
    mem_cd = (request.mem_cd or "").strip()
    print(f"\n[API v2] mem_cd={mem_cd} Q={user_question}\n")

    try:
        final_state = await asyncio.to_thread(
            library_agent_stream.invoke,
            _initial_state(user_question, mem_cd),
            config={"configurable": {"thread_id": request.thread_id}},
        )
    except Exception as e:
        if is_llm_connection_error(e):
            print(f"[API v2] LOCAL LLM (Ollama) UNREACHABLE: {e}")
            yield sse(
                "token",
                text=(
                    "स्थानीय AI मॉडल (Ollama) से कनेक्ट नहीं हो पाया। कृपया "
                    "सुनिश्चित करें कि Ollama server "
                    f"({OLLAMA_BASE_URL}) चालू है और मॉडल '{OLLAMA_MODEL}' "
                    "pull किया हुआ है। "
                    "Could not connect to the local Ollama LLM server — "
                    "please make sure it's running."
                ),
            )
            yield sse(
                "done",
                question=user_question,
                resolved_question=user_question,
                sql_query="",
                chart_base64=None,
                attempts=1,
                debug_error=f"Ollama connection error: {e}",
                report_available=False,
                report_id=None,
            )
            return
        print(f"[API v2] UNEXPECTED ERROR: {type(e).__name__}: {e}")
        yield sse("error", message=f"Library agent failed: {e}")
        return

    executed_sql = (
        final_state.get("sql_query", "")
        .replace("```sql", "")
        .replace("```", "")
        .strip()
    )
    db_error = final_state.get("error", None) or None
    chart_b64 = final_state.get("chart_base64", None)
    resolved_q = final_state.get("resolved_question", "") or user_question
    attempts = final_state.get("attempts", 0)

    yield sse("status", stage="sql_ready", sql_query=executed_sql, attempts=attempts)

    # Access control check
    if mem_cd:
        sql_lower = executed_sql.lower()
        if mem_cd.lower() not in sql_lower:
            sensitive_tables = [
                "m_member", "t_issue", "t_receive", "t_reserve", "t_memfine",
                "vmember", "vcmpltissuedetails",
            ]
            if any(table in sql_lower for table in sensitive_tables):
                blocked_answer = (
                    "🔒 Aap sirf apna data dekh sakte hain. "
                    "Yeh query access nahi ki ja sakti."
                )
                yield sse("token", text=blocked_answer)
                chat_memory.save_turn_safe(
                    mem_cd=mem_cd,
                    thread_id=request.thread_id,
                    question=user_question,
                    answer=blocked_answer,
                    resolved_question=None,
                    sql_query=None,
                    report_id=None,
                    had_chart=False,
                )
                yield sse(
                    "done",
                    question=user_question,
                    resolved_question="",
                    sql_query="",
                    chart_base64=None,
                    attempts=attempts,
                    debug_error="Blocked: Missing mem_cd filter",
                    report_available=False,
                    report_id=None,
                )
                return

    report_available, report_id = _save_report_if_data(
        final_state.get("report_data"), user_question, executed_sql,
    )

    yield sse("status", stage="answering")
    messages = build_generate_answer_messages(final_state)
    full_answer_parts: List[str] = []
    try:
        async for chunk in llm_answer.astream(
            messages,
            config={
                "callbacks": [token_tracker],
                "metadata": {"node_name": "generate_answer_stream"},
            },
        ):
            text = chunk.content or ""
            if text:
                full_answer_parts.append(text)
                yield sse("token", text=text)
    except Exception as e:
        if is_llm_connection_error(e):
            print(f"[API v2] LOCAL LLM (Ollama) UNREACHABLE DURING STREAM: {e}")
            yield sse(
                "error",
                message=(
                    f"Could not connect to the local Ollama LLM server "
                    f"({OLLAMA_BASE_URL}). Please make sure it's running."
                ),
            )
            return
        print(f"[API v2] ANSWER STREAM ERROR: {type(e).__name__}: {e}")
        yield sse("error", message=f"Answer generation failed: {e}")
        return

    full_answer = "".join(full_answer_parts).strip()
    if not full_answer:
        full_answer = (
            "माफ़ कीजिए, जवाब बनाने में समस्या आई। कृपया दोबारा पूछें."
        )
        yield sse("token", text=full_answer)

    try:
        _append_chat_history(request.thread_id, user_question, full_answer, final_state)
    except Exception as e:
        print(f"[API v2] chat_history update failed: {e}")

    chat_memory.save_turn_safe(
        mem_cd=mem_cd,
        thread_id=request.thread_id,
        question=user_question,
        answer=full_answer,
        resolved_question=resolved_q,
        sql_query=executed_sql,
        report_id=report_id,
        had_chart=bool(chart_b64),
    )

    yield sse(
        "done",
        question=user_question,
        resolved_question=resolved_q,
        sql_query=executed_sql,
        chart_base64=chart_b64,
        attempts=attempts,
        debug_error=db_error,
        report_available=report_available,
        report_id=report_id,
    )


@router.post("/ask/v2")
async def ask_library_agent_v2(request: QueryRequest):
    return StreamingResponse(
        _ask_v2_events(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ── Memory reset ───────────────────────────────────────────────────────────


@router.post("/reset-memory/{thread_id}")
def reset_memory(thread_id: str):
    try:
        library_agent.update_state(
            config={"configurable": {"thread_id": thread_id}},
            values={"chat_history": []},
        )
        return {
            "status": "ok",
            "detail": f"Memory cleared for thread_id={thread_id}",
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Could not reset memory: {e}",
        )


# ── Chat history ───────────────────────────────────────────────────────────


@router.get("/history/{mem_cd}/dates", response_model=List[ChatDateSummary])
def history_dates(mem_cd: str):
    return chat_memory.get_chat_dates(mem_cd)


@router.get(
    "/history/{mem_cd}/date/{chat_date}",
    response_model=List[ChatMessageOut],
)
def history_for_date(mem_cd: str, chat_date: str):
    rows = chat_memory.get_messages_for_date(mem_cd, chat_date)
    return [_row_to_message_out(r) for r in rows]


@router.get("/history/{mem_cd}", response_model=List[ChatMessageOut])
def history_all(mem_cd: str, limit: int = 50, offset: int = 0):
    rows = chat_memory.get_all_messages(mem_cd, limit=limit, offset=offset)
    return [_row_to_message_out(r) for r in rows]


@router.get("/history/{mem_cd}/search", response_model=List[ChatMessageOut])
def history_search(mem_cd: str, q: str, limit: int = 50):
    rows = chat_memory.search_messages(mem_cd, q, limit=limit)
    return [_row_to_message_out(r) for r in rows]


@router.delete("/history/{mem_cd}")
def history_clear(mem_cd: str):
    deleted = chat_memory.delete_member_history(mem_cd)
    return {"status": "ok", "deleted_rows": deleted}
