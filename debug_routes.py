"""Debug route for LangGraph node testing.

This module lets you exercise a single LangGraph node in isolation —
without running the whole /ask pipeline — so you can verify inputs,
outputs, and the LLM behavior of each node independently.

Each node endpoint accepts the input *state fields* that node reads,
runs the exact same business logic as the /ask pipeline, and returns
the *partial update* that node produces, plus a log trail.

The implementation is intentionally a mirror of the node logic in
``sql_agent.py`` (not a re-export) so each node stays independently
inspectable and debuggable here.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

import ollama
from config import OLLAMA_BASE_URL, OLLAMA_MODEL
from prompts import ASR_CORRECTION_PROMPT

router = APIRouter(prefix="/debug/node", tags=["debug"])


# ── Shared models ──────────────────────────────────────────────────────────


class ChatTurn(BaseModel):
    question: str
    answer: str


class CorrectAsrRequest(BaseModel):
    """Input state fields the correct_asr_errors node reads."""

    question: str
    chat_history: Optional[List[ChatTurn]] = None


class NodeOutput(BaseModel):
    """Standard envelope every debug node returns."""

    node: str
    input: Dict[str, Any]
    output: Dict[str, Any]
    logs: List[str]


class ASRCorrection(BaseModel):
    """Single ASR correction suggestion."""

    original: str
    replacement: str
    confidence: float = Field(ge=0.0, le=1.0)


class ASRCorrectionResult(BaseModel):
    """Structured ASR correction response from the LLM."""

    corrections: List[ASRCorrection]


# ── correct_asr_errors ─────────────────────────────────────────────────────


def _run_correct_asr_errors(req: CorrectAsrRequest) -> NodeOutput:
    """Mirror of ``correct_asr_errors_node`` in sql_agent.py.

    Builds the ASR-correction prompt with history, calls the local
    LLM with structured JSON output, validates the response, and
    falls back to the original question on failure.
    """
    logs: List[str] = []
    logs.append("STEP: correct_asr_errors")

    question = req.question
    history: List[dict] = [h.model_dump() for h in (req.chat_history or [])]

    history_str = "\n".join(
        f"User: {h['question']}\nAssistant: {h['answer']}"
        for h in history[-2:]
    )
    if not history_str:
        history_str = "No history"
    logs.append(f"HISTORY_USED: {history_str}")

    system_prompt = ASR_CORRECTION_PROMPT.format(history=history_str)
    logs.append(f"SYSTEM_PROMPT: {system_prompt}")

    schema = ASRCorrectionResult.model_json_schema()

    try:
        base_url = OLLAMA_BASE_URL.rstrip("/").removesuffix("/v1")
        client = ollama.Client(host=base_url)
        resp = client.chat(
            model=OLLAMA_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question},
            ],
            format=schema,
            options={"temperature": 0},
        )

        content = (resp.get("message", {}).get("content") or "").strip()
        print(f"[ASR CORRECTION] LLM returned: {content}")

        result = ASRCorrectionResult.model_validate_json(content)

        logs.append(f"LLM_REPLY: {result.model_dump_json()}")
        logs.append(f"ORIGINAL: {question}")

        output = {"corrections": result.model_dump()}

    except Exception as e:
        logs.append(f"ERROR: {e}; falling back to original.")
        output = {"corrections": {"corrections": []}, "corrected_question": question}

    return NodeOutput(
        node="correct_asr_errors",
        input={"question": question, "chat_history": history},
        output=output,
        logs=logs,
    )


@router.post("/correct_asr_errors", response_model=NodeOutput)
def debug_correct_asr_errors(req: CorrectAsrRequest):
    return _run_correct_asr_errors(req)
