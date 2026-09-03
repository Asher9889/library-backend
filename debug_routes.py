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
from pydantic import BaseModel

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


# ── correct_asr_errors ─────────────────────────────────────────────────────


def _run_correct_asr_errors(req: CorrectAsrRequest) -> NodeOutput:
    """Mirror of ``correct_asr_errors_node`` in sql_agent.py.

    Builds the ASR-correction prompt with history, calls the local
    LLM, and falls back to the original question on failure or on
    a suspiciously-long/garbled reply.
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

    prompt = ASR_CORRECTION_PROMPT.format(history=history_str, question=question)
    logs.append(f"PROMPT: {prompt}")

    try:
        base_url = OLLAMA_BASE_URL.rstrip("/").removesuffix("/v1")
        client = ollama.Client(host=base_url)
        resp = client.generate(model=OLLAMA_MODEL, prompt=prompt, options={"temperature": 0})
        corrected = (resp.get("response") or "").strip()
        
        print(f"[ASR CORRECTION] LLM returned: {corrected}")

        if not corrected or len(corrected) > len(question) * 3:
            logs.append(
                "LLM returned invalid response (empty or >3x length); "
                "falling back to original."
            )
            corrected = question
        else:
            print(f"[ASR CORRECTION] LLM returned valid response: {corrected}")
            logs.append(f"LLM_REPLY: {corrected}")

        logs.append(f"ORIGINAL: {question}")
        logs.append(f"CORRECTED: {corrected}")
        output = {"corrected_question": corrected}
    except Exception as e:
        logs.append(f"ERROR: {e}; falling back to original.")
        output = {"corrected_question": question}

    return NodeOutput(
        node="correct_asr_errors",
        input={"question": question, "chat_history": history},
        output=output,
        logs=logs,
    )


@router.post("/correct_asr_errors", response_model=NodeOutput)
def debug_correct_asr_errors(req: CorrectAsrRequest):
    return _run_correct_asr_errors(req)
