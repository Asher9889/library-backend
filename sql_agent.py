"""LangGraph text-to-SQL agent: state, nodes, and compiled workflow.

The agent corrects ASR errors, resolves follow-up references,
generates T-SQL via a local LLM, executes it against SQL Server,
optionally produces a chart, generates a natural-language answer,
and updates conversation memory.
"""

from __future__ import annotations

import ast
import base64
import io
import logging
import re
import threading
from collections import defaultdict
from typing import Any, Dict, List, Optional, TypedDict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from config import (
    MAX_HISTORY_TURNS,
    MAX_RETRIES,
    OLLAMA_API_KEY,
    OLLAMA_BASE_URL,
    OLLAMA_MODEL,
    RESOLVER_CONTEXT_TURNS,
)
from database import build_schema_error_hint, execute_query, run_sql
from llm.ollama import generate as ollama_generate
from prompts import ASR_CORRECTION_PROMPT, FOLLOWUP_RESOLVER_PROMPT, SYSTEM_PROMPT

logger = logging.getLogger(__name__)

# ── LangGraph state ────────────────────────────────────────────────────────


class ChatTurn(TypedDict):
    question: str
    answer: str


class AgentState(TypedDict):
    question: str
    corrected_question: str
    resolved_question: str
    sql_query: str
    previous_sql: str
    query_result: str
    report_data: Optional[List[Dict[str, Any]]]
    attempts: int
    error: str
    error_history: List[str]
    schema_hint: str
    chart_base64: Optional[str]
    chat_history: List[ChatTurn]
    mem_cd: Optional[str]


# ── Token tracker (shared, reset per request) ─────────────────────────────


class NodeTokenTracker(BaseCallbackHandler):
    """Generic token-usage tracker for any LangChain chat model that reports
    usage via llm_output.token_usage / generation_info.token_usage."""

    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()
        self.usage = defaultdict(
            lambda: {"prompt": 0, "completion": 0, "total": 0, "calls": 0}
        )
        self.grand_total = {"prompt": 0, "completion": 0, "total": 0, "calls": 0}
        self._run_node_map: Dict[str, str] = {}

    def _extract_usage(self, response):
        llm_output = getattr(response, "llm_output", None) or {}
        if isinstance(llm_output, dict):
            for key in ("token_usage", "usage"):
                u = llm_output.get(key)
                if u:
                    p = u.get("prompt_tokens") or u.get("prompt") or 0
                    c = u.get("completion_tokens") or u.get("completion") or 0
                    t = u.get("total_tokens") or (p + c) or 0
                    if t > 0:
                        return p, c, t

        gens = getattr(response, "generations", []) or []
        for batch in gens:
            for g in batch:
                gen_info = getattr(g, "generation_info", None) or {}
                u = gen_info.get("token_usage") or gen_info.get("usage")
                if u:
                    p = u.get("prompt_tokens", 0) or u.get("prompt", 0)
                    c = u.get("completion_tokens", 0) or u.get("completion", 0)
                    t = u.get("total_tokens", p + c)
                    if t > 0:
                        return p, c, t
                msg = getattr(g, "message", None)
                usage_meta = (
                    getattr(msg, "usage_metadata", None) if msg is not None else None
                )
                if usage_meta:
                    p = usage_meta.get("input_tokens", 0)
                    c = usage_meta.get("output_tokens", 0)
                    t = usage_meta.get("total_tokens", p + c)
                    if t > 0:
                        return p, c, t
        return 0, 0, 0

    def on_llm_start(
        self, serialized, prompts, *, run_id=None, parent_run_id=None,
        tags=None, metadata=None, **kwargs,
    ):
        node = "unknown"
        if metadata:
            node = metadata.get("node_name") or metadata.get("node") or (
                tags[0] if tags else "unknown"
            )
        elif tags:
            node = tags[0]
        if run_id is not None:
            with self._lock:
                self._run_node_map[str(run_id)] = node

    def on_llm_end(
        self, response, *, run_id=None, parent_run_id=None,
        tags=None, metadata=None, **kwargs,
    ):
        p, c, t = self._extract_usage(response)
        if t == 0:
            return
        node = "unknown"
        if metadata:
            node = metadata.get("node_name") or metadata.get("node") or (
                tags[0] if tags else "unknown"
            )
        elif tags:
            node = tags[0]
        if node == "unknown" and run_id is not None:
            with self._lock:
                node = self._run_node_map.pop(str(run_id), "unknown")
        with self._lock:
            self.usage[node]["prompt"] += p
            self.usage[node]["completion"] += c
            self.usage[node]["total"] += t
            self.usage[node]["calls"] += 1
            self.grand_total["prompt"] += p
            self.grand_total["completion"] += c
            self.grand_total["total"] += t
            self.grand_total["calls"] += 1

    def reset(self):
        with self._lock:
            self.usage.clear()
            self.grand_total = {
                "prompt": 0, "completion": 0, "total": 0, "calls": 0,
            }

    def summary(self) -> str:
        lines = [
            "\n" + "=" * 76,
            "LLM TOKEN USAGE SUMMARY (per LangGraph node)",
            "=" * 76,
            f"{'Node':<28}{'Calls':>7}{'Prompt':>12}{'Completion':>14}{'Total':>12}",
            "-" * 76,
        ]
        with self._lock:
            for node, u in sorted(
                self.usage.items(), key=lambda x: -x[1]["total"]
            ):
                lines.append(
                    f"{node:<28}{u['calls']:>7}{u['prompt']:>12}"
                    f"{u['completion']:>14}{u['total']:>12}"
                )
            lines.append("-" * 76)
            gt = self.grand_total
            lines.append(
                f"{'GRAND TOTAL':<28}{gt['calls']:>7}{gt['prompt']:>12}"
                f"{gt['completion']:>14}{gt['total']:>12}"
            )
            lines.append("=" * 76 + "\n")
        return "\n".join(lines)


token_tracker = NodeTokenTracker()
checkpointer = MemorySaver()

# ── LLM clients (one per node, sharing the same Ollama backend) ───────────

_COMMON_LLM_KWARGS = dict(
    model=OLLAMA_MODEL,
    base_url=OLLAMA_BASE_URL,
    api_key=OLLAMA_API_KEY,
    temperature=0,
    max_retries=2,
    timeout=120,
)

llm_sql = ChatOpenAI(**_COMMON_LLM_KWARGS, max_tokens=800)
llm_answer = ChatOpenAI(**_COMMON_LLM_KWARGS, max_tokens=2000)
llm = llm_sql  # backward-compat alias

# ── Graph nodes ────────────────────────────────────────────────────────────


def correct_asr_errors_node(state: AgentState):
    print("---CORRECTING ASR ERRORS---")
    question = state["question"]
    history: List[ChatTurn] = state.get("chat_history", []) or []

    history_str = "\n".join(
        [f"User: {h['question']}\nAssistant: {h['answer']}" for h in history[-2:]]
    )
    if not history_str:
        history_str = "No history"

    prompt = ASR_CORRECTION_PROMPT.format(history=history_str, question=question)

    try:
        corrected = ollama_generate(prompt)

        if not corrected or len(corrected) > len(question) * 3:
            print("[ASR] LLM returned invalid response. Falling back to original.")
            corrected = question

        print(f"[ASR] Original : {question}")
        print(f"[ASR] Corrected: {corrected}")
        return {"corrected_question": corrected}
    except Exception as e:
        print(f"[ASR] Error: {e}. Falling back to original.")
        return {"corrected_question": question}


def resolve_followup_node(state: AgentState):
    print("---RESOLVING FOLLOW-UP---")
    question = state.get("corrected_question") or state["question"]
    history: List[ChatTurn] = state.get("chat_history", []) or []

    if not history:
        print("[RESOLVER] No history yet -> treating as standalone.")
        return {"resolved_question": question}

    recent = history[-RESOLVER_CONTEXT_TURNS:]
    history_text = "\n".join(
        f"Q: {h['question']}\nA: {h['answer']}" for h in recent
    )

    prompt = FOLLOWUP_RESOLVER_PROMPT.format(history=history_text, question=question)

    try:
        resolved = ollama_generate(prompt)
        resolved = resolved.strip().strip('"').strip("'").strip()
        print(f"[RESOLVER] LLM resolved question: {resolved}")
        if not resolved:
            resolved = question
    except Exception as e:
        print(f"[RESOLVER ERROR] Falling back to raw question: {e}")
        resolved = question

    print(f"[RESOLVER] Original : {question}")
    print(f"[RESOLVER] Resolved : {resolved}")

    return {"resolved_question": resolved}


def generate_sql_node(state: AgentState):
    print("---GENERATING SQL---")
    prev_err = state.get("error", "")
    prev_sql = state.get("previous_sql", "")
    err_hist = state.get("error_history", [])
    schema_hint = state.get("schema_hint", "")
    user_mem_cd = state.get("mem_cd", "")

    active_question = state.get("resolved_question") or state["question"]

    access_control_block = ""
    if user_mem_cd:
        access_control_block = (
            f"[STRICT ACCESS CONTROL: The logged-in user's mem_cd is '{user_mem_cd}'. "
            f"You MUST filter all personal data (issues, returns, fines, member details) "
            f"by this exact mem_cd. Always use WHERE RTRIM(mem_cd) = '{user_mem_cd}'. "
            f"DO NOT show any other member's data under any circumstances.]\n\n"
        )

    if prev_err:
        schema_block = (
            f"ACTUAL LIVE DATABASE SCHEMA for the table(s) you used, fetched just now from "
            f"INFORMATION_SCHEMA — this is GROUND TRUTH. Use ONLY these exact names. "
            f"Do NOT invent, assume, or guess any column or table name not listed here:\n"
            f"{schema_hint}\n\n"
            if schema_hint
            else ""
        )
        user_msg = (
            f"{access_control_block}"
            f"User Question: {active_question}\n\n"
            f"Your PREVIOUS SQL (which FAILED):\n{prev_sql}\n\n"
            f"ERROR returned by SQL Server:\n{prev_err}\n\n"
            f"All errors so far: {err_hist}\n\n"
            f"{schema_block}"
            f"Fix the SQL using the real schema above if provided. Re-think the JOINs and "
            f"column names. Remember the RTRIM and Status='AV' rules. "
            f"Output ONLY the SQL without any markdown."
        )
    else:
        user_msg = (
            f"{access_control_block}"
            f"User Question: {active_question}\n\n"
            f"Generate the T-SQL query. Output ONLY the SQL without any markdown."
        )

    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=user_msg),
    ]
    response = llm_sql.invoke(
        messages,
        config={
            "callbacks": [token_tracker],
            "metadata": {"node_name": "generate_sql"},
        },
    )
    return {
        "sql_query": response.content,
        "previous_sql": response.content,
        "attempts": state.get("attempts", 0) + 1,
    }


def execute_sql_node(state: AgentState):
    print("---EXECUTING SQL---")
    clean_sql = (
        state["sql_query"].replace("```sql", "").replace("```", "").strip()
    )
    print(f"\n[DEBUG] SQL (attempt {state.get('attempts', 0)}):\n{clean_sql}\n")

    result = run_sql(clean_sql)
    if result["success"]:
        data_str = str(result["data"])
        if len(data_str) > 12000:
            data_str = (
                data_str[:12000]
                + f"\n... [TRUNCATED, total rows: {result['rows']}]"
            )
        return {
            "query_result": data_str,
            "report_data": result["data"],
            "error": "",
            "schema_hint": "",
        }
    else:
        print(f"[DEBUG] SQL ERROR: {result['error']}\n")
        hist = state.get("error_history", [])
        hist.append(result["error"])

        schema_hint = ""
        try:
            schema_hint = build_schema_error_hint(clean_sql, result["error"])
            if schema_hint:
                print(f"[SCHEMA] Ground-truth hint for retry:\n{schema_hint}\n")
        except Exception as e:
            print(f"[SCHEMA] Hint lookup failed, continuing without it: {e}")

        return {
            "error": result["error"],
            "error_history": hist,
            "report_data": None,
            "schema_hint": schema_hint,
        }


def check_status_node(state: AgentState):
    if state.get("error"):
        print("---ERROR FOUND, RETRYING---")
        if state.get("attempts", 0) >= MAX_RETRIES:
            print("---MAX ATTEMPTS REACHED, STOPPING---")
            return END
        return "retry_generate"
    else:
        print("---SUCCESS---")
        return "generate_chart"


def generate_chart_node(state: AgentState):
    print("---CHECKING FOR CHART DATA---")
    data_str = state.get("query_result", "[]")
    chart_b64 = None

    try:
        data = ast.literal_eval(data_str)
        if len(data) > 1 and "Label" in data[0] and "Value" in data[0]:
            print("---GENERATING CHART---")
            labels = [d["Label"] for d in data]
            values = [d["Value"] for d in data]

            plt.figure(figsize=(10, 5))

            if len(data) <= 6:
                plt.pie(values, labels=labels, autopct="%1.1f%%", startangle=90)
                plt.axis("equal")
                plt.title("Library Data Distribution")
            else:
                plt.plot(labels, values, marker="o", linestyle="-", color="b")
                plt.fill_between(labels, values, color="skyblue", alpha=0.4)
                plt.title("Library Data Trend")
                plt.xlabel("Time Period / Category")
                plt.ylabel("Count / Value")
                plt.xticks(rotation=45)
                plt.grid(True, linestyle="--", alpha=0.7)
                plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format="png")
            plt.close()
            buf.seek(0)
            chart_b64 = base64.b64encode(buf.read()).decode("utf-8")

    except Exception as e:
        print(f"Chart generation skipped: {e}")

    return {"chart_base64": chart_b64}


def build_generate_answer_messages(state: AgentState) -> List[Any]:
    chart_note = ""
    if state.get("chart_base64"):
        chart_note = (
            "A chart has been generated and attached for this data. "
            "Mention this in your response."
        )

    row_count_note = ""
    report_data_list = state.get("report_data") or []
    if isinstance(report_data_list, list):
        row_count = len(report_data_list)
        if row_count > 0:
            row_count_note = (
                f"\nIMPORTANT: The SQL query returned exactly {row_count} rows. "
                f"If the user asks 'how many' or for a count, you MUST use this "
                f"exact number ({row_count}) and do not guess or count manually."
            )

    active_question = state.get("resolved_question") or state["question"]

    return [
        SystemMessage(
            content=f"""You are a helpful, professional library assistant. Based on the user's question and the SQL query result, give a clear, highly accurate, and perfectly formatted answer.

LANGUAGE & FORMATTING RULES:
1. CRITICAL: DO NOT mention or hallucinate about downloading files, Excel copies, PDF copies, or "full report on file". The system handles file downloads automatically. You just format the data provided. If you mention files, it is a severe error.
2. Reply in the EXACT SAME language the user used (Hindi, English, or Hinglish).
3. If the result contains multiple rows (e.g., multiple members with the same name, or multiple books), ALWAYS format it as a clean Markdown Table. DO NOT write it as a paragraph.
4. If the user asks for member details AND issue history, present the details in a table, and if history is available, present it in a separate table. If history is empty, say "This member has not issued any books."
5. If the result is a count or aggregate, write a clear, concise sentence.
6. If the result is empty (e.g., []), politely say "No records found matching your query." Do not invent data.
7. For dates, format as DD-MM-YYYY.
8. For money, prefix with Rs. or ₹.
9. Do not include raw JSON or Python dictionary syntax in the output. Parse it and present it beautifully.
10. TABLE SERIAL NUMBERING:
   Whenever you format the result as a Markdown table with multiple rows, ALWAYS add a first column named "S.No.".
   - Start from 1.
   - Number every displayed row sequentially: 1, 2, 3, 4, 5, ...
   - If the table contains 64 rows, show S.No. from 1 to 64.
   - Never skip or duplicate a serial number.
   - S.No. must be the FIRST column.
   - S.No. is only for display and must not come from the SQL result.
   - Do not change, remove, or invent any database values.
   - If the result has only a single value/count and is not a table, do NOT add S.No.
11. SECURITY CONTEXT (CRITICAL): The user might ask for someone else's data (e.g., "Unnati ka data do"), but the SQL result will ONLY contain the logged-in user's data due to strict backend security. If the user asks for another person's name, but the database returns a different name (e.g., Somesh), you MUST explicitly tell the user: "Aap sirf apna data dekh sakte hain, kisi aur ka nahi." Then, present the logged-in user's data in the table using the exact name from the database. Do NOT use the name from the user's question in the table.
{chart_note}""",  # noqa: E501
        ),
        HumanMessage(
            content=(
                f"User Question (as originally typed): {state['question']}\n\n"
                f"Resolved standalone question used for SQL: {active_question}\n\n"
                f"Executed SQL:\n{state.get('sql_query', '')}\n\n"
                f"SQL Result Data:\n{state['query_result']}\n\n"
                f"{row_count_note}\n\n"
                f"Format this into a readable response."
            )
        ),
    ]


def generate_answer_node(state: AgentState):
    print("---GENERATING FINAL ANSWER---")

    if state.get("error") and not state.get("report_data"):
        return {
            "query_result": "Sorry, I couldn't fetch the data due to a database error."
        }

    data = state.get("report_data", [])
    question = state["resolved_question"]

    if not data:
        return {"query_result": "No data found for your query."}

    import pandas as pd

    df = pd.DataFrame(data)
    data_str = df.to_string(index=False)

    ans_prompt = f"""User question: {question}

    SQL Result Data:
    {data_str}

    Based on the above data, answer the user's question in a short, natural, conversational sentence in Hindi/Hinglish.
    CRITICAL RULES FOR VOICE OUTPUT (TTS):
    1. Output ONLY plain text that can be spoken by a Text-to-Speech engine.
    2. NEVER use markdown tables, asterisks (*), hash (#), or pipe (|) characters.
    3. If there are multiple rows, summarize them naturally (e.g., "The books are A, B, and C.").
    4. If it is a single name or detail, just speak it directly (e.g., "आपका नाम अनुभव वर्मा है।").
    5. Do not say "Here is the data" or "Based on the result". Just answer the question directly.
    """
    try:
        response = llm_answer.invoke(
            [HumanMessage(content=ans_prompt)],
            config={"metadata": {"node_name": "generate_answer"}},
        )
        answer = response.content.strip()
        answer = (
            answer.replace("|", "")
            .replace("---", "")
            .replace("*", "")
            .replace("#", "")
        )
        return {"query_result": answer}
    except Exception as e:
        print(f"[ANSWER GEN] Error: {e}")
        items = [", ".join([str(v) for v in row.values()]) for row in data]
        return {"query_result": " ".join(items)}


def update_memory_node(state: AgentState):
    print("---UPDATING CONVERSATION MEMORY---")
    history: List[ChatTurn] = state.get("chat_history", []) or []
    history = history + [
        {
            "question": state["question"],
            "answer": state.get("query_result", ""),
        }
    ]
    if len(history) > MAX_HISTORY_TURNS:
        history = history[-MAX_HISTORY_TURNS:]
    return {"chat_history": history}


# ── Full graph (for /ask) ─────────────────────────────────────────────────

workflow = StateGraph(AgentState)
workflow.add_node("correct_asr_errors", correct_asr_errors_node)
workflow.add_node("resolve_followup", resolve_followup_node)
workflow.add_node("generate_sql", generate_sql_node)
workflow.add_node("execute_sql", execute_sql_node)
workflow.add_node("generate_chart", generate_chart_node)
workflow.add_node("generate_answer", generate_answer_node)
workflow.add_node("update_memory", update_memory_node)

workflow.set_entry_point("correct_asr_errors")
workflow.add_edge("correct_asr_errors", "resolve_followup")
workflow.add_edge("resolve_followup", "generate_sql")
workflow.add_edge("generate_sql", "execute_sql")
workflow.add_conditional_edges(
    "execute_sql",
    check_status_node,
    {
        "retry_generate": "generate_sql",
        END: END,
        "generate_chart": "generate_chart",
    },
)
workflow.add_edge("generate_chart", "generate_answer")
workflow.add_edge("generate_answer", "update_memory")
workflow.add_edge("update_memory", END)

library_agent = workflow.compile(checkpointer=checkpointer)

# ── Partial graph (for /ask/v2 streaming) ─────────────────────────────────
# /ask/v2 serves TYPED text, so there are no STT phonetic errors to correct —
# we skip the ASR node entirely and start straight at follow-up resolution.

stream_workflow = StateGraph(AgentState)
stream_workflow.add_node("resolve_followup", resolve_followup_node)
stream_workflow.add_node("generate_sql", generate_sql_node)
stream_workflow.add_node("execute_sql", execute_sql_node)
stream_workflow.add_node("generate_chart", generate_chart_node)

stream_workflow.set_entry_point("resolve_followup")
stream_workflow.add_edge("resolve_followup", "generate_sql")
stream_workflow.add_edge("generate_sql", "execute_sql")
stream_workflow.add_conditional_edges(
    "execute_sql",
    check_status_node,
    {
        "retry_generate": "generate_sql",
        END: END,
        "generate_chart": "generate_chart",
    },
)
stream_workflow.add_edge("generate_chart", END)

library_agent_stream = stream_workflow.compile(checkpointer=checkpointer)
