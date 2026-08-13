import os
import time
import re
import ast
import io
import base64
import uuid
import json
import threading
from datetime import datetime, timedelta, timezone
from typing import TypedDict, Dict, Any, Optional, List
from langgraph.graph import StateGraph, END
from langchain_groq import ChatGroq
from langchain_core.messages import SystemMessage, HumanMessage
import pyodbc
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
# Matplotlib for Chart Generation
import matplotlib
matplotlib.use('Agg')  # Backend for server
import matplotlib.pyplot as plt
import tempfile
from langgraph.checkpoint.memory import MemorySaver

# === REPORT DOWNLOAD ===
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.units import inch

checkpointer = MemorySaver()

# ==========================================
# LIVEKIT VOICE AGENT (imported lazily-safe helpers)
# ==========================================
from livekit_agent.config import settings as lk_settings
from livekit_agent.tokens import create_join_token, dispatch_agent, generate_room_name

# ==========================================
# 1. CONFIGURATION & ENVIRONMENT
# ==========================================
# NOTE: Move these to real environment variables / a secrets manager before
# deploying. Hardcoded DB + API credentials in source is a security risk,
# especially since this file will end up in git history / logs.
os.environ["GROQ_API_KEY"] = os.environ.get("GROQ_API_KEY", "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")

CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=160.25.62.109,1433;"
    "Database=SOUL30;"
    "UID=sa;"
    "PWD=msspl@123;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=30;"
)

MAX_ROWS = 100
MAX_RETRIES = 5
MAX_HISTORY_TURNS = 6          # how many past Q/A turns to keep in memory
RESOLVER_CONTEXT_TURNS = 4     # how many of those to actually show the resolver
_conn = None

# ==========================================
# REPORT STORE (In-Memory with TTL)
# ==========================================
REPORT_STORE: Dict[str, Dict[str, Any]] = {}
REPORT_TTL_SECONDS = 600  # 10 minutes+
REPORT_LOCK = threading.Lock()


def cleanup_expired_reports():
    now = time.time()
    expired = [
        rid for rid, rdata in REPORT_STORE.items()
        if now - rdata["created_at"] > REPORT_TTL_SECONDS
    ]
    for rid in expired:
        REPORT_STORE.pop(rid, None)
    if expired:
        print(f"[REPORT_STORE] Cleaned up {len(expired)} expired reports.")


def store_report(data: List[Dict], question: str, sql: str) -> str:
    cleanup_expired_reports()
    report_id = str(uuid.uuid4())
    with REPORT_LOCK:
        REPORT_STORE[report_id] = {
            "data": data,
            "question": question,
            "sql": sql,
            "created_at": time.time(),
        }
    print(f"[REPORT_STORE] Stored report_id={report_id} with {len(data)} rows.")
    return report_id


def get_report(report_id: str) -> Optional[Dict[str, Any]]:
    with REPORT_LOCK:
        rdata = REPORT_STORE.get(report_id)
        if not rdata:
            return None
        if time.time() - rdata["created_at"] > REPORT_TTL_SECONDS:
            REPORT_STORE.pop(report_id, None)
            return None
        return rdata


# ==========================================
# 2. DATABASE CONNECTION & QUERY EXECUTION
# ==========================================
BACKUP_TABLE_PATTERNS = [
    r"_backup", r"_temp", r"weedout", r"before_restore", r"_bak\b",
    r"WO_\d+_Backup", r"NewWeedout", r"WeedoutBooks", r"_copy$"
]

def is_backup_table(name: str) -> bool:
    lname = name.lower()
    for pat in BACKUP_TABLE_PATTERNS:
        if re.search(pat, lname):
            return True
    return False

def create_connection():
    return pyodbc.connect(CONNECTION_STRING)

def get_connection():
    global _conn
    try:
        if _conn is None:
            print("Creating new DB connection...")
            _conn = create_connection()
        else:
            cursor = _conn.cursor()
            cursor.execute("SELECT 1")
    except Exception:
        print("Reconnecting DB...")
        try:
            if _conn: _conn.close()
        except: pass
        _conn = create_connection()
    return _conn

def validate_query(query: str):
    if not query or not query.strip():
        raise ValueError("Empty SQL")

    forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER",
                 "TRUNCATE", "MERGE", "GRANT", "REVOKE", "EXEC", "EXECUTE"]
    for word in forbidden:
        if re.search(rf"\b{word}\b", query, re.IGNORECASE):
            raise ValueError(f"Unsafe query detected: {word}")

    stripped = query.strip().lower()
    if not (stripped.startswith("select") or stripped.startswith("with")):
        raise ValueError("Only SELECT/WITH allowed")

    for pat in BACKUP_TABLE_PATTERNS:
        if re.search(pat, query, re.IGNORECASE):
            raise ValueError(f"Query references backup/temp table (pattern: {pat}). Use only live tables.")

def execute_query(query, limit=True):
    global _conn
    for attempt in range(MAX_RETRIES):
        cursor = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            time.sleep(0.1)
            cursor.execute(query)

            if not cursor.description:
                return []

            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            results = [{col: row[i] for i, col in enumerate(columns)} for row in rows]

            if limit and MAX_ROWS and len(results) > MAX_ROWS:
                results = results[:MAX_ROWS]
            return results

        except Exception as e:
            error_msg = str(e)
            print(f"DB Error (attempt {attempt+1}): {error_msg}")
            if "08S01" in error_msg or "Communication link failure" in error_msg or "HY000" in error_msg:
                try:
                    if _conn: _conn.close()
                except: pass
                _conn = None
                time.sleep(1)
                continue
            raise e
        finally:
            try:
                if cursor: cursor.close()
            except: pass
    raise Exception("Database failed after retries")

def run_sql(query, limit=True):
    try:
        validate_query(query)
        results = execute_query(query, limit=limit)
        return {"success": True, "rows": len(results), "data": results}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ==========================================
# 2b. LIVE SCHEMA LOOKUP (fixes hallucinated column/table names on retry)
# ==========================================
# Without this, when the LLM writes a column that doesn't exist (e.g.
# 'mem_fathername', 'mem_joindt'), all it gets back is "Invalid column name X"
# with no hint of what the REAL column is called — so on retry it just
# guesses a different wrong name, over and over, until MAX_RETRIES is
# exhausted. These helpers fetch the actual schema from SQL Server itself so
# the retry prompt can give the model ground truth instead of a blank error.
SCHEMA_CACHE: Dict[str, List[str]] = {}
SCHEMA_CACHE_LOCK = threading.Lock()

_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

def extract_table_names(sql: str) -> List[str]:
    """Crude but effective: grab identifiers that follow FROM / JOIN."""
    raw = re.findall(r'\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_\.]*)', sql, re.IGNORECASE)
    names = []
    for r in raw:
        name = r.split('.')[-1]  # strip schema prefix like dbo.
        if _IDENTIFIER_RE.match(name) and name not in names:
            names.append(name)
    return names

def get_table_columns(table_name: str) -> List[str]:
    """Live INFORMATION_SCHEMA lookup, cached in-process."""
    if not _IDENTIFIER_RE.match(table_name):
        return []
    with SCHEMA_CACHE_LOCK:
        cached = SCHEMA_CACHE.get(table_name)
    if cached is not None:
        return cached
    try:
        query = (
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_NAME = '{table_name}' ORDER BY ORDINAL_POSITION"
        )
        results = execute_query(query, limit=False)
        cols = [r["COLUMN_NAME"] for r in results]
        with SCHEMA_CACHE_LOCK:
            SCHEMA_CACHE[table_name] = cols
        return cols
    except Exception as e:
        print(f"[SCHEMA] Could not fetch columns for {table_name}: {e}")
        return []

def find_similar_table_names(fragment: str) -> List[str]:
    """Used when the bad identifier is a TABLE/VIEW name, not a column."""
    safe_fragment = re.sub(r'[^A-Za-z0-9_]', '', fragment)[:60]
    if not safe_fragment:
        return []
    try:
        query = (
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_NAME LIKE '%{safe_fragment}%'"
        )
        results = execute_query(query, limit=False)
        return [r["TABLE_NAME"] for r in results][:10]
    except Exception as e:
        print(f"[SCHEMA] Could not search similar table names: {e}")
        return []

def build_schema_error_hint(sql: str, error_msg: str) -> str:
    """
    Given a failed SQL query and the SQL Server error it produced, build a
    short ground-truth block to feed back into the retry prompt: real column
    lists for the tables actually used, and/or close table-name matches if
    the failure was an unknown table/view.
    """
    hints = []
    lower_err = error_msg.lower()

    if "invalid column name" in lower_err:
        for t in extract_table_names(sql):
            cols = get_table_columns(t)
            if cols:
                hints.append(f"{t}: {', '.join(cols)}")

    obj_match = re.search(r"Invalid object name '([^']+)'", error_msg, re.IGNORECASE)
    if obj_match:
        bad_name = obj_match.group(1).split('.')[-1]
        matches = find_similar_table_names(bad_name)
        if matches:
            hints.append(f"No table/view called '{bad_name}'. Closest real names: {', '.join(matches)}")

    return "\n".join(hints)


# ==========================================
# 3. THE SUPER-OPTIMIZED SYSTEM PROMPT
# ==========================================
SYSTEM_PROMPT = """You are an expert T-SQL Developer for SOUL 3.0 Library Management System. Convert natural language questions into accurate T-SQL queries.

==============================
SECTION 1: ARCHITECTURE — MARC 21 FORMAT
==============================
Book metadata (Title, Author, etc.) is stored as ROWS in `Biblidetails`, NOT columns. Use multiple self-LEFT JOINs to retrieve multiple fields.

MARC TAG DICTIONARY (Exact for this DB):
- 245 / a  → Title (Book/Journal Name)
- 100 / a  → Author
- 260 / a  → Publication Place
- 260 / b  → Publisher
- 260 / c  → Publication Year
- 250 / a  → Edition
- 300 / a  → Pages
- 022 / a  → ISSN
- 082 / a  → DDC / Classification Number
- 653 / a  → Subject
- 310 / a  → Frequency (e.g., Quarterly, Monthly)
- 210 / a  → Short Title
- 222 / a  → Main Title

==============================
SECTION 2: CRITICAL DATA TYPES & JOIN RULES (MUST FOLLOW)
==============================
- `t_issue.acc_no`, `t_receive.accn_no`, `t_book_transfer.acc_no`, `t_replace.old_accco` are **char** with TRAILING SPACES (e.g., '121613     ').
- `Location.p852` is **nvarchar** WITHOUT trailing spaces.
- `m_member.mem_cd` is `char` WITH TRAILING SPACES.

🚨 MANDATORY JOIN RULE: ALWAYS use `RTRIM()` when joining ANY of these char columns.
  ✅ CORRECT: `JOIN Location L ON L.p852 = RTRIM(t.acc_no)`
  ✅ CORRECT: `JOIN Location L ON L.p852 = RTRIM(r.accn_no)`
  ❌ WRONG:   `JOIN Location L ON L.p852 = t.acc_no`

==============================
SECTION 3: EXACT STATUS / ENUM CODES (CRITICAL FOR FILTERS)
==============================
- `Location.Status` = **'AV'** (Available). Use `WHERE L.Status = 'AV'` for available books.
- `m_member.mem_status` = **'A'** (Active).
- `t_book_transfer.Status` = **'T'** (Transferred), **'R'** (Returned)
- `t_receive.recv_status` = **EMPTY STRING ' ' or NULL**.
  🚨 CRITICAL: DO NOT use `WHERE recv_status = 'R'`.
  To check if a book is returned, check if it EXISTS in `t_receive`:
  `EXISTS (SELECT 1 FROM t_receive r WHERE r.accn_no = t.acc_no AND r.mem_cd = t.mem_cd AND r.iss_dt = t.iss_dt)`

==============================
SECTION 4: PREFERRED SHORTCUT VIEWS
==============================
USE these views for complex queries instead of writing 5+ JOINs:
1. **v_BookDetails** — `AccNo, ClassNo, Title, Author, Location, LocId, Department, Status`
2. **VMember** — `mem_cd, mem_firstnm, mem_lstnm, mem_email, mem_prmntphone, mem_dept, Fclty_dept_dscr (Dept Name), mem_status, mem_ctgry, ctgry_desc, Branch_name`
3. **VCmpltIssueDetails** — `mem_cd, acc_no, iss_dt, due_dt, FValue (Title), mem_firstnm, mem_lstnm, mem_dept`
4. **vlocation** — `recID, p852, status, lastoperateddt`

==============================
SECTION 5: SCHEMA MAP & EXACT RELATIONSHIPS (VERIFIED)
==============================
**m_member**: mem_cd, mem_firstnm, mem_lstnm, mem_status, mem_dept, mem_email
**t_issue**: mem_cd, acc_no, iss_dt, due_dt
**t_receive**: mem_cd, accn_no, iss_dt, recv_dt
**t_reserve**: mem_cd, record_no, resv_dt, hold_dt
**t_bookbankissue**: mem_cd, acc_no, issue_dt, due_dt
**t_bookbankreturn**: mem_cd, acc_no, issue_date, return_date
**t_memfine**: mem_cd, accn_no, fine_amt, slip_dt, fine_desc
**t_replace**: old_accno, mem_cd, title, author
**Location**: RecID, p852 (Accession No), Status ('AV'), DateofAcq
**Biblidetails**: RecID, Tag ('245'=Title, '100'=Author, '653'=Subject), SbFld ('a'), FValue

MANDATORY JOIN RULES (Based on DB Architecture):
1. Member to ANY table (Issue/Receive/Fine/Reserve):
   `JOIN m_member M ON M.mem_cd = RTRIM(t.mem_cd)`
2. Accession No (acc_no / accn_no) to Location:
   `JOIN Location L ON L.p852 = RTRIM(t.acc_no)`  -- (DO NOT join acc_no to RecID)
3. Location to Book Catalog (Metadata):
   `JOIN Biblidetails B ON B.RecID = L.RecID`
4. Reservation to Book Catalog (DIRECT, No Location needed):
   `JOIN Biblidetails B ON B.RecID = t_reserve.record_no`
5. Overdue Books (Issued but NOT in Receive):
   `WHERE t.due_dt < GETDATE() AND NOT EXISTS (SELECT 1 FROM t_receive r WHERE r.accn_no = t.acc_no AND r.mem_cd = t.mem_cd AND r.iss_dt = t.iss_dt)`
6. FULL ISSUE/RETURN HISTORY (For a specific book):
   To get the complete history of a book (both currently issued and returned), ALWAYS start from `t_issue` and LEFT JOIN `t_receive`. NEVER query only `t_receive` for history, otherwise currently issued books will be missed.
   `FROM t_issue t LEFT JOIN t_receive r ON RTRIM(r.accn_no) = RTRIM(t.acc_no) AND r.mem_cd = t.mem_cd AND r.iss_dt = t.iss_dt`

==============================
SECTION 6: STRICT RULES
==============================
1. Output ONLY valid T-SQL. No markdown fences (```), no comments, no explanations.
2. ALWAYS use `RTRIM()` for char↔nvarchar joins.
3. Start book-search queries `FROM Biblidetails` and LEFT JOIN Location.
4. Use `LIKE '%...%'` for text search. NEVER use exact `=`.
5. Use `COUNT(DISTINCT RecID)` for counting books.
6. ALWAYS alias selected columns uniquely with `AS`.
7. NEVER reference backup tables (`_backup`, `weedout`, `_temp`).
8. For "Top N books", use `SELECT DISTINCT` or `GROUP BY`.
9. COUNTING ISSUES: When counting how many times a book was issued, DO NOT join `Location` or `Biblidetails` if not strictly necessary, as it may duplicate rows. Just count from `t_issue`.
   ✅ CORRECT: `SELECT TOP 10 acc_no, COUNT(*) FROM t_issue GROUP BY acc_no`
10. MEMBER NAME SEARCH LOGIC:
    - If user provides ONLY FIRST NAME: `WHERE mem_firstnm LIKE '%UNNATI%' OR mem_lstnm LIKE '%UNNATI%'`
    - If user provides FULL NAME (e.g., "UNNATI SINGH"): ALWAYS use AND condition.
      `WHERE mem_firstnm LIKE '%UNNATI%' AND mem_lstnm LIKE '%SINGH%'`
11. TOTAL LIBRARY SIZE: If user asks for "total books in library", use `SELECT COUNT(*) FROM Location`.
12. AGGREGATE vs DETAIL: If user asks "how many" or "total", return a single scalar number using a subquery.
13. SINGLE QUERY RULE: NEVER write multiple SELECT statements in one response.
    ALWAYS combine them into a SINGLE query using `LEFT JOIN` or `UNION ALL`.
14. DAILY/MONTHLY TRENDS (ISSUED vs RETURNED):
    If user asks for "daily issued return count", DO NOT JOIN `t_issue` and `t_receive` directly (it causes duplicates and bad counts).
    Instead, query them separately using `UNION ALL` and then group by date.
    Example:
    `SELECT Date, SUM(Issued) AS Issued, SUM(Returned) AS Returned FROM (`
    `  SELECT CAST(iss_dt AS DATE) AS Date, COUNT(*) AS Issued, 0 AS Returned FROM t_issue WHERE iss_dt >= '2024-01-01' GROUP BY CAST(iss_dt AS DATE)`
    `  UNION ALL`
    `  SELECT CAST(recv_dt AS DATE) AS Date, 0 AS Issued, COUNT(*) AS Returned FROM t_receive WHERE recv_dt >= '2024-01-01' GROUP BY CAST(recv_dt AS DATE)`
    `) sub GROUP BY Date ORDER BY Date`
15. COMPLETE DAILY CIRCULATION FLOW (List of transactions):
    If user asks for "poora circulation flow", "all transactions of a day", or "us din ka poora data", DO NOT just query `t_issue`.
    You MUST fetch BOTH issues and returns for that date using `UNION ALL`.
    Add a `Transaction_Type` column to differentiate ('Issue' vs 'Return').
    IMPORTANT: Keep the exact Date and Time in the `Transaction_Date` column. Do NOT use CAST(... AS DATE).
16. EXACT ID LOOKUPS (CRITICAL):
    If user provides an exact Member ID (e.g., 'DPDCDC160001') or Accession Number to search, ALWAYS use RTRIM() or LIKE '%' to handle trailing spaces in the `char` columns.
    ✅ CORRECT: `WHERE RTRIM(mem_cd) = 'DPDCDC160001'`
    ✅ CORRECT: `WHERE mem_cd LIKE 'DPDCDC160001%'`
    ❌ WRONG: `WHERE mem_cd = 'DPDCDC160001'`

==============================
SECTION 7: COMPLEX QUERY EXAMPLES
==============================

--- Example A: Find available books by title ---
SELECT
    B.FValue AS Title,
    A.FValue AS Author,
    L.p852 AS Accession_No
FROM Biblidetails B
LEFT JOIN Biblidetails A ON A.RecID = B.RecID AND A.Tag = '100' AND A.SbFld = 'a'
LEFT JOIN Location L ON L.RecID = B.RecID
WHERE B.Tag = '245' AND B.SbFld = 'a'
  AND B.FValue LIKE '%data structure%'
  AND L.Status = 'AV';

--- Example B: COMPLETE Issue/Return History of a Book (INCLUDING CURRENTLY ISSUED) ---
SELECT
    t.iss_dt AS Issue_Date,
    t.due_dt AS Due_Date,
    r.recv_dt AS Return_Date,
    M.mem_firstnm + ' ' + M.mem_lstnm AS Member_Name
FROM t_issue t
LEFT JOIN t_receive r ON RTRIM(r.accn_no) = RTRIM(t.acc_no) AND r.mem_cd = t.mem_cd AND r.iss_dt = t.iss_dt
JOIN m_member M ON M.mem_cd = RTRIM(t.mem_cd)
WHERE RTRIM(t.acc_no) = '150315'
ORDER BY t.iss_dt DESC;

--- Example C: Active members from a specific department ---
SELECT
    M.mem_cd,
    M.mem_firstnm + ' ' + M.mem_lstnm AS Member_Name
FROM VMember M
WHERE M.mem_status = 'A'
  AND M.Fclty_dept_dscr LIKE '%Computer%';

--- Example D: Top 10 Most Issued Books (WITHOUT DUPLICATES) ---
SELECT TOP 10
    t.acc_no AS Accession_No,
    COUNT(*) AS Issue_Count
FROM t_issue t
GROUP BY t.acc_no
ORDER BY Issue_Count DESC;

--- Example E: Daily Issue and Return Count for a Month ---
SELECT
    Date,
    SUM(Issued) AS Issued_Books,
    SUM(Returned) AS Returned_Books
FROM (
    SELECT CAST(iss_dt AS DATE) AS Date, COUNT(*) AS Issued, 0 AS Returned
    FROM t_issue
    WHERE iss_dt >= '2026-05-01' AND iss_dt < '2026-06-01'
    GROUP BY CAST(iss_dt AS DATE)
    UNION ALL
    SELECT CAST(recv_dt AS DATE) AS Date, 0 AS Issued, COUNT(*) AS Returned
    FROM t_receive
    WHERE recv_dt >= '2026-05-01' AND recv_dt < '2026-06-01'
    GROUP BY CAST(recv_dt AS DATE)
) sub
GROUP BY Date
ORDER BY Date;

--- Example F: COMPLETE DAILY CIRCULATION FLOW (Both Issues AND Returns with EXACT DATE & TIME) ---
SELECT
    Transaction_Date,
    Transaction_Type,
    Member_Name,
    Book_Title,
    Author
FROM (
    -- 1. Books Issued on that date (with time)
    SELECT
        t.iss_dt AS Transaction_Date,
        'Issue' AS Transaction_Type,
        M.mem_firstnm + ' ' + M.mem_lstnm AS Member_Name,
        B.FValue AS Book_Title,
        A.FValue AS Author
    FROM t_issue t
    JOIN m_member M ON M.mem_cd = RTRIM(t.mem_cd)
    JOIN Location L ON L.p852 = RTRIM(t.acc_no)
    JOIN Biblidetails B ON B.RecID = L.RecID AND B.Tag = '245' AND B.SbFld = 'a'
    LEFT JOIN Biblidetails A ON A.RecID = L.RecID AND A.Tag = '100' AND A.SbFld = 'a'
    WHERE CAST(t.iss_dt AS DATE) = '2025-05-05'

    UNION ALL

    -- 2. Books Returned on that date (with time)
    SELECT
        r.recv_dt AS Transaction_Date,
        'Return' AS Transaction_Type,
        M.mem_firstnm + ' ' + M.mem_lstnm AS Member_Name,
        B.FValue AS Book_Title,
        A.FValue AS Author
    FROM t_receive r
    JOIN m_member M ON M.mem_cd = RTRIM(r.mem_cd)
    JOIN Location L ON L.p852 = RTRIM(r.accn_no)
    JOIN Biblidetails B ON B.RecID = L.RecID AND B.Tag = '245' AND B.SbFld = 'a'
    LEFT JOIN Biblidetails A ON A.RecID = L.RecID AND A.Tag = '100' AND A.SbFld = 'a'
    WHERE CAST(r.recv_dt AS DATE) = '2025-05-05'
) sub
ORDER BY Transaction_Date, Transaction_Type;

==============================
SECTION 8: CHART GENERATION DATA FORMAT
==============================
If the user asks for a "report", "graph", "chart", "trend", or "distribution" (e.g., department-wise, subject-wise, monthly), you MUST generate an SQL query that returns EXACTLY two columns named `Label` and `Value`.
- `Label`: The category or time period (e.g., 'Computer Science', '2024-01').
- `Value`: The numeric count or sum (e.g., 150, 5000).

CRITICAL: NEVER use aliases like `Department`, `Count`, `Active_Students`, or `Total` for report queries. ALWAYS use `Label` and `Value`.

Example SQL for Report:
`SELECT M.Fclty_dept_dscr AS Label, COUNT(*) AS Value FROM VMember M WHERE M.mem_status = 'A' GROUP BY M.Fclty_dept_dscr`

CRITICAL EXCEPTION:
If the user is just searching, listing, or asking "how many", DO NOT use `Label` and `Value` aliases. Use normal descriptive aliases like `Title`, `Total_Students`, `Department`, `Issued_Books`, `Returned_Books`.
"""

# ==========================================
# 3b. FOLLOW-UP RESOLVER PROMPT
# ==========================================
FOLLOWUP_RESOLVER_PROMPT = """You are a query-rewriting module for a Library Management System chatbot.
Your ONLY job is to look at the conversation history and the user's CURRENT question, and decide whether the
current question is a follow-up that depends on context from earlier turns (pronouns, "unke", "unka", "uska",
"usme se", "iski", "unhe", "in members ka", "list them", "and their emails", "wapas", "usne", "ismein",
"pichle wale", implicit topic continuation, etc.).

RULES:
1. If there is NO conversation history, OR the current question is already fully standalone (it names its own
   subject/entity and does not rely on anything from earlier turns), output the question EXACTLY AS-IS, with no
   changes.
2. If the current question DOES depend on earlier context, rewrite it into one fully standalone question by
   substituting in the specific entity/topic/filter from the conversation history (e.g. the department, the
   member name, the book title, the date range, the previous result set being referred to).
3. NEVER answer the question. NEVER add SQL. ONLY output the rewritten natural-language question.
4. Preserve the original language style of the CURRENT question (Hindi / English / Hinglish) — do not translate it.
5. Do not invent facts that aren't implied by the history. Keep the rewrite concise and faithful.
6. Output ONLY the rewritten question text. No quotes, no markdown, no explanation, no prefix like "Rewritten:".

CONVERSATION HISTORY (oldest to newest):
{history}

CURRENT QUESTION:
{question}

Rewritten standalone question:"""

# ==========================================
# 4. LANGGRAPH STATE & NODES
# ==========================================
class ChatTurn(TypedDict):
    question: str
    answer: str

class AgentState(TypedDict):
    question: str                       # raw question as typed by the user this turn
    resolved_question: str              # standalone version used for SQL generation
    sql_query: str
    previous_sql: str
    query_result: str
    report_data: Optional[List[Dict[str, Any]]]
    attempts: int
    error: str
    error_history: List[str]
    schema_hint: str                    # live ground-truth schema for tables in the failed query
    chart_base64: Optional[str]
    chat_history: List[ChatTurn]        # persisted across turns via the checkpointer

llm = ChatGroq(model="llama-3.3-70b-versatile", temperature=0)


def resolve_followup_node(state: AgentState):
    """
    Reads chat_history (carried forward automatically by MemorySaver for this
    thread_id) and decides whether state['question'] is a standalone question
    or a follow-up that needs to be rewritten using prior context.
    """
    print("---RESOLVING FOLLOW-UP---")
    question = state["question"]
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
        response = llm.invoke([HumanMessage(content=prompt)])
        resolved = (response.content or "").strip().strip('"').strip("'").strip()
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

    # Always generate SQL from the RESOLVED (standalone) question, not the raw one.
    active_question = state.get("resolved_question") or state["question"]

    if prev_err:
        schema_block = (
            f"ACTUAL LIVE DATABASE SCHEMA for the table(s) you used, fetched just now from "
            f"INFORMATION_SCHEMA — this is GROUND TRUTH. Use ONLY these exact names. "
            f"Do NOT invent, assume, or guess any column or table name not listed here:\n{schema_hint}\n\n"
            if schema_hint else ""
        )
        user_msg = (
            f"User Question: {active_question}\n\n"
            f"Your PREVIOUS SQL (which FAILED):\n{prev_sql}\n\n"
            f"ERROR returned by SQL Server:\n{prev_err}\n\n"
            f"All errors so far: {err_hist}\n\n"
            f"{schema_block}"
            f"Fix the SQL using the real schema above if provided. Re-think the JOINs and column names. "
            f"Remember the RTRIM and Status='AV' rules. "
            f"Output ONLY the SQL without any markdown."
        )
    else:
        user_msg = (
            f"User Question: {active_question}\n\n"
            f"Generate the T-SQL query. Output ONLY the SQL without any markdown."
        )

    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=user_msg),
    ]
    response = llm.invoke(messages)
    return {
        "sql_query": response.content,
        "previous_sql": response.content,
        "attempts": state.get("attempts", 0) + 1,
    }

def execute_sql_node(state: AgentState):
    print("---EXECUTING SQL---")
    clean_sql = state["sql_query"].replace("```sql", "").replace("```", "").strip()
    print(f"\n[DEBUG] SQL (attempt {state.get('attempts', 0)}):\n{clean_sql}\n")

    result = run_sql(clean_sql)
    if result["success"]:
        data_str = str(result["data"])
        if len(data_str) > 12000:
            data_str = data_str[:12000] + f"\n... [TRUNCATED, total rows: {result['rows']}]"
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
                plt.pie(values, labels=labels, autopct='%1.1f%%', startangle=90)
                plt.axis('equal')
                plt.title('Library Data Distribution')
            else:
                plt.plot(labels, values, marker='o', linestyle='-', color='b')
                plt.fill_between(labels, values, color='skyblue', alpha=0.4)
                plt.title('Library Data Trend')
                plt.xlabel('Time Period / Category')
                plt.ylabel('Count / Value')
                plt.xticks(rotation=45)
                plt.grid(True, linestyle='--', alpha=0.7)
                plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            plt.close()
            buf.seek(0)
            chart_b64 = base64.b64encode(buf.read()).decode('utf-8')

    except Exception as e:
        print(f"Chart generation skipped: {e}")

    return {"chart_base64": chart_b64}

def generate_answer_node(state: AgentState):
    print("---GENERATING FINAL ANSWER---")

    chart_note = ""
    if state.get("chart_base64"):
        chart_note = "A chart has been generated and attached for this data. Mention this in your response."

    row_count_note = ""
    data_str = state.get("query_result", "[]")
    try:
        actual_data = ast.literal_eval(data_str)
        if isinstance(actual_data, list):
            row_count = len(actual_data)
            row_count_note = f"\nIMPORTANT: The SQL query returned exactly {row_count} rows. If the user asks 'how many' or for a count, you MUST use this exact number ({row_count}) and do not guess or count manually."
    except Exception as e:
        print(f"Could not calculate row count for LLM: {e}")

    active_question = state.get("resolved_question") or state["question"]

    messages = [
        SystemMessage(content=f"""You are a helpful, professional library assistant. Based on the user's question and the SQL query result, give a clear, highly accurate, and perfectly formatted answer.

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
{chart_note}"""),
        HumanMessage(content=f"User Question (as originally typed): {state['question']}\n\n"
                              f"Resolved standalone question used for SQL: {active_question}\n\n"
                              f"Executed SQL:\n{state.get('sql_query','')}\n\n"
                              f"SQL Result Data:\n{state['query_result']}\n\n"
                              f"{row_count_note}\n\n"
                              f"Format this into a readable response.")
    ]
    response = llm.invoke(messages)
    return {"query_result": response.content}


def update_memory_node(state: AgentState):
    """
    Appends this turn (raw question + final formatted answer) to chat_history
    and trims it to the last MAX_HISTORY_TURNS turns. Because this state field
    is checkpointed per thread_id, it will be available to resolve_followup_node
    on the NEXT call for the same thread_id.
    """
    print("---UPDATING CONVERSATION MEMORY---")
    history: List[ChatTurn] = state.get("chat_history", []) or []
    history = history + [{
        "question": state["question"],
        "answer": state.get("query_result", ""),
    }]
    if len(history) > MAX_HISTORY_TURNS:
        history = history[-MAX_HISTORY_TURNS:]
    return {"chat_history": history}


# Compile Graph
workflow = StateGraph(AgentState)
workflow.add_node("resolve_followup", resolve_followup_node)
workflow.add_node("generate_sql", generate_sql_node)
workflow.add_node("execute_sql", execute_sql_node)
workflow.add_node("generate_chart", generate_chart_node)
workflow.add_node("generate_answer", generate_answer_node)
workflow.add_node("update_memory", update_memory_node)

workflow.set_entry_point("resolve_followup")
workflow.add_edge("resolve_followup", "generate_sql")
workflow.add_edge("generate_sql", "execute_sql")
workflow.add_conditional_edges(
    "execute_sql",
    check_status_node,
    {
        "retry_generate": "generate_sql",
        END: END,
        "generate_chart": "generate_chart"
    }
)
workflow.add_edge("generate_chart", "generate_answer")
workflow.add_edge("generate_answer", "update_memory")
workflow.add_edge("update_memory", END)

library_agent = workflow.compile(
    checkpointer=checkpointer
)

# ==========================================
# 5. FASTAPI APP SETUP
# ==========================================
app = FastAPI(
    title="SOUL30 Library Text-to-SQL API",
    version="3.1"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    question: str
    thread_id: str

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


# ==========================================
# LIVEKIT VOICE SESSION MODELS
# ==========================================
class LiveKitSessionRequest(BaseModel):
    """Request to start a voice chat session with the LiveKit agent.

    The backend generates the room + join token and (optionally) dispatches the
    agent, so the client only has to connect with ``url`` + ``token``.
    """
    room: Optional[str] = None          # optional; a fresh room is created if omitted
    identity: Optional[str] = None      # optional; defaults to a generated identity
    user_id: Optional[str] = None       # passed to the agent as dispatch metadata
    thread_id: Optional[str] = None     # conversation memory key for follow-ups
    metadata: Optional[Dict[str, Any]] = None  # extra metadata forwarded to the agent
    dispatch: bool = True               # spawn the agent for this session


class LiveKitDispatchRequest(BaseModel):
    """Explicitly dispatch the agent into an existing room via the server API."""
    room: str
    user_id: Optional[str] = None
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
    dispatch: str  # "token" | "api" | "none"


class LiveKitDispatchResponse(BaseModel):
    room: str
    agent_name: str
    dispatch_id: Optional[str] = None
    dispatch: str  # "api" | "token" | "none"

@app.get("/welcome")
def welcome_api():
    return {
        "message": "Welcome to the SOUL 3.0 Library AI Assistant! Here are 10 complex questions you can ask to test the system:",
        "sample_questions": [
            "Library me total kitni physical books hain aur usme se kitni unique titles (book names) hain? Sirf count batao.",
            "Computer science subject par 5 available books ke title aur author dikhao.",
            "Aise 10 members dikhao jinki books overdue (late) hain, yani abhi tak wapas nahi aayi aur due date nikal gayi. Member ka naam, book title aur due date do.",
            "UNNATI SINGH naam ki member ki poori details (ID, Department, Email, Phone) do aur usne kaun si book issue ki hai uski history bhi dikhao.",
            "Library me total kitni aisi books hain jinka subject 'Physics' hai? Sirf total count batao, list mat dena.",
            "Computer Centre department me kitne active students hain? Unke naam aur member ID list karo.",
            "Member ID 'UGCABC210047' ne aaj tak kaun-kaun si books issue ki hain? Book title aur issue date ke sath table banao.",
            "Aise 5 books dikhao jo pichle kuchh mahine me wapas return kar di gayi thi. Book title, member ID aur return date do.",
            "Management subject par kitni books hain aur library me total kitni books hain? Dono ka exact count ek hi answer me do.",
            "Pichle 1 saal me sabse zyada kitni baar issue ki gayi 3 books kaun si hain? Unka title aur total issue count batao."
        ]
    }

@app.get("/")
def read_root():
    return {"status": "SOUL30 Library Text-to-SQL API v3.1 (with follow-up memory) is running perfectly!"}

@app.post("/ask", response_model=QueryResponse)
def ask_library_agent(request: QueryRequest):
    user_question = request.question
    print(f"\n[API] User Question: {user_question}\n")

    # IMPORTANT: we intentionally do NOT pass "chat_history" here.
    # LangGraph's checkpointer will merge this input with the last checkpoint
    # for this thread_id, so any existing chat_history carries forward
    # automatically. Only on a brand-new thread_id will it start as [].
    final_state = library_agent.invoke(
        {
            "question": user_question,
            "resolved_question": "",
            "attempts": 0,
            "error": "",
            "previous_sql": "",
            "error_history": [],
            "schema_hint": "",
            "chart_base64": None,
            "report_data": None,
        },
        config={
            "configurable": {
                "thread_id": request.thread_id
            }
        }
    )

    final_answer = final_state.get("query_result", "Agent failed to generate answer.")
    executed_sql = (final_state.get("sql_query", "")
                    .replace("```sql", "").replace("```", "").strip())
    db_error = final_state.get("error", None) or None
    chart_b64 = final_state.get("chart_base64", None)
    resolved_q = final_state.get("resolved_question", "") or user_question

    report_available = False
    report_id = None
    report_data = final_state.get("report_data", None)
    if report_data and isinstance(report_data, list) and len(report_data) > 0:
        try:
            report_id = store_report(
                data=report_data,
                question=user_question,
                sql=executed_sql,
            )
            report_available = True
        except Exception as e:
            print(f"[REPORT_STORE] Failed to store report: {e}")
            report_available = False

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


@app.post("/reset-memory/{thread_id}")
def reset_memory(thread_id: str):
    """
    Clears conversation memory for a given thread_id by writing an empty
    chat_history into the checkpoint. Call this when the user starts a
    brand-new chat session in the frontend, so old context doesn't leak in.
    """
    try:
        library_agent.update_state(
            config={"configurable": {"thread_id": thread_id}},
            values={"chat_history": []},
        )
        return {"status": "ok", "detail": f"Memory cleared for thread_id={thread_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not reset memory: {e}")


# ==========================================
# 5b. LIVEKIT VOICE AGENT ENDPOINTS
# ==========================================
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
    expires = datetime.now(timezone.utc) + timedelta(seconds=lk_settings.token_ttl_seconds)
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


@app.get("/api/livekit/config")
def livekit_config():
    """Public, safe client config (no secrets)."""
    return {
        "url": lk_settings.livekit_url,
        "agent_name": lk_settings.agent_name,
        "token_ttl_seconds": lk_settings.token_ttl_seconds,
        "tts_language": lk_settings.tts_language,
    }


@app.post("/api/livekit/session", response_model=LiveKitResponse)
def livekit_session(request: LiveKitSessionRequest):
    """
    The main entry point for the web client.

    Generates a fresh room (unless one is provided), issues a join token that
    carries the agent dispatch, and spawns the LiveKit agent. The client then
    connects to ``url`` with ``token`` and the voice conversation begins.

    Use a fresh session (no room) per voice chat so the agent is dispatched on
    room creation. Pass ``thread_id`` to keep follow-up memory in the library
    text-to-SQL backend, and ``user_id`` for analytics.
    """
    room = request.room or generate_room_name("soul")
    identity = request.identity or f"user-{uuid.uuid4().hex[:10]}"

    dispatch_metadata = dict(request.metadata or {})
    if request.user_id:
        dispatch_metadata["user_id"] = request.user_id
    if request.thread_id:
        dispatch_metadata["thread_id"] = request.thread_id
    dispatch_metadata.setdefault("session_id", uuid.uuid4().hex[:16])

    dispatch_method = "none"
    if request.dispatch:
        try:
            dispatch_metadata["room"] = room
            dispatch_metadata["identity"] = identity
            # The token's room config dispatches the agent the instant the
            # client connects and creates the room.
            dispatch_method = "token"
            return _build_session_response(room, identity, dispatch_method, dispatch_metadata)
        except Exception as e:
            print(f"[LIVEKIT] Could not attach token dispatch: {e}")
            raise HTTPException(status_code=500, detail=f"LiveKit dispatch failed: {e}")

    return _build_session_response(room, identity, dispatch_method, None)


@app.post("/api/livekit/token", response_model=LiveKitResponse)
def livekit_token(request: LiveKitSessionRequest):
    """
    Plain join token without automatic agent dispatch. Use when the room
    already exists or the agent is dispatched separately via /api/livekit/dispatch.
    """
    room = request.room or generate_room_name("soul")
    identity = request.identity or f"user-{uuid.uuid4().hex[:10]}"
    return _build_session_response(room, identity, "none", None)


@app.post("/api/livekit/dispatch", response_model=LiveKitDispatchResponse)
async def livekit_dispatch(request: LiveKitDispatchRequest):
    """
    Explicitly spawn the agent into an existing room via the LiveKit server
    API (AgentDispatchService). Falls back to a token room-config dispatch
    warning if the HTTP API is unreachable.
    """
    dispatch_metadata = dict(request.metadata or {})
    if request.user_id:
        dispatch_metadata["user_id"] = request.user_id
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
        print(f"[LIVEKIT] AgentDispatchService unavailable ({e}); using token dispatch instead.")
        return LiveKitDispatchResponse(
            room=request.room,
            agent_name=lk_settings.agent_name,
            dispatch_id=None,
            dispatch="token",
        )


# ==========================================
# 6. REPORT DOWNLOAD ENDPOINTS
# ==========================================
@app.get("/report/{report_id}/excel")
def download_excel(report_id: str):
    rdata = get_report(report_id)
    if not rdata:
        raise HTTPException(status_code=404, detail="Report not found or expired.")

    try:
        df = pd.DataFrame(rdata["data"])
        df = df.where(pd.notnull(df), None)

        tmp_dir = tempfile.gettempdir()
        tmp_path = os.path.join(tmp_dir, f"report_{report_id}.xlsx")

        with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Report")
            worksheet = writer.sheets["Report"]
            for col_cells in worksheet.columns:
                max_length = max(
                    (len(str(cell.value)) if cell.value is not None else 0)
                    for cell in col_cells
                )
                col_letter = col_cells[0].column_letter
                worksheet.column_dimensions[col_letter].width = min(max_length + 2, 50)

        return FileResponse(
            path=tmp_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"library_report_{report_id[:8]}.xlsx",
        )
    except Exception as e:
        print(f"[EXCEL ERROR] {e}")
        raise HTTPException(status_code=500, detail=f"Excel generation failed: {e}")

@app.get("/report/{report_id}/pdf")
def download_pdf(report_id: str):
    rdata = get_report(report_id)
    if not rdata:
        raise HTTPException(status_code=404, detail="Report not found or expired.")

    try:
        data = rdata["data"]
        question = rdata.get("question", "")
        df = pd.DataFrame(data)

        tmp_dir = tempfile.gettempdir()
        tmp_path = os.path.join(tmp_dir, f"report_{report_id}.pdf")

        doc = SimpleDocTemplate(
            tmp_path,
            pagesize=landscape(A4),
            rightMargin=30, leftMargin=30,
            topMargin=30, bottomMargin=30,
        )

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            "CustomTitle",
            parent=styles["Title"],
            fontSize=16,
            textColor=colors.HexColor("#1a5276"),
            spaceAfter=6,
        )
        sub_style = ParagraphStyle(
            "CustomSub",
            parent=styles["Normal"],
            fontSize=9,
            textColor=colors.grey,
            spaceAfter=12,
        )

        elements = []
        elements.append(Paragraph("SOUL 3.0 Library Report", title_style))
        elements.append(Paragraph(f"<b>Question:</b> {question}", sub_style))
        elements.append(Paragraph(
            f"<b>Generated:</b> {time.strftime('%d-%m-%Y %H:%M:%S')}  |  "
            f"<b>Rows:</b> {len(df)}",
            sub_style,
        ))
        elements.append(Spacer(1, 10))

        col_headers = [str(c) for c in df.columns]
        table_data = [col_headers]
        for _, row in df.iterrows():
            row_vals = []
            for v in row.tolist():
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    row_vals.append("")
                elif isinstance(v, (pd.Timestamp,)):
                    row_vals.append(v.strftime("%d-%m-%Y %H:%M"))
                else:
                    s = str(v)
                    if len(s) > 80:
                        s = s[:77] + "..."
                    row_vals.append(s)
            table_data.append(row_vals)

        page_width = landscape(A4)[0] - 60
        n_cols = max(len(col_headers), 1)
        col_width = min(page_width / n_cols, 3 * inch)
        col_widths = [col_width] * n_cols

        table = Table(table_data, repeatRows=1, colWidths=col_widths)
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a5276")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 9),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
            ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#f8f9fa")),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#eef2f7")]),
            ("FONTSIZE", (0, 1), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#bdc3c7")),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ]))

        elements.append(table)
        doc.build(elements)

        return FileResponse(
            path=tmp_path,
            media_type="application/pdf",
            filename=f"library_report_{report_id[:8]}.pdf",
        )
    except Exception as e:
        print(f"[PDF ERROR] {e}")
        raise HTTPException(status_code=500, detail=f"PDF generation failed: {e}")

@app.get("/report/{report_id}/status")
def report_status(report_id: str):
    rdata = get_report(report_id)
    if not rdata:
        return {"valid": False, "detail": "Report not found or expired."}
    elapsed = time.time() - rdata["created_at"]
    remaining = max(0, REPORT_TTL_SECONDS - elapsed)
    return {
        "valid": True,
        "rows": len(rdata["data"]),
        "question": rdata["question"],
        "remaining_seconds": int(remaining),
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7698)