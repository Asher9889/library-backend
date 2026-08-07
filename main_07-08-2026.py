import os
import time
import re
from typing import TypedDict, Dict, Any, Optional, List
from langgraph.graph import StateGraph, END
from langchain_groq import ChatGroq
from langchain_core.messages import SystemMessage, HumanMessage
import pyodbc
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

# ==========================================
# 1. CONFIGURATION & ENVIRONMENT
# ==========================================
os.environ["GROQ_API_KEY"] = "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"

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

MAX_ROWS = 100  # Chart banane ke liye thode zyada rows chahiye
MAX_RETRIES = 5
_conn = None

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

    # Block backup tables
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
- `t_issue.acc_no`, `t_receive.accn_no`, `t_book_transfer.acc_no`, `t_replace.old_accno` are **char** with TRAILING SPACES (e.g., '121613     ').
- `Location.p852` is **nvarchar** WITHOUT trailing spaces.
- `m_member.mem_cd` is `char`.

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
SECTION 5: SCHEMA MAP & RELATIONSHIPS (UPDATED)
==============================
**m_member**: mem_cd, mem_firstnm, mem_lstnm, mem_status, mem_dept, mem_email, mem_prmntphone
**t_issue**: mem_cd, acc_no, iss_dt, due_dt
**t_receive**: mem_cd, accn_no, iss_dt, recv_dt (Use to check if book is returned)
**t_memfine**: mem_cd, accn_no, fine_amt, slip_dt, fine_desc
**t_replace**: old_accno, mem_cd, title, author (Use when querying lost/replaced books)
**Location**: RecID, p852 (Accession No), Status, DateofAcq
**Biblidetails**: RecID, Tag, SbFld, FValue

- Issue to Return: `t_issue` and `t_receive` are linked by (mem_cd + acc_no + iss_dt).
  To find OVERDUE books: 
  `WHERE t.due_dt < GETDATE() AND NOT EXISTS (SELECT 1 FROM t_receive r WHERE r.accn_no = t.acc_no AND r.mem_cd = t.mem_cd AND r.iss_dt = t.iss_dt)
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
9. MEMBER NAME SEARCH LOGIC:
   - If user provides ONLY FIRST NAME (e.g., "UNNATI"): `WHERE mem_firstnm LIKE '%UNNATI%' OR mem_lstnm LIKE '%UNNATI%'`
   - If user provides FULL NAME (e.g., "UNNATI SINGH"): ALWAYS use AND condition.
     `WHERE mem_firstnm LIKE '%UNNATI%' AND mem_lstnm LIKE '%SINGH%'`
     (OR use: `WHERE (mem_firstnm + ' ' + mem_lstnm) LIKE '%UNNATI SINGH%'`)
   - NEVER use `OR` between first and last name if the user provided both, otherwise it will fetch thousands of unrelated people.
11. TOTAL LIBRARY SIZE: If user asks for "total books in library" or "library size", NEVER filter by Status (e.g., avoid `WHERE Status = 'AV'`). Use `SELECT COUNT(*) FROM Location` to get the absolute total physical books.
12. AGGREGATE vs DETAIL: If user asks "how many" or "total" (e.g., "total kitni computer books hai"), DO NOT group by subject and return a long list. Return a single scalar number using a subquery.
    Example: SELECT (SELECT COUNT(*) FROM Location) AS Total_Books
13. AVOID HALLUCINATION: When summarizing data, strictly use the numbers present in the SQL result rows. Do not pick a random row's count and present it as the final total.
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
  AND L.Status = 'AV';  -- 'AV' means Available!

--- Example B: Member's issue history with book titles ---
SELECT 
    t.iss_dt AS Issue_Date,
    t.due_dt AS Due_Date,
    B.FValue AS Book_Title
FROM t_issue t
JOIN Location L ON L.p852 = RTRIM(t.acc_no)
JOIN Biblidetails B ON B.RecID = L.RecID AND B.Tag = '245' AND B.SbFld = 'a'
WHERE t.mem_cd = 'CCAPCC230001';

--- Example C: Active members from a specific department ---
SELECT 
    M.mem_cd, 
    M.mem_firstnm + ' ' + M.mem_lstnm AS Member_Name
FROM VMember M
WHERE M.mem_status = 'A' 
  AND M.Fclty_dept_dscr LIKE '%Computer%';

--- Example F: Get complete details of a member by their ID ---
SELECT 
    M.mem_cd AS Member_ID,
    M.mem_firstnm + ' ' + M.mem_lstnm AS Member_Name,
    M.Fclty_dept_dscr AS Department,
    M.mem_email AS Email,
    M.mem_prmntphone AS Phone,
    M.mem_status AS Status
FROM VMember M
WHERE M.mem_cd = 'UGCABC210047';

--- Example G: Search member by Name and show their current issued books ---
SELECT 
    M.mem_firstnm + ' ' + M.mem_lstnm AS Member_Name,
    B.FValue AS Book_Title,
    t.iss_dt AS Issue_Date,
    t.due_dt AS Due_Date
FROM VMember M
JOIN t_issue t ON RTRIM(t.mem_cd) = M.mem_cd
JOIN Location L ON L.p852 = RTRIM(t.acc_no)
JOIN Biblidetails B ON B.RecID = L.RecID AND B.Tag = '245' AND B.SbFld = 'a'
WHERE M.mem_firstnm LIKE '%UNNATI%' OR M.mem_lstnm LIKE '%UNNATI%';
"""

# ==========================================
# 4. LANGGRAPH STATE & NODES
# ==========================================
class AgentState(TypedDict):
    question: str
    sql_query: str
    previous_sql: str
    query_result: str
    attempts: int
    error: str
    error_history: List[str]

llm = ChatGroq(model="llama-3.3-70b-versatile", temperature=0)

def generate_sql_node(state: AgentState):
    print("---GENERATING SQL---")
    prev_err = state.get("error", "")
    prev_sql = state.get("previous_sql", "")
    err_hist = state.get("error_history", [])

    if prev_err:
        user_msg = (
            f"User Question: {state['question']}\n\n"
            f"Your PREVIOUS SQL (which FAILED):\n{prev_sql}\n\n"
            f"ERROR returned by SQL Server:\n{prev_err}\n\n"
            f"All errors so far: {err_hist}\n\n"
            f"Fix the SQL. Re-think the JOINs and column names. Remember the RTRIM and Status='AV' rules. "
            f"Output ONLY the SQL without any markdown."
        )
    else:
        user_msg = (
            f"User Question: {state['question']}\n\n"
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
        return {"query_result": data_str, "error": ""}
    else:
        print(f"[DEBUG] SQL ERROR: {result['error']}\n")
        hist = state.get("error_history", [])
        hist.append(result["error"])
        return {"error": result["error"], "error_history": hist}

def check_status_node(state: AgentState):
    if state.get("error"):
        print("---ERROR FOUND, RETRYING---")
        if state.get("attempts", 0) >= MAX_RETRIES:
            print("---MAX ATTEMPTS REACHED, STOPPING---")
            return END
        return "retry_generate"
    else:
        print("---SUCCESS---")
        return "generate_answer"

def generate_answer_node(state: AgentState):
    print("---GENERATING FINAL ANSWER---")
    messages = [
        SystemMessage(content="""You are a helpful, professional library assistant. Based on the user's question and the SQL query result, give a clear, highly accurate, and perfectly formatted answer.

LANGUAGE & FORMATTING RULES:
1. Reply in the EXACT SAME language the user used (Hindi, English, or Hinglish).
2. If the result is a list of items, ALWAYS format it as a clean Markdown Table or a bulleted list.
3. If the result is a count or aggregate, write a clear, concise sentence.
4. If the result is empty (e.g., []), politely say "No records found matching your query." Do not invent data.
5. For dates, format as DD-MM-YYYY.
6. For money, prefix with Rs. or ₹.
7. Do not include raw JSON or Python dictionary syntax in the output. Parse it and present it beautifully."""),
        HumanMessage(content=f"User Question: {state['question']}\n\n"
                              f"Executed SQL:\n{state.get('sql_query','')}\n\n"
                              f"SQL Result Data:\n{state['query_result']}\n\n"
                              f"Format this into a readable response.")
    ]
    response = llm.invoke(messages)
    return {"query_result": response.content}

# Compile Graph
workflow = StateGraph(AgentState)
workflow.add_node("generate_sql", generate_sql_node)
workflow.add_node("execute_sql", execute_sql_node)
workflow.add_node("generate_answer", generate_answer_node)
workflow.set_entry_point("generate_sql")
workflow.add_edge("generate_sql", "execute_sql")
workflow.add_conditional_edges(
    "execute_sql",
    check_status_node,
    {
        "retry_generate": "generate_sql",
        END: END,
        "generate_answer": "generate_answer",
    },
)
library_agent = workflow.compile()


# ==========================================
# 5. FASTAPI APP SETUP
# ==========================================
app = FastAPI(title="SOUL30 Library Text-to-SQL API", version="3.0")

class QueryRequest(BaseModel):
    question: str

class QueryResponse(BaseModel):
    question: str
    sql_query: str
    answer: str
    attempts: int
    debug_error: Optional[str] = None

@app.get("/")
def read_root():
    return {"status": "SOUL30 Library Text-to-SQL API v3.0 is running perfectly!"}

@app.post("/ask", response_model=QueryResponse)
def ask_library_agent(request: QueryRequest):
    user_question = request.question
    print(f"\n[API] User Question: {user_question}\n")
    
    final_state = library_agent.invoke({
        "question": user_question,
        "attempts": 0,
        "error": "",
        "previous_sql": "",
        "error_history": [],
    })
    
    final_answer = final_state.get("query_result", "Agent failed to generate answer.")
    executed_sql = (final_state.get("sql_query", "")
                    .replace("```sql", "").replace("```", "").strip())
    db_error = final_state.get("error", None) or None
    
    return QueryResponse(
        question=user_question,
        sql_query=executed_sql,
        answer=final_answer,
        attempts=final_state.get("attempts", 0),
        debug_error=db_error
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)