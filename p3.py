import os
import time
import re
from typing import TypedDict, Dict, Any
from langgraph.graph import StateGraph, END
from langchain_groq import ChatGroq
from langchain_core.messages import SystemMessage, HumanMessage
import pyodbc

# ==========================================
# 1. GROQ API KEY
# ==========================================
os.environ["GROQ_API_KEY"] = "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" 

# ==========================================
# 2. DATABASE CONFIG (Aapka code)
# ==========================================
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

MAX_ROWS = 20   # LLM ko 20 rows bhejne kaafi hain answer banane ke liye, warna context limit par ho jayega
MAX_RETRIES = 5

_conn = None

def create_connection():
    return pyodbc.connect(CONNECTION_STRING)

def get_connection():
    global _conn
    try:
        if _conn is None:
            print(" Creating new DB connection...")
            _conn = create_connection()
        else:
            cursor = _conn.cursor()
            cursor.execute("SELECT 1")
    except:
        print("🔄 Reconnecting DB...")
        _conn = create_connection()
    return _conn

def validate_query(query):
    if not query:
        raise ValueError("❌ Empty SQL")
    forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE"]
    for word in forbidden:
        if re.search(rf"\b{word}\b", query, re.IGNORECASE):
            raise ValueError(f"❌ Unsafe query detected: {word}")
    if not query.strip().lower().startswith("select"):
        raise ValueError("❌ Only SELECT allowed")

def execute_query(query, limit=True):
    global _conn
    for attempt in range(MAX_RETRIES):
        cursor = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            time.sleep(0.2)
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            results = [{col: row[i] for i, col in enumerate(columns)} for row in rows]
            if limit and MAX_ROWS and len(results) > MAX_ROWS:
                results = results[:MAX_ROWS]
            return results
        except Exception as e:
            error_msg = str(e)
            print(f"❌ DB Error: {error_msg}")
            if "08S01" in error_msg or "Communication link failure" in error_msg:
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
    raise Exception("❌ Database failed after retries")

def run_sql(query, limit=True):
    try:
        validate_query(query)
        results = execute_query(query, limit=limit)
        return {"success": True, "rows": len(results), "data": results}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==========================================
# 3. THE SYSTEM PROMPT
# ==========================================
SYSTEM_PROMPT = """You are an expert SQL Developer for a Library Management System (SOUL 3.0) using SQL Server (T-SQL). Your task is to convert natural language questions into highly accurate SQL queries.

### 1. CRITICAL ARCHITECTURE: MARC 21 FORMAT & DICTIONARY
Book metadata (Title, Author, Publisher, etc.) is NOT in columns. They are ROWS in the `Biblidetails` table. You MUST use multiple LEFT JOINs to get them based on the Tag and Subfield.

Use this MARC Tag Dictionary to map user requests to the correct Tags:
- Title: Tag='245', SbFld='a'
- Author (Main Entry): Tag='100', SbFld='a'
- Edition: Tag='250', SbFld='a'
- Publication Place: Tag='260', SbFld='a'
- Publisher: Tag='260', SbFld='b'
- Publication Year: Tag='260', SbFld='c'
- Physical Description (Pages): Tag='300', SbFld='a'
- ISBN: Tag='020', SbFld='a'
- ISSN: Tag='022', SbFld='a'
- Dewey Decimal Classification (DDC): Tag='082', SbFld='a'
- Subject (Topical): Tag='650', SbFld='a'
- Subject (Uncontrolled): Tag='653', SbFld='a'
- Series Statement: Tag='440', SbFld='a'
- General Note: Tag='500', SbFld='a'
- Title (Former): Tag='247', SbFld='a'
- Call Number (Local): Tag='090', SbFld='a'

Example JOIN for Title and Author:
LEFT JOIN Biblidetails TITLE ON Location.RecID = TITLE.RecID AND TITLE.Tag = '245' AND TITLE.SbFld = 'a'
LEFT JOIN Biblidetails AUTHOR ON Location.RecID = AUTHOR.RecID AND AUTHOR.Tag = '100' AND AUTHOR.SbFld = 'a'

### 2. CORE DATABASE RELATIONSHIPS & QUERY STRUCTURE (CRITICAL)
- **Base Table Rule:** When searching for a book by Title, Author, or ISBN, ALWAYS start the query `FROM Biblidetails` and LEFT JOIN `Location`. Do NOT start `FROM Location`, as some catalog records may not have physical copies available, which will hide the book details.
- Member to Issue/Return/Fine: `m_member.mem_cd = t_issue.mem_cd`
- Physical Book (Accession No): `t_issue.acc_no = Location.p852` 
- Physical Book to Catalog: `Location.RecID = Biblidetails.RecID`
- Fines: `t_memfine` JOIN on `mem_cd` AND `accn_no = Location.p852`

Example Query for searching a book by title:
SELECT TITLE.FValue, AUTHOR.FValue, L.p852 
FROM Biblidetails TITLE 
LEFT JOIN Biblidetails AUTHOR ON TITLE.RecID = AUTHOR.RecID AND AUTHOR.Tag = '100' AND AUTHOR.SbFld = 'a'
LEFT JOIN Location L ON TITLE.RecID = L.RecID
WHERE TITLE.Tag = '245' AND TITLE.SbFld = 'a' AND TITLE.FValue LIKE '%Asian journal%'

### 3. SCHEMA MAP (Key Tables)
- m_member: mem_cd(PK, System Code e.g., 'CCAPCC230001'), mem_firstnm, mem_midnm, mem_lstnm, mem_email, mem_prmntphone, mem_dept, mem_degree, mem_status, mem_effctupto
- t_issue: mem_cd, acc_no, iss_dt, due_dt
- t_memfine: mem_cd, accn_no, fine_amt, fine_desc
- Location: p852(Accession No), RecID
- Biblidetails: RecID, Tag, SbFld, FValue

### 4. STRICT INSTRUCTIONS
- Generate ONLY valid T-SQL syntax. No markdown blocks, no explanations.
- Always use `LEFT JOIN` for `Biblidetails` to fetch metadata.
- Ignore backup tables (e.g., tables containing '_backup', 'weedout', '_temp').
- **Base Table for Book Search:** When searching for a book by Title, Author, or ISBN, ALWAYS start the query `FROM Biblidetails` (or from the specific MARC tag table) and LEFT JOIN `Location`. Do NOT start `FROM Location`.
- **Text Search:** When searching for a book title, author, or subject, ALWAYS use the `LIKE` operator with wildcards (e.g., `WHERE TITLE.FValue LIKE '%Asian journal%'`). NEVER use exact `=`.
- **Counting Books:** When counting books or titles, ALWAYS use `COUNT(DISTINCT Biblidetails.RecID)`. NEVER use `COUNT(*)` on `Biblidetails`.
- **CRITICAL ALIASING:** ALWAYS use unique `AS` aliases for all selected columns.
- **Physical Location:** The `Location` table does NOT have a column named 'Location'. If a user asks for the "location" of a book, return `L.p852 AS Accession_No` and `L.Status AS Book_Status`.
- **Mandatory Filtering:** When a user asks about a specific book or subject, ALWAYS include a `WHERE` clause filtering that text. NEVER run a generic query without the requested filter.
- **Avoid Duplicates (Top N):** When showing a list of "Top N" books (e.g., Top 5 Books of a Subject), ALWAYS use `SELECT DISTINCT` or group by `TITLE.RecID` so the user doesn't see multiple copies of the same book.
- **Availability Check:** If a user asks if a book is "available for issue", filter the `Location` table by `L.Status = 'Available'` (or similar) or check if it is NOT in `t_issue`.
- **Member Search:** When searching for a member by name, ALWAYS use `WHERE mem_firstnm LIKE '%name%' OR mem_lstnm LIKE '%name%'`. NEVER search by name in `mem_cd` because it is a system code (e.g., 'CCAPCC230001'). When asked for "member details", return `mem_cd`, `mem_firstnm`, `mem_lstnm`, `mem_email`, `mem_dept`, and `mem_status`.

### 5. REPORT GENERATION (Monthly/Yearly Reports)
If the user asks for a "Monthly Report" or "Library Report", generate a SINGLE SQL query using `UNION ALL` to fetch all the following metrics for the requested time period. 

IMPORTANT: Since `UNION ALL` requires the same data type for all columns, CAST all numeric values to VARCHAR.

Metrics to include:
1. Total New Books Added (FROM Location WHERE DateofAcq)
2. Total Books Issued (FROM t_issue WHERE iss_dt)
3. Total Books Returned (FROM t_receive WHERE recv_dt)
4. Total Fine Collected/Levied (FROM t_memfine WHERE slip_dt)

Example SQL for Monthly Report (e.g., May 2024):
SELECT 'Total New Books Added' AS Metric, CAST(COUNT(*) AS VARCHAR) AS Value 
FROM Location WHERE DateofAcq >= '2024-05-01' AND DateofAcq < '2024-06-01'
UNION ALL
SELECT 'Total Books Issued', CAST(COUNT(*) AS VARCHAR) 
FROM t_issue WHERE iss_dt >= '2024-05-01' AND iss_dt < '2024-06-01'
UNION ALL
SELECT 'Total Books Returned', CAST(COUNT(*) AS VARCHAR) 
FROM t_receive WHERE recv_dt >= '2024-05-01' AND recv_dt < '2024-06-01'
UNION ALL
SELECT 'Total Fine Amount (Rs)', CAST(ISNULL(SUM(fine_amt), 0) AS VARCHAR) 
FROM t_memfine WHERE slip_dt >= '2024-05-01' AND slip_dt < '2024-06-01'
"""

# ==========================================
# 4. LANGGRAPH STATE & NODES
# ==========================================
class AgentState(TypedDict):
    question: str
    sql_query: str
    query_result: str
    attempts: int
    error: str

llm = ChatGroq(model="llama-3.3-70b-versatile", temperature=0)

def generate_sql_node(state: AgentState):
    print("---GENERATING SQL---")
    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=f"Question: {state['question']}\n\nPrevious Error (if any): {state.get('error', 'None')}\n\nGenerate a corrected SQL query. Output ONLY the SQL." if state.get('error') else f"Question: {state['question']}\n\nGenerate the SQL query. Output ONLY the SQL.")
    ]
    response = llm.invoke(messages)
    return {"sql_query": response.content, "attempts": state.get('attempts', 0) + 1}

def execute_sql_node(state: AgentState):
    print("---EXECUTING SQL---")
    clean_sql = state["sql_query"].replace("```sql", "").replace("```", "").strip()
    print(f"\n[DEBUG] Generated SQL:\n{clean_sql}\n")
    
    # Aapke custom run_sql function ka use kiya
    result = run_sql(clean_sql)
    
    if result["success"]:
        return {"query_result": str(result["data"]), "error": ""}
    else:
        print(f"[DEBUG] SQL ERROR: {result['error']}\n")
        return {"error": result["error"]}

def check_status_node(state: AgentState):
    if state.get("error"):
        print("---ERROR FOUND, RETRYING---")
        if state.get("attempts", 0) >= 3:
            print("---MAX ATTEMPTS REACHED, STOPPING---")
            return END
        return "retry_generate"
    else:
        print("---SUCCESS---")
        return "generate_answer"

# def generate_answer_node(state: AgentState):
#     print("---GENERATING FINAL ANSWER---")
#     messages = [
#         SystemMessage(content="You are a helpful library assistant. Based on the user's question and the SQL query result, provide a clear, human-readable answer in English/Hindi."),
#         HumanMessage(content=f"User Question: {state['question']}\n\nSQL Result Data: {state['query_result']}\n\nFormat this into a readable response.")
#     ]
#     response = llm.invoke(messages)
#     return {"query_result": response.content}
def generate_answer_node(state: AgentState):
    print("---GENERATING FINAL ANSWER---")
    messages = [
        SystemMessage(content="""You are a helpful library assistant. Based on the user's question and the SQL query result, provide a clear, human-readable answer.
        
        CRITICAL LANGUAGE RULE: You MUST answer in the EXACT SAME language the user used in their question. 
        - If the user asks in Hindi, reply in Hindi.
        - If the user asks in English, reply in English.
        
        REPORT FORMATTING RULE: If the user asked for a report, format the output as a professional report with clear headings, bullet points, and a summary. Do not just dump the raw data."""),
        HumanMessage(content=f"User Question: {state['question']}\n\nSQL Result Data: {state['query_result']}\n\nFormat this into a readable response.")
    ]
    response = llm.invoke(messages)
    return {"query_result": response.content}

# ==========================================
# 5. BUILDING THE LANGGRAPH WORKFLOW
# ==========================================
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
        "generate_answer": "generate_answer" 
    }
)

workflow.add_edge("generate_answer", END)
app = workflow.compile()

# ==========================================
# 6. RUN THE AGENT
# ==========================================
if __name__ == "__main__":
    user_question = "Prepare monthly library report for May 2024"
    print(f"User Question: {user_question}\n")
    
    final_state = app.invoke({
        "question": user_question,
        "attempts": 0,
        "error": ""
    })
    
    print("\n=== FINAL OUTPUT ===")
    print(final_state.get("query_result", f"Agent failed to generate answer. Last Error: {final_state.get('error')}"))