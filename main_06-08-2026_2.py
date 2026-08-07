import os
import time
import re
import ast
import io
import base64
from typing import TypedDict, Dict, Any, Optional
from langgraph.graph import StateGraph, END
from langchain_groq import ChatGroq
from langchain_core.messages import SystemMessage, HumanMessage
import pyodbc
from fastapi import FastAPI
from pydantic import BaseModel
import matplotlib
matplotlib.use('Agg') # Backend for server
import matplotlib.pyplot as plt

# ==========================================
# 1. GROQ API KEY & DB CONFIG
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

def create_connection():
    return pyodbc.connect(CONNECTION_STRING)

def get_connection():
    global _conn
    try:
        if _conn is None:
            _conn = create_connection()
        else:
            cursor = _conn.cursor()
            cursor.execute("SELECT 1")
    except:
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
# 2. THE SYSTEM PROMPT (With Chart Logic)
# ==========================================
SYSTEM_PROMPT = """You are an expert SQL Developer for a Library Management System (SOUL 3.0) using SQL Server (T-SQL).

### 1. CRITICAL ARCHITECTURE: MARC 21 FORMAT & DICTIONARY
Book metadata is ROWS in the `Biblidetails` table. Use multiple LEFT JOINs.
- Title: Tag='245', SbFld='a'
- Author: Tag='100', SbFld='a'
- Publisher: Tag='260', SbFld='b'
- Publication Year: Tag='260', SbFld='c'
- ISBN: Tag='020', SbFld='a'
- Subject: Tag='650', SbFld='a'

Example JOIN for Title:
LEFT JOIN Biblidetails TITLE ON Location.RecID = TITLE.RecID AND TITLE.Tag = '245' AND TITLE.SbFld = 'a'

### 2. CORE DATABASE RELATIONSHIPS
- Member to Issue/Return/Fine: `m_member.mem_cd = t_issue.mem_cd`
- Physical Book: `t_issue.acc_no = Location.p852` 
- Physical Book to Catalog: `Location.RecID = Biblidetails.RecID`
- Fines: `t_memfine` JOIN on `mem_cd` AND `accn_no = Location.p852`

### 3. SCHEMA MAP
- m_member: mem_cd(PK), mem_firstnm, mem_lstnm, mem_email, mem_dept, mem_status, mem_effctupto
- t_issue: mem_cd, acc_no, iss_dt, due_dt
- t_receive: mem_cd, accn_no, recv_dt
- t_memfine: mem_cd, accn_no, fine_amt, fine_desc, slip_dt
- Location: p852(Accession No), RecID, DateofAcq
- Biblidetails: RecID, Tag, SbFld, FValue

### 4. STRICT INSTRUCTIONS
- Generate ONLY valid T-SQL syntax. No markdown blocks, no explanations.
- Always use `LEFT JOIN` for `Biblidetails`.
- Ignore backup tables (e.g., '_backup', 'weedout').
- **Text Search:** ALWAYS use `LIKE '%text%'`. NEVER use exact `=`.
- **Counting Books:** ALWAYS use `COUNT(DISTINCT Biblidetails.RecID)`.
- **CRITICAL ALIASING:** ALWAYS use unique `AS` aliases.
- **Member Search:** `WHERE mem_firstnm LIKE '%name%' OR mem_lstnm LIKE '%name%'`.

### 5. CHART GENERATION DATA FORMAT (CRITICAL)
If the user asks for a trend, report over time (e.g., monthly, yearly), or distribution (e.g., books by subject, top departments), you MUST generate an SQL query that returns EXACTLY two columns named `Label` and `Value`.
- `Label`: The category or time period (e.g., '2024-01', 'Computer Science').
- `Value`: The numeric count or sum (e.g., 150, 5000).

Example for monthly issue trend:
SELECT FORMAT(iss_dt, 'yyyy-MM') AS Label, COUNT(*) AS Value 
FROM t_issue 
WHERE iss_dt >= '2023-01-01' 
GROUP BY FORMAT(iss_dt, 'yyyy-MM') 
ORDER BY Label;
"""

# ==========================================
# 3. LANGGRAPH STATE & NODES
# ==========================================
class AgentState(TypedDict):
    question: str
    sql_query: str
    query_result: str
    attempts: int
    error: str
    chart_base64: Optional[str]

llm = ChatGroq(model="llama-3.3-70b-versatile", temperature=0)

def generate_sql_node(state: AgentState):
    print("---GENERATING SQL---")
    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=f"Question: {state['question']}\n\nPrevious Error: {state.get('error', 'None')}\n\nGenerate SQL. Output ONLY SQL.")
    ]
    response = llm.invoke(messages)
    return {"sql_query": response.content, "attempts": state.get('attempts', 0) + 1}

def execute_sql_node(state: AgentState):
    print("---EXECUTING SQL---")
    clean_sql = state["sql_query"].replace("```sql", "").replace("```", "").strip()
    result = run_sql(clean_sql)
    
    if result["success"]:
        return {"query_result": str(result["data"]), "error": ""}
    else:
        return {"error": result["error"]}

def check_status_node(state: AgentState):
    if state.get("error"):
        if state.get("attempts", 0) >= 3:
            return END
        return "retry_generate"
    else:
        return "generate_chart"

def generate_chart_node(state: AgentState):
    """Node to automatically generate chart if data has Label & Value"""
    print("---CHECKING FOR CHART DATA---")
    data_str = state.get("query_result", "[]")
    chart_b64 = None
    
    try:
        data = ast.literal_eval(data_str)
        # Agar data me 'Label' aur 'Value' columns hain, toh chart banao
        if len(data) > 1 and "Label" in data[0] and "Value" in data[0]:
            print("---GENERATING CHART---")
            labels = [d["Label"] for d in data]
            values = [d["Value"] for d in data]
            
            plt.figure(figsize=(10, 5))
            
            # Agar 6 ya usse kam items hain toh Pie chart, warna Bar/Line chart
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
            
            # Chart ko Base64 me convert karna
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
    messages = [
        SystemMessage(content="""You are a helpful library assistant. Based on the user's question and the SQL query result, provide a clear, human-readable answer.
        CRITICAL LANGUAGE RULE: Answer in the EXACT SAME language the user used (Hindi/English/Hinglish).
        If a chart was generated, mention 'I have also attached a chart for this data.'"""),
        HumanMessage(content=f"User Question: {state['question']}\n\nSQL Result Data: {state['query_result']}\n\nFormat this into a readable response.")
    ]
    response = llm.invoke(messages)
    return {"query_result": response.content}

# Compile Graph
workflow = StateGraph(AgentState)
workflow.add_node("generate_sql", generate_sql_node)
workflow.add_node("execute_sql", execute_sql_node)
workflow.add_node("generate_chart", generate_chart_node)
workflow.add_node("generate_answer", generate_answer_node)

workflow.set_entry_point("generate_sql")
workflow.add_edge("generate_sql", "execute_sql")
workflow.add_conditional_edges("execute_sql", check_status_node, {
    "retry_generate": "generate_sql", 
    END: END,                         
    "generate_chart": "generate_chart" 
})
workflow.add_edge("generate_chart", "generate_answer")
workflow.add_edge("generate_answer", END)

library_agent = workflow.compile()

# ==========================================
# 4. FASTAPI APP SETUP
# ==========================================
app = FastAPI(title="Library Text-to-SQL & Chart API")

class QueryRequest(BaseModel):
    question: str

class QueryResponse(BaseModel):
    question: str
    sql_query: str
    answer: str
    chart_base64: Optional[str] = None

@app.post("/ask", response_model=QueryResponse)
def ask_library_agent(request: QueryRequest):
    user_question = request.question
    print(f"\n[API] User Question: {user_question}\n")
    
    final_state = library_agent.invoke({
        "question": user_question,
        "attempts": 0,
        "error": "",
        "chart_base64": None
    })
    
    return QueryResponse(
        question=user_question,
        sql_query=final_state.get("sql_query", "").replace("```sql", "").replace("```", "").strip(),
        answer=final_state.get("query_result", "Agent failed."),
        chart_base64=final_state.get("chart_base64")
    )