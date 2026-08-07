import os
from typing import TypedDict, Annotated, List, Dict, Any
from langgraph.graph import StateGraph, END
from langchain_groq import ChatGroq  # Groq Import kiya hai
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import SystemMessage, HumanMessage
import pyodbc
from sqlalchemy import create_engine, text

# ==========================================
# 1. GROQ API KEY & DATABASE SETUP
# ==========================================
# Apni Groq API Key yahan daalein
os.environ["GROQ_API_KEY"] = "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" 

# Database Credentials
DB_SERVER = "160.25.62.109,1433"
DB_DATABASE = "SOUL30"
DB_USERNAME = "sa" 
DB_PASSWORD = "msspl@123"

# Agar Windows Authentication use karte hain toh niche wali line use karein:
# connection_string = f"mssql+pyodbc://@{DB_SERVER}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"
# SQL Auth ke liye ye:
connection_string = f"mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}/{DB_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(connection_string)

def execute_sql_query(query: str) -> Dict[str, Any]:
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            rows = result.fetchmany(10)
            columns = list(result.keys())
            data = [dict(zip(columns, row)) for row in rows]
            return {"success": True, "data": data}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==========================================
# 2. THE SYSTEM PROMPT
# ==========================================
SYSTEM_PROMPT = """You are an expert SQL Developer for a Library Management System (SOUL 3.0) using SQL Server (T-SQL). Your task is to convert natural language questions into highly accurate SQL queries.

### 1. CRITICAL ARCHITECTURE: MARC 21 FORMAT
Book metadata (Title, Author, Publisher, etc.) is NOT in columns. They are ROWS in the `Biblidetails` table. You MUST use multiple LEFT JOINs to get them based on the Tag and Subfield.
- Title: Tag='245', SbFld='a'
- Author: Tag='100', SbFld='a'
- Publisher: Tag='260', SbFld='b'
- Publication Year: Tag='260', SbFld='c'
- ISBN: Tag='020', SbFld='a'

Example JOIN for Title and Author:
LEFT JOIN Biblidetails TITLE ON Location.RecID = TITLE.RecID AND TITLE.Tag = '245' AND TITLE.SbFld = 'a'

### 2. CORE DATABASE RELATIONSHIPS (IMPLICIT JOINS)
- Member to Issue/Return/Fine: `m_member.mem_cd = t_issue.mem_cd`
- Physical Book (Accession No): `t_issue.acc_no = Location.p852` 
- Physical Book to Catalog: `Location.RecID = Biblidetails.RecID`
- Fines: `t_memfine` JOIN on `mem_cd` AND `accn_no = Location.p852`

### 3. SCHEMA MAP (Key Tables)
- m_member: mem_cd(PK), mem_firstnm, mem_lstnm
- t_issue: mem_cd, acc_no, iss_dt, due_dt
- t_memfine: mem_cd, accn_no, fine_amt, fine_desc
- Location: p852(Accession No), RecID
- Biblidetails: RecID, Tag, SbFld, FValue

### 4. STRICT INSTRUCTIONS
- Generate ONLY valid T-SQL syntax. No markdown blocks, no explanations.
- Always use `LEFT JOIN` for `Biblidetails`.
- Ignore backup tables (e.g., '_backup', 'weedout').
"""

# ==========================================
# 3. LANGGRAPH STATE DEFINITION
# ==========================================
class AgentState(TypedDict):
    question: str
    sql_query: str
    query_result: str
    attempts: int
    error: str

# ==========================================
# 4. LANGGRAPH NODES
# ==========================================
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
    
    # DEBUG: SQL Print kar rahe hain taaki hum dekh sake AI kya bana raha hai
    print(f"\n[DEBUG] Generated SQL:\n{clean_sql}\n")
    
    result = execute_sql_query(clean_sql)
    
    if result["success"]:
        return {"query_result": str(result["data"]), "error": ""}
    else:
        # DEBUG: SQL Error print kar rahe hain
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

def generate_answer_node(state: AgentState):
    print("---GENERATING FINAL ANSWER---")
    messages = [
        SystemMessage(content="You are a helpful library assistant. Based on the user's question and the SQL query result, provide a clear, human-readable answer in English/Hindi."),
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
    user_question = "Mujhe un members ke naam batao jinki fine 100 rupees se zyada hai, aur unke book ka title kya hai?"
    
    print(f"User Question: {user_question}\n")
    
    final_state = app.invoke({
        "question": user_question,
        "attempts": 0,
        "error": ""
    })
    
    print("\n=== FINAL OUTPUT ===")
    # KeyError fix: agar query_result nahi hai, toh error print karega
    print(final_state.get("query_result", f"Agent failed to generate answer. Last Error: {final_state.get('error')}"))