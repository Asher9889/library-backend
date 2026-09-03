"""SQL Server connection, query validation, execution, and live-schema lookup.

All database access flows through this module.  The rest of the
application never opens its own ODBC connections.
"""

from __future__ import annotations

import os
import re
import threading
import time
from typing import Any, Dict, List, Optional

import pyodbc

from config import MAX_ROWS, MAX_RETRIES

# ── ODBC driver resolution ────────────────────────────────────────────────


def _resolve_odbc_driver() -> str:
    requested = os.environ.get("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    available = set(pyodbc.drivers())
    if requested in available:
        return requested
    if "FreeTDS" in available:
        print(f"[DB] Driver {requested!r} not installed; falling back to FreeTDS")
        return "FreeTDS"
    raise RuntimeError(
        "No SQL Server ODBC driver available. Install one (e.g. msodbcsql17) "
        f"or set DB_DRIVER. Found drivers: {sorted(available)}"
    )


DB_DRIVER = _resolve_odbc_driver()

CONNECTION_STRING = (
    f"Driver={{{DB_DRIVER}}};"
    f"Server={os.environ.get('DB_SERVER', '160.25.62.109,1433')};"
    f"Database={os.environ.get('DB_DATABASE', 'SOUL30')};"
    f"UID={os.environ.get('DB_USER', 'sa')};"
    f"PWD={os.environ.get('DB_PASSWORD', 'msspl@123')};"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=30;"
)

# ── Connection management ──────────────────────────────────────────────────

_conn = None


def create_connection():
    return pyodbc.connect(CONNECTION_STRING, autocommit=True)


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
            if _conn:
                _conn.close()
        except Exception:
            pass
        _conn = create_connection()
    return _conn


# ── Backup-table exclusion patterns ────────────────────────────────────────

BACKUP_TABLE_PATTERNS = [
    r"_backup", r"_temp", r"weedout", r"before_restore", r"_bak\b",
    r"WO_\d+_Backup", r"NewWeedout", r"WeedoutBooks", r"_copy$",
]


def is_backup_table(name: str) -> bool:
    lname = name.lower()
    for pat in BACKUP_TABLE_PATTERNS:
        if re.search(pat, lname):
            return True
    return False


# ── Query validation ───────────────────────────────────────────────────────

def validate_query(query: str):
    if not query or not query.strip():
        raise ValueError("Empty SQL")

    forbidden = [
        "DROP", "DELETE", "UPDATE", "INSERT", "ALTER",
        "TRUNCATE", "MERGE", "GRANT", "REVOKE", "EXEC", "EXECUTE",
    ]
    for word in forbidden:
        if re.search(rf"\b{word}\b", query, re.IGNORECASE):
            raise ValueError(f"Unsafe query detected: {word}")

    stripped = query.strip().lower()
    if not (stripped.startswith("select") or stripped.startswith("with")):
        raise ValueError("Only SELECT/WITH allowed")

    for pat in BACKUP_TABLE_PATTERNS:
        if re.search(pat, query, re.IGNORECASE):
            raise ValueError(
                f"Query references backup/temp table (pattern: {pat}). Use only live tables."
            )


# ── Query execution ────────────────────────────────────────────────────────

def execute_query(query: str, limit: bool = True, params: Optional[tuple] = None):
    global _conn
    for attempt in range(MAX_RETRIES):
        cursor = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            time.sleep(0.1)
            if params is not None:
                cursor.execute(query, params)
            else:
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
            if (
                "08S01" in error_msg
                or "Communication link failure" in error_msg
                or "HY000" in error_msg
            ):
                try:
                    if _conn:
                        _conn.close()
                except Exception:
                    pass
                _conn = None
                time.sleep(1)
                continue
            raise e
        finally:
            try:
                if cursor:
                    cursor.close()
            except Exception:
                pass
    raise Exception("Database failed after retries")


def run_sql(query: str, limit: bool = True):
    try:
        validate_query(query)
        results = execute_query(query, limit=limit)
        return {"success": True, "rows": len(results), "data": results}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ── Live schema lookup ─────────────────────────────────────────────────────

SCHEMA_CACHE: Dict[str, List[str]] = {}
SCHEMA_CACHE_LOCK = threading.Lock()
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def extract_table_names(sql: str) -> List[str]:
    raw = re.findall(
        r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_\.]*)", sql, re.IGNORECASE
    )
    names = []
    for r in raw:
        name = r.split(".")[-1]
        if _IDENTIFIER_RE.match(name) and name not in names:
            names.append(name)
    return names


def get_table_columns(table_name: str) -> List[str]:
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
    safe_fragment = re.sub(r"[^A-Za-z0-9_]", "", fragment)[:60]
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
    hints = []
    lower_err = error_msg.lower()

    if "invalid column name" in lower_err:
        for t in extract_table_names(sql):
            cols = get_table_columns(t)
            if cols:
                hints.append(f"{t}: {', '.join(cols)}")

    obj_match = re.search(
        r"Invalid object name '([^']+)'", error_msg, re.IGNORECASE
    )
    if obj_match:
        bad_name = obj_match.group(1).split(".")[-1]
        matches = find_similar_table_names(bad_name)
        if matches:
            hints.append(
                f"No table/view called '{bad_name}'. Closest real names: "
                f"{', '.join(matches)}"
            )

    if "invalid in the order by clause" in lower_err and "group by" in lower_err:
        hints.append(
            "ORDER BY RULE: When using GROUP BY, you can ONLY order by columns "
            "that are IN the GROUP BY clause, or by aggregate functions like COUNT(*). "
            "Use ORDER BY COUNT(*) DESC or ORDER BY <grouped_column>. "
            "NEVER order by a column not in GROUP BY."
        )

    if "invalid column name 'status'" in lower_err:
        hints.append(
            "Status column EXISTS ONLY in Location table. Use L.Status = 'AV', "
            "NOT t.Status or any other table alias."
        )

    return "\n".join(hints)
