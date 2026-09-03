"""In-memory report store with TTL for generated data exports.

Reports are stored briefly so the client can download them as Excel/PDF
without re-running the query.  Entries expire after REPORT_TTL_SECONDS.
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Dict, List, Optional

REPORT_TTL_SECONDS = 600  # 10 minutes

_report_store: Dict[str, Dict[str, Any]] = {}
_report_lock = threading.Lock()


def cleanup_expired_reports() -> None:
    now = time.time()
    expired = [
        rid
        for rid, rdata in _report_store.items()
        if now - rdata["created_at"] > REPORT_TTL_SECONDS
    ]
    for rid in expired:
        _report_store.pop(rid, None)
    if expired:
        print(f"[REPORT_STORE] Cleaned up {len(expired)} expired reports.")


def store_report(data: List[Dict], question: str, sql: str) -> str:
    cleanup_expired_reports()
    report_id = str(uuid.uuid4())
    with _report_lock:
        _report_store[report_id] = {
            "data": data,
            "question": question,
            "sql": sql,
            "created_at": time.time(),
        }
    print(f"[REPORT_STORE] Stored report_id={report_id} with {len(data)} rows.")
    return report_id


def get_report(report_id: str) -> Optional[Dict[str, Any]]:
    with _report_lock:
        rdata = _report_store.get(report_id)
        if not rdata:
            return None
        if time.time() - rdata["created_at"] > REPORT_TTL_SECONDS:
            _report_store.pop(report_id, None)
            return None
        return rdata
