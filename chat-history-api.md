# Chat History API — SOUL 3.0 Library AI Assistant

Documentation for the frontend team. All endpoints power the **user chat history** feature (date-wise browsing, infinite-scroll "all chats", search, and clear-history).

## Base URL

All endpoints below are relative to the backend base URL, e.g. `https://your-backend.example.com`. Append the endpoint paths directly.

## Authentication / Security

- The backend identifies a user by `mem_cd` (member code, e.g. `DPDCDC160001`), which is **taken from the URL path**.
- `mem_cd` is always trimmed of trailing spaces server-side (the member code in the SQL database is stored as a `char` column with trailing spaces).
- **Your frontend/gateway must only call these APIs with the *logged-in user's own* authenticated `mem_cd`** (derived from the RFID / face-login session). Never let the client type in an arbitrary `mem_cd` to read another member's history.
- Passing an empty/blank `mem_cd` returns `[]` (empty array) — history is only saved for logged-in members.

---

## Data Type: `ChatMessageOut`

Every message (one finished Q&A turn) is returned as this object:

```json
{
  "message_id": "3f2c8f4e-... (uuid string)",
  "thread_id": "voice-session-abc123 or conversation thread id",
  "question": "how many books are overdue?",
  "resolved_question": "How many books are currently overdue as of today?",
  "answer": "There are 14 books overdue as of today.",
  "sql_query": "SELECT COUNT(*) ... FROM t_issue ...",
  "report_id": "a35c5771 (only when an Excel/PDF report was generated)",
  "had_chart": false,
  "chat_date": "2026-08-26",
  "chat_time": "14:05:32",
  "created_at_utc": "2026-08-26T08:35:32.123456+00:00"
}
```

| Field | Type | Notes |
|-------|------|-------|
| `message_id` | `string` | Unique UUID per message. Use as React/Vue list `key`. |
| `thread_id` | `string` | Conversation thread this turn belongs to. Multiple messages can share one `thread_id` (follow-up questions). |
| `question` | `string` | The user's **original** raw question. |
| `resolved_question` | `string \| null` | The rephrased/standalone question after the follow-up resolver (enables "uska", "unka", pronouns). `null` if not applicable. |
| `answer` | `string` | The assistant's final formatted answer. May contain markdown / tables. |
| `sql_query` | `string \| null` | The exact SQL the system generated and executed. `null` if not stored. (Useful for debugging UIs / "view SQL".) |
| `report_id` | `string \| null` | Present only when the answer also generated an Excel/PDF report download. |
| `had_chart` | `boolean` | `true` when the answer included a generated chart. |
| `chat_date` | `string` | Calendar date in **IST** (`Asia/Kolkata`), format `YYYY-MM-DD`. Grouping is by IST day boundary. |
| `chat_time` | `string` | Local time in IST, format `HH:MM:SS`. |
| `created_at_utc` | `string` | Unambiguous UTC timestamp, ISO-8601. Good for sorting/debugging. |

> Ordering note: `chat_time` is HH:MM:SS so it sorts correctly as a string within a day.

---

## Endpoint 1 — List of Chat Dates

```
GET /history/{mem_cd}/dates
```

Returns every date this member has chatted on, **newest date first**, with a message count and first/last message time. Use this for a date list / calendar / sidebar view ("Aug 26 · 12 messages", "Aug 24 · 3 messages").

### Path params

| Param | Type | Description |
|-------|------|-------------|
| `mem_cd` | `string` | Logged-in member code. |

### Query params

None.

### Response 200 — `Array<ChatDateSummary>`

```json
[
  {
    "chat_date": "2026-08-26",
    "message_count": 12,
    "first_time": "09:12:03",
    "last_time": "18:47:22"
  },
  {
    "chat_date": "2026-08-24",
    "message_count": 3,
    "first_time": "10:05:00",
    "last_time": "10:11:44"
  }
]
```

| Field | Type | Notes |
|-------|------|-------|
| `chat_date` | `string` | Date in IST, `YYYY-MM-DD`. |
| `message_count` | `int` | Number of messages on that date. |
| `first_time` | `string` | First message time of that day in IST (`HH:MM:SS`). |
| `last_time` | `string` | Last message time of that day in IST (`HH:MM:SS`). |

---

## Endpoint 2 — Messages for a Specific Date

```
GET /history/{mem_cd}/date/{chat_date}
```

Returns the **full conversation for one member on one day** in **oldest-first** (natural reading) order. This is the payload to show after the user taps a date from the list above.

### Path params

| Param | Type | Description |
|-------|------|-------------|
| `mem_cd` | `string` | Logged-in member code. |
| `chat_date` | `string` | Date in IST, format `YYYY-MM-DD`. |

### Query params

None.

### Response 200 — `Array<ChatMessageOut>`

```json
[
  {
    "message_id": "3f2c8f4e-1111",
    "thread_id": "voice-session-abc",
    "question": "hello",
    "resolved_question": null,
    "answer": "Hello! I'm the SOUL Library assistant. How can I help?",
    "sql_query": null,
    "report_id": null,
    "had_chart": false,
    "chat_date": "2026-08-26",
    "chat_time": "09:12:03",
    "created_at_utc": "2026-08-26T03:42:03.123456+00:00"
  }
]
```

Empty array `[]` if the member has no messages on that date.

---

## Endpoint 3 — Paginated Full History (Newest First)

```
GET /history/{mem_cd}?limit=50&offset=0
```

Returns the member's **entire history, newest message first**, with simple offset pagination. Intended for an **infinite-scroll "All my chats"** panel.

### Path params

| Param | Type | Description |
|-------|------|-------------|
| `mem_cd` | `string` | Logged-in member code. |

### Query params

| Param | Type | Default | Notes |
|-------|------|---------|-------|
| `limit` | `int` | `50` | Max messages to return. |
| `offset` | `int` | `0` | Skip the first N messages. |

### Response 200 — `Array<ChatMessageOut>`

Newest first. Example:

```json
[
  {
    "message_id": "3f2c8f4e-9999",
    "thread_id": "voice-session-xyz",
    "question": "how many books are overdue?",
    "resolved_question": "How many books are currently overdue as of today?",
    "answer": "There are 14 overdue books.",
    "sql_query": "SELECT ...",
    "report_id": null,
    "had_chart": false,
    "chat_date": "2026-08-26",
    "chat_time": "18:47:22",
    "created_at_utc": "2026-08-26T13:17:22.123456+00:00"
  }
]
```

### Pagination pattern for frontend

```
fetch page 1: GET /history/{mem_cd}?limit=20&offset=0
fetch page 2: GET /history/{mem_cd}?limit=20&offset=20
fetch page 3: GET /history/{mem_cd}?limit=20&offset=40
```

- **Stop condition**: when the returned array is shorter than `limit` (or empty), there is no more data.
- The response does **not** include a total count header, so detect the end by a short/empty page.

---

## Endpoint 4 — Search History

```
GET /history/{mem_cd}/search?q=keyword&limit=50
```

Performs a **case-insensitive substring search** across the member's **own** questions and answers. Good enough for "find that chat where I asked about overdue books".

### Path params

| Param | Type | Description |
|-------|------|-------------|
| `mem_cd` | `string` | Logged-in member code. |

### Query params

| Param | Type | Required | Notes |
|-------|------|----------|-------|
| `q` | `string` | **Yes** | Search keyword. Substring match on `question` OR `answer`. |
| `limit` | `int` | No (default `50`) | Max results. |

### Response 200 — `Array<ChatMessageOut>`

Results are **newest first**. Matches either the question or the answer.

```json
[
  {
    "message_id": "3f2c8f4e-8888",
    "thread_id": "voice-session-xyz",
    "question": "which students have overdue books?",
    "resolved_question": "...",
    "answer": "The following members have overdue books: ...",
    "sql_query": "SELECT ...",
    "report_id": null,
    "had_chart": false,
    "chat_date": "2026-08-25",
    "chat_time": "11:30:00",
    "created_at_utc": "2026-08-25T06:00:00.123456+00:00"
  }
]
```

Empty array `[]` if no matches.

---

## Endpoint 5 — Clear History

```
DELETE /history/{mem_cd}
```

Wipes **ALL** long-term history for one member. Use for a "Clear my history" button in settings, or a data-deletion request.

> There is **no undo** — this deletes every row for that `mem_cd` permanently.

### Path params

| Param | Type | Description |
|-------|------|-------------|
| `mem_cd` | `string` | Logged-in member code. |

### Response 200

```json
{
  "status": "ok",
  "deleted_rows": 27
}
```

| Field | Type | Notes |
|-------|------|-------|
| `status` | `string` | Always `"ok"` on success. |
| `deleted_rows` | `int` | Number of history rows permanently deleted. |

---

## Suggested UI Flows

### Flow A — Date browser (sidebar + conversation)

1. On page load, call `GET /history/{mem_cd}/dates` → render date list with counts.
2. On date tap, call `GET /history/{mem_cd}/date/{YYYY-MM-DD}` → render messages oldest-first.
3. Update the sidebar count when a new chat turn completes (optional; simplest is to refetch `/dates` on each visit).

### Flow B — All chats (infinite scroll)

1. `GET /history/{mem_cd}?limit=20&offset=0` → initial page, newest first.
2. On scroll to bottom and if the page was full (20 items), fetch `offset=20`, append, repeat.
3. If a page returns fewer than `limit` items, stop loading more.

### Flow C — Search

1. User types in a search box → debounce → call `GET /history/{mem_cd}/search?q=...`.
2. Results are newest-first; clicking one could deep-link to that date view.
3. Clear the query to return to the normal date/list view.

### Displaying a single message (bubble)

- **User bubble**: `question` (optionally show `resolved_question` as a tooltip/subtitle for debugging).
- **Assistant bubble**: `answer`. Render as markdown where the app already supports it.
- **Enrichments** (optional, only when present):
  - `had_chart === true` → show a chart placeholder / link.
  - `report_id != null` → show a "Download report" entry.
  - `sql_query != null` → show a collapsible "View SQL" debug block.
- **Timestamp label**: use `chat_time` (IST `HH:MM:SS`) for intra-day display and `chat_date` for the date header.

---

## Error Handling

| Case | Response |
|------|----------|
| Blank / whitespace `mem_cd` | `200` with `[]` (or `{"status":"ok","deleted_rows":0}` for DELETE) |
| No data for the requested date / search / page | `200` with `[]` |
| Route not found | `404` |
| Server error | `500` (should not happen — persistence is best-effort and guarded) |

## Notes for the backend team

- Data lives in a local SQLite file (`chat_memory.sqlite3`) in project root — independent of the main LangGraph in-RAM memory (which is thread-scoped and wiped on restart).
- `chat_date` / `chat_time` are in **IST**; `created_at_utc` is UTC. Prefer `chat_date`/`chat_time` for any UI-facing grouping so day boundaries match the member's local day.
- `mem_cd` is `.strip()`-ed before every save and lookup, matching the SQL `char` column behavior.