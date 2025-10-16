import datetime
import json
import os
import random
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterable

DB_PATH =  os.getenv("OUTBOX_DB_PATH", "outbox.db")

_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS outbox_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_name TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        published_at INTEGER NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        next_retry_at INTEGER NOT NULL DEFAULT 0
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events(status, next_retry_at);
    """
]

@contextmanager
def _conn():
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None) 
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        for statement in _SCHEMA:
            conn.execute(statement)
        yield conn
    finally:
        conn.close()

def store_event(event_name: str, payload: Dict[str, Any], created_at: datetime) -> int:
    with _conn() as conn:
        cur = conn.execute(
            "INSERT INTO outbox_events (event_name, payload_json, created_at) VALUES (?, ?, ?)",
            (event_name, json.dumps(payload, ensure_ascii=False), created_at),
        )
        return int(cur.lastrowid)
    
def fetch_unpublished(limit: int = 100) -> Iterable[Dict[str, Any]]:
    """Fetches unpublished events that are ready for processing."""
    now = int(time.time())
    with _conn() as conn:
        rows = conn.execute(
            """
            SELECT id, event_name, payload_json, created_at, attempts 
            FROM outbox_events 
            WHERE 
                status IN ('pending', 'failed') AND
                next_retry_at <= ?
            ORDER BY id ASC 
            LIMIT ?
            """,
            (now, limit,),
        ).fetchall()
        for r in rows:
            yield {
                "id": r[0],
                "event_name": r[1],
                "payload": json.loads(r[2]),
                "created_at": r[3],
                "attempts": r[4],
            }

def mark_published(event_id: int) -> None:
    now = int(time.time())
    status = 'published'
    with _conn() as conn:
        conn.execute("UPDATE outbox_events SET published_at = ?, status = ?, last_error = NULL WHERE id = ?", (now, status, event_id))

def mark_failed(event_id: int, error: str, current_attempts: int, max_retries: int, base_delay: int) -> None:
    new_attempts = current_attempts + 1
    with _conn() as conn:
        if new_attempts >= max_retries:
            status = 'permanently_failed'

            conn.execute(
                "UPDATE outbox_events SET attempts = ?, last_error = ?, status = ? WHERE id = ?",
                (new_attempts,error[:500],status, event_id),
            )
        else:
            backoff_delay = base_delay * (2 ** current_attempts)
            jitter = random.uniform(0, 0.2 * backoff_delay) # Add up to 20% jitter
            next_attempt_time = int(time.time() + backoff_delay + jitter)
            status = 'failed'

            conn.execute(
                """
                UPDATE outbox_events 
                SET attempts = ?, last_error = ?, status = ?, next_retry_at = ? 
                WHERE id = ?
                """,
                (new_attempts, error[:500], status, next_attempt_time, event_id),
            )
