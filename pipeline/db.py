"""SQLite access layer.

One table, `targets`, one row per domain. Every stage reads and writes it, so
the schema is created idempotently and each stage owns a disjoint set of
columns:

    stage 1  identity + classification   (domain .. source_sheet)
    stage 2  dns_*                       (dns_resolves .. dns_checked_at)
    stage 3  http_*/platform/signals     (http_status .. http_checked_at)
    stage 4  score/tier                  (score, tier, drop_reason, scored_at)
    verify   deliverability API results  (verify_*)

`status` is the one column several stages touch; it always reflects the latest
thing we learned about the domain.
"""
from __future__ import annotations

import sqlite3
from typing import Iterable

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS targets (
    domain            TEXT PRIMARY KEY,
    email             TEXT NOT NULL,
    local_part        TEXT,
    created           TEXT,
    created_year      INTEGER,
    email_count       INTEGER DEFAULT 1,
    is_freemail       INTEGER NOT NULL DEFAULT 0,
    is_role           INTEGER NOT NULL DEFAULT 0,
    tld               TEXT,
    jurisdiction      TEXT,
    source_file       TEXT,
    source_sheet      TEXT,
    status            TEXT NOT NULL DEFAULT 'pending',

    dns_resolves      INTEGER,
    has_mx            INTEGER,
    mx_provider       TEXT,
    dns_error         TEXT,
    dns_checked_at    TEXT,

    http_status       INTEGER,
    final_url         TEXT,
    page_title        TEXT,
    response_time_ms  INTEGER,
    content_length    INTEGER,
    platform          TEXT,
    has_cart          INTEGER,
    has_product_schema INTEGER,
    is_parked         INTEGER,
    lang              TEXT,
    robots_allowed    INTEGER,
    http_error        TEXT,
    http_checked_at   TEXT,

    score             INTEGER,
    tier              TEXT,
    drop_reason       TEXT,
    scored_at         TEXT,

    verify_status     TEXT,
    verify_result     TEXT,
    verified_at       TEXT
);

CREATE INDEX IF NOT EXISTS idx_targets_status       ON targets(status);
CREATE INDEX IF NOT EXISTS idx_targets_dns_checked  ON targets(dns_checked_at);
CREATE INDEX IF NOT EXISTS idx_targets_http_checked ON targets(http_checked_at);
CREATE INDEX IF NOT EXISTS idx_targets_dns_resolves ON targets(dns_resolves);
CREATE INDEX IF NOT EXISTS idx_targets_tier         ON targets(tier);

-- Which input workbooks have already been ingested, so stage 1 resumes at
-- file granularity. Re-ingesting is harmless (upsert), just slow.
CREATE TABLE IF NOT EXISTS ingest_log (
    path        TEXT PRIMARY KEY,
    size        INTEGER,
    mtime       INTEGER,
    rows_seen   INTEGER,
    emails_seen INTEGER,
    domains_new INTEGER,
    ingested_at TEXT
);
"""


def connect(path: str | None = None) -> sqlite3.Connection:
    """Open the database with settings tuned for long batched writes."""
    conn = sqlite3.connect(path or config.DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    return conn


def count(conn: sqlite3.Connection, where: str = "", params: Iterable = ()) -> int:
    sql = "SELECT COUNT(*) FROM targets"
    if where:
        sql += " WHERE " + where
    return conn.execute(sql, tuple(params)).fetchone()[0]


def summarize(conn: sqlite3.Connection, column: str, where: str = "") -> list[tuple]:
    """Count rows grouped by a column, most common first."""
    sql = f"SELECT {column} AS k, COUNT(*) AS n FROM targets"
    if where:
        sql += " WHERE " + where
    sql += " GROUP BY k ORDER BY n DESC"
    return [(r["k"], r["n"]) for r in conn.execute(sql)]
