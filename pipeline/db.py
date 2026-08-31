"""SQLite access layer.

One table, `targets`, one row per target. Every stage reads and writes it, so
the schema is created idempotently and each stage owns a disjoint set of
columns:

    stage 1  identity + classification   (domain .. source_sheet)
    stage 2  dns_*/a_host                (dns_resolves .. dns_checked_at)
    stage 3  http_*/platform/signals     (http_status .. http_checked_at)
    stage 4  score/tier                  (score, tier, tier_reason, scored_at)
    stage 5  outreach profile            (linkedin_url .. profile_checked_at)
    verify   deliverability API results  (verify_*)

`status` is the one column several stages touch; it always reflects the latest
thing we learned about the target.

The primary key is `target_key`, which is the domain for a store on its own
domain and the full email address for a freemail contact. Freemail addresses
share a handful of domains (26k of the sampled contacts sit on gmail.com
alone), so keying those by domain would collapse the entire paid-audience
seed list into one row; keying them by address keeps them. `domain` is a plain
column and is unique across every row the crawl stages touch, since freemail
rows never reach them.
"""
from __future__ import annotations

import sqlite3
from typing import Iterable

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS targets (
    target_key        TEXT PRIMARY KEY,
    domain            TEXT NOT NULL,
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
    a_host            TEXT,
    has_mx            INTEGER,
    mx_provider       TEXT,
    dns_error         TEXT,
    dns_attempts      INTEGER NOT NULL DEFAULT 0,
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
    http_attempts     INTEGER NOT NULL DEFAULT 0,
    http_checked_at   TEXT,

    score             INTEGER,
    tier              TEXT,
    tier_reason       TEXT,
    scored_at         TEXT,

    linkedin_url      TEXT,
    linkedin_kind     TEXT,
    image_count       INTEGER,
    images_no_alt     INTEGER,
    images_lazy       INTEGER,
    images_srcset     INTEGER,
    images_offsite    INTEGER,
    images_modern     INTEGER,
    has_og_image      INTEGER,
    has_meta_desc     INTEGER,
    sample_image_url  TEXT,
    sample_image_bytes INTEGER,
    sample_image_type TEXT,
    legal_url         TEXT,
    owner_name        TEXT,
    owner_source      TEXT,
    profile_error     TEXT,
    profile_attempts  INTEGER NOT NULL DEFAULT 0,
    profile_checked_at TEXT,

    verify_status     TEXT,
    verify_result     TEXT,
    verified_at       TEXT
);

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

# Kept apart from the table definitions and applied last: an index on a column
# that a migration is about to add would fail on an older database.
INDEXES = """
CREATE INDEX IF NOT EXISTS idx_targets_status       ON targets(status);
CREATE INDEX IF NOT EXISTS idx_targets_dns_checked  ON targets(dns_checked_at);
CREATE INDEX IF NOT EXISTS idx_targets_http_checked ON targets(http_checked_at);
CREATE INDEX IF NOT EXISTS idx_targets_dns_resolves ON targets(dns_resolves);
CREATE INDEX IF NOT EXISTS idx_targets_tier         ON targets(tier);
CREATE INDEX IF NOT EXISTS idx_targets_domain       ON targets(domain);
CREATE INDEX IF NOT EXISTS idx_targets_profiled     ON targets(profile_checked_at);
"""


def connect(path: str | None = None) -> sqlite3.Connection:
    """Open the database with settings tuned for long batched writes."""
    conn = sqlite3.connect(path or config.DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    _add_missing_columns(conn)
    conn.executescript(INDEXES)
    return conn


def _declared_columns() -> list[tuple[str, str]]:
    """(name, type) for every column of `targets` declared in SCHEMA."""
    body = SCHEMA.split("CREATE TABLE IF NOT EXISTS targets (", 1)[1].split(");", 1)[0]
    columns = []
    for line in body.splitlines():
        line = line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        name, _, rest = line.partition(" ")
        columns.append((name, rest.strip()))
    return columns


def _add_missing_columns(conn: sqlite3.Connection) -> None:
    """Bring an older database up to the current schema.

    A run can span hours, so a half-enriched database has real value and must
    survive a schema change. CREATE TABLE IF NOT EXISTS silently ignores new
    columns, so add them here rather than making the user start over.
    """
    existing = {r["name"] for r in conn.execute("PRAGMA table_info(targets)")}
    for name, decl in _declared_columns():
        if name not in existing:
            # NOT NULL/PRIMARY KEY only ever apply to stage 1 columns, which
            # exist from the first run; added columns are always nullable.
            conn.execute(f"ALTER TABLE targets ADD COLUMN {name} {decl}")
    conn.commit()


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
