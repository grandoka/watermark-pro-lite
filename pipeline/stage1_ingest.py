"""Stage 1 -- read the workbooks, normalize, dedupe by domain, classify.

Handles both input shapes found in the source files:

  * a sheet with `created` / `emails` header columns
  * a headerless sheet with emails scattered across up to 14 unnamed columns

Nothing about column positions is assumed: every cell of every sheet is
scanned with an email regex, and the `created` value for a row is whatever
datetime cell that row happens to carry (none, for the headerless sheets).

Deduplication is by domain, not by email -- several contacts at one store are
one target -- keeping the row with the most recent `created` date. Freemail
contacts are the exception: they are keyed by full address, because a freemail
domain is a mailbox provider rather than a store and collapsing them by domain
would throw away the whole paid-audience seed list.

    python -m pipeline.stage1_ingest [--limit N] [--force] [--data-dir DIR]
"""
from __future__ import annotations

import datetime as dt
import glob
import os
import sys
import time

import openpyxl

from . import config, db
from .cli import base_parser, heading, table

BATCH = 2000


def parse_args(argv=None):
    p = base_parser(__doc__.strip().splitlines()[0])
    p.add_argument("--data-dir", default=config.DATA_DIR,
                   help="directory holding the input .xlsx files (default: %(default)s)")
    p.add_argument("--glob", default="*.xlsx", dest="pattern",
                   help="filename pattern within --data-dir (default: %(default)s)")
    return p.parse_args(argv)


# --- extraction ------------------------------------------------------------

def clean_email(raw: str) -> str | None:
    """Lowercase, strip, and validate one candidate. None if not an email."""
    e = raw.strip().strip(".,;:'\"<>()[]").lower()
    if e.startswith("mailto:"):
        e = e[7:]
    if not config.EMAIL_VALID.match(e):
        return None
    if e.count("@") != 1:
        return None
    return e


def row_created(row) -> dt.datetime | None:
    """First datetime-ish cell in the row, whatever position it sits in."""
    for cell in row:
        if isinstance(cell, dt.datetime):
            return cell
        if isinstance(cell, dt.date):
            return dt.datetime(cell.year, cell.month, cell.day)
    return None


def iter_records(path: str):
    """Yield (email, created, sheet_name) for every email in every sheet."""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        for ws in wb.worksheets:
            for i, row in enumerate(ws.iter_rows(values_only=True)):
                if i == 0 and _is_header(row):
                    continue
                created = row_created(row)
                for cell in row:
                    if not isinstance(cell, str):
                        continue
                    for candidate in config.EMAIL_SCAN.findall(cell):
                        email = clean_email(candidate)
                        if email:
                            yield email, created, ws.title
    finally:
        wb.close()


def _is_header(row) -> bool:
    """A first row of plain labels with no email in it is a header."""
    values = [str(c).strip().lower() for c in row if isinstance(c, str)]
    if not values:
        return False
    if any(config.EMAIL_SCAN.search(v) for v in values):
        return False
    return any(v in {"created", "emails", "email", "date", "domain"} for v in values)


# --- classification --------------------------------------------------------

def classify(email: str) -> dict:
    """Split an address into the identity + flags one target row carries."""
    local, domain = email.split("@", 1)
    tld = domain.rsplit(".", 1)[-1]
    freemail = config.is_freemail_domain(domain)
    return {
        # A freemail contact is keyed by address: the domain belongs to the
        # mailbox provider, not to a store, so it is not a target on its own.
        "target_key": email if freemail else domain,
        "domain": domain,
        "email": email,
        "local_part": local,
        "is_freemail": int(freemail),
        "is_role": int(local in config.ROLE_LOCALS),
        "tld": tld,
        "jurisdiction": config.jurisdiction_for_tld(tld),
        # Freemail rows have no store site to crawl, so they bypass stages 2
        # and 3 and go straight to the ads tier.
        "status": config.STATUS_FREEMAIL if freemail else config.STATUS_PENDING,
    }


# --- upsert ----------------------------------------------------------------

# Keeps the most recent contact per target. A row with a date always beats a
# row without one; enrichment columns written by later stages are never touched
# here, so stage 1 can be re-run at any point.
_NEWER = ("excluded.created IS NOT NULL AND "
          "(targets.created IS NULL OR excluded.created > targets.created)")

# Columns describing *which* contact represents the target; they all move
# together to whichever row turns out to be the most recent one.
_CONTACT_COLUMNS = ["email", "local_part", "is_role", "created", "created_year",
                    "source_file", "source_sheet"]

_INSERT_COLUMNS = ["target_key", "domain", "email", "local_part", "created",
                   "created_year", "is_freemail", "is_role", "tld",
                   "jurisdiction", "source_file", "source_sheet", "status"]

UPSERT = (
    "INSERT INTO targets ({cols}, email_count) VALUES ({binds}, 1) "
    "ON CONFLICT(target_key) DO UPDATE SET {carry}, "
    "email_count = targets.email_count + 1"
).format(
    cols=", ".join(_INSERT_COLUMNS),
    binds=", ".join(f":{c}" for c in _INSERT_COLUMNS),
    carry=", ".join(
        f"{c} = CASE WHEN {_NEWER} THEN excluded.{c} ELSE targets.{c} END"
        for c in _CONTACT_COLUMNS),
)


def ingest_file(conn, path: str, limit: int | None) -> dict:
    """Read one workbook into the targets table. Returns per-file counters."""
    seen_emails: set[str] = set()
    stats = {"rows": 0, "emails": 0, "invalid": 0, "dupe_emails": 0,
             "truncated": False}
    pending: list[dict] = []
    before = db.count(conn)
    t0 = time.monotonic()

    for email, created, sheet in iter_records(path):
        stats["rows"] += 1
        if email in seen_emails:
            stats["dupe_emails"] += 1
            continue
        seen_emails.add(email)
        stats["emails"] += 1

        rec = classify(email)
        rec["created"] = created.strftime("%Y-%m-%d %H:%M:%S") if created else None
        rec["created_year"] = created.year if created else None
        rec["source_file"] = os.path.basename(path)
        rec["source_sheet"] = sheet
        pending.append(rec)

        if len(pending) >= BATCH:
            conn.executemany(UPSERT, pending)
            conn.commit()
            pending.clear()
        if limit and stats["emails"] >= limit:
            stats["truncated"] = True
            break

    if pending:
        conn.executemany(UPSERT, pending)
        conn.commit()

    stats["domains_new"] = db.count(conn) - before
    stats["seconds"] = time.monotonic() - t0
    return stats


def record_ingest(conn, path: str, stats: dict) -> None:
    st = os.stat(path)
    conn.execute(
        "INSERT INTO ingest_log (path, size, mtime, rows_seen, emails_seen, "
        "domains_new, ingested_at) VALUES (?,?,?,?,?,?,?) "
        "ON CONFLICT(path) DO UPDATE SET size=excluded.size, mtime=excluded.mtime, "
        "rows_seen=excluded.rows_seen, emails_seen=excluded.emails_seen, "
        "domains_new=excluded.domains_new, ingested_at=excluded.ingested_at",
        (os.path.abspath(path), st.st_size, int(st.st_mtime), stats["rows"],
         stats["emails"], stats["domains_new"],
         dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")),
    )
    conn.commit()


def already_ingested(conn, path: str) -> bool:
    st = os.stat(path)
    row = conn.execute("SELECT size, mtime FROM ingest_log WHERE path = ?",
                       (os.path.abspath(path),)).fetchone()
    return bool(row) and row["size"] == st.st_size and row["mtime"] == int(st.st_mtime)


# --- summary ---------------------------------------------------------------

def print_summary(conn) -> None:
    total = db.count(conn)
    stores = conn.execute(
        "SELECT COUNT(*) FROM targets WHERE is_freemail = 0").fetchone()[0]
    freemail = total - stores
    heading(f"Stage 1 summary -- {total:,} targets "
            f"({stores:,} store domains + {freemail:,} freemail addresses)")

    print("\nBy jurisdiction:")
    print(table([(k, f"{n:,}", f"{100*n/total:.1f}%")
                 for k, n in db.summarize(conn, "jurisdiction")],
                ["jurisdiction", "targets", "share"]))

    print("\nFreemail vs. own domain:")
    print(table([("freemail address" if k else "own store domain",
                  f"{n:,}", f"{100*n/total:.1f}%")
                 for k, n in db.summarize(conn, "is_freemail")],
                ["kind", "targets", "share"]))

    print("\nRole vs. personal local part:")
    print(table([("role inbox" if k else "personal/other", f"{n:,}", f"{100*n/total:.1f}%")
                 for k, n in db.summarize(conn, "is_role")],
                ["kind", "targets", "share"]))

    print("\nBy created_year:")
    years = sorted(db.summarize(conn, "created_year"),
                   key=lambda r: (r[0] is None, r[0]))
    print(table([("unknown" if k is None else k, f"{n:,}", f"{100*n/total:.1f}%")
                 for k, n in years],
                ["created_year", "targets", "share"]))

    print("\nTop 20 TLDs:")
    print(table([(k, f"{n:,}") for k, n in db.summarize(conn, "tld")[:20]],
                ["tld", "targets"]))


def main(argv=None) -> int:
    args = parse_args(argv)
    paths = sorted(glob.glob(os.path.join(args.data_dir, args.pattern)))
    if not paths:
        print(f"No input files matching {args.pattern!r} in {args.data_dir}", file=sys.stderr)
        return 1

    conn = db.connect(args.db)
    heading(f"Stage 1 -- ingesting {len(paths)} file(s) from {args.data_dir}")
    for path in paths:
        if not args.force and already_ingested(conn, path):
            print(f"  skip (already ingested) {os.path.basename(path)}"
                  f" -- use --force to re-read")
            continue
        print(f"  reading {os.path.basename(path)} ...")
        stats = ingest_file(conn, path, args.limit)
        print(f"    {stats['rows']:,} email hits, {stats['emails']:,} unique emails, "
              f"{stats['dupe_emails']:,} repeats, +{stats['domains_new']:,} new targets "
              f"in {stats['seconds']:.1f}s")
        if stats["truncated"]:
            # A --limit smoke test read part of the file. Recording it as
            # ingested would make the real run skip it without a word.
            print(f"    partial read (--limit {args.limit}): not recorded as "
                  f"ingested, so a full run will still read this file")
        else:
            record_ingest(conn, path, stats)

    print_summary(conn)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
