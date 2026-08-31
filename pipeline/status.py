"""Report where the pipeline is, without running anything.

A full run takes hours across four stages, so the first question at any point
is "what is done and what is left". This answers it read-only, and is safe to
run while a stage is in flight.

    python -m pipeline.status
"""
from __future__ import annotations

import argparse

from . import config, db
from .cli import heading, table


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    p.add_argument("--db", default=config.DB_PATH, help="database path")
    p.add_argument("--min-year", type=int, default=2020, metavar="YEAR",
                   help="the year cutoff stage 3 will apply (default: %(default)s), "
                        "so the crawl queue here matches what it will actually fetch")
    return p.parse_args(argv)


def funnel(conn, min_year: int) -> list[tuple[str, int, str]]:
    """The stage-by-stage narrowing, each step with what it is a share of."""
    total = db.count(conn)
    stores = db.count(conn, "is_freemail = 0")
    rows = [
        ("targets ingested", total, ""),
        ("  freemail addresses (ads only, never crawled)",
         db.count(conn, "is_freemail = 1"), "of targets"),
        ("  store domains", stores, "of targets"),
        ("stage 2: DNS settled", db.count(conn, "is_freemail = 0 AND dns_checked_at IS NOT NULL"),
         "of store domains"),
        ("  resolving", db.count(conn, "dns_resolves = 1"), "of store domains"),
        ("  awaiting retry (only ever timed out)",
         db.count(conn, "is_freemail = 0 AND dns_checked_at IS NULL AND dns_attempts > 0"),
         "of store domains"),
        ("  not yet looked up",
         db.count(conn, "is_freemail = 0 AND dns_checked_at IS NULL AND dns_attempts = 0"),
         "of store domains"),
        # Stage 3 marks rows aged_out as it starts, so before it has run most
        # pre-cutoff rows are not flagged yet. Applying the cutoff here as well
        # keeps "still to fetch" from counting domains that will never be
        # fetched -- the number an ETA gets built from.
        (f"stage 3: below the {min_year} cutoff (never fetched)",
         db.count(conn, "is_freemail = 0 AND dns_resolves = 1 "
                        "AND created_year IS NOT NULL AND created_year < ?",
                  (min_year,)),
         "of store domains"),
        ("stage 3: fetched", db.count(conn, "http_checked_at IS NOT NULL"),
         "of store domains"),
        ("  live", db.count(conn, f"status = '{config.STATUS_LIVE}'"), "of store domains"),
        ("  awaiting retry (only ever timed out)",
         db.count(conn, "is_freemail = 0 AND dns_resolves = 1 "
                        "AND http_checked_at IS NULL AND http_attempts > 0"),
         "of store domains"),
        ("  still to fetch",
         db.count(conn, "is_freemail = 0 AND dns_resolves = 1 "
                        "AND http_checked_at IS NULL AND http_attempts = 0 "
                        f"AND status != '{config.STATUS_AGED_OUT}' "
                        "AND (created_year IS NULL OR created_year >= ?)",
                  (min_year,)),
         "of store domains"),
        ("stage 4: tiered", db.count(conn, "tier IS NOT NULL"), "of targets"),
    ]
    denominators = {"of targets": total, "of store domains": stores, "": 0}
    return [(label, n, f"{100 * n / denominators[basis]:.1f}% {basis}"
             if denominators.get(basis) else "")
            for label, n, basis in rows]


def main(argv=None) -> int:
    args = parse_args(argv)
    conn = db.connect(args.db)
    heading(f"Pipeline status -- {args.db}")
    print(table([(label, f"{n:,}", share) for label, n, share in funnel(conn, args.min_year)],
                ["step", "count", "share"]))

    tiers = db.summarize(conn, "tier", "tier IS NOT NULL")
    if tiers:
        print("\nTiers:")
        print(table([(k, f"{n:,}") for k, n in tiers], ["tier", "targets"]))

    ingested = conn.execute(
        "SELECT path, emails_seen, domains_new, ingested_at FROM ingest_log "
        "ORDER BY path").fetchall()
    if ingested:
        print("\nInput files ingested:")
        print(table([(r["path"].rsplit("/", 1)[-1], f"{r['emails_seen']:,}",
                      f"{r['domains_new']:,}", r["ingested_at"]) for r in ingested],
                    ["file", "emails", "new targets", "at"]))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
