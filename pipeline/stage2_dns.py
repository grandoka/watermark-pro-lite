"""Stage 2 -- resolve A and MX records for every store domain.

This runs before any HTTP work because it is the cheapest filter available:
a DNS lookup is one UDP round trip, and it typically removes a third to a half
of a scraped list. Crawling a domain that does not resolve is pure waste.

    python -m pipeline.stage2_dns [--limit N] [--force] [--concurrency 200]

Resumable: rows whose `dns_checked_at` is set are skipped unless `--force` is
given, and results are committed in batches, so an interrupted run loses at
most the current batch.
"""
from __future__ import annotations

import asyncio
import datetime as dt

import dns.asyncresolver
import dns.exception
import dns.resolver

from . import config, db
from .cli import Progress, base_parser, heading, table

BATCH = 500
PROGRESS_EVERY = 1000

# Errors that mean "ask again later", as opposed to an authoritative answer.
TRANSIENT = (dns.exception.Timeout, dns.resolver.NoNameservers, dns.resolver.LifetimeTimeout)


def parse_args(argv=None):
    p = base_parser(__doc__.strip().splitlines()[0])
    p.add_argument("--concurrency", type=int, default=200,
                   help="in-flight lookups (default: %(default)s)")
    p.add_argument("--timeout", type=float, default=5.0,
                   help="per-query timeout in seconds (default: %(default)s)")
    p.add_argument("--retries", type=int, default=2,
                   help="retries after a transient failure (default: %(default)s)")
    p.add_argument("--max-attempts", type=int, default=3,
                   help="give up on a domain that never answers after this "
                        "many runs (default: %(default)s)")
    p.add_argument("--nameservers", default=None,
                   help="comma-separated resolvers to use instead of the system ones")
    return p.parse_args(argv)


def build_resolver(args) -> dns.asyncresolver.Resolver:
    resolver = dns.asyncresolver.Resolver(configure=True)
    if args.nameservers:
        resolver.nameservers = [ns.strip() for ns in args.nameservers.split(",") if ns.strip()]
    resolver.timeout = args.timeout
    resolver.lifetime = args.timeout
    return resolver


async def query(resolver, name: str, rdtype: str, retries: int):
    """Run one query. Returns (answer|None, error|None).

    An authoritative "no such name" or "no such record" is an answer, not an
    error, and is never retried.
    """
    attempt = 0
    while True:
        try:
            return await resolver.resolve(name, rdtype), None
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
            return None, None
        except TRANSIENT as exc:
            attempt += 1
            if attempt > retries:
                return None, f"{type(exc).__name__}"
            await asyncio.sleep(0.25 * attempt)
        except Exception as exc:  # malformed names, and anything else odd
            return None, f"{type(exc).__name__}"


async def check_domain(resolver, domain: str, retries: int) -> dict:
    """Resolve A (apex, then www) and MX for one domain."""
    a_answer, a_err = await query(resolver, domain, "A", retries)
    a_host = domain if a_answer else None

    # A surprising number of small stores publish only www. Finding that here
    # saves stage 3 a guaranteed-failed apex fetch, and saves the domain from
    # being written off as dead.
    if not a_answer and not a_err:
        www_answer, _ = await query(resolver, f"www.{domain}", "A", retries)
        if www_answer:
            a_answer, a_host = www_answer, f"www.{domain}"

    mx_answer, mx_err = await query(resolver, domain, "MX", retries)
    mx_provider = None
    if mx_answer:
        # Lowest preference wins -- that is the mailbox host that matters.
        best = min(mx_answer, key=lambda r: r.preference)
        mx_provider = str(best.exchange).rstrip(".").lower() or None

    resolves = bool(a_answer)
    has_mx = bool(mx_answer)
    error = "; ".join(e for e in (a_err, mx_err) if e) or None

    # A lookup that timed out is not an answer, and recording it as a dead
    # domain silently deletes a real store from the list -- so leave it for
    # another pass. But an authoritative "no such name" for the A record *is*
    # an answer: the domain does not exist, so it can have no mail either, and
    # a flaky MX query alongside it is no reason to look again.
    inconclusive = not resolves and not has_mx and a_err is not None

    if resolves:
        status = None  # stage 3 decides; leave whatever status it has
    elif has_mx:
        status = config.STATUS_MX_ONLY
    else:
        status = config.STATUS_DEAD

    return {
        "domain": domain,
        "dns_resolves": int(resolves),
        "a_host": a_host,
        "has_mx": int(has_mx),
        "mx_provider": mx_provider,
        "dns_error": error,
        "status": status,
        "inconclusive": inconclusive,
    }


UPDATE = """
UPDATE targets
   SET dns_resolves = :dns_resolves,
       a_host       = :a_host,
       has_mx       = :has_mx,
       mx_provider  = :mx_provider,
       dns_error    = :dns_error,
       dns_attempts = dns_attempts + 1,
       status       = COALESCE(:status, status),
       dns_checked_at = :checked_at
 WHERE domain = :domain AND is_freemail = 0
"""

# Nothing came back at all. Count the attempt but leave dns_checked_at unset so
# the next run picks the domain up again -- unless we have now tried
# --max-attempts times, at which point unreachable is the honest answer.
UPDATE_INCONCLUSIVE = """
UPDATE targets
   SET dns_error    = :dns_error,
       dns_attempts = dns_attempts + 1,
       dns_resolves = CASE WHEN dns_attempts + 1 >= :max_attempts THEN 0 END,
       has_mx       = CASE WHEN dns_attempts + 1 >= :max_attempts THEN 0 END,
       status       = CASE WHEN dns_attempts + 1 >= :max_attempts
                           THEN :dead_status ELSE status END,
       dns_checked_at = CASE WHEN dns_attempts + 1 >= :max_attempts
                             THEN :checked_at END
 WHERE domain = :domain AND is_freemail = 0
"""


def pending_domains(conn, force: bool, limit: int | None) -> list[str]:
    sql = "SELECT domain FROM targets WHERE is_freemail = 0"
    if not force:
        sql += " AND dns_checked_at IS NULL"
    sql += " ORDER BY domain"
    if limit:
        sql += f" LIMIT {int(limit)}"
    domains = [r["domain"] for r in conn.execute(sql)]

    if force and domains:
        # --force asks for a fresh verdict, so the earlier failed attempts
        # should not count against --max-attempts and settle the domain as
        # dead on its first timeout.
        conn.executemany("UPDATE targets SET dns_attempts = 0 WHERE domain = ?",
                         [(d,) for d in domains])
        conn.commit()
    return domains


async def run(conn, domains: list[str], args) -> None:
    resolver = build_resolver(args)
    queue: asyncio.Queue = asyncio.Queue()
    for d in domains:
        queue.put_nowait(d)

    progress = Progress(len(domains), PROGRESS_EVERY, "domains resolved")
    buffer: list[dict] = []
    lock = asyncio.Lock()

    async def flush() -> None:
        if not buffer:
            return
        rows, buffer[:] = list(buffer), []
        settled = [r for r in rows if not r["inconclusive"]]
        unsettled = [r for r in rows if r["inconclusive"]]
        if settled:
            conn.executemany(UPDATE, settled)
        if unsettled:
            conn.executemany(UPDATE_INCONCLUSIVE, unsettled)
        conn.commit()

    async def worker() -> None:
        while True:
            try:
                domain = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                result = await check_domain(resolver, domain, args.retries)
            except Exception as exc:  # never let one domain kill a worker
                result = {"domain": domain, "dns_resolves": 0, "a_host": None,
                          "has_mx": 0, "mx_provider": None,
                          "dns_error": f"worker:{type(exc).__name__}",
                          "status": config.STATUS_DEAD, "inconclusive": True}
            result["checked_at"] = dt.datetime.now(dt.timezone.utc).isoformat(
                timespec="seconds")
            result["max_attempts"] = args.max_attempts
            result["dead_status"] = config.STATUS_DEAD
            async with lock:
                buffer.append(result)
                if len(buffer) >= BATCH:
                    await flush()
                progress.tick()

    workers = [asyncio.create_task(worker()) for _ in range(args.concurrency)]
    try:
        await asyncio.gather(*workers)
    finally:
        async with lock:
            await flush()


def print_summary(conn) -> None:
    checked = db.count(conn, "dns_checked_at IS NOT NULL")
    heading(f"Stage 2 summary -- {checked:,} domains checked")
    total = max(checked, 1)
    rows = [
        ("resolves (A record)", db.count(conn, "dns_resolves = 1")),
        ("has MX", db.count(conn, "has_mx = 1")),
        ("A + MX", db.count(conn, "dns_resolves = 1 AND has_mx = 1")),
        ("www only (no apex A)", db.count(conn, "a_host LIKE 'www.%'")),
        ("MX but no website", db.count(conn, f"status = '{config.STATUS_MX_ONLY}'")),
        ("dead (nothing resolves)", db.count(conn, f"status = '{config.STATUS_DEAD}'")),
    ]
    print(table([(k, f"{n:,}", f"{100*n/total:.1f}%") for k, n in rows],
                ["result", "domains", "share"]))

    providers = conn.execute(
        "SELECT mx_provider, COUNT(*) n FROM targets WHERE mx_provider IS NOT NULL "
        "GROUP BY 1 ORDER BY n DESC LIMIT 15").fetchall()
    if providers:
        print("\nTop MX hosts:")
        print(table([(r["mx_provider"], f"{r['n']:,}") for r in providers],
                    ["mx host", "domains"]))

    retryable = db.count(conn, "is_freemail = 0 AND dns_checked_at IS NULL "
                               "AND dns_attempts > 0")
    if retryable:
        print(f"\n{retryable:,} domains never answered and were left unresolved "
              f"rather than written off as dead. Re-run this stage to retry them; "
              f"a high number here means the concurrency is too high for the "
              f"network path, not that the domains are gone.")

    remaining = db.count(conn, "is_freemail = 0 AND dns_checked_at IS NULL")
    print(f"\n{remaining:,} domains still unchecked; "
          f"{db.count(conn, 'dns_resolves = 1'):,} go on to stage 3.")


def main(argv=None) -> int:
    args = parse_args(argv)
    conn = db.connect(args.db)
    domains = pending_domains(conn, args.force, args.limit)
    heading(f"Stage 2 -- DNS for {len(domains):,} domains "
            f"(concurrency {args.concurrency}, timeout {args.timeout}s, "
            f"{args.retries} retries)")
    if domains:
        asyncio.run(run(conn, domains, args))
    else:
        print("  nothing to do -- all domains already checked (use --force to re-check)")
    print_summary(conn)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
