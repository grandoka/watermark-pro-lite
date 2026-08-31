"""Stage 3 -- fetch each live domain's root page and fingerprint the store.

The expensive stage: one HTTPS request per domain, so it only ever runs on
domains stage 2 proved resolvable, and it reads at most 200KB of any response.
Whole pages are never buffered -- the body is streamed and cut off at the cap,
which is more than enough for a platform fingerprint and keeps memory flat
across a run of hundreds of thousands of domains.

    python -m pipeline.stage3_http [--limit N] [--force] [--concurrency 50]

Resumable: rows whose `http_checked_at` is set are skipped unless `--force`,
and results commit in small batches, so an interrupted run loses seconds.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import html as html_module
import ssl
import time
import urllib.robotparser
from urllib.parse import urlsplit

import aiohttp

from . import config, db
from .cli import Progress, base_parser, heading, table

# Smaller than stage 2's batch: HTTP is ~100x slower per row, so 100 rows is a
# comparable amount of wall-clock work to lose on a crash.
BATCH = 100
PROGRESS_EVERY = 1000


def parse_args(argv=None):
    p = base_parser(__doc__.strip().splitlines()[0])
    p.add_argument("--concurrency", type=int, default=50,
                   help="in-flight domain fetches (default: %(default)s)")
    p.add_argument("--connect-timeout", type=float, default=10.0,
                   help="seconds to establish a connection (default: %(default)s)")
    p.add_argument("--read-timeout", type=float, default=15.0,
                   help="seconds to wait on a read (default: %(default)s)")
    p.add_argument("--min-year", type=int, default=2020, metavar="YEAR",
                   help="skip crawling contacts older than this; they are held "
                        "for the nurture tier (default: %(default)s, 0 to crawl all)")
    p.add_argument("--max-attempts", type=int, default=3,
                   help="give up on a domain that only ever times out after "
                        "this many runs (default: %(default)s)")
    p.add_argument("--ignore-robots", action="store_true",
                   help="do not fetch robots.txt (for domains you own only)")
    return p.parse_args(argv)


# --- fetching --------------------------------------------------------------

def candidate_urls(domain: str, a_host: str | None) -> list[str]:
    """URLs to try, in order: https, then http, then the www variant.

    Stage 2 records which hostname actually had an A record, so a www-only
    store starts at its www URL instead of burning an attempt on the apex.
    """
    host = a_host or domain
    if host.startswith("www."):
        return [f"https://{host}/", f"http://{host}/", f"https://{domain}/"]
    return [f"https://{domain}/", f"http://{domain}/", f"https://www.{domain}/"]


async def read_capped(response) -> tuple[bytes, bool]:
    """Read at most MAX_BODY_BYTES of the body. Returns (body, was_truncated)."""
    chunks: list[bytes] = []
    size = 0
    async for chunk in response.content.iter_chunked(16384):
        chunks.append(chunk)
        size += len(chunk)
        if size >= config.MAX_BODY_BYTES:
            return b"".join(chunks)[:config.MAX_BODY_BYTES], True
    return b"".join(chunks), False


async def fetch(session, url: str, retry_timeout: bool = True,
                timeout: aiohttp.ClientTimeout | None = None) -> dict:
    """Fetch one URL. Returns a dict with either a response or an error.

    Timeouts get exactly one retry; a 4xx is a real answer and is never
    retried.
    """
    started = time.monotonic()
    kwargs = {"timeout": timeout} if timeout else {}
    try:
        async with session.get(url, allow_redirects=True, max_redirects=3,
                               **kwargs) as response:
            body, truncated = await read_capped(response)
            declared = response.headers.get("Content-Length")
            return {
                "status": response.status,
                "final_url": str(response.url),
                "headers": {k.lower(): v for k, v in response.headers.items()},
                "body": body,
                "content_length": int(declared) if declared and declared.isdigit() else len(body),
                "truncated": truncated,
                "elapsed_ms": int((time.monotonic() - started) * 1000),
                "error": None,
            }
    except asyncio.TimeoutError:
        if retry_timeout:
            return await fetch(session, url, retry_timeout=False, timeout=timeout)
        return _error("timeout", started)
    except aiohttp.TooManyRedirects:
        return _error("too_many_redirects", started)
    except aiohttp.ClientError as exc:
        return _error(f"{type(exc).__name__}", started)
    except UnicodeDecodeError as exc:  # malformed headers on broken servers
        return _error(f"{type(exc).__name__}", started)
    except Exception as exc:
        return _error(f"{type(exc).__name__}", started)


def _error(reason: str, started: float) -> dict:
    return {"status": None, "final_url": None, "headers": {}, "body": b"",
            "content_length": None, "truncated": False,
            "elapsed_ms": int((time.monotonic() - started) * 1000), "error": reason}


# robots.txt is a few hundred bytes. Giving it the full page budget doubles
# the cost of every unresponsive host for no information, so it gets its own
# short leash; failing to answer within it is treated as "no rules published".
ROBOTS_TIMEOUT = aiohttp.ClientTimeout(connect=5, sock_read=5, total=8)


async def robots_allows(session, origin: str) -> tuple[bool, str | None]:
    """Check robots.txt for one origin before fetching its root.

    Follows RFC 9309 where it matters: a 4xx (including 404) means no rules
    and full access, an explicit 5xx means the server is telling us to stay
    away. A connection-level failure is treated as allowed -- there are no
    rules to honour, and the page fetch that follows will fail on its own if
    the host really is down.
    """
    # No timeout retry here: robots.txt is a gate, not the payload, and
    # doubling its cost on every unresponsive host dominates the run.
    result = await fetch(session, f"{origin}/robots.txt", retry_timeout=False,
                         timeout=ROBOTS_TIMEOUT)
    if result["error"]:
        return True, None
    if result["status"] and 500 <= result["status"] < 600:
        return False, f"robots_http_{result['status']}"
    if result["status"] and result["status"] >= 400:
        return True, None
    parser = urllib.robotparser.RobotFileParser()
    try:
        parser.parse(result["body"].decode("utf-8", "ignore").splitlines())
    except Exception:
        return True, None
    if parser.can_fetch(config.USER_AGENT, f"{origin}/"):
        return True, None
    return False, "robots_disallow"


# --- fingerprinting --------------------------------------------------------

def detect_platform(html: str, headers: dict) -> str | None:
    """First matching platform signature, or None."""
    header_blob = " ".join(f"{k}:{v}" for k, v in headers.items()).lower()
    for name, body_re, header_rules in config.PLATFORM_SIGNATURES:
        if body_re.search(html):
            return name
        for header, needle in header_rules:
            if needle is None:
                if any(k.startswith(header) for k in headers):
                    return name
            elif needle in header_blob:
                return name
    return None


def extract_title(html: str) -> str | None:
    match = config.TITLE_RE.search(html)
    if not match:
        return None
    title = html_module.unescape(match.group(1))
    title = " ".join(title.split())
    return title[:300] or None


def fingerprint(body: bytes, headers: dict) -> dict:
    """Everything we can learn from the first 200KB of a response."""
    html = body.decode("utf-8", "ignore")
    title = extract_title(html)
    lang_match = config.HTML_LANG_RE.search(html)
    haystack = f"{title or ''}\n{html}"
    return {
        "page_title": title,
        "lang": (lang_match.group(1).lower()[:10] if lang_match else None),
        "platform": detect_platform(html, headers),
        "has_cart": int(any(p.search(html) for p in config.CART_PATTERNS)),
        "has_product_schema": int(any(p.search(html) for p in config.PRODUCT_SCHEMA_PATTERNS)),
        "is_parked": int(any(p.search(haystack) for p in config.PARKED_PATTERNS)),
    }


EMPTY_FINGERPRINT = {"page_title": None, "lang": None, "platform": None,
                     "has_cart": 0, "has_product_schema": 0, "is_parked": 0}


async def check_domain(session, row, ignore_robots: bool) -> dict:
    """Fetch and fingerprint one domain, trying each candidate URL in turn."""
    domain = row["domain"]
    urls = candidate_urls(domain, row["a_host"])
    record = {"domain": domain, "robots_allowed": 1, **EMPTY_FINGERPRINT,
              "http_status": None, "final_url": None, "response_time_ms": None,
              "content_length": None, "http_error": None,
              "status": config.STATUS_ERROR, "inconclusive": False}

    result = None
    checked: set[str] = set()
    for url in urls:
        origin = "{0}://{1}".format(*urlsplit(url)[:2])
        # Consult robots for the origin we are about to fetch, not just the
        # first one we hoped would work: an http-only store's rules would
        # otherwise never be read at all. In the common case where https
        # answers, this is still one robots fetch for the domain.
        if not ignore_robots and origin not in checked:
            checked.add(origin)
            allowed, reason = await robots_allows(session, origin)
            if not allowed:
                record.update(robots_allowed=0, http_error=reason,
                              status=config.STATUS_BLOCKED)
                return record

        result = await fetch(session, url)
        if result["status"] is not None:
            break
        # A host that hangs on https will hang on http too. Only try the next
        # scheme when this one failed fast (refused, TLS error, bad DNS);
        # chaining full timeouts is what turns a crawl into a week-long job.
        if result["error"] == "timeout":
            break

    record["response_time_ms"] = result["elapsed_ms"]
    if result["status"] is None:
        # No response at all. A refused connection or a TLS failure is the
        # network answering for the host, so we can call it dead. A timeout is
        # not an answer -- it is just as likely to be our own concurrency --
        # and marking those dead deletes live stores from the list, so they go
        # back in the queue instead.
        record.update(http_error=result["error"], status=config.STATUS_DEAD,
                      inconclusive=(result["error"] == "timeout"))
        return record

    record.update(
        http_status=result["status"],
        final_url=result["final_url"],
        content_length=result["content_length"],
        http_error=result["error"],
    )
    record.update(fingerprint(result["body"], result["headers"]))

    if record["is_parked"]:
        record["status"] = config.STATUS_PARKED
    elif result["status"] == 200:
        record["status"] = config.STATUS_LIVE
    else:
        record["status"] = config.STATUS_ERROR
    return record


# --- persistence -----------------------------------------------------------

UPDATE = """
UPDATE targets
   SET http_status        = :http_status,
       final_url          = :final_url,
       page_title         = :page_title,
       response_time_ms   = :response_time_ms,
       content_length     = :content_length,
       platform           = :platform,
       has_cart           = :has_cart,
       has_product_schema = :has_product_schema,
       is_parked          = :is_parked,
       lang               = :lang,
       robots_allowed     = :robots_allowed,
       http_error         = :http_error,
       status             = :status,
       http_attempts      = http_attempts + 1,
       http_checked_at    = :checked_at
 WHERE domain = :domain AND is_freemail = 0
"""

# Nothing answered, and the reason was a timeout. Count the attempt but leave
# http_checked_at unset so a later pass retries the domain -- unless it has now
# failed --max-attempts times, at which point unreachable is the honest answer.
UPDATE_INCONCLUSIVE = """
UPDATE targets
   SET http_error     = :http_error,
       response_time_ms = :response_time_ms,
       http_attempts  = http_attempts + 1,
       status         = CASE WHEN http_attempts + 1 >= :max_attempts
                            THEN :dead_status ELSE status END,
       http_checked_at = CASE WHEN http_attempts + 1 >= :max_attempts
                             THEN :checked_at END
 WHERE domain = :domain AND is_freemail = 0
"""


def age_out(conn, min_year: int) -> int:
    """Park contacts older than min_year before the crawl, without fetching."""
    if not min_year:
        return 0
    cursor = conn.execute(
        "UPDATE targets SET status = ? "
        " WHERE is_freemail = 0 AND dns_resolves = 1 AND http_checked_at IS NULL "
        "   AND created_year IS NOT NULL AND created_year < ? AND status != ?",
        (config.STATUS_AGED_OUT, min_year, config.STATUS_AGED_OUT))
    conn.commit()
    return cursor.rowcount


def pending_rows(conn, args) -> list:
    sql = ("SELECT domain, a_host FROM targets "
           "WHERE is_freemail = 0 AND dns_resolves = 1")
    params: list = []
    if not args.force:
        sql += " AND http_checked_at IS NULL"
    if args.min_year:
        sql += " AND (created_year IS NULL OR created_year >= ?)"
        params.append(args.min_year)
    # Newest contacts first: if a run is cut short, the best prospects are the
    # ones already done.
    sql += " ORDER BY created_year DESC, domain"
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"
    return conn.execute(sql, params).fetchall()


async def run(conn, rows, args) -> None:
    timeout = aiohttp.ClientTimeout(connect=args.connect_timeout,
                                    sock_read=args.read_timeout,
                                    total=args.connect_timeout + args.read_timeout)
    # Certificates are verified. Plenty of small stores have expired or
    # mismatched certs; the https attempt fails for them and the http fallback
    # picks them up, which is the honest outcome -- turning verification off
    # to paper over it would hide exactly the sites worth knowing about.
    ssl_context = ssl.create_default_context()
    # limit_per_host=1 keeps us to a single connection per store, so a big
    # concurrency number never turns into a burst against one small server.
    connector = aiohttp.TCPConnector(limit=args.concurrency, limit_per_host=1,
                                     ttl_dns_cache=300, ssl=ssl_context)
    headers = {"User-Agent": config.USER_AGENT,
               "Accept": "text/html,application/xhtml+xml",
               "Accept-Language": "en;q=0.8,*;q=0.5"}

    queue: asyncio.Queue = asyncio.Queue()
    for row in rows:
        queue.put_nowait(row)

    progress = Progress(len(rows), PROGRESS_EVERY, "domains fetched")
    buffer: list[dict] = []
    lock = asyncio.Lock()

    async def flush() -> None:
        if not buffer:
            return
        batch, buffer[:] = list(buffer), []
        settled = [r for r in batch if not r["inconclusive"]]
        unsettled = [r for r in batch if r["inconclusive"]]
        if settled:
            conn.executemany(UPDATE, settled)
        if unsettled:
            conn.executemany(UPDATE_INCONCLUSIVE, unsettled)
        conn.commit()

    async with aiohttp.ClientSession(timeout=timeout, connector=connector,
                                     headers=headers) as session:
        async def worker() -> None:
            while True:
                try:
                    row = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    record = await check_domain(session, row, args.ignore_robots)
                except Exception as exc:  # one bad host must not kill a worker
                    record = {"domain": row["domain"], "robots_allowed": 1,
                              **EMPTY_FINGERPRINT, "http_status": None,
                              "final_url": None, "response_time_ms": None,
                              "content_length": None,
                              "http_error": f"worker:{type(exc).__name__}",
                              "status": config.STATUS_ERROR,
                              "inconclusive": False}
                record["checked_at"] = dt.datetime.now(dt.timezone.utc).isoformat(
                    timespec="seconds")
                record["max_attempts"] = args.max_attempts
                record["dead_status"] = config.STATUS_DEAD
                async with lock:
                    buffer.append(record)
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
    checked = db.count(conn, "http_checked_at IS NOT NULL")
    heading(f"Stage 3 summary -- {checked:,} domains fetched")
    print(table([(k or "(none)", f"{n:,}") for k, n in db.summarize(conn, "status")],
                ["status", "targets"]))

    live = db.count(conn, f"status = '{config.STATUS_LIVE}'")
    if live:
        print(f"\nPlatform distribution among {live:,} live domains:")
        rows = db.summarize(conn, "platform", f"status = '{config.STATUS_LIVE}'")
        print(table([(k or "(unidentified)", f"{n:,}", f"{100*n/live:.1f}%")
                     for k, n in rows], ["platform", "domains", "share"]))
        signals = [
            ("has cart", db.count(conn, f"status='{config.STATUS_LIVE}' AND has_cart=1")),
            ("has product schema",
             db.count(conn, f"status='{config.STATUS_LIVE}' AND has_product_schema=1")),
            ("both", db.count(conn, f"status='{config.STATUS_LIVE}' "
                                    "AND has_cart=1 AND has_product_schema=1")),
        ]
        print("\nEcommerce liveness signals:")
        print(table([(k, f"{n:,}", f"{100*n/live:.1f}%") for k, n in signals],
                    ["signal", "domains", "share"]))

    retryable = db.count(conn, "is_freemail = 0 AND dns_resolves = 1 "
                               "AND http_checked_at IS NULL AND http_attempts > 0")
    if retryable:
        print(f"\n{retryable:,} domains only ever timed out and were left "
              f"unsettled rather than recorded as dead. Re-run this stage to "
              f"retry them; a large number here means the concurrency is too "
              f"high for the network path.")

    errors = conn.execute(
        "SELECT http_error, COUNT(*) n FROM targets WHERE http_error IS NOT NULL "
        "GROUP BY 1 ORDER BY n DESC LIMIT 10").fetchall()
    if errors:
        print("\nTop fetch errors:")
        print(table([(r["http_error"], f"{r['n']:,}") for r in errors],
                    ["error", "domains"]))


def main(argv=None) -> int:
    args = parse_args(argv)
    conn = db.connect(args.db)

    aged = age_out(conn, args.min_year)
    if aged:
        print(f"  held {aged:,} pre-{args.min_year} contacts for the nurture tier "
              f"(not crawled)")

    rows = pending_rows(conn, args)
    heading(f"Stage 3 -- fetching {len(rows):,} domains "
            f"(concurrency {args.concurrency}, 1 connection per host, "
            f"{config.MAX_BODY_BYTES // 1024}KB body cap)")
    print(f"  identifying as: {config.USER_AGENT}")
    if rows:
        asyncio.run(run(conn, rows, args))
    else:
        print("  nothing to do -- all resolvable domains already fetched "
              "(use --force to re-check)")
    print_summary(conn)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
