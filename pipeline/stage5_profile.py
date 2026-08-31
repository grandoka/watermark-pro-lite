"""Stage 5 -- profile live shops for outreach.

Stages 1-4 answer "is this a real shop worth contacting". This one answers
"who do I contact, and what do I say to them", which is what turns a list into
leads:

  * **A LinkedIn account** -- personal profile in preference to a company page,
    taken from wherever the shop publishes it: footer, header, or the
    legal-notice page, which is scanned for free since it is fetched anyway.
  * **A named human** -- from the legal-notice page. EU e-commerce law requires
    the operator's name on it, so for a European shop this beats LinkedIn
    outright: a family-run shop with no LinkedIn presence at all still has to
    publish an Impressum.
  * **An image audit** -- how many images the home page serves, how many carry
    no alt text, whether they are lazy-loaded, responsive, modern formats or
    off a CDN, plus the byte size of one real product image. These are the
    findings a watermarking pitch is actually about.

    python -m pipeline.stage5_profile [--limit N] [--force] [--tier tier1_email]

Same contract as the other stages: at most two page fetches plus one HEAD per
shop, robots.txt honoured for every path, 200KB read cap, batched commits, and
resume by skipping rows that already have `profile_checked_at`.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import re
import ssl
import urllib.robotparser
from urllib.parse import urljoin, urlsplit

import aiohttp

from . import config, db
from .cli import Progress, base_parser, heading, table
from .stage3_http import ROBOTS_TIMEOUT, candidate_urls, fetch

BATCH = 100
PROGRESS_EVERY = 1000

# Images that are furniture rather than product photography.
CHROME_IMAGE = re.compile(
    r"(logo|icon|sprite|badge|payment|visa|mastercard|paypal|klarna|flag|avatar"
    r"|placeholder|spinner|loader|pixel|1x1|blank|spacer|arrow|star|rating"
    r"|social|facebook|instagram|whatsapp|cookie|banner-?ad)", re.I)


def parse_args(argv=None):
    p = base_parser(__doc__.strip().splitlines()[0])
    p.add_argument("--concurrency", type=int, default=50,
                   help="in-flight shops (default: %(default)s)")
    p.add_argument("--connect-timeout", type=float, default=10.0)
    p.add_argument("--read-timeout", type=float, default=15.0)
    p.add_argument("--max-attempts", type=int, default=3,
                   help="give up on a shop that only ever times out after this "
                        "many runs (default: %(default)s)")
    p.add_argument("--tld", default=None,
                   help="restrict to these TLDs, comma separated (e.g. de,at,ch). "
                        "The legal-notice route only pays off in the EU, so it is "
                        "worth aiming the run.")
    p.add_argument("--tier", default=None,
                   help="profile only one tier (e.g. tier1_email); default is "
                        "every live shop")
    p.add_argument("--no-image-probe", action="store_true",
                   help="skip the HEAD request that sizes one product image")
    return p.parse_args(argv)


# --- robots ----------------------------------------------------------------

async def load_robots(session, origin: str):
    """Parse robots.txt once per origin so several paths can be checked.

    Returns (parser | None, temporary_failure). A parser of None with
    temporary_failure False means "no rules published, crawl freely".
    """
    result = await fetch(session, f"{origin}/robots.txt", retry_timeout=False,
                         timeout=ROBOTS_TIMEOUT)
    if result["error"]:
        return None, False
    if result["status"] and 500 <= result["status"] < 600:
        return None, True          # unavailable: back off, ask again later
    if result["status"] and result["status"] >= 400:
        return None, False         # no rules
    parser = urllib.robotparser.RobotFileParser()
    try:
        parser.parse(result["body"].decode("utf-8", "ignore").splitlines())
    except Exception:
        return None, False
    return parser, False


def allowed(parser, url: str) -> bool:
    return True if parser is None else parser.can_fetch(config.USER_AGENT, url)


# --- extraction ------------------------------------------------------------

def extract_linkedin(html: str) -> dict:
    """The shop's LinkedIn account, or nothing.

    A personal profile beats a company page: the pitch goes to a human, and a
    company page is another hop before you have someone to write to. Patterns
    are tried in that order.
    """
    for kind, pattern in config.LINKEDIN_PATTERNS:
        for handle in pattern.findall(html):
            slug = handle.strip("/")
            if not slug or slug.split("/")[0].lower() in config.LINKEDIN_NOISE:
                continue
            if kind == "personal":
                path = "in" if "/" not in slug else "pub"
                return {"linkedin_url": f"https://www.linkedin.com/{path}/{slug}",
                        "linkedin_kind": "personal"}
            if kind == "company":
                return {"linkedin_url": f"https://www.linkedin.com/company/{slug}",
                        "linkedin_kind": "company"}
            return {"linkedin_url": f"https://lnkd.in/{slug}",
                    "linkedin_kind": "short"}
    return {"linkedin_url": None, "linkedin_kind": None}


def attrs_of(tag: str) -> dict:
    """Attribute dict for one HTML tag."""
    return {m.group(1).lower(): (m.group(2) or m.group(3) or m.group(4) or "")
            for m in config.ATTR_RE.finditer(tag)}


def audit_images(html: str, base_url: str) -> dict:
    """Count the image practices a watermarking pitch is built on."""
    host = urlsplit(base_url).netloc.lower().removeprefix("www.")
    total = no_alt = lazy = srcset = offsite = modern = 0
    sample = None
    for tag in config.IMG_TAG_RE.findall(html):
        a = attrs_of(tag)
        src = (a.get("src") or a.get("data-src") or a.get("data-lazy-src") or "").strip()
        if not src or src.startswith("data:"):
            continue
        total += 1
        if not a.get("alt", "").strip():
            no_alt += 1
        if a.get("loading", "").lower() == "lazy" or "lazy" in a.get("class", "").lower():
            lazy += 1
        if a.get("srcset") or a.get("data-srcset"):
            srcset += 1

        absolute = urljoin(base_url, src)
        src_host = urlsplit(absolute).netloc.lower().removeprefix("www.")
        if src_host and src_host != host:
            offsite += 1
        path = urlsplit(absolute).path.lower()
        if path.endswith(config.MODERN_IMAGE_EXT):
            modern += 1
        # One real product photo to weigh: skip logos, icons and payment marks.
        if (sample is None and path.endswith(config.RASTER_IMAGE_EXT)
                and not CHROME_IMAGE.search(absolute)):
            sample = absolute
    return {"image_count": total, "images_no_alt": no_alt, "images_lazy": lazy,
            "images_srcset": srcset, "images_offsite": offsite,
            "images_modern": modern, "sample_image_url": sample,
            "has_og_image": int(bool(config.OG_IMAGE_RE.search(html))),
            "has_meta_desc": int(bool(config.META_DESC_RE.search(html)))}


def find_legal_url(html: str, base_url: str) -> str | None:
    match = config.LEGAL_PAGE_RE.search(html)
    return urljoin(base_url, match.group(1)) if match else None


def strip_tags(html: str) -> str:
    text = re.sub(r"<(script|style)\b[^>]*>.*?</\1>", " ", html, flags=re.I | re.S)
    text = re.sub(r"<br\s*/?>|</p>|</div>|</li>|</tr>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    return re.sub(r"[ \t]{2,}", " ", text)


# Lowercase surname particles, as a trailing-word check. A name that ends on
# one has been cut short by the line ending, and the particle is noise.
TRAILING_PARTICLE = re.compile(
    r"[ \-](?:de|del|della|di|da|dos|du|van|von|der|den|ter|te|le|la|el|al)$", re.I)


def extract_owner(html: str) -> str | None:
    """The operator's name from a legal-notice page, if one is stated."""
    for candidate in config.OWNER_LABELS.findall(strip_tags(html)):
        words = candidate.split()
        # Drop leading honorifics and job titles: the pattern captures
        # "Ministerialdirektor Hubert Bittlmayer" whole.
        while words and words[0].strip(".,").lower() in config.OWNER_TITLES:
            words.pop(0)
        name = TRAILING_PARTICLE.sub("", " ".join(words)).strip()
        if not name or " " not in name:
            continue
        # A company form is not a person to write to.
        if any(w.strip(".,").lower() in config.OWNER_STOPWORDS for w in name.split()):
            continue
        if len(name) < 5 or len(name) > 70:
            continue
        return name
    return None


# --- per-shop work ---------------------------------------------------------

EMPTY = {"linkedin_url": None, "linkedin_kind": None, "image_count": None,
         "images_no_alt": None, "images_lazy": None, "images_srcset": None,
         "images_offsite": None, "images_modern": None, "has_og_image": None,
         "has_meta_desc": None, "sample_image_url": None,
         "sample_image_bytes": None, "sample_image_type": None,
         "legal_url": None, "owner_name": None, "owner_source": None}


async def profile(session, row, probe_image: bool) -> dict:
    record = {"domain": row["domain"], **EMPTY, "profile_error": None,
              "inconclusive": False}

    # Stage 3 already worked out which URL answers; start from it.
    start = row["final_url"] or candidate_urls(row["domain"], row["a_host"])[0]
    origin = "{0}://{1}".format(*urlsplit(start)[:2])

    parser, robots_unavailable = await load_robots(session, origin)
    if robots_unavailable:
        record.update(profile_error="robots_unavailable", inconclusive=True)
        return record
    if not allowed(parser, start):
        record["profile_error"] = "robots_disallow"
        return record

    home = await fetch(session, start)
    if home["status"] is None:
        record.update(profile_error=home["error"],
                      inconclusive=(home["error"] == "timeout"))
        return record
    if home["status"] != 200:
        record["profile_error"] = f"http_{home['status']}"
        return record

    html = home["body"].decode("utf-8", "ignore")
    base = home["final_url"] or start
    record.update(extract_linkedin(html))
    record.update(audit_images(html, base))
    record["legal_url"] = find_legal_url(html, base)

    # The legal notice: for an EU shop this is where the owner's name is.
    if record["legal_url"] and allowed(parser, record["legal_url"]):
        legal = await fetch(session, record["legal_url"], retry_timeout=False)
        if legal["status"] == 200:
            legal_html = legal["body"].decode("utf-8", "ignore")
            name = extract_owner(legal_html)
            if name:
                record.update(owner_name=name, owner_source="legal_notice")
            # The page is already in hand, so scan it for a LinkedIn account
            # too -- shops that put one nowhere else often put it here.
            if not record["linkedin_url"]:
                record.update(extract_linkedin(legal_html))

    # Weigh one real product image. HEAD, so nothing is downloaded.
    if probe_image and record["sample_image_url"] and allowed(parser, record["sample_image_url"]):
        head = await head_request(session, record["sample_image_url"])
        record["sample_image_bytes"] = head.get("bytes")
        record["sample_image_type"] = head.get("type")
    return record


async def head_request(session, url: str) -> dict:
    try:
        async with session.head(url, allow_redirects=True,
                                timeout=ROBOTS_TIMEOUT) as response:
            if response.status != 200:
                return {}
            length = response.headers.get("Content-Length")
            return {"bytes": int(length) if length and length.isdigit() else None,
                    "type": (response.headers.get("Content-Type") or "")[:60] or None}
    except Exception:
        return {}


# --- persistence -----------------------------------------------------------

FIELDS = [k for k in EMPTY] + ["profile_error"]

UPDATE = (
    "UPDATE targets SET " +
    ", ".join(f"{f} = :{f}" for f in FIELDS) +
    ", profile_attempts = profile_attempts + 1, profile_checked_at = :checked_at "
    "WHERE domain = :domain AND is_freemail = 0")

UPDATE_INCONCLUSIVE = """
UPDATE targets
   SET profile_error    = :profile_error,
       profile_attempts = profile_attempts + 1,
       profile_checked_at = CASE WHEN profile_attempts + 1 >= :max_attempts
                                 THEN :checked_at END
 WHERE domain = :domain AND is_freemail = 0
"""


def pending_rows(conn, args) -> list:
    sql = ("SELECT domain, final_url, a_host FROM targets "
           f"WHERE status = '{config.STATUS_LIVE}'")
    params: list = []
    if args.tier:
        sql += " AND tier = ?"
        params.append(args.tier)
    if args.tld:
        tlds = [t.strip().lower() for t in args.tld.split(",") if t.strip()]
        sql += " AND tld IN (" + ",".join("?" * len(tlds)) + ")"
        params.extend(tlds)
    if not args.force:
        sql += " AND profile_checked_at IS NULL"
    sql += " ORDER BY score DESC, domain"
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"
    rows = conn.execute(sql, params).fetchall()
    if args.force and rows:
        conn.executemany("UPDATE targets SET profile_attempts = 0 WHERE domain = ?",
                         [(r["domain"],) for r in rows])
        conn.commit()
    return rows


async def run(conn, rows, args) -> None:
    timeout = aiohttp.ClientTimeout(connect=args.connect_timeout,
                                    sock_read=args.read_timeout,
                                    total=args.connect_timeout + args.read_timeout)
    connector = aiohttp.TCPConnector(limit=args.concurrency, limit_per_host=1,
                                     ttl_dns_cache=300, ssl=ssl.create_default_context())
    headers = {"User-Agent": config.USER_AGENT,
               "Accept": "text/html,application/xhtml+xml",
               "Accept-Language": "en;q=0.8,*;q=0.5"}

    queue: asyncio.Queue = asyncio.Queue()
    for row in rows:
        queue.put_nowait(row)
    progress = Progress(len(rows), PROGRESS_EVERY, "shops profiled")
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
                    record = await profile(session, row, not args.no_image_probe)
                except Exception as exc:
                    record = {"domain": row["domain"], **EMPTY, "inconclusive": False,
                              "profile_error": f"worker:{type(exc).__name__}"}
                record["checked_at"] = dt.datetime.now(dt.timezone.utc).isoformat(
                    timespec="seconds")
                record["max_attempts"] = args.max_attempts
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
    done = db.count(conn, "profile_checked_at IS NOT NULL")
    heading(f"Stage 5 summary -- {done:,} shops profiled")
    if not done:
        return
    rows = [
        ("LinkedIn (any)", db.count(conn, "linkedin_url IS NOT NULL")),
        ("  a named person", db.count(conn, "linkedin_kind = 'personal'")),
        ("  a company page", db.count(conn, "linkedin_kind = 'company'")),
        ("  an lnkd.in short link", db.count(conn, "linkedin_kind = 'short'")),
        ("legal-notice page found", db.count(conn, "legal_url IS NOT NULL")),
        ("OWNER NAMED", db.count(conn, "owner_name IS NOT NULL")),
        ("reachable by name (owner or LinkedIn person)",
         db.count(conn, "owner_name IS NOT NULL OR linkedin_kind = 'personal'")),
    ]
    print(table([(k, f"{n:,}", f"{100*n/done:.1f}%") for k, n in rows],
                ["contact route", "shops", "share"]))

    img = db.count(conn, "image_count IS NOT NULL AND image_count > 0")
    if img:
        signals = [
            ("images with no alt text", db.count(conn, "images_no_alt > 0")),
            ("no responsive srcset at all", db.count(conn, "image_count > 0 AND images_srcset = 0")),
            ("no lazy loading at all", db.count(conn, "image_count > 0 AND images_lazy = 0")),
            ("no modern format (webp/avif)", db.count(conn, "image_count > 0 AND images_modern = 0")),
            ("images served from own origin", db.count(conn, "images_offsite = 0 AND image_count > 0")),
            ("no og:image for sharing", db.count(conn, "has_og_image = 0")),
            ("no meta description", db.count(conn, "has_meta_desc = 0")),
            ("sample product image over 500KB",
             db.count(conn, "sample_image_bytes > 512000")),
        ]
        print(f"\nImage-audit findings across {img:,} shops with images:")
        print(table([(k, f"{n:,}", f"{100*n/img:.1f}%") for k, n in signals],
                    ["finding", "shops", "share"]))

    errors = conn.execute(
        "SELECT profile_error, COUNT(*) n FROM targets WHERE profile_error IS NOT NULL "
        "GROUP BY 1 ORDER BY n DESC LIMIT 8").fetchall()
    if errors:
        print("\nCould not profile:")
        print(table([(r["profile_error"], f"{r['n']:,}") for r in errors],
                    ["reason", "shops"]))


def main(argv=None) -> int:
    args = parse_args(argv)
    conn = db.connect(args.db)
    rows = pending_rows(conn, args)
    heading(f"Stage 5 -- profiling {len(rows):,} live shops "
            f"(concurrency {args.concurrency}, 1 connection per host)")
    if rows:
        asyncio.run(run(conn, rows, args))
    else:
        print("  nothing to do -- all matching shops already profiled "
              "(use --force to re-check)")
    print_summary(conn)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
