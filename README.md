# Domain enrichment & segmentation pipeline

Turns scraped ecommerce contact emails (Excel) into a ranked, tiered target
list for cold outreach and paid-ads audiences.

Every stage is a separate module, runs independently, and is safe to re-run.
State lives in one SQLite database (`targets.db`), one row per domain; each
stage reads it, enriches it, and writes back.

```
data/*.xlsx  ->  stage1_ingest  ->  targets.db  ->  stage2_dns  ->  stage3_http  ->  stage4_score  ->  out/*.xlsx
```

## Install

```bash
pip install -r requirements.txt
```

## Running

```bash
python -m pipeline.stage1_ingest                 # read workbooks, dedupe, classify
python -m pipeline.stage2_dns                    # A + MX lookups
python -m pipeline.stage3_http                   # fetch, fingerprint platform
python -m pipeline.stage4_score                  # score, tier, write workbooks

python -m pipeline.status                        # where is the run? (read-only)
```

`status` is safe to run while a stage is in flight, which is the point -- a
full run takes hours, and the first question at any moment is what is done and
what is left.

Flags supported by every stage:

| Flag | Meaning |
|---|---|
| `--limit N` | process at most N rows -- smoke-test before a full run |
| `--resume` | skip work already recorded (**default**) |
| `--force` | re-check rows that were already done |
| `--db PATH` | database location (default `./targets.db`) |

Configuration comes from the environment, never from code:

| Variable | Purpose |
|---|---|
| `PIPELINE_DB`, `PIPELINE_DATA_DIR`, `PIPELINE_OUT_DIR` | paths |
| `CRAWLER_CONTACT_URL`, `CRAWLER_USER_AGENT` | identify the crawler truthfully |
| `NEVERBOUNCE_API_KEY` / `ZEROBOUNCE_API_KEY` | deliverability verification (tier 1 only) |

## Tests

```bash
python -m pytest tests/ -q
```

107 tests. Mostly pure logic -- extraction, classification, dedupe,
fingerprinting, scoring, tiering -- plus end-to-end coverage of stage 1
against a synthetic workbook carrying both input shapes and the messes the
real data contains, and of stage 4's output workbooks. Nothing touches the
network: the DNS resolver and the HTTP fetch are both scripted.

## Stage 1 -- ingest

Reads every sheet of every workbook and scans **all** cells with an email
regex, so neither column positions nor a header row are assumed. Both observed
input shapes work: `created`/`emails` columns, and headerless sheets with
emails spread across up to 14 unnamed columns.

Emails are lowercased, stripped and validated, then **deduplicated by domain**
(not by email), keeping the contact with the most recent `created` date.
Freemail contacts are the exception and are keyed by full address: a mailbox
provider is not a store, and collapsing by domain turned 31,048 consumer
contacts into 129 rows -- 26k of them on gmail.com alone -- which would have
emptied the ads tier before it was built.
Each domain is flagged with `is_freemail`, `is_role`, `tld`, `created_year`
and `jurisdiction` (`EU` / `CASL` / `PECR` / `PERMISSIVE` / `OTHER`).

## Stage 2 -- DNS

Resolves A and MX for every store domain with `dnspython` over asyncio. It runs
before any HTTP work because it is the cheapest filter there is: one UDP round
trip removes a large slice of a scraped list, and crawling a domain that does
not resolve is pure waste. Only transient failures are retried -- NXDOMAIN is
an answer, not an error. When the apex has no A record the `www` host is tried,
so www-only stores are not written off as dead, and the hostname that answered
is recorded for stage 3.

Outcomes: `dead` (nothing resolves), `mx_only` (mail but no website), or
onwards to stage 3.

**A timeout is not an answer.** When neither query comes back with anything --
no records and no authoritative "no such name" -- the domain is left
unresolved for a later pass instead of being written down as dead, and
`dns_attempts` is incremented so an unreachable domain is still given up on
after `--max-attempts` runs. This matters: a first run at concurrency 600
timed out on half its queries and recorded 26,000 real stores as dead. If the
summary reports a large "never answered" residue, the concurrency is too high
for the network path.

Throughput is bounded by the network, not by the concurrency setting. On the
path used here it flattens at ~12.5 domains/s from concurrency 100 upward;
going higher only converts real answers into timeouts.

## Stage 3 -- fetch and fingerprint

One request per resolvable domain, streaming **at most 200KB** of the body --
enough for a platform fingerprint, and flat in memory across hundreds of
thousands of domains. Tries `https://`, then `http://`, then `https://www.`;
retries once on timeout, never on a 4xx. Certificates are verified, so a store
with a broken cert falls back to http rather than being silently accepted.

`robots.txt` is consulted for the origin actually being fetched -- one request
per domain in the common case where https answers, but an http-only store's
rules get read too instead of being skipped because the https origin never
replied. A disallowed root is never requested. A 4xx there means no rules and full
access. A 5xx means *unavailable*, which RFC 9309 treats as back-off rather
than a standing refusal -- most of the time it is a CDN rate-limiting the
crawler, not the site refusing anyone -- so those domains are retried on a
later pass instead of being written off. On the first run this affected 2,784
shops, 5.7% of everything fetched.

Detects Shopify, WooCommerce, BigCommerce, Magento, PrestaShop, Wix,
Squarespace and OpenCart from body and header signatures, plus `has_cart`,
`has_product_schema`, `is_parked` and `lang`.

By default contacts older than `--min-year` (2020) are **not crawled**; they
are held for the nurture tier, which roughly halves the run.

As in stage 2, **a timeout is not an answer.** A refused connection or a TLS
failure is the network answering for the host, so the domain is settled as
dead; a timeout is just as likely to be our own concurrency, so the domain
goes back in the queue with `http_attempts` incremented and is given up on
only after `--max-attempts` runs.

### Concurrency

The default of 50, with one connection per host, is not just politeness -- it
is what the network can actually sustain. Raising it to 200 in testing made
574 of 600 fetches time out, which would have recorded live stores as dead.
Raise it only after checking that the timeout rate stays low.

## Stage 4 -- score and tier

Each live domain scores 0-100:

| Signal | Weight |
|---|---|
| reached a 200 | +5 |
| Shopify / WooCommerce | +35 |
| BigCommerce / Magento / PrestaShop / OpenCart | +22 |
| Wix / Squarespace | +10 |
| `has_cart` | +12 |
| `has_product_schema` | +13 |
| created 2024+ / 2022-23 / 2020-21 | +15 / +12 / +6 |
| `has_mx` | +15 |
| responds in under 2s | +5 |
| `is_role` | -5 |

Parked pages, a missing cart and any non-200 response are disqualifying.
Targets the earlier stages have not reached yet get **no** tier and appear in
no workbook -- "we have not looked" is not the same as "we looked and it was
bad".

### Tiering order

Tiering is a compliance split before it is a quality one. Anything that may
never be cold-emailed goes to tier 2 whatever it scores, so **tier 3 contains
only targets that could legally be emailed later** -- otherwise a low-scoring
German store would sit in the nurture pile until someone decided to "email the
nurture list".

1. not reached by the earlier stages -> no tier, no workbook
2. dead, parked, non-200, or no cart -> `dropped.xlsx`
3. freemail, EU/CASL/other jurisdiction, or no MX -> `tier2_ads.xlsx`
4. not crawled (older than the cutoff), or below `--min-score` -> `tier3_nurture.xlsx`
5. everything left -> `tier1_email.xlsx`

## Outreach policy

* **tier 1** is the only tier that gets cold email.
* **tier 2** email addresses are for paid-audience upload (Meta / Google
  Customer Match, lookalike seeding) only -- never for sending.
* No SMTP mailbox probing, anywhere. Deliverability verification is a paid API
  call on tier 1 only, after tiering (`pipeline/verify.py`).
* `robots.txt` is honoured for the HTTP stage.
