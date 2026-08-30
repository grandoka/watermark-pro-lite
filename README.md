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
```

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

## Stage 1 -- ingest

Reads every sheet of every workbook and scans **all** cells with an email
regex, so neither column positions nor a header row are assumed. Both observed
input shapes work: `created`/`emails` columns, and headerless sheets with
emails spread across up to 14 unnamed columns.

Emails are lowercased, stripped and validated, then **deduplicated by domain**
(not by email), keeping the contact with the most recent `created` date.
Each domain is flagged with `is_freemail`, `is_role`, `tld`, `created_year`
and `jurisdiction` (`EU` / `CASL` / `PECR` / `PERMISSIVE` / `OTHER`).

## Outreach policy

* **tier 1** is the only tier that gets cold email.
* **tier 2** email addresses are for paid-audience upload (Meta / Google
  Customer Match, lookalike seeding) only -- never for sending.
* No SMTP mailbox probing, anywhere. Deliverability verification is a paid API
  call on tier 1 only, after tiering (`pipeline/verify.py`).
* `robots.txt` is honoured for the HTTP stage.
