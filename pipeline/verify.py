"""Deliverability verification -- stub.

Tier 1 is the only tier that ever gets cold email, and it is the only tier
worth paying to verify. Verification happens here, through a paid API
(NeverBounce or ZeroBounce), *after* tiering.

There is deliberately no SMTP probing anywhere in this pipeline. Connecting to
a stranger's mail server to test whether a mailbox exists is unreliable from
cloud IPs -- catch-all domains answer yes to everything, greylisting answers
"try later", and the main thing it reliably achieves is getting the sending IP
onto a blocklist, which costs far more than the API does.

Not implemented: wire up whichever vendor you have a contract with, then call
`verify_tier1()` after stage 4 and write the results into targets.verify_*.
"""
from __future__ import annotations

import os

from . import db

# Read from the environment; never commit an API key.
NEVERBOUNCE_API_KEY = os.environ.get("NEVERBOUNCE_API_KEY")
ZEROBOUNCE_API_KEY = os.environ.get("ZEROBOUNCE_API_KEY")


def verify_emails(emails: list[str]) -> dict[str, str]:
    """Submit addresses to the deliverability vendor. NOT IMPLEMENTED.

    Should return {email: result}, where result is the vendor's verdict
    normalised to one of: valid, invalid, catchall, disposable, unknown.

    Implementation notes for whoever wires this up:
      * Both vendors bill per address, so send tier 1 only, and only once --
        cache the verdict in targets.verify_result and skip anything already
        verified.
      * Use the bulk/batch endpoint, not the single-address one; per-address
        calls on 80k addresses will be slow and more expensive.
      * Treat `catchall` as its own bucket, not as valid: the domain accepts
        everything, so the address is unproven.
    """
    raise NotImplementedError(
        "Deliverability verification is not implemented. Set NEVERBOUNCE_API_KEY "
        "or ZEROBOUNCE_API_KEY and implement this against the vendor's bulk API.")


def verify_tier1(db_path: str | None = None, limit: int | None = None) -> int:
    """Verify unverified tier 1 addresses and store the verdicts. NOT IMPLEMENTED."""
    conn = db.connect(db_path)
    sql = ("SELECT target_key, email FROM targets "
           "WHERE tier = 'tier1_email' AND verify_result IS NULL")
    if limit:
        sql += f" LIMIT {int(limit)}"
    rows = conn.execute(sql).fetchall()
    if not rows:
        return 0
    results = verify_emails([r["email"] for r in rows])
    conn.executemany(
        "UPDATE targets SET verify_result = :result, verify_status = 'done', "
        "verified_at = datetime('now') WHERE target_key = :key",
        [{"key": r["target_key"], "result": results.get(r["email"], "unknown")}
         for r in rows])
    conn.commit()
    return len(rows)
