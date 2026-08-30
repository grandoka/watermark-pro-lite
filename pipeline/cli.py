"""Argument parsing and small console helpers shared by every stage."""
from __future__ import annotations

import argparse
import sys
import time

from . import config


def base_parser(description: str) -> argparse.ArgumentParser:
    """Parser carrying the flags every stage supports."""
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--db", default=config.DB_PATH,
                   help="SQLite database path (default: %(default)s)")
    p.add_argument("--limit", type=int, default=None, metavar="N",
                   help="process at most N rows -- use for smoke tests")
    p.add_argument("--resume", action="store_true", default=True,
                   help="skip work already recorded (default)")
    p.add_argument("--force", action="store_true",
                   help="re-do work that was already recorded")
    return p


def table(rows, headers) -> str:
    """Render a small aligned text table."""
    rows = [[("" if c is None else str(c)) for c in r] for r in rows]
    headers = [str(h) for h in headers]
    widths = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(c))
    line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    out = [line, "  ".join("-" * w for w in widths)]
    for r in rows:
        out.append("  ".join(
            c.rjust(widths[i]) if i else c.ljust(widths[i]) for i, c in enumerate(r)))
    return "\n".join(out)


def heading(text: str) -> None:
    print(f"\n{text}\n{'=' * len(text)}")


class Progress:
    """Prints a progress line every `every` items with rate and ETA."""

    def __init__(self, total: int, every: int = 1000, label: str = "processed"):
        self.total = total
        self.every = every
        self.label = label
        self.done = 0
        self.start = time.monotonic()

    def tick(self, n: int = 1) -> None:
        before = self.done
        self.done += n
        if self.done // self.every != before // self.every or self.done >= self.total:
            self.emit()

    def emit(self) -> None:
        elapsed = max(time.monotonic() - self.start, 1e-6)
        rate = self.done / elapsed
        remaining = max(self.total - self.done, 0)
        eta = remaining / rate if rate else 0
        pct = (100.0 * self.done / self.total) if self.total else 100.0
        sys.stdout.write(
            f"  {self.done:>7,}/{self.total:,} ({pct:5.1f}%) {self.label} "
            f"| {rate:7.1f}/s | elapsed {_hms(elapsed)} | ETA {_hms(eta)}\n")
        sys.stdout.flush()


def _hms(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:d}:{m:02d}:{s:02d}"
