"""Locate and download RIPE RIS MRT files (bviews and updates).

RIS layout: {base}/{collector}/{YYYY.MM}/bview.YYYYMMDD.HHMM.gz   (every 8h: 00, 08, 16 UTC)
            {base}/{collector}/{YYYY.MM}/updates.YYYYMMDD.HHMM.gz (every 5 min)
"""
from __future__ import annotations

import argparse
import gzip
import re
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

DEFAULT_BASE = "https://data.ris.ripe.net"
BVIEW_RE = re.compile(r"bview\.(\d{8})\.(\d{4})\.gz")


def _month_dirs(now: datetime) -> list[str]:
    """Current month and previous month (a bview just after midnight on the 1st lives in the new month)."""
    prev = (now.replace(day=1) - timedelta(days=1))
    return [now.strftime("%Y.%m"), prev.strftime("%Y.%m")]


def _http_get_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "aspa-validation/0.1"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", "replace")


def latest_bview(collector: str, base: str = DEFAULT_BASE, now: datetime | None = None) -> tuple[str, datetime]:
    """Return (url, timestamp) of the newest bview published for a collector."""
    now = now or datetime.now(timezone.utc)
    for month in _month_dirs(now):
        index_url = f"{base}/{collector}/{month}/"
        try:
            listing = _http_get_text(index_url)
        except Exception as exc:  # month dir may not exist yet
            print(f"warn: cannot list {index_url}: {exc}", file=sys.stderr)
            continue
        found = sorted(
            (datetime.strptime(d + t, "%Y%m%d%H%M").replace(tzinfo=timezone.utc), d, t)
            for d, t in set(BVIEW_RE.findall(listing)))
        found = [f for f in found if f[0] <= now]  # newest bview at or before `now`
        if found:
            ts, d, t = found[-1]
            return f"{index_url}bview.{d}.{t}.gz", ts
    raise RuntimeError(f"no bview found for {collector}")


UPDATES_RE = re.compile(r"updates\.(\d{8})\.(\d{4})\.gz")
UPDATE_INTERVAL = timedelta(minutes=5)


def list_updates(collector: str, start: datetime, end: datetime,
                 base: str = DEFAULT_BASE) -> tuple[list[tuple[str, datetime]], list[datetime]]:
    """Update files covering [start, end), in time order.

    A file named updates.YYYYMMDD.HHMM.gz holds messages received in [HHMM, HHMM+5min).
    Returns (available [(url, file_start)], missing [file_start]) where missing are slots that
    should exist (fully before `end`) but are not published.
    """
    months = sorted({(start + timedelta(days=d)).strftime("%Y.%m")
                     for d in range(0, (end - start).days + 2)})
    published: dict[datetime, str] = {}
    for month in months:
        index_url = f"{base}/{collector}/{month}/"
        try:
            listing = _http_get_text(index_url)
        except Exception as exc:
            print(f"warn: cannot list {index_url}: {exc}", file=sys.stderr)
            continue
        for d, t in set(UPDATES_RE.findall(listing)):
            ts = datetime.strptime(d + t, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
            published[ts] = f"{index_url}updates.{d}.{t}.gz"

    available, missing = [], []
    slot = start
    while slot + UPDATE_INTERVAL <= end:
        if slot in published:
            available.append((published[slot], slot))
        else:
            missing.append(slot)
        slot += UPDATE_INTERVAL
    # Trailing missing slots are just "not published yet": stop at the last available file.
    last = available[-1][1] if available else start
    missing_gaps = [m for m in missing if m < last]
    return available, missing_gaps


def download(url: str, dest: Path, retries: int = 5) -> Path:
    """Download with resume + retries, then verify gzip integrity. Skips if a valid file already exists."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "aspa-validation/0.1"})
    with urllib.request.urlopen(req, timeout=60) as r:
        expected = int(r.headers.get("Content-Length", 0))

    for attempt in range(1, retries + 1):
        have = dest.stat().st_size if dest.exists() else 0
        if expected and have == expected:
            break
        if have > expected:
            dest.unlink()
            have = 0
        headers = {"User-Agent": "aspa-validation/0.1"}
        if have:
            headers["Range"] = f"bytes={have}-"
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=120) as r, \
                    open(dest, "ab" if have else "wb") as f:
                while chunk := r.read(1 << 20):
                    f.write(chunk)
        except Exception as exc:
            print(f"warn: download attempt {attempt} failed: {exc}", file=sys.stderr)
            time.sleep(min(30, 2 ** attempt))

    if expected and dest.stat().st_size != expected:
        raise RuntimeError(f"incomplete download {dest}: {dest.stat().st_size}/{expected} bytes")
    _verify_gzip(dest)
    return dest


def _verify_gzip(path: Path) -> None:
    with gzip.open(path, "rb") as f:
        while f.read(1 << 24):
            pass


def main() -> None:
    ap = argparse.ArgumentParser(description="Download the latest RIS bview for a collector")
    ap.add_argument("collector")
    ap.add_argument("--raw-dir", default="data/raw")
    ap.add_argument("--base", default=DEFAULT_BASE)
    args = ap.parse_args()

    url, ts = latest_bview(args.collector, args.base)
    dest = Path(args.raw_dir) / args.collector / url.rsplit("/", 1)[1]
    print(f"latest bview: {url} ({ts.isoformat()})")
    download(url, dest)
    print(dest)


if __name__ == "__main__":
    main()
