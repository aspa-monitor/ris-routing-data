"""Locate and download MRT files (full table dumps and updates) from RIPE RIS or RouteViews.

RIS layout:        {base}/{collector}/{YYYY.MM}/bview.YYYYMMDD.HHMM.gz          (every 8h: 00, 08, 16 UTC)
                   {base}/{collector}/{YYYY.MM}/updates.YYYYMMDD.HHMM.gz        (every 5 min)
RouteViews layout: {base}/{collector}/bgpdata/{YYYY.MM}/RIBS/rib.YYYYMMDD.HHMM.bz2       (every 2h)
                   {base}/{collector}/bgpdata/{YYYY.MM}/UPDATES/updates.YYYYMMDD.HHMM.bz2 (every 15 min)
                   (route-views2 has no {collector}/ part.)
Only RouteViews RIBs at 00, 08 and 16 UTC are used, so every source starts fresh at the same times.
"""
from __future__ import annotations

import argparse
import bz2
import gzip
import re
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pipeline.ris.timefmt import ist_text

DEFAULT_BASE = "https://data.ris.ripe.net"
# Any full table dump file name, RIS or RouteViews (used for --mrt).
BVIEW_RE = re.compile(r"(?:bview|rib)\.(\d{8})\.(\d{4})\.(?:gz|bz2)")


@dataclass(frozen=True)
class Archive:
    """Where and how one archive publishes its files."""
    kind: str
    dump_prefix: str          # "bview" / "rib"
    ext: str                  # "gz" / "bz2"
    update_interval: timedelta
    dump_hours: tuple | None  # UTC hours of the dumps to use (None: all)

    def dump_dir(self, base: str, collector: str, month: str) -> str:
        if self.kind == "ris":
            return f"{base}/{collector}/{month}/"
        return f"{self._rv_root(base, collector)}/bgpdata/{month}/RIBS/"

    def updates_dir(self, base: str, collector: str, month: str) -> str:
        if self.kind == "ris":
            return f"{base}/{collector}/{month}/"
        return f"{self._rv_root(base, collector)}/bgpdata/{month}/UPDATES/"

    @staticmethod
    def _rv_root(base: str, collector: str) -> str:
        return base if collector == "route-views2" else f"{base}/{collector}"

    @property
    def dump_re(self):
        return re.compile(rf"{self.dump_prefix}\.(\d{{8}})\.(\d{{4}})\.{self.ext}\b")

    @property
    def updates_re(self):
        return re.compile(rf"updates\.(\d{{8}})\.(\d{{4}})\.{self.ext}\b")


RIS = Archive("ris", "bview", "gz", timedelta(minutes=5), None)
ROUTEVIEWS = Archive("routeviews", "rib", "bz2", timedelta(minutes=15), (0, 8, 16))
ARCHIVES = {"ris": RIS, "routeviews": ROUTEVIEWS}


def archive_for(cfg: dict, collector: str) -> tuple[Archive, str]:
    """(archive, base URL) of a collector: [collectors.<name>] archive = "ris" or "routeviews"
    (default: "routeviews" for route-views* collectors, else "ris")."""
    default = "routeviews" if collector.startswith("route-views") else "ris"
    kind = cfg.get("collectors", {}).get(collector, {}).get("archive", default)
    if kind not in ARCHIVES:
        raise ValueError(f"{collector}: unknown archive {kind!r}")
    base = cfg.get(kind, {}).get("base_url") or (DEFAULT_BASE if kind == "ris" else "https://archive.routeviews.org")
    return ARCHIVES[kind], base


def _month_dirs(now: datetime) -> list[str]:
    """Current month and previous month (a bview just after midnight on the 1st lives in the new month)."""
    prev = (now.replace(day=1) - timedelta(days=1))
    return [now.strftime("%Y.%m"), prev.strftime("%Y.%m")]


def _http_get_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "aspa-validation/0.1"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", "replace")


def latest_bview(collector: str, base: str = DEFAULT_BASE, now: datetime | None = None,
                 archive: Archive = RIS) -> tuple[str, datetime]:
    """Return (url, timestamp) of the newest full table dump published for a collector."""
    now = now or datetime.now(timezone.utc)
    for month in _month_dirs(now):
        index_url = archive.dump_dir(base, collector, month)
        try:
            listing = _http_get_text(index_url)
        except Exception as exc:  # month dir may not exist yet
            print(f"warn: cannot list {index_url}: {exc}", file=sys.stderr)
            continue
        found = sorted(
            (datetime.strptime(d + t, "%Y%m%d%H%M").replace(tzinfo=timezone.utc), d, t)
            for d, t in set(archive.dump_re.findall(listing)))
        found = [f for f in found if f[0] <= now  # newest dump at or before `now`
                 and (archive.dump_hours is None or (f[0].hour in archive.dump_hours and f[0].minute == 0))]
        if found:
            ts, d, t = found[-1]
            return f"{index_url}{archive.dump_prefix}.{d}.{t}.{archive.ext}", ts
    raise RuntimeError(f"no bview found for {collector}")


UPDATE_INTERVAL = RIS.update_interval


def list_updates(collector: str, start: datetime, end: datetime, base: str = DEFAULT_BASE,
                 archive: Archive = RIS) -> tuple[list[tuple[str, datetime]], list[datetime]]:
    """Update files covering [start, end), in time order.

    A file named updates.YYYYMMDD.HHMM.<ext> holds messages received in [HHMM, HHMM + interval)
    (5 minutes on RIS, 15 on RouteViews).
    Returns (available [(url, file_start)], missing [file_start]) where missing are slots that
    should exist (fully before `end`) but are not published.
    """
    months = sorted({(start + timedelta(days=d)).strftime("%Y.%m")
                     for d in range(0, (end - start).days + 2)})
    published: dict[datetime, str] = {}
    for month in months:
        index_url = archive.updates_dir(base, collector, month)
        try:
            listing = _http_get_text(index_url)
        except Exception as exc:
            print(f"warn: cannot list {index_url}: {exc}", file=sys.stderr)
            continue
        for d, t in set(archive.updates_re.findall(listing)):
            ts = datetime.strptime(d + t, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
            published[ts] = f"{index_url}updates.{d}.{t}.{archive.ext}"

    available, missing = [], []
    slot = start
    while slot + archive.update_interval <= end:
        if slot in published:
            available.append((published[slot], slot))
        else:
            missing.append(slot)
        slot += archive.update_interval
    # Trailing missing slots are just "not published yet": stop at the last available file.
    last = available[-1][1] if available else start
    missing_gaps = [m for m in missing if m < last]
    return available, missing_gaps


def download(url: str, dest: Path, retries: int = 5) -> Path:
    """Download with resume + retries, then verify gzip/bz2 integrity. Skips if a valid file already exists."""
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
    _verify_compressed(dest)
    return dest


def _verify_compressed(path: Path) -> None:
    opener = bz2.open if path.suffix == ".bz2" else gzip.open
    with opener(path, "rb") as f:
        while f.read(1 << 24):
            pass


def main() -> None:
    ap = argparse.ArgumentParser(description="Download the latest full table dump for a collector")
    ap.add_argument("collector")
    ap.add_argument("--raw-dir", default="data/raw")
    ap.add_argument("--base", help="archive base URL (default: from the archive)")
    args = ap.parse_args()

    archive, base = archive_for({}, args.collector)
    url, ts = latest_bview(args.collector, args.base or base, archive=archive)
    dest = Path(args.raw_dir) / args.collector / url.rsplit("/", 1)[1]
    print(f"latest bview: {url} ({ist_text(ts)})")
    download(url, dest)
    print(dest)


if __name__ == "__main__":
    main()
