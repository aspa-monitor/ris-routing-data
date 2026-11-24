"""Display times in IST.

Internally the pipeline works in UTC: RIPE names its files in UTC and the saved state keeps UTC.
Every time a person sees (meta.json, manifest.json, release names, logs) is written in IST
(Asia/Kolkata, UTC+05:30, no daylight saving).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30), "IST")


def ist_iso(dt: datetime | str) -> str:
    """ISO 8601 in IST with offset, e.g. 2026-11-24T19:55:00+05:30."""
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    return dt.astimezone(IST).isoformat(timespec="seconds")


def ist_text(dt: datetime | str) -> str:
    """Short human form, e.g. 2026-11-24 19:55 IST."""
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M IST")


def now_ist() -> str:
    return ist_iso(datetime.now(timezone.utc))
