"""In-memory RIB for a set of peer sessions: load from a bview, then apply update files.

RIB key:   (peer_ip, prefix)
RIB value: Route tuple (see ROUTE_FIELDS)

Several RIBs (one per source on the same collector) are filled from ONE pass over each MRT
file: MRT files have no index, so reading a bview costs the same for one peer or for all.
"""
from __future__ import annotations

import gzip
import ipaddress
import pickle
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, NamedTuple

import pybgpkit_parser as bgpkit


class Route(NamedTuple):
    timestamp: float
    peer_ip: str
    peer_asn: int
    prefix: str
    as_path: str
    communities: tuple | None
    atomic: bool
    aggr_asn: int | None
    aggr_ip: str | None
    otc: int | None


ROUTE_FIELDS = ["timestamp", "peer_ip", "peer_asn", "prefix", "as_path", "communities",
                "atomic", "aggr_asn", "aggr_ip", "only_to_customer"]
Rib = dict[tuple[str, str], Route]


class _Interner:
    """Keeps one copy of each equal value. Most route fields repeat: every peer carries the same
    prefixes, and paths, community sets and peer addresses recur across routes. Sharing them
    cuts RIB memory by about half."""

    def __init__(self):
        self._seen: dict = {}

    def __call__(self, v):
        return self._seen.setdefault(v, v)

    def communities(self, c):
        if c is None:
            return None
        return self(tuple(self(x) for x in c))


_intern = _Interner()


def reset_interning() -> None:
    """Drop shared values (call between collectors, once their RIBs are gone)."""
    global _intern
    _intern = _Interner()


def make_route(ts, peer_ip, peer_asn, prefix, as_path, communities, atomic, aggr_asn, aggr_ip, otc) -> Route:
    i = _intern
    return Route(ts, i(peer_ip), i(peer_asn), i(prefix), i(as_path), i.communities(communities),
                 atomic, i(aggr_asn), i(aggr_ip), otc)


def _parser(path: Path, peer_ips: list[str]):
    return bgpkit.Parser(url=str(path), filters={"peer_ips": ",".join(peer_ips)})


@dataclass(eq=False)
class Target:
    """One RIB filled from a shared pass over MRT files.

    For update files, only files lying completely inside [start, end) are applied
    (None = open-ended); `applied` records their start times.
    """
    peer_asn: int
    peer_ips: list[str]
    rib: Rib = field(default_factory=dict)
    stats: Counter = field(default_factory=Counter)
    start: datetime | None = None
    end: datetime | None = None
    applied: list[datetime] = field(default_factory=list)


def _norm_ip(ip: str) -> str:
    return ipaddress.ip_address(ip).compressed


class _PeerIndex:
    """peer_ip (as the parser returns it) -> Target. A session may feed only one target."""

    def __init__(self, targets: list[Target]):
        self._by_ip: dict[str, Target] = {}
        for t in targets:
            for ip in t.peer_ips:
                key = _norm_ip(ip)
                if key in self._by_ip:
                    raise ValueError(f"peer session {ip} is used by more than one source")
                self._by_ip[key] = t
        self._seen: dict[str, Target | None] = {}
        self.filter = ",".join(ip for t in targets for ip in t.peer_ips)

    def get(self, ip: str) -> Target | None:
        if ip not in self._seen:
            self._seen[ip] = self._by_ip.get(_norm_ip(ip))
        return self._seen[ip]


def load_bview_multi(mrt: Path, targets: list[Target]) -> float:
    """Fill each target's RIB from one pass over a bview. Returns seconds taken."""
    index = _PeerIndex(targets)
    t0 = time.time()
    for batch in _parser(mrt, index.filter.split(",")).iter_tuple_batches(fields=ROUTE_FIELDS, batch_size=100_000):
        for t in batch:
            tgt = index.get(t[1])
            if tgt is None:
                continue
            if t[2] != tgt.peer_asn:
                tgt.stats["skipped_peer_asn_mismatch"] += 1
                continue
            r = make_route(*t)
            tgt.rib[(r.peer_ip, r.prefix)] = r
    for tgt in targets:
        tgt.stats["routes"] = len(tgt.rib)
    return round(time.time() - t0, 1)


def apply_update_elems(rib: Rib, elems: Iterable[tuple], peer_asn: int, stats: Counter) -> None:
    """Apply (timestamp, elem_type, peer_ip, peer_asn, prefix, as_path, communities,
    atomic, aggr_asn, aggr_ip, otc) tuples in order."""
    for ts, etype, p_ip, p_asn, prefix, as_path, comms, atomic, aggr_asn, aggr_ip, otc in elems:
        if p_asn != peer_asn:
            stats["skipped_peer_asn_mismatch"] += 1
            continue
        if etype == "W":
            key = (p_ip, prefix)
            if rib.pop(key, None) is None:
                stats["withdraw_unknown_prefix"] += 1
            else:
                stats["withdrawals"] += 1
            continue
        new = make_route(ts, p_ip, p_asn, prefix, as_path, comms, atomic, aggr_asn, aggr_ip, otc)
        key = (new.peer_ip, new.prefix)
        old = rib.get(key)
        if old is None:
            stats["announce_new_prefix"] += 1
        elif old.as_path == new.as_path and old.communities == new.communities:
            stats["announce_duplicate"] += 1
            new = new._replace(timestamp=old.timestamp)  # nothing changed: keep original age
        else:
            stats["announce_replace"] += 1
        rib[key] = new


UPDATE_FIELDS = ["timestamp", "elem_type", "peer_ip", "peer_asn", "prefix", "as_path", "communities",
                 "atomic", "aggr_asn", "aggr_ip", "only_to_customer"]


def apply_updates_multi(files: list[tuple[Path, datetime, datetime]], targets: list[Target]) -> float:
    """Apply update files (path, file_start, file_end), in time order, each read once.

    A file goes to every target whose window contains it. Returns seconds taken.
    """
    t0 = time.time()
    for i, (path, f_start, f_end) in enumerate(files, 1):
        active = [t for t in targets
                  if (t.start is None or f_start >= t.start) and (t.end is None or f_end <= t.end)]
        if not active:
            continue
        index = _PeerIndex(active)
        for batch in _parser(path, index.filter.split(",")).iter_tuple_batches(fields=UPDATE_FIELDS,
                                                                                batch_size=50_000):
            buckets: dict[Target, list[tuple]] = defaultdict(list)
            for e in batch:
                tgt = index.get(e[2])
                if tgt is not None:
                    buckets[tgt].append(e)
            for tgt, elems in buckets.items():
                apply_update_elems(tgt.rib, elems, tgt.peer_asn, tgt.stats)
        for t in active:
            t.stats["files"] += 1
            t.applied.append(f_start)
        if i % 12 == 0:
            print(f"  applied {i}/{len(files)} update files ({time.time() - t0:.0f}s)", file=sys.stderr)
    return round(time.time() - t0, 1)


STATE_VERSION = 1


def state_path(state_dir: Path, collector: str, peer_asn: int) -> Path:
    return state_dir / f"{collector}_AS{peer_asn}.state.pkl.gz"


def save_state(path: Path, rib: Rib, info: dict) -> None:
    """Persist the RIB between runs (stored in the GitHub Actions cache, written only by our own job)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with gzip.open(tmp, "wb", compresslevel=3) as f:
        pickle.dump({"version": STATE_VERSION, "info": info, "routes": [tuple(r) for r in rib.values()]},
                    f, protocol=pickle.HIGHEST_PROTOCOL)
    tmp.replace(path)


def load_state(path: Path) -> tuple[Rib, dict] | None:
    if not path.exists():
        return None
    try:
        with gzip.open(path, "rb") as f:
            data = pickle.load(f)
    except Exception as exc:
        print(f"warn: unreadable state {path}: {exc}", file=sys.stderr)
        return None
    if data.get("version") != STATE_VERSION:
        return None
    rib = {}
    for t in data["routes"]:
        r = make_route(*t)  # older states stored communities as lists
        rib[(r.peer_ip, r.prefix)] = r
    return rib, data["info"]


def rib_digest(rib: Rib) -> dict:
    """(peer_ip, prefix) -> (as_path, communities): all diff_ribs needs. The values are shared
    (interned) objects, so this is far smaller than the RIB it came from."""
    return {k: (r.as_path, r.communities) for k, r in rib.items()}


def _as_digest(m: dict) -> dict:
    return rib_digest(m) if m and isinstance(next(iter(m.values())), Route) else m


def diff_ribs(built, reference) -> dict:
    """Compare a RIB built from updates against a reference (a fresh bview). Either side may be
    a Rib or a rib_digest."""
    built, reference = _as_digest(built), _as_digest(reference)
    only_built = built.keys() - reference.keys()
    only_ref = reference.keys() - built.keys()
    path_diff = comm_diff = 0
    for k in built.keys() & reference.keys():
        (a_path, a_comm), (b_path, b_comm) = built[k], reference[k]
        if a_path != b_path:
            path_diff += 1
        elif a_comm != b_comm:
            comm_diff += 1
    total = len(built.keys() | reference.keys())
    identical = total - len(only_built) - len(only_ref) - path_diff - comm_diff
    return {"routes_built": len(built), "routes_bview": len(reference), "identical": identical,
            "identical_pct": round(100 * identical / total, 4) if total else 100.0,
            "only_in_built": len(only_built), "only_in_bview": len(only_ref),
            "path_differs": path_diff, "communities_differ": comm_diff}
