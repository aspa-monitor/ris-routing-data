"""In-memory RIB for a set of peer sessions: load from a bview, then apply update files.

RIB key:   (peer_ip, prefix)
RIB value: Route tuple (see ROUTE_FIELDS)
"""
from __future__ import annotations

import gzip
import pickle
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Iterable, NamedTuple

import pybgpkit_parser as bgpkit


class Route(NamedTuple):
    timestamp: float
    peer_ip: str
    peer_asn: int
    prefix: str
    as_path: str
    communities: list | None
    atomic: bool
    aggr_asn: int | None
    aggr_ip: str | None
    otc: int | None


ROUTE_FIELDS = ["timestamp", "peer_ip", "peer_asn", "prefix", "as_path", "communities",
                "atomic", "aggr_asn", "aggr_ip", "only_to_customer"]
Rib = dict[tuple[str, str], Route]


def _parser(path: Path, peer_ips: list[str]):
    return bgpkit.Parser(url=str(path), filters={"peer_ips": ",".join(peer_ips)})


def load_bview(mrt: Path, peer_ips: list[str], peer_asn: int) -> tuple[Rib, Counter]:
    rib: Rib = {}
    stats: Counter = Counter()
    t0 = time.time()
    for batch in _parser(mrt, peer_ips).iter_tuple_batches(fields=ROUTE_FIELDS, batch_size=100_000):
        for t in batch:
            if t[2] != peer_asn:
                stats["skipped_peer_asn_mismatch"] += 1
                continue
            r = Route(*t)
            rib[(r.peer_ip, r.prefix)] = r
    stats["routes"] = len(rib)
    stats["seconds"] = round(time.time() - t0, 1)
    return rib, stats


def apply_update_elems(rib: Rib, elems: Iterable[tuple], peer_asn: int, stats: Counter) -> None:
    """Apply (timestamp, elem_type, peer_ip, peer_asn, prefix, as_path, communities,
    atomic, aggr_asn, aggr_ip, otc) tuples in order."""
    for ts, etype, p_ip, p_asn, prefix, as_path, comms, atomic, aggr_asn, aggr_ip, otc in elems:
        if p_asn != peer_asn:
            stats["skipped_peer_asn_mismatch"] += 1
            continue
        key = (p_ip, prefix)
        if etype == "W":
            if rib.pop(key, None) is None:
                stats["withdraw_unknown_prefix"] += 1
            else:
                stats["withdrawals"] += 1
            continue
        new = Route(ts, p_ip, p_asn, prefix, as_path, comms, atomic, aggr_asn, aggr_ip, otc)
        old = rib.get(key)
        if old is None:
            stats["announce_new_prefix"] += 1
        elif old.as_path == new.as_path and old.communities == new.communities:
            stats["announce_duplicate"] += 1
            new = new._replace(timestamp=old.timestamp)  # nothing changed: keep original age
        else:
            stats["announce_replace"] += 1
        rib[key] = new


def apply_update_file(rib: Rib, path: Path, peer_ips: list[str], peer_asn: int, stats: Counter) -> None:
    fields = ["timestamp", "elem_type", "peer_ip", "peer_asn", "prefix", "as_path", "communities",
              "atomic", "aggr_asn", "aggr_ip", "only_to_customer"]
    for batch in _parser(path, peer_ips).iter_tuple_batches(fields=fields, batch_size=50_000):
        apply_update_elems(rib, batch, peer_asn, stats)
    stats["files"] += 1


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
        r = Route(*t)
        rib[(r.peer_ip, r.prefix)] = r
    return rib, data["info"]


def diff_ribs(built: Rib, reference: Rib) -> dict:
    """Compare a RIB built from updates against a reference (a fresh bview)."""
    only_built = built.keys() - reference.keys()
    only_ref = reference.keys() - built.keys()
    path_diff = comm_diff = 0
    for k in built.keys() & reference.keys():
        a, b = built[k], reference[k]
        if a.as_path != b.as_path:
            path_diff += 1
        elif a.communities != b.communities:
            comm_diff += 1
    total = len(built.keys() | reference.keys())
    identical = total - len(only_built) - len(only_ref) - path_diff - comm_diff
    return {"routes_built": len(built), "routes_bview": len(reference), "identical": identical,
            "identical_pct": round(100 * identical / total, 4) if total else 100.0,
            "only_in_built": len(only_built), "only_in_bview": len(only_ref),
            "path_differs": path_diff, "communities_differ": comm_diff}


def apply_updates(rib: Rib, files: list[Path], peer_ips: list[str], peer_asn: int) -> Counter:
    stats: Counter = Counter()
    t0 = time.time()
    for i, path in enumerate(files, 1):
        apply_update_file(rib, path, peer_ips, peer_asn, stats)
        if i % 12 == 0:
            print(f"  applied {i}/{len(files)} update files ({time.time() - t0:.0f}s)", file=sys.stderr)
    stats["seconds"] = round(time.time() - t0, 1)
    return stats
