"""Build a routing snapshot for each configured (collector, peer) source.

Steps: latest bview (or --mrt) -> apply 5-minute update files up to --until (default: newest
published) -> write, in output/<collector>_AS<peer>/<as-of YYYYMMDD.HHMM>/:
  routes.tsv.gz     one row per route (prefix as seen from the peer)
  paths.tsv.gz      one row per unique raw AS_PATH, with cleaned form and flags
  adjacency.tsv.gz  (asn, upstream_asn) pairs seen in cleaned paths
  meta.json         source, update stats, counts, flag statistics
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
import time
import tomllib
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from pipeline.ris.fetch import BVIEW_RE, UPDATE_INTERVAL, download, latest_bview, list_updates
from pipeline.ris.normalize import FLAGS, CleanPath, normalize
from pipeline.ris.rib import Rib, apply_updates, diff_ribs, load_bview, load_state, save_state, state_path

ROUTE_COLUMNS = ["afi", "prefix", "peer_ip", "peer_asn", "path_id", "origin_asn", "communities",
                 "otc", "aggr_asn", "aggr_ip", "atomic", "timestamp"]
PATH_COLUMNS = ["path_id", "path", "raw_path", "length", "origin_asn", "flags", "set_members",
                "routes_v4", "routes_v6"]
ADJ_COLUMNS = ["asn", "upstream_asn", "paths", "routes_v4", "routes_v6"]
TS_FMT = "%Y%m%d.%H%M"
SCHEMA_VERSION = 1  # bump when output columns or meta layout change


def path_id(raw: str) -> str:
    """Stable id for a raw AS_PATH, so the same path keeps its id across snapshots."""
    return hashlib.blake2b(raw.encode(), digest_size=8).hexdigest()


def _cell(v) -> str:
    if v is None or v is False:
        return ""
    if v is True:
        return "1"
    return str(v)


def _write_tsv(path: Path, header: list[str], rows) -> int:
    n = 0
    with gzip.open(path, "wt", compresslevel=6, newline="") as f:
        f.write("\t".join(header) + "\n")
        for row in rows:
            f.write("\t".join(_cell(v) for v in row) + "\n")
            n += 1
    return n


def write_snapshot(rib: Rib, out_dir: Path, peer_asn: int, meta: dict) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    paths: dict[str, tuple[str, CleanPath]] = {}
    path_routes: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    session_routes: Counter = Counter()

    def route_rows():
        for (p_ip, prefix), r in sorted(rib.items(), key=lambda kv: (":" in kv[0][1], kv[0])):
            raw = r.as_path or ""
            entry = paths.get(raw)
            if entry is None:
                entry = paths[raw] = (path_id(raw), normalize(raw, peer_asn))
            pid, cp = entry
            afi = 6 if ":" in prefix else 4
            path_routes[pid][afi == 6] += 1
            session_routes[f"{p_ip} (IPv{afi})"] += 1
            yield (afi, prefix, p_ip, r.peer_asn, pid, cp.origin,
                   " ".join(r.communities) if r.communities else "",
                   r.otc, r.aggr_asn, r.aggr_ip, r.atomic, int(r.timestamp))

    n_routes = _write_tsv(out_dir / "routes.tsv.gz", ROUTE_COLUMNS, route_rows())

    n_paths = _write_tsv(out_dir / "paths.tsv.gz", PATH_COLUMNS, (
        (pid, cp.text, raw, cp.length, cp.origin, ",".join(f for f in FLAGS if f in cp.flags),
         ",".join(map(str, cp.set_members)), *path_routes[pid])
        for raw, (pid, cp) in paths.items()))

    adj_paths: dict[tuple[int, int], set[str]] = defaultdict(set)
    adj_routes: dict[tuple[int, int], list[int]] = defaultdict(lambda: [0, 0])
    for pid, cp in paths.values():
        v4, v6 = path_routes[pid]
        for pair in set(cp.adjacencies):
            adj_paths[pair].add(cp.text)
            adj_routes[pair][0] += v4
            adj_routes[pair][1] += v6
    n_adj = _write_tsv(out_dir / "adjacency.tsv.gz", ADJ_COLUMNS,
                       ((a, u, len(adj_paths[(a, u)]), *adj_routes[(a, u)]) for a, u in sorted(adj_paths)))

    flag_paths, flag_routes, lengths, origins = Counter(), Counter(), Counter(), set()
    for pid, cp in paths.values():
        r = sum(path_routes[pid])
        lengths[cp.length] += r
        if cp.origin is not None:
            origins.add(cp.origin)
        for f in cp.flags:
            flag_paths[f] += 1
            flag_routes[f] += r

    meta.update({
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "counts": {
            "routes": n_routes,
            "routes_by_session": dict(sorted(session_routes.items())),
            "unique_raw_paths": n_paths,
            "unique_clean_paths": len({cp.text for _, cp in paths.values()}),
            "origin_asns": len(origins),
            "adjacencies": n_adj,
            "asns_with_observed_upstream": len({a for a, _ in adj_paths}),
            "routes_with_any_flag": sum(sum(path_routes[pid]) for pid, cp in paths.values() if cp.flags),
        },
        "flags": {f: {"paths": flag_paths[f], "routes": flag_routes[f]} for f in FLAGS},
        "path_length_by_routes": dict(sorted(lengths.items())),
        "files_bytes": {n: (out_dir / n).stat().st_size
                        for n in ("routes.tsv.gz", "paths.tsv.gz", "adjacency.tsv.gz")},
    })
    meta.setdefault("timing_seconds", {})["write"] = round(time.time() - t0, 1)
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2))
    return out_dir


def _parse_ts(s: str) -> datetime:
    return datetime.strptime(s, TS_FMT).replace(tzinfo=timezone.utc)


def plan_run(state_info: dict | None, latest_bview_ts: datetime, source: dict) -> tuple[str, str]:
    """Decide whether this run starts fresh from the latest bview or continues the saved state."""
    if state_info is None:
        return "fresh", "no saved state"
    if (state_info.get("peer_asn"), sorted(state_info.get("peer_ips", []))) != \
            (source["peer_asn"], sorted(source["peer_ips"])):
        return "fresh", "saved state is for a different peer"
    state_bview = datetime.fromisoformat(state_info["bview_time"])
    if latest_bview_ts > state_bview:
        return "fresh", f"new bview {latest_bview_ts:%Y%m%d.%H%M}"
    return "continue", f"saved state as of {state_info['as_of']}"


def fetch_and_apply(rib: Rib, cfg: dict, source: dict, start: datetime, end: datetime,
                    timing: dict, label: str) -> tuple[datetime, dict]:
    """Download update files covering [start, end) and apply them. Returns (as_of, meta)."""
    base, raw_dir = cfg["ris"]["base_url"], Path(cfg["ris"]["raw_dir"])
    collector, peer_asn, peer_ips = source["collector"], source["peer_asn"], source["peer_ips"]
    available, gaps = list_updates(collector, start, end, base)
    upd_dir = raw_dir / collector / "updates"
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=8) as pool:
        files = list(pool.map(lambda u: download(u[0], upd_dir / u[0].rsplit("/", 1)[1]), available))
    timing[f"download_updates_{label}"] = round(time.time() - t0, 1)
    print(f"  [{label}] applying {len(files)} update files ({len(gaps)} missing)", file=sys.stderr)
    stats = apply_updates(rib, files, peer_ips, peer_asn)
    timing[f"apply_updates_{label}"] = stats.pop("seconds")
    as_of = available[-1][1] + UPDATE_INTERVAL if available else start
    return as_of, {
        "first_file": available[0][0] if available else None,
        "last_file": available[-1][0] if available else None,
        "files": len(available),
        "missing_files": [f"{m:%Y%m%d.%H%M}" for m in gaps],
        "stats": dict(stats),
    }


def run_source(cfg: dict, source: dict, mrt_arg: str | None, until: datetime | None,
               bview_only: bool, state_dir: Path | None = None) -> Path:
    base, raw_dir = cfg["ris"]["base_url"], Path(cfg["ris"]["raw_dir"])
    collector, peer_asn, peer_ips = source["collector"], source["peer_asn"], source["peer_ips"]
    t_start = time.time()
    end = until or datetime.now(timezone.utc)
    timing: dict = {}

    # 1. Where to start: an explicit bview, the latest bview, or the saved state.
    if mrt_arg:
        mrt = Path(mrt_arg)
        d, t = BVIEW_RE.search(mrt.name).groups()
        bview_ts = _parse_ts(f"{d}.{t}")
        bview_url = f"{base}/{collector}/{bview_ts:%Y.%m}/{mrt.name}"
    else:
        bview_url, bview_ts = latest_bview(collector, base, now=end)
        mrt = None

    saved = None
    spath = state_path(state_dir, collector, peer_asn) if state_dir else None
    if spath and not mrt_arg:
        saved = load_state(spath)
    mode, reason = plan_run(saved[1] if saved else None, bview_ts, source)
    if mrt_arg:
        mode, reason = "fresh", "bview given with --mrt"
    print(f"{collector} AS{peer_asn}: {mode} ({reason})", file=sys.stderr)

    self_check = None
    if mode == "continue":
        rib, info = saved
        start = datetime.fromisoformat(info["as_of"])
        bview_routes = info.get("bview_routes")
    else:
        if mrt is None:
            mrt = download(bview_url, raw_dir / collector / bview_url.rsplit("/", 1)[1])
        rib, bview_stats = load_bview(mrt, peer_ips, peer_asn)
        timing["load_bview"] = bview_stats["seconds"]
        bview_routes = bview_stats["routes"]
        print(f"  {bview_routes:,} routes from {mrt.name}", file=sys.stderr)
        start = bview_ts
        # Self-check: bring the previous state up to this bview's time and compare.
        if saved and reason.startswith("new bview"):
            old_rib, old_info = saved
            old_as_of = datetime.fromisoformat(old_info["as_of"])
            reached, _ = fetch_and_apply(old_rib, cfg, source, old_as_of, bview_ts, timing, "self_check")
            if reached >= bview_ts:
                self_check = {"previous_bview": old_info["bview_time"], "compared_at": bview_ts.isoformat(),
                              **diff_ribs(old_rib, rib)}
            else:
                self_check = {"skipped": f"update files up to {bview_ts:%Y%m%d.%H%M} not all available"}
            print(f"  self-check: {self_check}", file=sys.stderr)
            del old_rib

    # 2. Apply updates up to `end`.
    update_meta = {"applied": False}
    as_of = start
    if not bview_only:
        as_of, update_meta = fetch_and_apply(rib, cfg, source, start, end, timing, "run")
        update_meta["applied"] = True

    meta = {
        "schema_version": SCHEMA_VERSION,
        "as_of": as_of.isoformat(),
        "run": {"mode": mode, "reason": reason},
        "source": {"collector": collector, "peer_asn": peer_asn, "peer_ips": peer_ips,
                   "bview_url": bview_url, "bview_time": bview_ts.isoformat(),
                   "bview_routes": bview_routes},
        "updates": update_meta,
        "self_check": self_check,
        "notes": ["BGP session resets are not detected: routes withdrawn while a session was down "
                  "may linger until the next bview (at most 8 hours)."],
        "timing_seconds": timing,
    }
    out_dir = Path(cfg["ris"]["output_dir"]) / f"{collector}_AS{peer_asn}" / as_of.strftime(TS_FMT)
    write_snapshot(rib, out_dir, peer_asn, meta)

    # 3. Save state for the next run.
    if spath:
        t0 = time.time()
        save_state(spath, rib, {"peer_asn": peer_asn, "peer_ips": peer_ips, "bview_time": bview_ts.isoformat(),
                                "as_of": as_of.isoformat(), "bview_routes": bview_routes})
        meta["timing_seconds"]["save_state"] = round(time.time() - t0, 1)
    meta["timing_seconds"]["total"] = round(time.time() - t_start, 1)
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2))
    return out_dir


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config/pipeline.toml")
    ap.add_argument("--mrt", help="local bview file to start from instead of the latest one")
    ap.add_argument("--until", type=_parse_ts, help="apply updates up to this time (YYYYMMDD.HHMM UTC)")
    ap.add_argument("--bview-only", action="store_true", help="do not apply update files")
    ap.add_argument("--output-dir", help="override [ris].output_dir")
    ap.add_argument("--state-dir", type=Path, help="keep the RIB here between runs (continue instead of re-reading the bview)")
    args = ap.parse_args()

    cfg = tomllib.loads(Path(args.config).read_text())
    if args.output_dir:
        cfg["ris"]["output_dir"] = args.output_dir
    for source in cfg["sources"]:
        print(run_source(cfg, source, args.mrt, args.until, args.bview_only, args.state_dir))


if __name__ == "__main__":
    main()
