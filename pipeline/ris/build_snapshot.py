"""Build a routing snapshot for each configured (collector, peer) source.

Steps, per collector: latest bview (or --mrt) -> apply 5-minute update files up to --until
(default: newest published) -> write, in output/<collector>_AS<peer>/<as-of YYYYMMDD.HHMM>/:
  routes.tsv.gz     one row per route (prefix as seen from the peer)
  paths.tsv.gz      one row per unique raw AS_PATH, with cleaned form and flags
  adjacency.tsv.gz  (asn, upstream_asn) pairs seen in cleaned paths
  meta.json         source, update stats, counts, flag statistics

All sources on one collector share the work that does not depend on the peer: the bview and
each update file are downloaded and read once, and every source's RIB is filled from that pass.
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
from pipeline.ris.rib import (Rib, Target, apply_updates_multi, diff_ribs, load_bview_multi, load_state,
                             reset_interning, rib_digest, save_state, state_path)
from pipeline.ris.timefmt import IST, ist_iso, ist_text, now_ist

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
        "generated_at": now_ist(),
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
    """RIPE file time stamp (UTC)."""
    return datetime.strptime(s, TS_FMT).replace(tzinfo=timezone.utc)


def _parse_ist(s: str) -> datetime:
    """Time typed by a person (IST)."""
    return datetime.strptime(s, TS_FMT).replace(tzinfo=IST).astimezone(timezone.utc)


def plan_run(state_info: dict | None, latest_bview_ts: datetime, source: dict) -> tuple[str, str]:
    """Decide whether this run starts fresh from the latest bview or continues the saved state."""
    if state_info is None:
        return "fresh", "no saved state"
    if (state_info.get("peer_asn"), sorted(state_info.get("peer_ips", []))) != \
            (source["peer_asn"], sorted(source["peer_ips"])):
        return "fresh", "saved state is for a different peer"
    state_bview = datetime.fromisoformat(state_info["bview_time"])
    if latest_bview_ts > state_bview:
        return "fresh", f"new bview {ist_text(latest_bview_ts)}"
    return "continue", f"saved state as of {ist_text(state_info['as_of'])}"


def source_name(source: dict) -> str:
    return f"{source['collector']}_AS{source['peer_asn']}"


def group_by_collector(sources: list[dict]) -> dict[str, list[dict]]:
    """Sources grouped by collector, keeping config order. Each (collector, AS) may appear once,
    because output, asset and state names are <collector>_AS<peer>."""
    groups: dict[str, list[dict]] = {}
    seen = set()
    for src in sources:
        name = source_name(src)
        if name in seen:
            raise ValueError(f"{name} is configured twice; list all its sessions in one entry")
        seen.add(name)
        groups.setdefault(src["collector"], []).append(src)
    return groups


def update_meta(target: Target, available: list[tuple[str, datetime]], gaps: list[datetime]) -> dict:
    """What one target got from the shared update pass (same shape as before the shared pass)."""
    mine = set(target.applied)
    files = [(u, ts) for u, ts in available if ts in mine]
    last = files[-1][1] if files else target.start
    return {
        "first_file": files[0][0] if files else None,
        "last_file": files[-1][0] if files else None,
        "files": len(files),
        "missing_files": [ist_iso(m) for m in gaps
                          if m >= target.start and m < last and (target.end is None or m < target.end)],
        "stats": dict(target.stats),
    }


def _update_pass(cfg: dict, collector: str, targets: list[Target], timing: dict,
                 label: str) -> tuple[list[tuple[str, datetime]], list[datetime]]:
    """Download the update files covering all targets' windows and apply them in one pass."""
    base, raw_dir = cfg["ris"]["base_url"], Path(cfg["ris"]["raw_dir"])
    available, gaps = list_updates(collector, min(t.start for t in targets), max(t.end for t in targets), base)
    upd_dir = raw_dir / collector / "updates"
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=8) as pool:
        paths = list(pool.map(lambda u: download(u[0], upd_dir / u[0].rsplit("/", 1)[1]), available))
    timing[f"download_updates_{label}"] = round(time.time() - t0, 1)
    print(f"  {collector} [{label}]: applying {len(paths)} update files ({len(gaps)} missing) "
          f"to {len(targets)} RIBs", file=sys.stderr)
    timing[f"apply_updates_{label}"] = apply_updates_multi(
        [(pth, ts, ts + UPDATE_INTERVAL) for pth, (_, ts) in zip(paths, available)], targets)
    return available, gaps


def run_collector(cfg: dict, collector: str, sources: list[dict], mrt_arg: str | None,
                  until: datetime | None, bview_only: bool, state_dir: Path | None = None) -> list[Path]:
    base, raw_dir = cfg["ris"]["base_url"], Path(cfg["ris"]["raw_dir"])
    t_start = time.time()
    end = until or datetime.now(timezone.utc)
    timing: dict = {}  # shared by all sources of this collector
    reset_interning()

    # 1. Where each source starts: an explicit bview, the latest bview, or its saved state.
    if mrt_arg:
        mrt = Path(mrt_arg)
        d, t = BVIEW_RE.search(mrt.name).groups()
        bview_ts = _parse_ts(f"{d}.{t}")
        bview_url = f"{base}/{collector}/{bview_ts:%Y.%m}/{mrt.name}"
    else:
        bview_url, bview_ts = latest_bview(collector, base, now=end)
        mrt = None

    jobs = []
    for source in sources:
        saved = None
        spath = state_path(state_dir, collector, source["peer_asn"]) if state_dir else None
        if spath and not mrt_arg:
            saved = load_state(spath)
        mode, reason = plan_run(saved[1] if saved else None, bview_ts, source)
        if mrt_arg:
            mode, reason = "fresh", "bview given with --mrt"
        print(f"{source_name(source)}: {mode} ({reason}); updates up to {ist_text(end)}", file=sys.stderr)
        run = Target(source["peer_asn"], list(source["peer_ips"]), end=end)
        job = {"source": source, "spath": spath, "mode": mode, "reason": reason, "run": run, "check": None}
        if mode == "continue":
            run.rib, info = saved
            run.start = datetime.fromisoformat(info["as_of"])
            job["bview_routes"] = info.get("bview_routes")
        else:
            run.start = bview_ts
            if saved and reason.startswith("new bview"):
                # Self-check: bring the previous state up to this bview's time and compare.
                old_rib, old_info = saved
                job["check"] = Target(source["peer_asn"], list(source["peer_ips"]), rib=old_rib,
                                      start=datetime.fromisoformat(old_info["as_of"]), end=bview_ts)
                job["previous_bview"] = old_info["bview_time"]
        del saved
        jobs.append(job)

    # 2. Self-check: bring each previous state up to the new bview's time (one pass over those
    #    update files), then keep only a compact digest so old and new RIBs are never in memory together.
    checks = [j for j in jobs if j["check"]]
    if checks:
        _update_pass(cfg, collector, [j["check"] for j in checks], timing, "self_check")
        for j in checks:
            chk = j["check"]
            j["check_reached"] = chk.applied[-1] + UPDATE_INTERVAL if chk.applied else chk.start
            j["check_digest"] = rib_digest(chk.rib)
            j["check"] = None
        del chk

    # 3. One pass over the bview for every source that starts fresh.
    fresh = [j for j in jobs if j["mode"] == "fresh"]
    if fresh:
        if mrt is None:
            mrt = download(bview_url, raw_dir / collector / bview_url.rsplit("/", 1)[1])
        timing["load_bview"] = load_bview_multi(mrt, [j["run"] for j in fresh])
        for j in fresh:
            j["bview_routes"] = j["run"].stats.pop("routes")
            j["bview_stats"] = dict(j["run"].stats)
            j["run"].stats.clear()
            if "check_digest" in j:  # compare with the bview itself, before this run's updates
                if j["check_reached"] >= bview_ts:
                    j["self_check"] = {"previous_bview": ist_iso(j["previous_bview"]),
                                       "compared_at": ist_iso(bview_ts),
                                       **diff_ribs(j.pop("check_digest"), j["run"].rib)}
                else:
                    j.pop("check_digest")
                    j["self_check"] = {"skipped": f"update files up to {ist_text(bview_ts)} not all available"}
                print(f"  {source_name(j['source'])} self-check: {j['self_check']}", file=sys.stderr)
            print(f"  {source_name(j['source'])}: {j['bview_routes']:,} routes from {mrt.name}", file=sys.stderr)

    # 4. One pass over the update files since then, each applied to every RIB whose time window needs it.
    available, gaps = [], []
    if not bview_only:
        available, gaps = _update_pass(cfg, collector, [j["run"] for j in jobs], timing, "run")

    # 5. Per source: self-check result, snapshot, state.
    names = [source_name(s) for s in sources]
    out_dirs = []
    for j in jobs:
        source, run = j["source"], j["run"]
        peer_asn, peer_ips = source["peer_asn"], source["peer_ips"]
        self_check = j.get("self_check")

        as_of = run.start
        upd = {"applied": False}
        if not bview_only:
            if run.applied:
                as_of = run.applied[-1] + UPDATE_INTERVAL
            upd = {**update_meta(run, available, gaps), "applied": True}

        meta = {
            "schema_version": SCHEMA_VERSION,
            "as_of": ist_iso(as_of),
            "run": {"mode": j["mode"], "reason": j["reason"], "collector_sources": names},
            "source": {"collector": collector, "peer_asn": peer_asn, "peer_ips": peer_ips,
                       "display_name": source.get("display_name"),
                       "collector_location": cfg.get("collectors", {}).get(collector, {}).get("location"),
                       "bview_url": bview_url, "bview_time": ist_iso(bview_ts),
                       "bview_routes": j.get("bview_routes")},
            "updates": upd,
            "self_check": self_check,
            "notes": ["BGP session resets are not detected: routes withdrawn while a session was down "
                      "may linger until the next bview (at most 8 hours).",
                      "load_bview, download_updates_* and apply_updates_* timings cover all collector_sources "
                      "(one shared pass); write and save_state are for this source."],
            "timing_seconds": dict(timing),
        }
        if j.get("bview_stats"):
            meta["source"]["bview_stats"] = j["bview_stats"]
        out_dir = Path(cfg["ris"]["output_dir"]) / source_name(source) / as_of.strftime(TS_FMT)
        write_snapshot(run.rib, out_dir, peer_asn, meta)

        if j["spath"]:
            t0 = time.time()
            save_state(j["spath"], run.rib, {"peer_asn": peer_asn, "peer_ips": peer_ips,
                                             "bview_time": bview_ts.isoformat(), "as_of": as_of.isoformat(),
                                             "bview_routes": j.get("bview_routes")})
            meta["timing_seconds"]["save_state"] = round(time.time() - t0, 1)
        meta["timing_seconds"]["total"] = round(time.time() - t_start, 1)
        (out_dir / "meta.json").write_text(json.dumps(meta, indent=2))
        out_dirs.append(out_dir)
        j["run"] = run = None  # free this RIB before writing the next one
    return out_dirs


def prune_states(state_dir: Path, sources: list[dict]) -> None:
    """Delete saved states of sources no longer in the config, so the cache does not keep them."""
    keep = {state_path(state_dir, s["collector"], s["peer_asn"]).name for s in sources}
    for f in state_dir.glob("*.state.pkl.gz"):
        if f.name not in keep:
            print(f"removing state of a source no longer configured: {f.name}", flush=True)
            f.unlink()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config/pipeline.toml")
    ap.add_argument("--collector", action="append", help="only build sources on this collector (repeatable)")
    ap.add_argument("--mrt", help="local bview file to start from instead of the latest one (needs one collector)")
    ap.add_argument("--until", type=_parse_ist, help="apply updates up to this time (YYYYMMDD.HHMM IST)")
    ap.add_argument("--bview-only", action="store_true", help="do not apply update files")
    ap.add_argument("--output-dir", help="override [ris].output_dir")
    ap.add_argument("--state-dir", type=Path, help="keep the RIB here between runs (continue instead of re-reading the bview)")
    args = ap.parse_args()

    cfg = tomllib.loads(Path(args.config).read_text())
    if args.output_dir:
        cfg["ris"]["output_dir"] = args.output_dir
    groups = group_by_collector(cfg["sources"])
    if args.collector:
        unknown = set(args.collector) - groups.keys()
        if unknown:
            ap.error(f"no sources configured on {', '.join(sorted(unknown))}")
        groups = {c: s for c, s in groups.items() if c in args.collector}
    if args.mrt and len(groups) != 1:
        ap.error("--mrt is one collector's bview: choose the collector with --collector")
    if args.state_dir and not args.collector:
        prune_states(args.state_dir, cfg["sources"])
    for collector, sources in groups.items():
        for out_dir in run_collector(cfg, collector, sources, args.mrt, args.until, args.bview_only,
                                     args.state_dir):
            print(out_dir, flush=True)


if __name__ == "__main__":
    main()
