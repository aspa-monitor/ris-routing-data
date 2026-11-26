"""Package snapshot directories for publishing, and write a run summary.

  python -m pipeline.ris.publish <snapshot_dir> [...] --dist dist --latest latest

dist/    release assets: <source>_routes.tsv.gz, <source>_paths.tsv.gz, <source>_adjacency.tsv.gz,
         <source>_meta.json, manifest.json
latest/  small files committed to the repo: manifest.json and <source>_meta.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from datetime import datetime
from pathlib import Path

from pipeline.ris.timefmt import ist_text, now_ist

DATA_FILES = ("routes.tsv.gz", "paths.tsv.gz", "adjacency.tsv.gz")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def package(snapshots: list[Path], dist: Path, latest: Path) -> dict:
    dist.mkdir(parents=True, exist_ok=True)
    latest.mkdir(parents=True, exist_ok=True)
    sources = []
    for snap in snapshots:
        meta = json.loads((snap / "meta.json").read_text())
        name = snap.parent.name  # e.g. rrc00_AS7018
        files = {}
        for fn in DATA_FILES:
            dest = dist / f"{name}_{fn.replace('.tsv.gz', '')}.tsv.gz"
            shutil.copyfile(snap / fn, dest)
            files[fn.split(".")[0]] = {"asset": dest.name, "bytes": dest.stat().st_size, "sha256": _sha256(dest)}
        meta_name = f"{name}_meta.json"
        shutil.copyfile(snap / "meta.json", dist / meta_name)
        shutil.copyfile(snap / "meta.json", latest / meta_name)
        sources.append({"name": name, "collector": meta["source"]["collector"],
                        "peer_asn": meta["source"]["peer_asn"], "as_of": meta["as_of"],
                        "meta": meta_name, "files": files})
    manifest = {
        "schema_version": max(json.loads((s / "meta.json").read_text()).get("schema_version", 1)
                              for s in snapshots),
        "generated_at": now_ist(),
        "as_of": min((s["as_of"] for s in sources), key=datetime.fromisoformat),
        "sources": sources,
    }
    for d in (dist, latest):
        (d / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def summary_markdown(snapshots: list[Path]) -> str:
    lines = ["## RIS routing snapshot", ""]
    for snap in snapshots:
        m = json.loads((snap / "meta.json").read_text())
        c, u, sc = m["counts"], m["updates"], m.get("self_check")
        lines += [
            f"### {snap.parent.name} — as of {ist_text(m['as_of'])}",
            "",
            "| | |", "|---|---|",
            f"| Run mode | {m['run']['mode']} ({m['run']['reason']}) |",
            f"| bview | {ist_text(m['source']['bview_time'])} |",
            f"| Routes | {c['routes']:,} ({', '.join(f'{k}: {v:,}' for k, v in c['routes_by_session'].items())}) |",
            f"| Unique paths | {c['unique_raw_paths']:,} raw / {c['unique_clean_paths']:,} clean |",
            f"| Adjacencies | {c['adjacencies']:,} |",
            f"| Update files applied | {u.get('files', 0)} (missing: {len(u.get('missing_files', [])) or 'none'}) |",
            f"| Update stats | {', '.join(f'{k}: {v:,}' for k, v in u.get('stats', {}).items()) or '-'} |",
            f"| Flagged routes | {c['routes_with_any_flag']:,} |",
            f"| Self-check | {_self_check_text(sc)} |",
            f"| Timing (s) | {', '.join(f'{k}: {v}' for k, v in m['timing_seconds'].items())} |",
            "",
        ]
    return "\n".join(lines)


def _self_check_text(sc: dict | None) -> str:
    if not sc:
        return "not run (no new bview this run)"
    if "skipped" in sc:
        return f"skipped: {sc['skipped']}"
    mark = "✅" if sc["identical_pct"] >= 99.9 else "⚠️"
    return (f"{mark} {sc['identical_pct']}% identical vs bview {ist_text(sc['compared_at'])} "
            f"(missing {sc['only_in_bview']}, extra {sc['only_in_built']}, path diff {sc['path_differs']})")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("snapshots", nargs="+", type=Path)
    ap.add_argument("--dist", type=Path, default=Path("dist"))
    ap.add_argument("--latest", type=Path, default=Path("latest"))
    args = ap.parse_args()

    manifest = package(args.snapshots, args.dist, args.latest)
    bview = max((json.loads((s / "meta.json").read_text())["source"]["bview_time"] for s in args.snapshots),
                key=datetime.fromisoformat)
    md = summary_markdown(args.snapshots)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as f:
            f.write(md + "\n")
    print(md)
    print(f"as_of={manifest['as_of']}")
    print(f"bview_time={bview}")


if __name__ == "__main__":
    main()
