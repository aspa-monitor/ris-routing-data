"""Compare the routes of two snapshot directories, keyed by (peer_ip, prefix).

Typical use: check that "old bview + updates" reproduces the next real bview.
  python -m pipeline.ris.compare <snapshot_a> <snapshot_b>
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import sys
from pathlib import Path


def load_routes(snapshot: Path) -> dict[tuple[str, str], tuple[str, str]]:
    """(peer_ip, prefix) -> (path_id, communities)"""
    routes = {}
    with gzip.open(snapshot / "routes.tsv.gz", "rt", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            routes[(row["peer_ip"], row["prefix"])] = (row["path_id"], row["communities"])
    return routes


def load_paths(snapshot: Path) -> dict[str, str]:
    with gzip.open(snapshot / "paths.tsv.gz", "rt", newline="") as f:
        return {r["path_id"]: r["raw_path"] for r in csv.DictReader(f, delimiter="\t")}


def compare(a: Path, b: Path, samples: int = 5) -> dict:
    ra, rb = load_routes(a), load_routes(b)
    pa, pb = load_paths(a), load_paths(b)
    only_a = sorted(set(ra) - set(rb))
    only_b = sorted(set(rb) - set(ra))
    common = set(ra) & set(rb)
    path_diff = sorted(k for k in common if ra[k][0] != rb[k][0])
    comm_diff = sorted(k for k in common if ra[k][0] == rb[k][0] and ra[k][1] != rb[k][1])
    total = len(set(ra) | set(rb))
    identical = len(common) - len(path_diff) - len(comm_diff)
    return {
        "a": str(a), "b": str(b),
        "routes_a": len(ra), "routes_b": len(rb),
        "identical": identical,
        "identical_pct": round(100 * identical / total, 4) if total else 100.0,
        "only_in_a": len(only_a), "only_in_b": len(only_b),
        "path_differs": len(path_diff), "communities_differ": len(comm_diff),
        "samples": {
            "only_in_a": [f"{k[1]} {pa.get(ra[k][0], '')}" for k in only_a[:samples]],
            "only_in_b": [f"{k[1]} {pb.get(rb[k][0], '')}" for k in only_b[:samples]],
            "path_differs": [f"{k[1]}: {pa.get(ra[k][0])} | {pb.get(rb[k][0])}" for k in path_diff[:samples]],
        },
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("a", type=Path)
    ap.add_argument("b", type=Path)
    ap.add_argument("--samples", type=int, default=5)
    args = ap.parse_args()
    json.dump(compare(args.a, args.b, args.samples), sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()
