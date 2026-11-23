"""List the peers of a RIS collector from its latest bview, with IPv4/IPv6 prefix counts.

Used to choose which full-feed peer(s) the pipeline follows.
Output: output/peers_<collector>.json  plus a table on stdout.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import tomllib
import urllib.request
from collections import defaultdict
from pathlib import Path

import pybgpkit_parser as bgpkit

from pipeline.ris.fetch import download, latest_bview


def count_prefixes(mrt_path: Path) -> dict[tuple[str, int], list[int]]:
    """Return {(peer_ip, peer_asn): [ipv4_count, ipv6_count]}."""
    counts: dict[tuple[str, int], list[int]] = defaultdict(lambda: [0, 0])
    parser = bgpkit.Parser(url=str(mrt_path))
    n = 0
    t0 = time.time()
    for batch in parser.iter_tuple_batches(fields=["peer_ip", "peer_asn", "prefix"], batch_size=100_000):
        for peer_ip, peer_asn, prefix in batch:
            counts[(peer_ip, peer_asn)][1 if ":" in prefix else 0] += 1
        n += len(batch)
        if n % 5_000_000 < len(batch):
            print(f"  {n:,} routes parsed ({time.time() - t0:.0f}s)", file=sys.stderr)
    print(f"  done: {n:,} routes in {time.time() - t0:.0f}s", file=sys.stderr)
    return counts


def as_names(asns: list[int]) -> dict[int, str]:
    """Best-effort AS holder names from RIPEstat (only used for display)."""
    names: dict[int, str] = {}
    for asn in asns:
        url = f"https://stat.ripe.net/data/as-overview/data.json?resource=AS{asn}&sourceapp=aspa-validation"
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "aspa-validation/0.1"}),
                                        timeout=20) as r:
                names[asn] = json.load(r)["data"].get("holder") or ""
        except Exception:
            names[asn] = ""
    return names


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("collector", nargs="?", default="rrc00")
    ap.add_argument("--config", default="config/pipeline.toml")
    ap.add_argument("--mrt", help="use this local bview instead of downloading the latest")
    ap.add_argument("--no-names", action="store_true", help="skip RIPEstat name lookups")
    args = ap.parse_args()

    cfg = tomllib.loads(Path(args.config).read_text())
    min4 = cfg["full_feed"]["min_ipv4_prefixes"]
    min6 = cfg["full_feed"]["min_ipv6_prefixes"]

    if args.mrt:
        mrt = Path(args.mrt)
        source = str(mrt)
    else:
        url, _ = latest_bview(args.collector, cfg["ris"]["base_url"])
        mrt = download(url, Path(cfg["ris"]["raw_dir"]) / args.collector / url.rsplit("/", 1)[1])
        source = url

    print(f"counting prefixes per peer in {mrt} ...", file=sys.stderr)
    counts = count_prefixes(mrt)

    peers = []
    for (ip, asn), (v4, v6) in counts.items():
        full4, full6 = v4 >= min4, v6 >= min6
        peers.append({"peer_ip": ip, "peer_asn": asn, "ipv4_prefixes": v4, "ipv6_prefixes": v6,
                      "full_feed_ipv4": full4, "full_feed_ipv6": full6, "full_feed": full4 or full6})
    peers.sort(key=lambda p: (-p["full_feed"], -(p["ipv4_prefixes"] + p["ipv6_prefixes"])))

    if not args.no_names:
        names = as_names(sorted({p["peer_asn"] for p in peers if p["full_feed"]}))
        for p in peers:
            p["as_name"] = names.get(p["peer_asn"], "")

    out_dir = Path(cfg["ris"]["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"peers_{args.collector}.json"
    out.write_text(json.dumps({"collector": args.collector, "source": source,
                               "thresholds": {"ipv4": min4, "ipv6": min6}, "peers": peers}, indent=2))

    full = [p for p in peers if p["full_feed"]]
    print(f"\n{args.collector}: {len(peers)} peers, {len(full)} full-feed (v4>={min4:,} or v6>={min6:,})\n")
    print(f"{'peer_ip':<40} {'asn':>8} {'ipv4':>10} {'ipv6':>9}  feed  name")
    for p in peers:
        feed = ("4" if p["full_feed_ipv4"] else "-") + ("6" if p["full_feed_ipv6"] else "-")
        print(f"{p['peer_ip']:<40} {p['peer_asn']:>8} {p['ipv4_prefixes']:>10,} {p['ipv6_prefixes']:>9,}  "
              f"{feed:<4}  {p.get('as_name', '')[:40]}")
    print(f"\nwritten: {out}")


if __name__ == "__main__":
    main()
