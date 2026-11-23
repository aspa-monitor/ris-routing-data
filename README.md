# ris-routing-data

Hourly snapshots of BGP routing as seen by RIPE RIS, published as GitHub Releases.
Part of the ASPA validation project:

| Repo | Role |
|---|---|
| **ris-routing-data** (this) | RIS bview + 5-minute updates → routes / paths / adjacency |
| rpki-aspa-data | Validated RPKI ASPA objects |
| aspa-validation | Combines both, finds ASPA invalids, website |

Current source: collector `rrc00`, peer **AS7018** (AT&T), IPv4 + IPv6 sessions (`config/pipeline.toml`).

## Getting the data
Latest snapshot (always overwritten):
```
https://github.com/aspa-monitor/ris-routing-data/releases/download/latest/manifest.json
https://github.com/aspa-monitor/ris-routing-data/releases/download/latest/rrc00_AS7018_routes.tsv.gz
https://github.com/aspa-monitor/ris-routing-data/releases/download/latest/rrc00_AS7018_paths.tsv.gz
https://github.com/aspa-monitor/ris-routing-data/releases/download/latest/rrc00_AS7018_adjacency.tsv.gz
https://github.com/aspa-monitor/ris-routing-data/releases/download/latest/rrc00_AS7018_meta.json
```
Timestamped releases `snapshot-YYYYMMDD-HHMM` are kept for 7 days. `latest/` in this repo holds the newest
`manifest.json` and meta files. `manifest.json` lists every asset with size and SHA-256, plus `schema_version`.

## Files
- `routes.tsv.gz`: afi, prefix, peer_ip, peer_asn, path_id, origin_asn, communities, otc, aggr_asn, aggr_ip, atomic, timestamp (last change)
- `paths.tsv.gz`: path_id, path (prepends collapsed, AS_SETs sorted), raw_path, length, origin_asn, flags (`as_set, confed, loop, private, reserved, as_trans, peer_mismatch, empty`), set_members, routes_v4, routes_v6
- `adjacency.tsv.gz`: asn, upstream_asn, paths, routes_v4, routes_v6. `upstream_asn` received the route directly from `asn` (provider **or peer**). Only from the ordered part of paths, between public ASNs, never from looped paths.
- `meta.json`: run mode, source, update stats, self-check, counts, flags, path lengths, timings

## How a run works (hourly, `:20` UTC)
1. If a bview newer than the saved state exists (00/08/16 UTC) → start fresh from it. Before switching,
   the old state is brought up to the bview time and compared with it (**self-check**, reported in `meta.json`).
2. Otherwise continue from the saved state (GitHub Actions cache).
3. Apply all 5-minute update files published since, write the snapshot, save state, publish.

Known limitation: BGP session resets are not detected; stale routes can survive until the next bview (≤ 8 h).

## Local use
```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pipeline.ris.build_snapshot --state-dir state     # one run
.venv/bin/python -m pipeline.ris.list_peers rrc00                     # peers of a collector
.venv/bin/python -m pipeline.ris.compare <snapshot_a> <snapshot_b>    # compare two snapshots
.venv/bin/python -m unittest discover -s tests -t .
```
