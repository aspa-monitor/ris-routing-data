# ris-routing-data

Hourly snapshots of BGP routing as seen by RIPE RIS (and RouteViews), published as GitHub Releases.
Part of the ASPA validation project:

| Repo | Role |
|---|---|
| **ris-routing-data** (this) | RIS / RouteViews table dumps + updates → routes / paths / adjacency |
| rpki-aspa-data | Validated RPKI ASPA objects |
| aspa-validation | Combines both, finds ASPA invalids |
| aspa-monitor.github.io | The web portal |

## Sources
Each source is one peer AS on one collector, named `<collector>_AS<peer>` (`config/pipeline.toml`):

| Source | Peer | Sessions |
|---|---|---|
| `rrc00_AS7018` | AT&T | IPv4 + IPv6 |
| `rrc00_AS3549` | Lumen (Level 3) | IPv4 only |
| `rrc00_AS3333` | RIPE NCC | IPv4 + IPv6 (one session) |
| `rrc00_AS4608` | APNIC Services | IPv4 only |
| `rrc00_AS24482` | SG.GS (Singapore) | IPv4 + IPv6 |
| `rrc06_AS4777` | APNIC-COLO (Japan) | IPv4 + IPv6 |
| `rrc23_AS3491` | PCCW Global (Hong Kong) | IPv4 + IPv6 |
| `rrc03_AS6453` | Tata Communications | IPv4 + IPv6 |
| `rrc25_AS1221` | Telstra (Australia) | IPv4 + IPv6 |
| `rrc26_AS200612` | Gulf Bridge International (Middle East) | IPv4 + IPv6 |
| `route-views.eqix_AS6939` | Hurricane Electric | IPv4 + IPv6 |
| `route-views.napafrica_AS37468` | Angola Cables | IPv4 + IPv6 |
| `route-views.napafrica_AS30844` | Liquid Telecom | IPv4 + IPv6 |
| `route-views8_AS151704` | Bharat Datacenter (India; ~91% of routes via Bharti Airtel) | IPv4 + IPv6 |
| `route-views8_AS174` | Cogent | IPv4 only |

`rrc00` and `rrc25` are RIPE's multihop collectors in Amsterdam (peers connect to them over the Internet from
anywhere); `rrc03` is at AMS-IX / NL-IX in Amsterdam, `rrc06` at DIX-IE / JPIX in Tokyo and `rrc23` at Equinix Singapore
and `rrc26` at UAE-IX in Dubai (`[collectors]` in the config). The `route-views*` collectors are RouteViews: `route-views.eqix` at Equinix
Ashburn, `route-views.napafrica` at NAPAfrica Johannesburg, and `route-views8`, multihop in Eugene, Oregon.
Hurricane Electric and Liquid are taken from RouteViews because on RIS collectors they send only part of their
table (HE ~170k IPv4 peering routes, Liquid ~4k). Cogent's only full feed is on `route-views8`.
Read the list of sources from
`manifest.json`, not from this table: sources may be added.

## Getting the data
Latest snapshot (always overwritten):
```
https://github.com/aspa-monitor/ris-routing-data/releases/download/latest/manifest.json
https://github.com/aspa-monitor/ris-routing-data/releases/download/latest/rrc00_AS7018_routes.tsv.gz
https://github.com/aspa-monitor/ris-routing-data/releases/download/latest/rrc00_AS7018_paths.tsv.gz
https://github.com/aspa-monitor/ris-routing-data/releases/download/latest/rrc00_AS7018_adjacency.tsv.gz
https://github.com/aspa-monitor/ris-routing-data/releases/download/latest/rrc00_AS7018_meta.json
```
(and the same four files for every other source in `manifest.json`).
Timestamped releases `snapshot-YYYYMMDD-HHMM-IST` are made once per bview (the first hourly snapshot after
05:30, 13:30 and 21:30 IST, so 3 a day) and kept for 7 days; only `latest` is updated every hour. `latest/` in this repo holds the newest
`manifest.json` and meta files. `manifest.json` lists every asset with size and SHA-256, plus `schema_version`. Its top-level `as_of` is the
oldest source's; each source has its own `as_of` (RouteViews sources usually lag RIS by 15–30 minutes).
Each `sources[]` entry also has `display_name` (short peer name, e.g. `AT&T`) and `collector_location`
(the collector's city and exchange point, e.g. `Amsterdam (AMS-IX / NL-IX)`, or `multihop`), both from `config/pipeline.toml`. They are optional and for display
only: fall back to the collector and peer AS if either is missing. Some sources also have `note`, one plain-text
sentence of context (absent when there is none). The same fields are in each `meta.json` `source`.

## Files
- `routes.tsv.gz`: afi, prefix, peer_ip, peer_asn, path_id, origin_asn, communities, otc, aggr_asn, aggr_ip, atomic, timestamp (last change)
- `paths.tsv.gz`: path_id, path (prepends collapsed, AS_SETs sorted), raw_path, length, origin_asn, flags (`as_set, confed, loop, private, reserved, as_trans, peer_mismatch, empty`), set_members, routes_v4, routes_v6
- `adjacency.tsv.gz`: asn, upstream_asn, paths, routes_v4, routes_v6. `upstream_asn` received the route directly from `asn` (provider **or peer**). Only from the ordered part of paths, between public ASNs, never from looped paths.
- `meta.json`: run mode, source, update stats, self-check, counts, flags, path lengths, timings

**Times:** every time in `manifest.json`, `meta.json`, release names and logs is **IST** (UTC+05:30),
written as ISO 8601 with the offset, e.g. `2026-11-24T19:55:00+05:30`. Parse it with the offset; do not
compare the text. Exceptions: the `timestamp` column in `routes.tsv.gz` is Unix seconds (no time zone), and
archive file URLs (`bview_url`, `first_file`, `last_file`) keep the archive's own UTC names. `meta.json`
`source.archive` is `ris` or `routeviews`.

## How a run works (hourly, `:50` IST)
1. If a bview newer than the saved state exists (RIPE publishes one at 05:30, 13:30 and 21:30 IST; for RouteViews,
   the RIB of the same times is used and the 2-hourly ones in between are skipped) → start fresh from it. Before switching,
   the old state is brought up to the bview time and compared with it (**self-check**, reported in `meta.json`).
2. Otherwise continue from the saved state (GitHub Actions cache).
3. Apply all update files published since (5-minute on RIS, 15-minute on RouteViews), write the snapshot, save state, publish.

Sources on the same collector share one pass: the bview and each update file are downloaded and read
once, and every source's RIB is filled from it (MRT files have no index, so reading a bview costs the same
for one peer as for all of them). Each source still decides fresh/continue and keeps its own state.

Known limitation: BGP session resets are not detected; stale routes can survive until the next bview (≤ 8 h).

## Local use
```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pipeline.ris.build_snapshot --state-dir state     # one run, all collectors
.venv/bin/python -m pipeline.ris.build_snapshot --collector rrc06 --mrt data/raw/rrc06/bview.YYYYMMDD.HHMM.gz
.venv/bin/python -m pipeline.ris.list_peers rrc00                     # peers of a collector
.venv/bin/python -m pipeline.ris.compare <snapshot_a> <snapshot_b>    # compare two snapshots
.venv/bin/python -m unittest discover -s tests -t .
```
