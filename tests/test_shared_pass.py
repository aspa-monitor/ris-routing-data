"""One pass over each MRT file feeds the RIBs of every source on a collector."""
import io
import json
from contextlib import redirect_stdout
import tempfile
import tomllib
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from pipeline.ris import rib as ribmod
from pipeline.ris.build_snapshot import group_by_collector, prune_states, update_meta
from pipeline.ris.publish import package
from pipeline.ris.rib import Target, apply_updates_multi, load_bview_multi

T0 = datetime(2026, 11, 23, 8, 0, tzinfo=timezone.utc)
FIVE = timedelta(minutes=5)


def bview_row(ip, asn, prefix, path):
    # ROUTE_FIELDS: timestamp, peer_ip, peer_asn, prefix, as_path, communities, atomic, aggr_asn, aggr_ip, otc
    return (1.0, ip, asn, prefix, path, None, False, None, None, None)


def upd(ip, asn, kind, prefix, path=None, ts=2.0):
    return (ts, kind, ip, asn, prefix, path, None, False, None, None, None)


class FakeParser:
    """Stands in for bgpkit: yields the rows of a fake file, keeping only the filtered sessions."""
    files: dict = {}
    reads: list = []

    def __init__(self, path, peer_ips):
        self.path, self.ips = path, set(peer_ips)
        FakeParser.reads.append((path, tuple(sorted(peer_ips))))

    def iter_tuple_batches(self, fields, batch_size):
        ip_col = 1 if fields[1] == "peer_ip" else 2
        rows = [r for r in FakeParser.files[self.path] if r[ip_col] in self.ips]
        yield rows


class SharedPassTest(unittest.TestCase):
    def setUp(self):
        FakeParser.files, FakeParser.reads = {}, []
        patcher = mock.patch.object(ribmod, "_parser", FakeParser)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_bview_read_once_and_split_by_peer(self):
        FakeParser.files["bview"] = [
            bview_row("12.0.1.63", 7018, "10.0.0.0/8", "7018 1"),
            bview_row("2001:1890:111d:1::63", 7018, "2001:db8::/32", "7018 2"),
            bview_row("193.0.0.56", 3333, "10.0.0.0/8", "3333 1"),
            bview_row("193.0.0.56", 3333, "2001:db8::/32", "3333 2"),
            bview_row("193.0.0.56", 64512, "11.0.0.0/8", "64512 3"),  # AS does not match the source
        ]
        att = Target(7018, ["12.0.1.63", "2001:1890:111d:1::63"])
        ripe = Target(3333, ["193.0.0.56"])
        load_bview_multi("bview", [att, ripe])
        self.assertEqual(len(FakeParser.reads), 1)
        self.assertEqual(set(att.rib), {("12.0.1.63", "10.0.0.0/8"), ("2001:1890:111d:1::63", "2001:db8::/32")})
        self.assertEqual(set(ripe.rib), {("193.0.0.56", "10.0.0.0/8"), ("193.0.0.56", "2001:db8::/32")})
        self.assertEqual(ripe.stats["skipped_peer_asn_mismatch"], 1)
        self.assertEqual((att.stats["routes"], ripe.stats["routes"]), (2, 2))

    def test_ipv6_written_differently_in_config(self):
        iij = Target(2497, ["2001:200:0:fe00:0:0:9c1:0"])
        index = ribmod._PeerIndex([iij])
        self.assertIs(index.get("2001:200:0:fe00::9c1:0"), iij)

    def test_session_in_two_sources_rejected(self):
        with self.assertRaises(ValueError):
            ribmod._PeerIndex([Target(1, ["192.0.2.1"]), Target(2, ["192.0.2.1"])])

    def test_updates_follow_each_targets_window(self):
        ip = "12.0.1.63"
        files = []
        for i in range(4):  # 08:00, 08:05, 08:10, 08:15
            name = f"u{i}"
            FakeParser.files[name] = [upd(ip, 7018, "A", f"10.{i}.0.0/16", f"7018 {i}"),
                                      upd("193.0.0.56", 3333, "A", f"10.{i}.0.0/16", f"3333 {i}")]
            files.append((name, T0 + i * FIVE, T0 + (i + 1) * FIVE))
        # Old state of AT&T catching up to the 08:10 bview, the new AT&T RIB from 08:10 on,
        # and RIPE continuing from 08:05: same session twice, but never in the same file.
        check = Target(7018, [ip], start=T0, end=T0 + 2 * FIVE)
        att = Target(7018, [ip], start=T0 + 2 * FIVE, end=T0 + 4 * FIVE)
        ripe = Target(3333, ["193.0.0.56"], start=T0 + FIVE, end=T0 + 4 * FIVE)
        apply_updates_multi(files, [check, att, ripe])
        self.assertEqual(len(FakeParser.reads), 4)  # each file read once
        self.assertEqual(sorted(p for _, p in check.rib), ["10.0.0.0/16", "10.1.0.0/16"])
        self.assertEqual(sorted(p for _, p in att.rib), ["10.2.0.0/16", "10.3.0.0/16"])
        self.assertEqual(sorted(p for _, p in ripe.rib), ["10.1.0.0/16", "10.2.0.0/16", "10.3.0.0/16"])
        self.assertEqual(ripe.applied, [T0 + FIVE, T0 + 2 * FIVE, T0 + 3 * FIVE])
        self.assertEqual(att.stats["files"], 2)

    def test_file_ending_after_window_not_applied(self):
        FakeParser.files["u"] = [upd("12.0.1.63", 7018, "A", "10.0.0.0/8", "7018 1")]
        t = Target(7018, ["12.0.1.63"], start=T0, end=T0 + timedelta(minutes=3))
        apply_updates_multi([("u", T0, T0 + FIVE)], [t])
        self.assertEqual(t.rib, {})
        self.assertEqual(FakeParser.reads, [])


class UpdateMetaTest(unittest.TestCase):
    def test_only_own_files_and_gaps(self):
        available = [(f"url{i}", T0 + i * FIVE) for i in (0, 1, 3, 5)]
        gaps = [T0 + 2 * FIVE, T0 + 4 * FIVE]
        t = Target(7018, ["12.0.1.63"], start=T0 + FIVE, end=T0 + 6 * FIVE)
        t.applied = [T0 + FIVE, T0 + 3 * FIVE]  # e.g. the 08:25 file ended up outside its window
        m = update_meta(t, available, gaps)
        self.assertEqual((m["first_file"], m["last_file"], m["files"]), ("url1", "url3", 2))
        self.assertEqual(len(m["missing_files"]), 1)  # 08:10 only; 08:20 is after its last file


class ConfigTest(unittest.TestCase):
    def test_grouping_keeps_order_and_rejects_duplicates(self):
        srcs = [{"collector": "rrc00", "peer_asn": 1}, {"collector": "rrc06", "peer_asn": 2},
                {"collector": "rrc00", "peer_asn": 3}]
        self.assertEqual({c: [s["peer_asn"] for s in v] for c, v in group_by_collector(srcs).items()},
                         {"rrc00": [1, 3], "rrc06": [2]})
        with self.assertRaises(ValueError):
            group_by_collector(srcs + [{"collector": "rrc00", "peer_asn": 1}])

    def test_shipped_config_is_consistent(self):
        cfg = tomllib.loads(Path("config/pipeline.toml").read_text())
        groups = group_by_collector(cfg["sources"])
        for sources in groups.values():
            ribmod._PeerIndex([Target(s["peer_asn"], s["peer_ips"]) for s in sources])  # no shared sessions

    def test_every_source_has_a_display_name_and_location(self):
        cfg = tomllib.loads(Path("config/pipeline.toml").read_text())
        for s in cfg["sources"]:
            self.assertTrue(s.get("display_name"), s)
            self.assertTrue(cfg["collectors"][s["collector"]]["location"], s)


class ManifestTest(unittest.TestCase):
    def _snap(self, root, name, source):
        snap = root / name / "20261126T0850Z"
        snap.mkdir(parents=True)
        for fn in ("routes.tsv.gz", "paths.tsv.gz", "adjacency.tsv.gz"):
            (snap / fn).write_bytes(b"x")
        (snap / "meta.json").write_text(json.dumps(
            {"schema_version": 1, "as_of": "2026-11-26T14:20:00+05:30", "source": source}))
        return snap

    def test_display_fields_copied_only_when_present(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            a = self._snap(root, "rrc00_AS7018", {"collector": "rrc00", "peer_asn": 7018,
                                                 "display_name": "AT&T", "collector_location": "Amsterdam",
                                                 "note": "A note.", })
            b = self._snap(root, "rrc06_AS2497", {"collector": "rrc06", "peer_asn": 2497})  # older meta
            m = package([a, b], root / "dist", root / "latest")
        self.assertEqual(m["schema_version"], 1)
        first, second = m["sources"]
        self.assertEqual((first["display_name"], first["collector_location"], first["note"]),
                         ("AT&T", "Amsterdam", "A note."))
        self.assertNotIn("display_name", second)
        self.assertNotIn("note", second)
        self.assertNotIn("collector_location", second)

    def test_removed_source_leaves_no_meta(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "latest").mkdir()
            (root / "latest" / "rrc06_AS2497_meta.json").write_text("{}")
            a = self._snap(root, "rrc00_AS7018", {"collector": "rrc00", "peer_asn": 7018})
            package([a], root / "dist", root / "latest")
            self.assertEqual(sorted(f.name for f in (root / "latest").iterdir()),
                             ["manifest.json", "rrc00_AS7018_meta.json"])

    def test_prune_states(self):
        with tempfile.TemporaryDirectory() as d:
            sd = Path(d)
            for n in ("rrc00_AS7018", "rrc06_AS2497"):
                (sd / f"{n}.state.pkl.gz").write_bytes(b"x")
            out = io.StringIO()
            with redirect_stdout(out):  # stdout is the snapshot list the workflow publishes
                prune_states(sd, [{"collector": "rrc00", "peer_asn": 7018}])
            self.assertEqual(out.getvalue(), "")
            self.assertEqual([f.name for f in sd.iterdir()], ["rrc00_AS7018.state.pkl.gz"])


if __name__ == "__main__":
    unittest.main()
