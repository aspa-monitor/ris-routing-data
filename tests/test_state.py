import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from pipeline.ris.build_snapshot import plan_run
from collections import Counter

from pipeline.ris.rib import Route, apply_update_elems, diff_ribs, load_state, rib_digest, save_state

SRC = {"collector": "rrc00", "peer_asn": 7018, "peer_ips": ["12.0.1.63", "2001:1890:111d:1::63"]}
B08 = datetime(2026, 11, 23, 8, 0, tzinfo=timezone.utc)
B16 = datetime(2026, 11, 23, 16, 0, tzinfo=timezone.utc)


def info(bview=B08, as_of="2026-11-23T15:00:00+00:00", **kw):
    return {"peer_asn": 7018, "peer_ips": list(SRC["peer_ips"]), "bview_time": bview.isoformat(), "as_of": as_of, **kw}


def r(prefix, path, comms=None):
    return Route(1.0, "12.0.1.63", 7018, prefix, path, comms, False, None, None, None)


class TestPlanRun(unittest.TestCase):
    def test_no_state(self):
        self.assertEqual(plan_run(None, B08, SRC)[0], "fresh")

    def test_continue(self):
        self.assertEqual(plan_run(info(), B08, SRC)[0], "continue")

    def test_new_bview(self):
        mode, reason = plan_run(info(), B16, SRC)
        self.assertEqual(mode, "fresh")
        self.assertTrue(reason.startswith("new bview"))

    def test_peer_changed(self):
        self.assertEqual(plan_run(info(peer_asn=3333), B08, SRC)[0], "fresh")


class TestState(unittest.TestCase):
    def test_roundtrip(self):
        rib = {("12.0.1.63", "10.0.0.0/8"): r("10.0.0.0/8", "7018 3356 1", ("7018:5000",))}
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "s.pkl.gz"
            save_state(p, rib, info())
            loaded, inf = load_state(p)
        self.assertEqual(loaded, rib)
        self.assertEqual(inf["as_of"], "2026-11-23T15:00:00+00:00")

    def test_old_state_with_list_communities(self):
        # States saved before interning hold communities as lists; they load as tuples, so an
        # unchanged re-announcement still counts as a duplicate.
        rib = {("12.0.1.63", "10.0.0.0/8"): r("10.0.0.0/8", "7018 3356 1", ["7018:5000"])}
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "s.pkl.gz"
            save_state(p, rib, info())
            loaded, _ = load_state(p)
        self.assertEqual(loaded[("12.0.1.63", "10.0.0.0/8")].communities, ("7018:5000",))
        stats = Counter()
        apply_update_elems(loaded, [(5.0, "A", "12.0.1.63", 7018, "10.0.0.0/8", "7018 3356 1",
                                     ["7018:5000"], False, None, None, None)], 7018, stats)
        self.assertEqual(stats["announce_duplicate"], 1)
        self.assertEqual(loaded[("12.0.1.63", "10.0.0.0/8")].timestamp, 1.0)

    def test_diff_digest_matches_rib(self):
        a = {("p", "1"): r("1", "7018 1"), ("p", "2"): r("2", "7018 2", ("7018:1",))}
        b = {("p", "1"): r("1", "7018 1"), ("p", "2"): r("2", "7018 2", ("7018:2",))}
        self.assertEqual(diff_ribs(rib_digest(a), b), diff_ribs(a, b))
        self.assertEqual(diff_ribs(a, b)["communities_differ"], 1)

    def test_missing_or_corrupt(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(load_state(Path(d) / "none"))
            bad = Path(d) / "bad"
            bad.write_bytes(b"not gzip")
            self.assertIsNone(load_state(bad))

    def test_diff(self):
        a = {("p", "1"): r("1", "7018 1"), ("p", "2"): r("2", "7018 2"), ("p", "3"): r("3", "7018 3")}
        b = {("p", "1"): r("1", "7018 1"), ("p", "2"): r("2", "7018 9 2"), ("p", "4"): r("4", "7018 4")}
        d = diff_ribs(a, b)
        self.assertEqual((d["identical"], d["path_differs"], d["only_in_built"], d["only_in_bview"]), (1, 1, 1, 1))


if __name__ == "__main__":
    unittest.main()
