import unittest
from collections import Counter
from datetime import datetime, timezone
from unittest import mock

from pipeline.ris import fetch
from pipeline.ris.rib import Route, apply_update_elems

P4 = "12.0.1.63"


def route(prefix, path, ts=100.0, comms=None):
    return Route(ts, P4, 7018, prefix, path, comms, False, None, None, None)


def ann(prefix, path, ts, comms=None):
    return (ts, "A", P4, 7018, prefix, path, comms, False, None, None, None)


def wd(prefix, ts):
    return (ts, "W", P4, 7018, prefix, None, None, False, None, None, None)


class TestApplyUpdates(unittest.TestCase):
    def setUp(self):
        self.rib = {(P4, "10.0.0.0/8"): route("10.0.0.0/8", "7018 3356 1"),
                    (P4, "11.0.0.0/8"): route("11.0.0.0/8", "7018 1299 2")}
        self.stats = Counter()

    def test_withdraw(self):
        apply_update_elems(self.rib, [wd("10.0.0.0/8", 200)], 7018, self.stats)
        self.assertNotIn((P4, "10.0.0.0/8"), self.rib)
        self.assertEqual(self.stats["withdrawals"], 1)

    def test_withdraw_unknown(self):
        apply_update_elems(self.rib, [wd("12.0.0.0/8", 200)], 7018, self.stats)
        self.assertEqual(self.stats["withdraw_unknown_prefix"], 1)
        self.assertEqual(len(self.rib), 2)

    def test_replace_and_new(self):
        apply_update_elems(self.rib, [ann("10.0.0.0/8", "7018 174 1", 200),
                                      ann("12.0.0.0/8", "7018 174 3", 201)], 7018, self.stats)
        self.assertEqual(self.rib[(P4, "10.0.0.0/8")].as_path, "7018 174 1")
        self.assertEqual(self.rib[(P4, "10.0.0.0/8")].timestamp, 200)
        self.assertIn((P4, "12.0.0.0/8"), self.rib)
        self.assertEqual(self.stats["announce_replace"], 1)
        self.assertEqual(self.stats["announce_new_prefix"], 1)

    def test_duplicate_keeps_age(self):
        apply_update_elems(self.rib, [ann("11.0.0.0/8", "7018 1299 2", 300)], 7018, self.stats)
        self.assertEqual(self.rib[(P4, "11.0.0.0/8")].timestamp, 100.0)
        self.assertEqual(self.stats["announce_duplicate"], 1)

    def test_order_matters(self):
        apply_update_elems(self.rib, [wd("10.0.0.0/8", 200), ann("10.0.0.0/8", "7018 6453 1", 201)],
                           7018, self.stats)
        self.assertEqual(self.rib[(P4, "10.0.0.0/8")].as_path, "7018 6453 1")

    def test_other_peer_asn_ignored(self):
        e = (200, "W", P4, 3356, "10.0.0.0/8", None, None, False, None, None, None)
        apply_update_elems(self.rib, [e], 7018, self.stats)
        self.assertIn((P4, "10.0.0.0/8"), self.rib)


class TestListUpdates(unittest.TestCase):
    def test_slots_and_gaps(self):
        listing = " ".join(f"updates.20261123.{t}.gz" for t in ["0800", "0805", "0815", "0820"])
        with mock.patch.object(fetch, "_http_get_text", return_value=listing):
            start = datetime(2026, 11, 23, 8, 0, tzinfo=timezone.utc)
            end = datetime(2026, 11, 23, 8, 40, tzinfo=timezone.utc)
            avail, gaps = fetch.list_updates("rrc00", start, end)
        self.assertEqual([ts.strftime("%H%M") for _, ts in avail], ["0800", "0805", "0815", "0820"])
        # 0810 is a real gap; 0825+ are simply not published yet
        self.assertEqual([g.strftime("%H%M") for g in gaps], ["0810"])

    def test_latest_bview_respects_now(self):
        listing = "bview.20261123.0000.gz bview.20261123.0800.gz bview.20261123.1600.gz"
        with mock.patch.object(fetch, "_http_get_text", return_value=listing):
            url, ts = fetch.latest_bview("rrc00", now=datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc))
        self.assertTrue(url.endswith("bview.20261123.0800.gz"))


if __name__ == "__main__":
    unittest.main()
