import unittest
from datetime import datetime, timezone

from pipeline.ris.build_snapshot import _parse_ist, _parse_ts, plan_run
from pipeline.ris.timefmt import ist_iso, ist_text


class TestIST(unittest.TestCase):
    def test_iso_from_utc(self):
        self.assertEqual(ist_iso(datetime(2026, 11, 24, 14, 25, tzinfo=timezone.utc)), "2026-11-24T19:55:00+05:30")
        self.assertEqual(ist_iso("2026-11-24T14:25:00+00:00"), "2026-11-24T19:55:00+05:30")

    def test_crosses_midnight(self):
        self.assertEqual(ist_text("2026-11-23T20:00:00+00:00"), "2026-11-24 01:30 IST")

    def test_same_instant(self):
        utc = datetime(2026, 11, 24, 8, 0, tzinfo=timezone.utc)
        self.assertEqual(datetime.fromisoformat(ist_iso(utc)), utc)

    def test_until_is_ist_ripe_stamp_is_utc(self):
        self.assertEqual(_parse_ist("20261124.1330"), datetime(2026, 11, 24, 8, 0, tzinfo=timezone.utc))
        self.assertEqual(_parse_ts("20261124.0800"), datetime(2026, 11, 24, 8, 0, tzinfo=timezone.utc))

    def test_plan_reason_in_ist(self):
        src = {"peer_asn": 7018, "peer_ips": ["12.0.1.63"]}
        state = {"peer_asn": 7018, "peer_ips": ["12.0.1.63"], "bview_time": "2026-11-24T00:00:00+00:00",
                 "as_of": "2026-11-24T03:55:00+00:00"}
        self.assertEqual(plan_run(state, datetime(2026, 11, 24, 8, 0, tzinfo=timezone.utc), src),
                         ("fresh", "new bview 2026-11-24 13:30 IST"))
        self.assertEqual(plan_run(state, datetime(2026, 11, 24, 0, 0, tzinfo=timezone.utc), src),
                         ("continue", "saved state as of 2026-11-24 09:25 IST"))


if __name__ == "__main__":
    unittest.main()
