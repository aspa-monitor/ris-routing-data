import unittest

from pipeline.ris.normalize import asn_category, normalize, parse_segments


class TestNormalize(unittest.TestCase):
    def test_simple(self):
        cp = normalize("7018 3356 749", 7018)
        self.assertEqual(cp.tokens, [7018, 3356, 749])
        self.assertEqual(cp.flags, set())
        self.assertEqual(cp.origin, 749)
        self.assertEqual(cp.adjacencies, [(3356, 7018), (749, 3356)])

    def test_prepend_collapse(self):
        cp = normalize("7018 1299 1299 1299 65000 65000", 7018)
        self.assertEqual(cp.text, "7018 1299 65000")
        self.assertIn("private", cp.flags)
        self.assertNotIn("loop", cp.flags)
        # private origin: only the public pair survives
        self.assertEqual(cp.adjacencies, [(1299, 7018)])

    def test_as_set(self):
        cp = normalize("7018 174 32440 {2055,2048,2048}", 7018)
        self.assertEqual(cp.text, "7018 174 32440 {2048,2055}")
        self.assertIn("as_set", cp.flags)
        self.assertIsNone(cp.origin)
        self.assertEqual(cp.set_members, (2048, 2055))
        self.assertEqual(cp.adjacencies, [(174, 7018), (32440, 174)])
        self.assertEqual(cp.length, 4)

    def test_set_repeating_sequence_asn_is_not_loop(self):
        cp = normalize("7018 3356 26585 {26585}", 7018)
        self.assertNotIn("loop", cp.flags)

    def test_loop(self):
        cp = normalize("7018 3356 1299 3356 65", 7018)
        self.assertIn("loop", cp.flags)
        self.assertEqual(cp.adjacencies, [])

    def test_as_trans_and_peer_mismatch(self):
        cp = normalize("3356 23456 15169", 7018)
        self.assertEqual(cp.flags, {"as_trans", "peer_mismatch"})
        self.assertEqual(cp.adjacencies, [])

    def test_confed(self):
        segs = parse_segments("7018 (65001 65002) 3356")
        self.assertEqual([s.kind for s in segs], ["seq", "confed_seq", "seq"])
        cp = normalize("7018 (65001 65002) 3356", 7018)
        self.assertEqual(cp.text, "7018 3356")
        self.assertIn("confed", cp.flags)

    def test_empty(self):
        self.assertEqual(normalize("", 7018).flags, {"empty"})

    def test_categories(self):
        self.assertIsNone(asn_category(7018))
        self.assertEqual(asn_category(0), "reserved")
        self.assertEqual(asn_category(64496), "reserved")
        self.assertEqual(asn_category(65000), "private")
        self.assertEqual(asn_category(4200000001), "private")
        self.assertEqual(asn_category(23456), "as_trans")
        self.assertIsNone(asn_category(131072))


if __name__ == "__main__":
    unittest.main()
