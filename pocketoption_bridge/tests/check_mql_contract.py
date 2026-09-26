"""Static checks for the MQL advisor contract; this does not compile MQL5."""

from pathlib import Path
import unittest


ADVISOR = Path(__file__).resolve().parents[1] / "Free_OTC.mq5"


class MqlContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = ADVISOR.read_text(encoding="utf-8")

    def test_only_pocketoption_and_no_external_json_dependency(self):
        self.assertNotIn("JAson.mqh", self.source)
        self.assertNotIn("BINODEX", self.source.upper())
        self.assertIn("_OTCpo", self.source)
        self.assertIn("/v1/pairs", self.source)
        self.assertIn("/v1/klines", self.source)

    def test_candles_are_imported_without_synthetic_ticks_or_deletion(self):
        self.assertIn("CustomRatesUpdate", self.source)
        for forbidden in (
            "CustomTicksAdd",
            "CustomRatesReplace",
            "CustomRatesDelete",
            "CustomSymbolDelete",
            "ChartIndicatorDelete",
            "ObjectsDeleteAll",
        ):
            self.assertNotIn(forbidden, self.source)

    def test_request_and_history_safety_contract(self):
        self.assertEqual(self.source.count("WebRequest("), 1)
        self.assertIn('"Authorization: Bearer "', self.source)
        self.assertIn("protocol==2", self.source)
        self.assertIn("g_pairs[index].needs_full=true", self.source)
        self.assertIn("g_pairs[index].candidate_epoch", self.source)
        self.assertNotIn("SelectQueuedFullPair", self.source)
        self.assertIn("SelectNextPair(now_ms)", self.source)
        self.assertIn("g_next_pair_index=(index+1)%count", self.source)
        self.assertIn('(pair_status=="UNAVAILABLE") ? 60 : 2', self.source)
        self.assertIn('if(pair_status=="UNAVAILABLE")', self.source)
        self.assertIn("updated!=expected || error!=0", self.source)
        self.assertIn("InpBarsHistory>3000", self.source)
        self.assertIn("time_ms%60000!=0", self.source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
