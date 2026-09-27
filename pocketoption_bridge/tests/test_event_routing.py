"""Regression tests for binary envelopes and history routing diagnostics."""

import asyncio
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pocketoption_bridge import server


SYMBOL = "EURUSD_otc"
MINUTE = 1_800_000_000_000


def history(index=123):
    return {"index": index, "period": 60, "data": [{
        "time": MINUTE // 1000, "open": 1.1, "close": 1.2, "high": 1.3, "low": 1.0,
    }]}


class EventRoutingTests(unittest.IsolatedAsyncioTestCase):
    def make_session(self):
        store = server.Store(server.Settings(), [{"symbol": SYMBOL, "digits": 5, "category": "FOREX"}],
                             wall=lambda: MINUTE / 1000 + 30)
        store.begin_session()
        session = server.BrokerSession(server.Settings(), store,
                                      '42["auth",{"session":"offline-only","isDemo":1}]')
        session.pending = server.PendingHistory(SYMBOL, 123, asyncio.get_running_loop().create_future())
        return session, store

    def route(self, session, frames):
        for frame in frames:
            for event, payload in session.decoder.feed(frame):
                session.handle_event(event, payload)

    async def test_fast_binary_history_with_matching_index(self):
        session, store = self.make_session()
        self.route(session, [
            '451-["loadHistoryPeriodFast",{"_placeholder":true,"num":0}]',
            json.dumps(history()).encode(),
        ])
        self.assertTrue(session.pending.future.done())
        self.assertEqual(len(store.klines(SYMBOL, 3000, 0)["data"]), 1)

    async def test_raw_text_history_with_matching_index(self):
        session, store = self.make_session()
        self.route(session, [json.dumps(history())])
        self.assertTrue(session.pending.future.done())
        self.assertEqual(len(store.klines(SYMBOL, 3000, 0)["data"]), 1)

    async def test_fast_reply_cannot_bypass_asset_index_or_period_validation(self):
        for fields in ({"index": 124}, {"asset": "GBPUSD_otc"}, {"period": 5}, {"index": None}):
            with self.subTest(fields=fields):
                session, store = self.make_session()
                self.route(session, ['42' + json.dumps(["loadHistoryPeriodFast", {**history(), **fields}])])
                self.assertFalse(session.pending.future.done())
                self.assertEqual(store.klines(SYMBOL, 3000, 0)["data"], [])

    async def test_malformed_history_envelope_is_counted_and_rejected(self):
        session, store = self.make_session()
        with self.assertLogs(server.LOG, level="WARNING") as captured:
            session.handle_event("loadHistoryPeriodFast", [[SYMBOL, MINUTE / 1000, 1.2]])
        self.assertEqual(session.stats["history_packets"], 1)
        self.assertEqual(session.stats["ignored_history"], 1)
        self.assertIn("unsupported_history_envelope", "\n".join(captured.output))
        self.assertEqual(store.klines(SYMBOL, 3000, 0)["data"], [])

    async def test_unreferenced_binary_body_is_visible_not_assigned_to_request(self):
        session, store = self.make_session()
        self.route(session, ['451-["loadHistoryPeriodFast"]', json.dumps(history()).encode()])
        self.assertEqual(session.decoder.stats["unreferenced_attachments"], 1)
        self.assertEqual(session.decoder.last_binary["fields"]["index"], "int")
        self.assertFalse(session.pending.future.done())
        self.assertEqual(store.klines(SYMBOL, 3000, 0)["data"], [])

    async def test_binary_diagnostics_never_include_auth_or_unknown_keys(self):
        session, _ = self.make_session()
        private = "offline-private-envelope"
        self.route(session, [
            '451-' + json.dumps([private, {"_placeholder": True, "num": 0}]),
            json.dumps({private: private, "data": [{"time": private, "price": private}]}).encode(),
        ])
        with self.assertLogs(server.LOG, level="WARNING") as captured:
            session.log_diagnostics("history_timeout")
        self.assertNotIn(private, "\n".join(captured.output))
        self.assertEqual(session.decoder.stats["placeholder_events"], 1)

    async def test_diagnostic_storage_is_bounded_for_unknown_event_names(self):
        session, _ = self.make_session()
        for i in range(1000):
            session.handle_event(f"unknown-{i}", {f"field-{i}": [i]})
        self.assertEqual(session.event_counts, {"other": 1000})
        self.assertEqual(len(session.event_shapes), 1)

    async def test_unhandled_envelope_shape_is_visible_without_private_values(self):
        session, _ = self.make_session()
        marker = "offline-sensitive-marker"
        with self.assertLogs(server.LOG, level="WARNING") as captured:
            session.handle_event("unknown-" + marker, {"session": marker, "data": history()["data"]})
            session.handle_event("updateStream", [[SYMBOL, MINUTE / 1000, 1.2]])
            session.log_diagnostics("history_timeout")
        text = "\n".join(captured.output)
        self.assertNotIn(marker, text)
        self.assertIn("events=", text)
        self.assertIn("other", text)
        self.assertIn("updateStream", text)
        self.assertIn("event_shapes=", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
