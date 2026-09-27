"""Offline regression tests for indexed period-0 tick history."""

import asyncio
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pocketoption_bridge import server


MINUTE_MS = 60_000
BASE_MS = 1_800_000_000_000
NOW_MS = BASE_MS + 210_000
SYMBOL = "EURUSD_otc"
OTHER_SYMBOL = "GBPUSD_otc"
AUTH_PACKET = '42["auth",{"session":"offline-tick-history-secret","isDemo":0}]'


def tick(at_ms, price, *, asset=None, milliseconds=False):
    row = {"time": at_ms if milliseconds else at_ms // 1000, "price": price}
    if asset is not None:
        row["asset"] = asset
    return row


def packet_ticks():
    return [
        tick(BASE_MS + 1_000, 9.0, asset=SYMBOL),
        tick(BASE_MS + 65_000, 1.12, milliseconds=True),
        tick(BASE_MS + 80_000, 1.30),
        tick(BASE_MS + 80_000, 1.25),
        tick(BASE_MS + 119_000, 1.20),
        tick(BASE_MS + 121_000, 1.50),
        tick(BASE_MS + 179_000, 1.40),
        tick(BASE_MS + 181_000, 8.0, asset=SYMBOL),
    ]


class TickHistoryParserTests(unittest.TestCase):
    def parse(self, raw):
        return server.parse_tick_history(raw, NOW_MS, symbol=SYMBOL)

    def test_sorts_ticks_and_builds_only_closed_internal_minutes(self):
        ticks = packet_ticks()
        rows = self.parse(ticks[4:] + ticks[:4])

        self.assertEqual(rows, [
            [BASE_MS + MINUTE_MS, 1.12, 1.30, 1.12, 1.20],
            [BASE_MS + 2 * MINUTE_MS, 1.50, 1.50, 1.40, 1.40],
        ])

    def test_drops_absent_interior_minutes_without_filling_them(self):
        raw = [
            tick(BASE_MS + 1_000, 1.0),
            tick(BASE_MS + 65_000, 1.1),
            tick(BASE_MS + 181_000, 1.2),
        ]

        self.assertEqual(self.parse(raw), [[BASE_MS + MINUTE_MS, 1.1, 1.1, 1.1, 1.1]])

    def test_rejects_whole_packet_for_invalid_rows_or_conflicting_assets(self):
        valid = packet_ticks()
        invalid_rows = (
            {"time": True, "price": 1.1},
            {"time": BASE_MS // 1000, "price": 0},
            {"time": BASE_MS // 1000, "price": float("nan")},
            {"time": BASE_MS // 1000, "price": float("inf")},
            "not-a-tick",
        )
        for bad_row in invalid_rows:
            with self.subTest(bad_row=bad_row):
                self.assertEqual(self.parse(valid + [bad_row]), [])

        self.assertEqual(self.parse(valid + [tick(BASE_MS + 90_000, 1.1,
                                                   asset=OTHER_SYMBOL)]), [])

    def test_empty_and_non_list_packets_return_no_bars(self):
        for raw in (None, {}, [], [None], [1, 2]):
            with self.subTest(raw=raw):
                self.assertEqual(self.parse(raw), [])

    def test_output_is_capped_at_three_thousand_bars(self):
        first_minute = BASE_MS - 3_010 * MINUTE_MS
        raw = [
            tick(first_minute + minute * MINUTE_MS + 1_000, 1.1)
            for minute in range(3_003)
        ]

        rows = self.parse(raw)

        self.assertEqual(len(rows), 3_000)
        self.assertEqual(rows[0][0], first_minute + 2 * MINUTE_MS)
        self.assertEqual(rows[-1][0], first_minute + 3_001 * MINUTE_MS)


class FakeWebSocket:
    def __init__(self, session, response):
        self.session = session
        self.response = response
        self.sent = []
        self.pending_future = None
        self.future_resolved_after_response = None

    async def send(self, frame):
        self.sent.append(frame)
        event, payload = json.loads(frame[2:])
        self.pending_future = self.session.pending.future
        self.session.handle_event("loadHistoryPeriod", self.response(payload))
        self.future_resolved_after_response = self.pending_future.done()


def make_session(*, timeout=0.02):
    settings = server.Settings(request_timeout=timeout)
    pairs = [
        {"symbol": SYMBOL, "category": "FOREX", "digits": 5},
        {"symbol": OTHER_SYMBOL, "category": "FOREX", "digits": 5},
    ]
    store = server.Store(settings, pairs, wall=lambda: NOW_MS / 1000, clock=lambda: 10.0)
    session = server.BrokerSession(settings, store, AUTH_PACKET)
    return session, store


class IndexedTickHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_indexed_period_zero_packet_populates_candles_and_source(self):
        session, store = make_session()
        session.ws = FakeWebSocket(session, lambda request: {
            "asset": SYMBOL,
            "period": 0,
            "index": request["index"],
            "data": packet_ticks(),
        })

        await session.snapshot(SYMBOL)

        event, request = json.loads(session.ws.sent[0][2:])
        self.assertEqual(event, "loadHistoryPeriod")
        self.assertEqual(request["asset"], SYMBOL)
        self.assertEqual(request["period"], 60)
        self.assertEqual(request["time"], NOW_MS // 1000)
        self.assertEqual(request["offset"], 1000)
        self.assertIs(type(request["index"]), int)
        result = store.klines(SYMBOL, 3_000, 0)
        self.assertEqual(result["data"], [
            [BASE_MS + MINUTE_MS, 1.12, 1.30, 1.12, 1.20],
            [BASE_MS + 2 * MINUTE_MS, 1.50, 1.50, 1.40, 1.40],
        ])
        self.assertEqual(result["history_source"], "historical_ticks")

    async def test_indexed_tick_history_allows_absent_or_empty_asset(self):
        for asset_value in (None, ""):
            with self.subTest(asset=asset_value):
                session, store = make_session()

                def response(request):
                    payload = {
                        "period": 0,
                        "index": request["index"],
                        "data": packet_ticks(),
                    }
                    if asset_value is not None:
                        payload["asset"] = asset_value
                    return payload

                session.ws = FakeWebSocket(session, response)
                await session.snapshot(SYMBOL)
                self.assertEqual(len(store.klines(SYMBOL, 3_000, 0)["data"]), 2)

    async def test_mismatched_asset_index_and_period_do_not_resolve_request(self):
        bad_responses = (
            lambda request: {
                "asset": OTHER_SYMBOL, "period": 0, "index": request["index"],
                "data": packet_ticks(),
            },
            lambda request: {
                "asset": SYMBOL, "period": 0, "index": request["index"] + 1,
                "data": packet_ticks(),
            },
            lambda request: {
                "asset": SYMBOL, "period": 60, "index": request["index"],
                "data": packet_ticks(),
            },
        )
        for response in bad_responses:
            with self.subTest(response=response):
                session, store = make_session()
                session.ws = FakeWebSocket(session, response)

                with self.assertRaises(asyncio.TimeoutError):
                    await session.snapshot(SYMBOL)

                self.assertFalse(session.ws.future_resolved_after_response)
                self.assertEqual(store.klines(SYMBOL, 3_000, 0)["data"], [])

    async def test_protocol_diagnostics_do_not_include_auth_or_packet_values(self):
        session, _ = make_session()
        session.pending = server.PendingHistory(
            SYMBOL, 77, asyncio.get_running_loop().create_future()
        )
        secret = "offline-private-auth-marker"
        payload = {
            "asset": secret,
            "period": 0,
            "index": 77,
            "ssid": AUTH_PACKET,
            "data": [{"time": NOW_MS // 1000, "price": 1.0, "token": secret}],
        }

        with self.assertLogs(server.LOG, level="WARNING") as captured:
            session.handle_event("loadHistoryPeriod", payload)
            session.log_diagnostics("test")

        diagnostic_text = "\n".join(captured.output)
        self.assertNotIn(secret, diagnostic_text)
        self.assertNotIn(AUTH_PACKET, diagnostic_text)
        self.assertNotIn(secret, json.dumps(session.last_history))

    async def test_indexed_period_zero_accepts_explicit_ohlc(self):
        for row in (
            {"time": BASE_MS // 1000, "open": 1.1, "close": 1.2, "high": 1.3, "low": 1.0},
            [BASE_MS // 1000, 1.1, 1.2, 1.3, 1.0],
            [BASE_MS // 1000, 1.1, 1.2, 1.3, 1.0, 100],
            [66, BASE_MS // 1000, 1.1, 1.2, 1.3, 1.0, SYMBOL],
        ):
            with self.subTest(row=row):
                session, store = make_session()
                session.ws = FakeWebSocket(session, lambda request: {
                    "period": 0, "index": request["index"], "data": [row],
                })
                await session.snapshot(SYMBOL)
                result = store.klines(SYMBOL, 3000, 0)
                self.assertEqual(result["data"], [[BASE_MS, 1.1, 1.3, 1.0, 1.2]])
                self.assertEqual(result["history_source"], "ohlc")

    async def test_late_response_cannot_satisfy_another_pair(self):
        session, store = make_session()
        session.ws = FakeWebSocket(session, lambda request: {})
        with self.assertRaises(TimeoutError):
            await session.snapshot(SYMBOL)
        old_index = json.loads(session.ws.sent[0][2:])[1]["index"]

        def second_response(request):
            session.handle_event("loadHistoryPeriod", {
                "index": old_index, "period": 0, "data": packet_ticks(),
            })
            self.assertFalse(session.pending.future.done())
            self.assertEqual(store.klines(SYMBOL, 3000, 0)["data"], [])
            return {"index": request["index"], "period": 0,
                    "data": [{**row, "asset": OTHER_SYMBOL} for row in packet_ticks()]}

        session.ws = FakeWebSocket(session, second_response)
        await session.snapshot(OTHER_SYMBOL)
        self.assertEqual(len(store.klines(OTHER_SYMBOL, 3000, 0)["data"]), 2)
        self.assertEqual(store.klines(SYMBOL, 3000, 0)["data"], [])

    async def test_stream_ticks_do_not_become_history_even_with_matching_index(self):
        session, store = make_session()
        session.pending = server.PendingHistory(SYMBOL, 123, asyncio.get_running_loop().create_future())
        session.handle_event("updateStream", {
            "asset": SYMBOL, "period": 0, "index": 123, "data": packet_ticks(),
        })
        self.assertFalse(session.pending.future.done())
        self.assertEqual(store.klines(SYMBOL, 3000, 0)["data"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
