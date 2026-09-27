"""Offline behavioral tests for the PocketOption bridge."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pocketoption_bridge import server
from websockets.legacy.client import connect as websocket_connect
from websockets.legacy.server import serve as websocket_serve


BASE_MINUTE_MS = (1_700_000_000 // 60) * 60_000
NOW_MS = BASE_MINUTE_MS + 180_000
PAIR_NAMES = ("EURUSD_otc", "GBPUSD_otc", "USDJPY_otc")
TEST_SSID = '42["auth",{"session":"offline-test-session","isDemo":0}]'


class MutableClock:
    def __init__(self):
        self.wall_seconds = NOW_MS / 1000
        self.monotonic_seconds = 100.0

    def wall(self):
        return self.wall_seconds

    def monotonic(self):
        return self.monotonic_seconds


def make_settings(**overrides):
    values = {
        "stale_after": 60,
        "max_candle_age": 3600,
        "request_timeout": 0.1,
        "connect_timeout": 2,
    }
    values.update(overrides)
    return server.Settings(**values)


def make_store(symbols=PAIR_NAMES, *, clock=None, settings=None):
    clock = clock or MutableClock()
    pairs = [{"symbol": symbol, "category": "FOREX", "digits": 5}
             for symbol in symbols]
    store = server.Store(settings or make_settings(), pairs,
                         wall=clock.wall, clock=clock.monotonic)
    return store, clock


def bar(timestamp=BASE_MINUTE_MS, open_=1.1, high=1.2, low=1.0, close=1.15):
    return [timestamp, open_, high, low, close]


class StoreTests(unittest.TestCase):
    def test_batch_ticks_update_all_pairs_and_preserve_ohlc_order(self):
        store, _ = make_store(PAIR_NAMES[:2])
        store.begin_session()
        for symbol in PAIR_NAMES[:2]:
            self.assertTrue(store.merge_snapshot(symbol, [bar()]))

        tick_time = BASE_MINUTE_MS + 30_000
        store.ticks([
            ["EURUSD_otc", tick_time, 1.18],
            ["GBPUSD_otc", tick_time, 1.07],
            ["UNKNOWN_otc", tick_time, 9.0],
        ])

        eur = store.klines("EURUSD_otc", 10, 0)["data"][0]
        gbp = store.klines("GBPUSD_otc", 10, 0)["data"][0]
        self.assertEqual(eur, [BASE_MINUTE_MS, 1.1, 1.2, 1.0, 1.18])
        self.assertEqual(gbp, [BASE_MINUTE_MS, 1.1, 1.2, 1.0, 1.07])
        self.assertEqual(store.pairs["EURUSD_otc"].last_tick_ms, tick_time)
        self.assertEqual(store.pairs["GBPUSD_otc"].last_tick_ms, tick_time)

    def test_wire_candles_convert_T_O_C_H_L_to_http_T_O_H_L_C(self):
        parsed = server.parse_bars(
            [[BASE_MINUTE_MS // 1000, 1.1, 1.16, 1.23, 1.02]], NOW_MS
        )
        self.assertEqual(parsed, [[BASE_MINUTE_MS, 1.1, 1.23, 1.02, 1.16]])

    def test_boolean_prices_are_not_valid_candles(self):
        self.assertEqual(server.parse_bars([
            [BASE_MINUTE_MS // 1000, True, 1.1, 1.2, 0.9]
        ], NOW_MS), [])

    def test_concurrent_cache_writers_produce_a_valid_atomic_snapshot(self):
        store, _ = make_store(("EURUSD_otc",))
        store.merge_snapshot("EURUSD_otc", [bar()])
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "cache.json"
            with ThreadPoolExecutor(max_workers=4) as executor:
                list(executor.map(lambda _: store.save(target), range(12)))
            saved = json.loads(target.read_text())
            self.assertEqual(saved["bars"]["EURUSD_otc"], [bar()])
            self.assertFalse(target.with_suffix(".json.tmp").exists())

    def test_old_snapshot_cannot_keep_pair_fresh_indefinitely(self):
        store, clock = make_store(("EURUSD_otc",))
        store.begin_session()
        store.merge_snapshot("EURUSD_otc", [bar()])
        self.assertEqual(store.pairs_payload()["data"][0]["status"], "LIVE")

        clock.monotonic_seconds += store.settings.stale_after + 1
        self.assertEqual(store.pairs_payload()["data"][0]["status"], "STALE")
        self.assertEqual(store.pairs_payload()["status"], "WAITING_DATA")

    def test_pair_freshness_and_epoch_are_reset_on_reconnect(self):
        store, _ = make_store(PAIR_NAMES[:2])
        store.begin_session()
        first_epoch = store.epoch
        store.merge_snapshot("EURUSD_otc", [bar()])
        store.merge_snapshot("GBPUSD_otc", [bar()])
        self.assertEqual(store.pairs_payload()["status"], "READY")

        store.begin_session()
        self.assertEqual(store.epoch, first_epoch + 1)
        store.merge_snapshot("EURUSD_otc", [bar()])
        payload = store.pairs_payload()
        self.assertEqual(payload["status"], "DEGRADED")
        statuses = {item["symbol"]: item["status"] for item in payload["data"]}
        self.assertEqual(statuses, {"EURUSD_otc": "LIVE", "GBPUSD_otc": "STALE"})

    def test_snapshot_cannot_rewind_a_live_candle_close(self):
        store, clock = make_store(("EURUSD_otc",))
        clock.wall_seconds = (BASE_MINUTE_MS + 40_000) / 1000
        store.begin_session()
        store.merge_snapshot("EURUSD_otc", [bar()])
        store.ticks([["EURUSD_otc", BASE_MINUTE_MS + 30_000, 1.19]])

        older_snapshot = bar(open_=1.08, high=1.14, low=1.04, close=1.10)
        store.merge_snapshot("EURUSD_otc", [older_snapshot])
        self.assertEqual(
            store.klines("EURUSD_otc", 10, 0)["data"][0],
            [BASE_MINUTE_MS, 1.08, 1.2, 1.0, 1.19],
        )

    def test_closed_candle_can_be_reconciled_with_broker_history(self):
        store, clock = make_store(("EURUSD_otc",))
        clock.wall_seconds = (BASE_MINUTE_MS + 40_000) / 1000
        store.begin_session()
        store.merge_snapshot("EURUSD_otc", [bar()])
        store.ticks([["EURUSD_otc", BASE_MINUTE_MS + 30_000, 1.19]])
        clock.wall_seconds += 60
        final_bar = bar(close=1.17)
        store.merge_snapshot("EURUSD_otc", [final_bar])
        self.assertEqual(store.klines("EURUSD_otc", 10, 0)["data"], [final_bar])

    def test_repeated_old_snapshots_do_not_refresh_source_timestamp(self):
        store, clock = make_store(("EURUSD_otc",))
        store.begin_session()
        store.merge_snapshot("EURUSD_otc", [bar()])
        clock.wall_seconds += store.settings.max_candle_age + 1
        clock.monotonic_seconds += store.settings.max_candle_age + 1
        for _ in range(3):
            store.merge_snapshot("EURUSD_otc", [bar()])
        self.assertEqual(store.pairs_payload()["data"][0]["status"], "STALE")

    def test_ticks_without_snapshot_do_not_fabricate_a_candle(self):
        store, _ = make_store(("EURUSD_otc",))
        store.begin_session()
        store.ticks([["EURUSD_otc", BASE_MINUTE_MS + 30_000, 1.1]])

        self.assertEqual(store.pairs["EURUSD_otc"].bars, {})
        self.assertGreater(store.pairs["EURUSD_otc"].last_tick_ms, 0)
        self.assertEqual(store.pairs_payload()["data"][0]["status"], "LOADING")

    def test_out_of_order_ticks_and_invalid_or_nan_bars_are_ignored(self):
        store, _ = make_store(("EURUSD_otc",))
        store.begin_session()
        store.merge_snapshot("EURUSD_otc", [bar()])
        store.ticks([
            ["EURUSD_otc", BASE_MINUTE_MS + 30_000, 1.18],
            ["EURUSD_otc", BASE_MINUTE_MS + 20_000, 0.5],
        ])
        self.assertEqual(store.pairs["EURUSD_otc"].bars[BASE_MINUTE_MS][4], 1.18)
        self.assertEqual(store.pairs["EURUSD_otc"].last_tick_ms, BASE_MINUTE_MS + 30_000)

        parsed = server.parse_bars([
            [BASE_MINUTE_MS // 1000 + 60, 1.1, 1.15, 1.2, 1.0],
            [BASE_MINUTE_MS // 1000, 1.1, 1.15, 1.05, 1.0],
            [BASE_MINUTE_MS // 1000 + 120, 1.1, float("nan"), 1.2, 1.0],
            [BASE_MINUTE_MS // 1000 + 180, 1.1, 1.15, 1.05, 1.0],
        ], NOW_MS)
        self.assertEqual([row[0] for row in parsed], [BASE_MINUTE_MS + 60_000])

    def test_cache_restore_does_not_mark_history_fresh(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory) / "history-cache.json"
            source, _ = make_store(("EURUSD_otc",))
            source.merge_snapshot("EURUSD_otc", [bar()])
            source.save(cache)

            restored, _ = make_store(("EURUSD_otc",))
            restored.restore(cache)
            restored.begin_session()

            self.assertEqual(restored.pairs["EURUSD_otc"].bars[BASE_MINUTE_MS], bar())
            self.assertEqual(restored.pairs_payload()["data"][0]["status"], "STALE")

    def test_load_pairs_reads_valid_file_and_rejects_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "pairs.json"
            config.write_text(json.dumps([{"symbol": "EURUSD_otc", "digits": 5}]),
                              encoding="utf-8")
            self.assertEqual(server.load_pairs(config), [
                {"symbol": "EURUSD_otc", "category": "FOREX", "digits": 5}
            ])

            config.write_text(json.dumps([
                {"symbol": "EURUSD_otc"}, {"symbol": "EURUSD_otc"}
            ]), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "duplicate"):
                server.load_pairs(config)


class FlaskApiTests(unittest.TestCase):
    TOKEN = "offline-test-api-token"

    def setUp(self):
        self.store, _ = make_store(PAIR_NAMES[:2])
        self.client = server.create_app(self.store, self.TOKEN).test_client()
        self.headers = {"Authorization": f"Bearer {self.TOKEN}"}

    def test_token_is_required_for_every_endpoint_and_responses_are_uncached(self):
        for path in ("/v1/pairs", "/v1/klines?symbol=EURUSD_otc", "/healthz",
                     "/server_status", "/readyz"):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 401)
                self.assertEqual(response.json["error"], "UNAUTHORIZED")
                self.assertEqual(
                    self.client.get(path, headers={"Authorization": "Bearer wrong"}).status_code,
                    401,
                )

        response = self.client.get("/v1/pairs", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")

    def test_klines_since_limit_symbol_alias_and_range_validation(self):
        self.store.begin_session()
        rows = [
            bar(BASE_MINUTE_MS, close=1.11),
            bar(BASE_MINUTE_MS + 60_000, close=1.12),
            bar(BASE_MINUTE_MS + 120_000, close=1.13),
        ]
        self.store.merge_snapshot("EURUSD_otc", rows)
        query = ("/v1/klines?symbol=EURUSD-OTCp&interval=1m&limit=2&since="
                 f"{BASE_MINUTE_MS + 60_000}")
        response = self.client.get(query, headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["data"], rows[1:])
        self.assertEqual(response.json["pair_status"], "LIVE")

        invalid_queries = (
            "symbol=EURUSD_otc&limit=0",
            f"symbol=EURUSD_otc&limit={server.MAX_BARS + 1}",
            "symbol=EURUSD_otc&since=-1",
            "symbol=EURUSD_otc&limit=NaN",
            "symbol=EURUSD_otc&interval=5m",
        )
        for query in invalid_queries:
            with self.subTest(query=query):
                response = self.client.get("/v1/klines?" + query, headers=self.headers)
                self.assertEqual(response.status_code, 400)

        self.assertEqual(
            self.client.get("/v1/klines?symbol=missing", headers=self.headers).status_code,
            404,
        )

    def test_pairs_health_and_readiness_report_worker_and_market_states(self):
        response = self.client.get("/v1/pairs", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["protocol"], 2)
        self.assertEqual(response.json["status"], "CONNECTING")
        self.assertEqual(len(response.json["data"]), 2)

        health = self.client.get("/healthz", headers=self.headers)
        self.assertEqual(health.status_code, 503)
        self.assertFalse(health.json["worker_alive"])
        self.assertEqual(health.json["active"], 0)
        self.assertEqual(health.json["total"], 2)

        self.store.worker_alive = True
        self.store.begin_session()
        health = self.client.get("/server_status", headers=self.headers)
        ready = self.client.get("/readyz", headers=self.headers)
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json["status"], "WAITING_DATA")
        self.assertEqual(ready.status_code, 503)

        self.store.merge_snapshot("EURUSD_otc", [bar()])
        health = self.client.get("/healthz", headers=self.headers)
        self.assertEqual(health.json["status"], "DEGRADED")
        self.assertEqual(health.json["active"], 1)
        self.assertEqual(self.client.get("/readyz", headers=self.headers).status_code, 503)

        self.store.merge_snapshot("GBPUSD_otc", [bar()])
        self.assertEqual(self.client.get("/healthz", headers=self.headers).json["status"], "READY")
        self.assertEqual(self.client.get("/readyz", headers=self.headers).status_code, 200)


class BrokerSessionTests(unittest.IsolatedAsyncioTestCase):
    @asynccontextmanager
    async def local_broker(self, handler):
        async with websocket_serve(handler, "127.0.0.1", 0, ping_interval=None) as broker:
            port = broker.sockets[0].getsockname()[1]
            url = f"ws://127.0.0.1:{port}/socket.io/?EIO=4&transport=websocket"

            async def connect_local(uri, **kwargs):
                self.assertEqual(uri, url)
                kwargs.pop("ssl", None)
                return await websocket_connect(uri, **kwargs)

            with patch.object(server, "connect", new=connect_local):
                yield url

    def make_session(self, *, timeout=0.1):
        store, _ = make_store(("EURUSD_otc", "GBPUSD_otc"))
        settings = make_settings(request_timeout=timeout, connect_timeout=1)
        return server.BrokerSession(settings, store, TEST_SSID), store

    async def test_engineio_auth_heartbeat_binary_event_and_labelled_m1_snapshot(self):
        wrong_events_processed = asyncio.Event()
        release_valid_snapshot = asyncio.Event()
        received = {"namespace": [], "auth": [], "heartbeat": [], "request": []}

        async def handler(ws):
            await ws.send('0{"sid":"offline","pingInterval":1000,"pingTimeout":1000}')
            received["namespace"].append(await ws.recv())
            await ws.send("40")
            received["auth"].append(await ws.recv())
            await ws.send('42["successauth",{}]')
            await ws.send("2test-heartbeat")
            received["heartbeat"].append(await ws.recv())
            received["request"].append(await ws.recv())
            history_request = json.loads(received["request"][0][2:])[1]

            valid = [[BASE_MINUTE_MS // 1000, 1.1, 1.16, 1.23, 1.02]]
            await ws.send('42["loadHistoryPeriod",' + json.dumps({
                "asset": "GBPUSD_otc", "period": 60, "candles": valid,
            }, separators=(",", ":")) + "]")
            await ws.send('42["loadHistoryPeriod",' + json.dumps({
                "asset": "EURUSD_otc", "period": 5, "candles": valid,
            }, separators=(",", ":")) + "]")
            await ws.send('42["loadHistoryPeriod",' + json.dumps({"candles": valid}) + "]")
            await ws.send('42["offline-test-barrier",{}]')
            await release_valid_snapshot.wait()
            await ws.send(
                '451-["loadHistoryPeriod",{"asset":"EURUSD_otc","period":60,'
                '"index":' + str(history_request["index"]) + ',"candles":{"_placeholder":true,"num":0}}]'
            )
            await ws.send(json.dumps(valid, separators=(",", ":")).encode())
            try:
                await ws.wait_closed()
            except Exception:
                pass

        session, store = self.make_session()
        handle_event = session.handle_event

        def observe_barrier(event, payload):
            handle_event(event, payload)
            if event == "offline-test-barrier":
                wrong_events_processed.set()

        session.handle_event = observe_barrier
        async with self.local_broker(handler) as url:
            session.url = url
            try:
                await session.open()
                snapshot = asyncio.create_task(session.snapshot("EURUSD_otc"))
                await asyncio.wait_for(wrong_events_processed.wait(), 1)
                self.assertFalse(snapshot.done(), "unlabelled or mismatched history resolved the request")
                self.assertIn(BASE_MINUTE_MS, store.pairs["GBPUSD_otc"].bars)

                release_valid_snapshot.set()
                await asyncio.wait_for(snapshot, 1)
                self.assertEqual(received["namespace"], ["40"])
                self.assertEqual(received["auth"], [TEST_SSID])
                self.assertEqual(received["heartbeat"], ["3test-heartbeat"])
                request_event, request_data = json.loads(received["request"][0][2:])
                self.assertEqual(request_event, "loadHistoryPeriod")
                self.assertEqual(request_data["asset"], "EURUSD_otc")
                self.assertEqual(request_data["period"], 60)
                self.assertEqual(request_data["offset"], 3600)
                self.assertEqual(request_data["time"], NOW_MS // 1000)
                self.assertIs(type(request_data["index"]), int)
                self.assertEqual(store.pairs["EURUSD_otc"].bars[BASE_MINUTE_MS],
                                 [BASE_MINUTE_MS, 1.1, 1.23, 1.02, 1.16])
                self.assertIn(BASE_MINUTE_MS, store.pairs["GBPUSD_otc"].bars)
            finally:
                release_valid_snapshot.set()
                await session.close()
        self.assertTrue(session.receiver.done())

    async def test_failed_authentication_receiver_is_cleaned_up(self):
        auth_received = asyncio.Event()

        async def handler(ws):
            await ws.send('0{"sid":"offline","pingInterval":1000,"pingTimeout":1000}')
            await ws.recv()
            await ws.send("40")
            await ws.recv()
            auth_received.set()
            await ws.send('42["notAuthorized",{}]')
            await ws.wait_closed()

        session, _ = self.make_session()
        async with self.local_broker(handler) as url:
            session.url = url
            with self.assertRaises(server.AuthRequired):
                await session.open()
            await asyncio.wait_for(auth_received.wait(), 1)
            receiver = session.receiver
            self.assertIsNotNone(receiver)
            self.assertFalse(receiver.done())
            await session.close()
            self.assertTrue(receiver.done())
            self.assertTrue(receiver.cancelled())

    async def test_snapshot_timeout_clears_pending_request_and_receiver(self):
        request_received = asyncio.Event()

        async def handler(ws):
            await ws.send('0{"sid":"offline","pingInterval":1000,"pingTimeout":1000}')
            await ws.recv()
            await ws.send("40")
            await ws.recv()
            await ws.send('42["successauth",{}]')
            await ws.recv()
            request_received.set()
            await ws.wait_closed()

        session, _ = self.make_session(timeout=0.1)
        async with self.local_broker(handler) as url:
            session.url = url
            await session.open()
            with self.assertRaises(TimeoutError):
                await session.snapshot("EURUSD_otc")
            await asyncio.wait_for(request_received.wait(), 1)
            self.assertIsNone(session.pending)
            await session.close()
            self.assertTrue(session.receiver.done())


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_reconnects_with_fake_sessions_and_stops_cleanly(self):
        store, _ = make_store(("EURUSD_otc",))
        stop = threading.Event()
        created = []
        sessions = []
        failures = []

        class TransportDisconnect(ConnectionError):
            pass

        class FakeSession:
            def __init__(self, *_args):
                self.index = len(created) + 1
                created.append(self)
                self.receiver = None
                self.auth_error = False
                self.snapshot_requests = 0
                self.keepalive_calls = 0

            async def open(self):
                self.receiver = asyncio.create_task(self.finish_after_activity())

            async def finish_after_activity(self):
                while not self.snapshot_requests or not self.keepalive_calls:
                    await asyncio.sleep(0)
                if self.index == 1:
                    raise TransportDisconnect("mock socket closed")
                stop.set()

            async def snapshot(self, _symbol):
                self.snapshot_requests += 1
                await asyncio.Event().wait()

            async def keepalive(self):
                self.keepalive_calls += 1
                await asyncio.Event().wait()

            async def close(self):
                sessions.append(self)

        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            (base / "ssid.txt").write_text(TEST_SSID, encoding="utf-8")
            worker = server.Worker(
                make_settings(), store, base, stop, session_factory=FakeSession
            )

            async def fast_pause(seconds):
                if seconds >= 30:
                    while not stop.is_set():
                        await asyncio.sleep(0.001)
                else:
                    await asyncio.sleep(0)

            worker.pause = fast_pause
            with patch.object(
                server, "safe_failure",
                side_effect=lambda action, error: failures.append((action, error)),
            ):
                await asyncio.wait_for(worker.run(), 2)

        self.assertEqual(len(created), 2)
        self.assertEqual(len(sessions), 2)
        for session in sessions:
            self.assertEqual(session.snapshot_requests, 1)
            self.assertEqual(session.keepalive_calls, 1)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0][0], "Session failed")
        self.assertIs(type(failures[0][1]), TransportDisconnect)
        self.assertEqual(store.epoch, 2)
        self.assertFalse(store.worker_alive)
        self.assertEqual(store.connection, "STOPPED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
