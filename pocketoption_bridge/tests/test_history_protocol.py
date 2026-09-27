"""Regression fixtures based on public PocketOption protocol implementations."""

import asyncio
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from websockets.legacy.client import connect as websocket_connect
from websockets.legacy.server import serve

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pocketoption_bridge import server


MINUTE = 1_800_000_000_000
SYMBOL = "EURUSD_otc"
BAR = [MINUTE, 1.1, 1.3, 1.0, 1.2]
PROCESSED = [66, MINUTE // 1000, 1.1, 1.2, 1.3, 1.0, SYMBOL]


class HistoryShapeTests(unittest.TestCase):
    def test_processed_seven_field_candle(self):
        self.assertEqual(server.parse_bars([PROCESSED], MINUTE + 30000), [BAR])

    def test_two_field_tick_history_is_not_ohlc(self):
        self.assertEqual(server.parse_bars([[MINUTE // 1000, 1.1]], MINUTE + 30000), [])

    def test_binary_header_with_full_payload_attachment(self):
        decoder = server.EventDecoder()
        self.assertEqual(decoder.feed('451-["loadHistoryPeriod",{"_placeholder":true,"num":0}]'), [])
        payload = {"index": 123, "period": 60, "data": [PROCESSED]}
        self.assertEqual(decoder.feed(json.dumps(payload).encode()), [("loadHistoryPeriod", payload)])


class HistoryNetworkTests(unittest.IsolatedAsyncioTestCase):
    async def test_three_pairs_indexed_binary_history_reaches_http(self):
        symbols = ["EURUSD_otc", "GBPUSD_otc", "USDJPY_otc"]
        now_ms = MINUTE + 210000
        store = server.Store(server.Settings(), [
            {"symbol": s, "category": "FOREX", "digits": 5} for s in symbols
        ], wall=lambda: now_ms / 1000)
        store.begin_session()
        store.worker_alive = True
        requests = []

        async def broker(ws):
            await ws.send('0{"sid":"local","pingInterval":20000,"pingTimeout":20000}')
            self.assertEqual(await ws.recv(), "40")
            await ws.send("40")
            await ws.recv()
            await ws.send('42["successauth",{}]')
            for symbol in symbols:
                event, request = json.loads((await ws.recv())[2:])
                self.assertEqual(event, "loadHistoryPeriod")
                self.assertEqual(request["asset"], symbol)
                requests.append(request)
                if symbol == symbols[0]:
                    payload = {"index": request["index"], "period": 60,
                               "data": [[66, MINUTE // 1000 + 120, 1.1, 1.2, 1.3, 1.0, symbol]]}
                else:
                    ticks = [{"asset": symbol, "time": (MINUTE + offset) / 1000, "price": price}
                             for offset, price in [(10000, 1.0), (65000, 1.1), (90000, 1.2),
                                                   (125000, 1.3), (150000, 1.1), (190000, 1.4)]]
                    payload = {"asset": "", "index": request["index"], "period": 0, "data": ticks}
                await ws.send('451-["loadHistoryPeriod",{"_placeholder":true,"num":0}]')
                await ws.send(json.dumps(payload).encode())
            await ws.wait_closed()

        async with serve(broker, "127.0.0.1", 0, ping_interval=None) as listener:
            url = f"ws://127.0.0.1:{listener.sockets[0].getsockname()[1]}"

            async def local_connect(uri, **kwargs):
                self.assertEqual(uri, url)
                kwargs.pop("ssl")
                return await websocket_connect(uri, **kwargs)

            session = server.BrokerSession(server.Settings(), store,
                '42["auth",{"session":"offline-only","isDemo":1}]')
            session.url = url
            with patch.object(server, "connect", new=local_connect):
                try:
                    await session.open()
                    for symbol in symbols:
                        await asyncio.wait_for(session.snapshot(symbol), 2)
                    client = server.create_app(store).test_client()
                    self.assertEqual(client.get("/readyz").status_code, 200)
                    for symbol in symbols:
                        response = client.get(f"/v1/klines?symbol={symbol}&limit=3000")
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual(response.json["pair_status"], "LIVE")
                        self.assertGreater(len(response.json["data"]), 0)
                    self.assertEqual(len({r["index"] for r in requests}), 3)
                finally:
                    await session.close()
            self.assertTrue(session.receiver.done())


if __name__ == "__main__":
    unittest.main(verbosity=2)
