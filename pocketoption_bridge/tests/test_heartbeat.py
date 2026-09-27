"""Regression tests for Engine.IO heartbeat handling."""

import asyncio
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from websockets.legacy.client import connect as websocket_connect
from websockets.legacy.server import serve

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pocketoption_bridge import server


AUTH_PACKET = '42["auth",{"session":"offline-heartbeat-only","isDemo":0}]'
SYMBOL = "EURUSD_otc"


def make_session():
    settings = server.Settings(connect_timeout=2)
    store = server.Store(settings, [{"symbol": SYMBOL, "category": "FOREX"}])
    return server.BrokerSession(settings, store, AUTH_PACKET)


class MutableClock:
    def __init__(self):
        self.value = 0.0

    def monotonic(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class FakeWebSocket:
    def __init__(self, frames=(), *, clock=None, step=0, fallback=None, max_reads=100):
        self.frames = list(frames)
        self.clock = clock
        self.step = step
        self.fallback = fallback
        self.max_reads = max_reads
        self.reads = 0
        self.sent = []
        self.recv_cancelled = False

    async def recv(self):
        self.reads += 1
        if self.reads > self.max_reads:
            raise AssertionError("receive deadline did not stop the frame stream")
        if self.clock is not None:
            self.clock.advance(self.step)
        if self.frames:
            return self.frames.pop(0)
        if self.fallback == "stall":
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                self.recv_cancelled = True
                raise
        return self.fallback or "42[\"updateStream\",{}]"

    async def send(self, frame):
        self.sent.append(frame)


class HeartbeatClockTests(unittest.IsolatedAsyncioTestCase):
    async def test_silent_receive_times_out_and_cancels_recv(self):
        session = make_session()
        session.receive_timeout = 0.03
        ws = FakeWebSocket(fallback="stall")
        session.ws = ws

        with self.assertRaises(TimeoutError):
            await session.receive()

        self.assertTrue(ws.recv_cancelled)
        self.assertFalse(any(
            task is not asyncio.current_task() and not task.done()
            for task in asyncio.all_tasks()
        ))

    async def test_data_frames_do_not_renew_engine_heartbeat_deadline(self):
        session = make_session()
        clock = MutableClock()
        ws = FakeWebSocket(
            ['0{"sid":"local","pingInterval":1000,"pingTimeout":1000}'],
            clock=clock,
            step=2,
            max_reads=20,
        )
        session.ws = ws

        with patch.object(server, "time", SimpleNamespace(monotonic=clock.monotonic)):
            with self.assertRaises(TimeoutError):
                await session.receive()

        self.assertGreater(clock.value, 9)
        self.assertEqual(ws.sent, ["40"])
        self.assertLess(ws.reads, 10)

    async def test_engine_ping_renews_deadline_and_is_echoed(self):
        session = make_session()
        clock = MutableClock()
        pings = [f"2ping-{index}" for index in range(6)]
        ws = FakeWebSocket(
            ['0{"sid":"local","pingInterval":1000,"pingTimeout":1000}', *pings],
            clock=clock,
            step=2,
            max_reads=30,
        )
        session.ws = ws

        with patch.object(server, "time", SimpleNamespace(monotonic=clock.monotonic)):
            with self.assertRaises(TimeoutError):
                await session.receive()

        self.assertGreater(clock.value, 9)
        self.assertEqual(ws.sent, ["40", *("3" + ping[1:] for ping in pings)])
        self.assertEqual(session.stats["engine_pings"], len(pings))
        self.assertEqual(session.stats["engine_pongs"], len(pings))

    async def test_malformed_hello_heartbeat_is_rejected(self):
        session = make_session()
        session.ws = FakeWebSocket(['0{"sid":"local","pingInterval":0,"pingTimeout":0}'])

        with self.assertRaises(server.ProtocolError):
            await session.receive()


class HeartbeatNetworkTests(unittest.IsolatedAsyncioTestCase):
    async def test_open_disables_websocket_keepalive_and_handles_both_pings(self):
        engine_pong = asyncio.Event()
        control_pong = asyncio.Event()
        connect_options = {}

        async def broker(ws):
            await ws.send('0{"sid":"local","pingInterval":1000,"pingTimeout":1000}')
            self.assertEqual(await asyncio.wait_for(ws.recv(), 1), "40")
            await ws.send("40")
            self.assertEqual(await asyncio.wait_for(ws.recv(), 1), AUTH_PACKET)
            await ws.send('42["successauth",{}]')
            await ws.send("2heartbeat-token")
            self.assertEqual(await asyncio.wait_for(ws.recv(), 1), "3heartbeat-token")
            engine_pong.set()

            pong_waiter = await ws.ping(b"websocket-control-ping")
            await asyncio.wait_for(pong_waiter, 1)
            control_pong.set()
            await ws.wait_closed()

        async with serve(broker, "127.0.0.1", 0, ping_interval=None) as listener:
            url = f"ws://127.0.0.1:{listener.sockets[0].getsockname()[1]}"

            async def local_connect(uri, **kwargs):
                self.assertEqual(uri, url)
                connect_options.update(kwargs)
                kwargs.pop("ssl")
                return await websocket_connect(uri, **kwargs)

            session = make_session()
            session.url = url
            with patch.object(server, "connect", new=local_connect):
                try:
                    await session.open()
                    await asyncio.wait_for(engine_pong.wait(), 1)
                    await asyncio.wait_for(control_pong.wait(), 1)
                finally:
                    await session.close()

        self.assertIsNone(connect_options["ping_interval"])
        self.assertTrue(session.receiver.done())
        self.assertFalse(any(
            task is not asyncio.current_task()
            and not task.done()
            and task.get_name() == "broker-receiver"
            for task in asyncio.all_tasks()
        ))


if __name__ == "__main__":
    unittest.main(verbosity=2)
