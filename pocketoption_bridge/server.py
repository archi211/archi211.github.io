"""Read-only PocketOption M1 bridge. Python 3.11+, protocol v2 for Free_OTC."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import hmac
import json
import logging
from logging.handlers import RotatingFileHandler
import math
import os
from pathlib import Path
import random
import re
import signal
import ssl
import threading
import time
from urllib.parse import urlparse
import uuid

from flask import Flask, jsonify, request
from waitress import create_server
from websockets.exceptions import ConnectionClosed, InvalidStatusCode
from websockets.legacy.client import connect


BASE = Path(__file__).resolve().parent
LOG = logging.getLogger("bridge")
BRIDGE_VERSION = "2.1.0"
MAX_BARS = 3000
SYMBOL = re.compile(r"^[A-Za-z0-9_#.-]{1,20}_[oO][tT][cC]$")


class ProtocolError(Exception):
    pass


class AuthRequired(Exception):
    pass


def safe_failure(action: str, error: BaseException) -> None:
    # Remote messages and exception strings can contain session credentials.
    if isinstance(error, ConnectionClosed):
        LOG.warning("%s (%s; received_code=%s; sent_code=%s; ping_timeout=%s)",
                    action, type(error).__name__,
                    error.rcvd.code if error.rcvd else None,
                    error.sent.code if error.sent else None,
                    bool(error.sent and error.sent.reason == "keepalive ping timeout"))
        return
    LOG.warning("%s (%s)", action, type(error).__name__)


def read_secret(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8-sig").strip()
    except FileNotFoundError:
        return ""


def parse_ssid(text: str) -> tuple[str, bool]:
    try:
        value = json.loads(text[2:]) if text.startswith("42") else None
        if not isinstance(value, list) or len(value) != 2 or value[0] != "auth":
            raise ValueError
        auth = value[1]
        if not isinstance(auth, dict) or not isinstance(auth.get("session"), str):
            raise ValueError
        if not auth["session"] or auth.get("isDemo") not in (0, 1, False, True):
            raise ValueError
        return "42" + json.dumps(value, separators=(",", ":")), bool(auth["isDemo"])
    except (ValueError, TypeError):
        raise AuthRequired("ssid.txt must contain a complete auth packet") from None


def atomic_json(path: Path, data: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, separators=(",", ":"), allow_nan=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


@dataclass(frozen=True)
class Settings:
    host: str = "127.0.0.1"
    port: int = 5000
    broker_url: str = ""
    request_spacing: float = 2.0
    history_interval: float = 60.0
    request_timeout: float = 12.0
    stale_after: float = 180.0
    max_candle_age: float = 180.0
    connect_timeout: float = 30.0
    auth_wait_timeout: float = 300.0
    bas_trigger: bool = False

    @classmethod
    def load(cls, path: Path) -> Settings:
        raw = json.loads(path.read_text(encoding="utf-8-sig")) if path.exists() else {}
        if not isinstance(raw, dict) or set(raw) - set(cls.__dataclass_fields__):
            raise ValueError("Unknown bridge.json setting")
        settings = cls(**raw)
        ranges = {
            "port": (1, 65535), "request_spacing": (1, 60),
            "history_interval": (30, 3600), "request_timeout": (3, 60),
            "stale_after": (60, 3600), "max_candle_age": (60, 3600),
            "connect_timeout": (5, 120), "auth_wait_timeout": (10, 3600),
        }
        for key, (low, high) in ranges.items():
            value = getattr(settings, key)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"Invalid {key}")
            if not math.isfinite(value) or not low <= value <= high:
                raise ValueError(f"Invalid {key}")
        if not isinstance(settings.port, int) or not isinstance(settings.host, str):
            raise ValueError("Invalid listen address")
        if not isinstance(settings.bas_trigger, bool):
            raise ValueError("bas_trigger must be boolean")
        if not isinstance(settings.broker_url, str):
            raise ValueError("Invalid broker_url")
        if settings.broker_url:
            url = urlparse(settings.broker_url)
            if url.scheme != "wss" or not url.hostname or url.username or url.password:
                raise ValueError("broker_url must be a wss URL without credentials")
            if not any(url.hostname.endswith("." + domain) for domain in ("po.market", "pocketoption.com")):
                raise ValueError("broker_url must be a PocketOption host")
        return settings


def load_pairs(path: Path) -> list[dict]:
    raw = json.loads(path.read_text(encoding="utf-8-sig")) if path.exists() else [
        {"symbol": "EURUSD_otc"}, {"symbol": "GBPUSD_otc"}, {"symbol": "USDJPY_otc"},
    ]
    if not isinstance(raw, list) or not 1 <= len(raw) <= 100:
        raise ValueError("pairs.json must contain 1..100 instruments")
    pairs, seen = [], set()
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("Invalid pair")
        symbol = item.get("symbol", "")
        if not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol) or symbol in seen:
            raise ValueError("Invalid or duplicate symbol")
        digits = item.get("digits", 3 if "JPY" in symbol else 5)
        category = item.get("category", "FOREX")
        if type(digits) is not int or not 0 <= digits <= 8 or not isinstance(category, str):
            raise ValueError("Invalid pair metadata")
        seen.add(symbol)
        pairs.append({"symbol": symbol, "category": category.upper()[:32], "digits": digits})
    return pairs


def timestamp_ms(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("Invalid timestamp")
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError("Invalid timestamp")
    return int(number * 1000 if number < 20_000_000_000 else number)


def valid_bar(row: object, now_ms: int) -> bool:
    if not isinstance(row, (list, tuple)) or len(row) != 5:
        return False
    t, *prices = row
    return (
        type(t) is int and 946684800000 <= t <= now_ms and t % 60000 == 0
        and all(type(p) in (int, float) and math.isfinite(p) and p > 0 for p in prices)
        and row[3] <= min(row[1], row[4]) <= max(row[1], row[4]) <= row[2]
    )


def parse_bars(raw: object, now_ms: int, *, symbol: str | None = None) -> list[list]:
    if not isinstance(raw, list):
        return []
    bars = {}
    for item in raw[-10000:]:
        try:
            if isinstance(item, dict):
                if symbol is not None and item.get("asset", symbol) != symbol:
                    continue
                t = item.get("time", item.get("timestamp"))
                o, h, low, close = (item[k] for k in ("open", "high", "low", "close"))
            elif isinstance(item, list) and len(item) == 7 and type(item[0]) is int and isinstance(item[6], str):
                # ProcessedCandle: symbol_id,T,O,C,H,L,asset (not T,O,C,H,L).
                _, t, o, close, h, low, asset = item
                if symbol is not None and asset != symbol:
                    continue
            elif isinstance(item, list) and len(item) in (5, 6):
                t, o, close, h, low = item[:5]
            else:
                continue
            if any(isinstance(value, bool) for value in (o, h, low, close)):
                continue
            row = [timestamp_ms(t), float(o), float(h), float(low), float(close)]
            if valid_bar(row, now_ms):
                bars[row[0]] = row
        except (ValueError, TypeError, KeyError, OverflowError):
            continue
    return [bars[t] for t in sorted(bars)][-MAX_BARS:]


def parse_tick_history(raw: object, now_ms: int, *, symbol: str) -> list[list]:
    """Aggregate an indexed historical packet, excluding its partial boundaries."""
    if not isinstance(raw, list) or not raw:
        return []
    ticks = []
    for item in raw[-10000:]:
        try:
            if not isinstance(item, dict) or item.get("asset") not in (None, "", symbol):
                return []
            if any(key in item for key in ("open", "close", "high", "low")):
                return []
            t = timestamp_ms(item["time"])
            if isinstance(item["price"], bool):
                return []
            price = float(item["price"])
            if not 946684800000 <= t <= now_ms or not math.isfinite(price) or price <= 0:
                return []
            ticks.append((t, price))
        except (ValueError, TypeError, KeyError, OverflowError):
            return []
    ticks.sort(key=lambda tick: tick[0])
    first_minute = ticks[0][0] // 60000 * 60000
    last_minute = ticks[-1][0] // 60000 * 60000
    bars = {}
    for t, price in ticks:
        minute = t // 60000 * 60000
        if not first_minute < minute < last_minute:
            continue
        row = bars.setdefault(minute, [minute, price, price, price, price])
        row[2], row[3], row[4] = max(row[2], price), min(row[3], price), price
    return list(bars.values())[-MAX_BARS:]


@dataclass
class PairState:
    metadata: dict
    bars: dict[int, list] = field(default_factory=dict)
    last_tick_ms: int = 0
    tick_at: float = 0.0
    snapshot_at: float = 0.0
    source_ms: int = 0
    error: str = ""
    history_source: str = "unknown"


class Store:
    def __init__(self, settings: Settings, pairs: list[dict], *, wall=time.time, clock=time.monotonic):
        self.settings, self.wall, self.clock = settings, wall, clock
        self.lock = threading.RLock()
        self.cache_lock = threading.Lock()
        self.pairs = {p["symbol"]: PairState(p.copy()) for p in pairs}
        self.run_id = uuid.uuid4().hex
        self.epoch = 0
        self.connection = "CONNECTING"
        self.connected = False
        self.worker_alive = False

    def set_connection(self, status: str) -> None:
        with self.lock:
            self.connection = status
            self.connected = False

    def begin_session(self) -> None:
        with self.lock:
            self.epoch += 1
            self.connection, self.connected = "WAITING_DATA", True
            for state in self.pairs.values():
                state.tick_at = state.snapshot_at = 0
                state.error = ""

    def _pair_status(self, state: PairState) -> str:
        if not state.bars:
            return "LOADING"
        arrival = max(state.tick_at, state.snapshot_at)
        age = self.wall() - state.source_ms / 1000
        if (self.connected and arrival > 0 and self.clock() - arrival <= self.settings.stale_after
                and -5 <= age <= self.settings.max_candle_age):
            return "LIVE"
        return "STALE"

    def _status(self) -> str:
        if not self.connected:
            return self.connection
        live = sum(self._pair_status(p) == "LIVE" for p in self.pairs.values())
        return "READY" if live == len(self.pairs) else "DEGRADED" if live else "WAITING_DATA"

    def mark_error(self, symbol: str, code: str) -> None:
        with self.lock:
            self.pairs[symbol].error = code

    def merge_snapshot(self, symbol: str, bars: list[list], *, source: str = "ohlc") -> bool:
        if not bars or symbol not in self.pairs:
            return False
        now_ms = int(self.wall() * 1000)
        bars = [b for b in bars if valid_bar(b, now_ms)]
        if not bars:
            return False
        with self.lock:
            state = self.pairs[symbol]
            live_minute = state.last_tick_ms // 60000 * 60000
            for row in bars:
                old = state.bars.get(row[0])
                if old and state.tick_at and row[0] == live_minute == now_ms // 60000 * 60000:
                    # A history response cannot rewind a more recent live close.
                    row = [row[0], row[1], max(row[2], old[2]), min(row[3], old[3]), old[4]]
                state.bars[row[0]] = row.copy()
            state.bars = dict(sorted(state.bars.items())[-MAX_BARS:])
            latest = bars[-1][0]
            if now_ms - latest <= self.settings.max_candle_age * 1000:
                state.snapshot_at = self.clock()
                state.source_ms = max(state.source_ms, latest)
            state.error = ""
            state.history_source = source
            return True

    def ticks(self, raw: object) -> None:
        if not isinstance(raw, list):
            return
        now_ms = int(self.wall() * 1000)
        for tick in raw:
            try:
                if not isinstance(tick, list) or len(tick) < 3 or tick[0] not in self.pairs:
                    continue
                symbol, t, price = tick[:3]
                t, price = timestamp_ms(t), float(price)
                if not math.isfinite(price) or price <= 0 or not 0 <= now_ms - t <= self.settings.max_candle_age * 1000:
                    continue
                with self.lock:
                    state = self.pairs[symbol]
                    if not self.connected or t <= state.last_tick_ms:
                        continue
                    minute = t // 60000 * 60000
                    # Never invent an OHLC bar from a partial tick stream.
                    row = state.bars.get(minute)
                    if row:
                        row[2], row[3], row[4] = max(row[2], price), min(row[3], price), price
                        state.tick_at, state.source_ms = self.clock(), t
                    state.last_tick_ms = t
            except (ValueError, TypeError, OverflowError):
                continue

    def pairs_payload(self) -> dict:
        with self.lock:
            return {"protocol": 2, "version": BRIDGE_VERSION, "run_id": self.run_id, "epoch": self.epoch,
                    "status": self._status(), "data": [
                        {**s.metadata, "status": self._pair_status(s), "error": s.error,
                         "bars": len(s.bars), "history_source": s.history_source}
                        for s in self.pairs.values()]}

    def klines(self, symbol: str, limit: int, since: int) -> dict:
        with self.lock:
            state = self.pairs[symbol]
            rows = [v.copy() for t, v in sorted(state.bars.items()) if t >= since][-limit:]
            return {"protocol": 2, "run_id": self.run_id, "epoch": self.epoch,
                    "status": self._status(), "pair_status": self._pair_status(state),
                    "last_tick_ms": state.last_tick_ms or None, "data": rows,
                    "history_complete": False, "history_source": state.history_source, "error": state.error}

    def save(self, path: Path) -> None:
        with self.cache_lock:
            with self.lock:
                payload = {"version": 1, "bars": {s: [r.copy() for _, r in sorted(p.bars.items())]
                                                 for s, p in self.pairs.items()}}
            atomic_json(path, payload)

    def restore(self, path: Path) -> None:
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload.get("version") != 1 or not isinstance(payload.get("bars"), dict):
                raise ValueError
            with self.lock:
                for symbol, rows in payload["bars"].items():
                    if symbol in self.pairs and isinstance(rows, list):
                        self.pairs[symbol].bars = dict(sorted({r[0]: r for r in rows
                            if valid_bar(r, int(self.wall() * 1000))}.items())[-MAX_BARS:])
                        self.pairs[symbol].history_source = "cache"
        except (OSError, ValueError, TypeError, AttributeError) as error:
            safe_failure("Cache ignored", error)


class EventDecoder:
    """Socket.IO root-namespace events, including JSON binary attachments."""

    def __init__(self):
        self.pending = None
        self.parts = []
        self.expected = 0

    def feed(self, frame: str | bytes) -> list[tuple[str, object]]:
        try:
            if isinstance(frame, bytes):
                value = json.loads(frame.decode("utf-8"))
                if self.pending is None:
                    return [("raw", value)]
                self.parts.append(value)
                if len(self.parts) < self.expected:
                    return []
                event = self.replace(self.pending)
                self.pending, self.parts, self.expected = None, [], 0
                return self.event(event)
            if frame.startswith("45"):
                count, data = frame[2:].split("-", 1)
                if self.pending is not None or not count.isdigit() or not 1 <= int(count) <= 8:
                    raise ProtocolError("Invalid binary event")
                self.pending, self.expected, self.parts = json.loads(data), int(count), []
                return []
            if frame.startswith("42"):
                return self.event(json.loads(frame[2:]))
            return []
        except (ValueError, UnicodeError, TypeError, IndexError, KeyError, RecursionError):
            raise ProtocolError("Unsupported event payload") from None

    def replace(self, obj):
        if isinstance(obj, dict):
            if obj.get("_placeholder") is True:
                index = obj.get("num")
                if type(index) is not int or not 0 <= index < len(self.parts):
                    raise ProtocolError("Invalid binary placeholder")
                return self.parts[index]
            return {key: self.replace(value) for key, value in obj.items()}
        if isinstance(obj, list):
            return [self.replace(value) for value in obj]
        return obj

    @staticmethod
    def event(value) -> list[tuple[str, object]]:
        if not isinstance(value, list) or not value or not isinstance(value[0], str):
            raise ProtocolError("Invalid event")
        return [(value[0], value[1] if len(value) > 1 else {})]


@dataclass
class PendingHistory:
    symbol: str
    index: int
    future: asyncio.Future


def history_shape(event: str, payload: object) -> dict:
    """Only fixed names and container types; never payload values or unknown keys."""
    safe_events = {"raw", "loadHistoryPeriod", "updateHistoryNew", "updateStream"}
    summary = {"event": event if event in safe_events else "other", "type": type(payload).__name__}
    if isinstance(payload, dict):
        fields = ("asset", "period", "index", "candles", "data", "history")
        summary["fields"] = {key: type(payload[key]).__name__ for key in fields if key in payload}
        for key in ("candles", "data", "history"):
            rows = payload.get(key)
            if isinstance(rows, list):
                summary["rows"] = len(rows)
                if rows:
                    row = rows[0]
                    summary["row_type"] = type(row).__name__
                    if isinstance(row, list):
                        summary["row_width"] = len(row)
                        summary["row_types"] = [type(value).__name__ for value in row[:8]]
                    elif isinstance(row, dict):
                        summary["row_fields"] = [k for k in ("asset", "symbol_id", "time", "timestamp",
                                                            "open", "close", "high", "low", "price") if k in row]
                break
    return summary


class BrokerSession:
    def __init__(self, settings: Settings, store: Store, ssid: str):
        self.settings, self.store = settings, store
        self.auth_packet, demo = parse_ssid(ssid)
        host = "demo-api-eu.po.market" if demo else "api-eu.po.market"
        self.url = settings.broker_url or f"wss://{host}/socket.io/?EIO=4&transport=websocket"
        self.ws = None
        self.receiver = None
        self.authed = asyncio.Event()
        self.auth_sent = False
        self.auth_error = False
        self.pending: PendingHistory | None = None
        self.next_index = int(time.time() * 1000)
        self.decoder = EventDecoder()
        self.receive_timeout = 60.0
        self.stats = dict(text_frames=0, binary_frames=0, engine_pings=0, engine_pongs=0,
                          history_packets=0, valid_bars=0, ignored_history=0)
        self.last_history = {}
        self.shape_logs = set()

    def log_diagnostics(self, cause: str) -> None:
        LOG.warning("Protocol diagnostics (%s): counters=%s last_history=%s", cause,
                    json.dumps(self.stats, sort_keys=True), json.dumps(self.last_history, sort_keys=True))

    def ignore_history(self, reason: str) -> None:
        self.stats["ignored_history"] += 1
        self.last_history["reason"] = reason
        shape = json.dumps(self.last_history, sort_keys=True)
        if shape not in self.shape_logs and len(self.shape_logs) < 12:
            self.shape_logs.add(shape)
            LOG.warning("History response ignored: %s", shape)

    async def open(self) -> None:
        self.ws = await connect(
            self.url, ssl=ssl.create_default_context(), origin="https://pocketoption.com",
            open_timeout=self.settings.connect_timeout, ping_interval=20, ping_timeout=30,
            close_timeout=5, max_size=4 * 1024 * 1024, max_queue=32,
            logger=logging.getLogger("bridge.transport"),
        )
        self.receiver = asyncio.create_task(self.receive(), name="broker-receiver")
        auth_waiter = asyncio.create_task(self.authed.wait())
        try:
            done, _ = await asyncio.wait([auth_waiter, self.receiver],
                                        timeout=self.settings.connect_timeout,
                                        return_when=asyncio.FIRST_COMPLETED)
            if self.receiver in done:
                await self.receiver
                raise ConnectionError("Receiver stopped during authentication")
            if not auth_waiter.done():
                raise TimeoutError("Authentication response timeout")
            if self.auth_error:
                raise AuthRequired("Broker rejected authentication")
        finally:
            auth_waiter.cancel()
            await asyncio.gather(auth_waiter, return_exceptions=True)

    async def receive(self) -> None:
        while True:
            frame = await asyncio.wait_for(self.ws.recv(), self.receive_timeout)
            self.stats["binary_frames" if isinstance(frame, bytes) else "text_frames"] += 1
            if isinstance(frame, str):
                if frame.startswith("0"):
                    hello = json.loads(frame[1:])
                    timeout = (float(hello["pingInterval"]) + float(hello["pingTimeout"])) / 1000
                    if not math.isfinite(timeout) or not 1 <= timeout <= 180:
                        raise ProtocolError("Invalid heartbeat settings")
                    self.receive_timeout = timeout + 5
                    await self.ws.send("40")
                    continue
                if frame.startswith("2"):
                    self.stats["engine_pings"] += 1
                    await self.ws.send("3" + frame[1:])
                    self.stats["engine_pongs"] += 1
                    continue
                if frame.startswith("40") and not self.auth_sent:
                    self.auth_sent = True
                    await self.ws.send(self.auth_packet)
                    continue
                if frame.startswith(("41", "44")) or frame == "1":
                    raise ConnectionError("Namespace disconnected or rejected")
            for event, payload in self.decoder.feed(frame):
                self.handle_event(event, payload)

    def handle_event(self, event: str, payload: object) -> None:
        rejected = event.lower() in ("notauthorized", "auth_error", "errorauth")
        if event.lower() in ("auth", "error"):
            rejected = rejected or payload == "NotAuthorized"
            if isinstance(payload, dict):
                rejected = rejected or any(payload.get(k) == "NotAuthorized"
                                           for k in ("error", "message", "code", "status"))
        if rejected:
            self.auth_error = True
            self.authed.set()
            if self.pending and not self.pending.future.done():
                self.pending.future.set_exception(AuthRequired("Broker rejected authentication"))
            return
        if event == "successauth":
            self.authed.set()
            return
        if event not in ("raw", "updateStream", "loadHistoryPeriod", "updateHistoryNew"):
            return
        if isinstance(payload, list):
            self.store.ticks(payload)
            return
        if not isinstance(payload, dict):
            return
        if event in ("raw", "updateStream") and not any(k in payload for k in ("candles", "data", "history")):
            return
        self.stats["history_packets"] += 1
        self.last_history = history_shape(event, payload)
        symbol, period = payload.get("asset"), payload.get("period")
        pending = self.pending
        indexed = "index" in payload
        if indexed:
            index = payload["index"]
            if type(index) is not int or pending is None or index != pending.index:
                self.ignore_history("unmatched_index")
                return
            if symbol in (None, ""):
                symbol = pending.symbol
            elif symbol != pending.symbol:
                self.ignore_history("index_asset_conflict")
                return
        if not isinstance(symbol, str) or symbol not in self.store.pairs:
            self.ignore_history("unidentified_asset")
            return
        if type(period) is not int or period not in (0, 60):
            self.ignore_history("not_m1")
            return
        raw = payload.get("candles", payload.get("data", payload.get("history")))
        now_ms = int(self.store.wall() * 1000)
        source = "ohlc"
        if period == 0:
            if not indexed or event not in ("loadHistoryPeriod", "raw"):
                self.ignore_history("uncorrelated_tick_history")
                return
            if isinstance(raw, list) and raw and all(
                (isinstance(row, dict) and all(k in row for k in ("open", "high", "low", "close")))
                or (isinstance(row, list) and len(row) in (5, 6, 7))
                for row in raw
            ):
                bars = parse_bars(raw, now_ms, symbol=symbol)
            else:
                bars = parse_tick_history(raw, now_ms, symbol=symbol)
                source = "historical_ticks"
        else:
            bars = parse_bars(raw, now_ms, symbol=symbol)
        if not self.store.merge_snapshot(symbol, bars, source=source):
            self.ignore_history("no_valid_ohlc")
            return
        self.stats["valid_bars"] += len(bars)
        if pending and indexed and symbol == pending.symbol and not pending.future.done():
            pending.future.set_result(True)
            LOG.info("History loaded: %s; bars=%d; source=%s", symbol, len(bars), source)

    async def snapshot(self, symbol: str) -> None:
        if self.pending is not None:
            raise RuntimeError("Only one history request may be pending")
        future = asyncio.get_running_loop().create_future()
        self.next_index += 1
        self.pending = PendingHistory(symbol, self.next_index, future)
        try:
            payload = {"asset": symbol, "period": 60, "time": int(self.store.wall()),
                       "offset": 1000, "index": self.pending.index}
            await self.ws.send("42" + json.dumps(["loadHistoryPeriod", payload], separators=(",", ":")))
            await asyncio.wait_for(future, self.settings.request_timeout)
        except TimeoutError:
            self.log_diagnostics("history_timeout")
            raise
        finally:
            self.pending = None
            if not future.done():
                future.cancel()

    async def keepalive(self) -> None:
        while True:
            await asyncio.sleep(20)
            await self.ws.send('42["ps"]')

    async def close(self) -> None:
        if self.receiver:
            self.receiver.cancel()
            await asyncio.gather(self.receiver, return_exceptions=True)
        if self.pending and not self.pending.future.done():
            self.pending.future.cancel()
        if self.ws:
            try:
                await asyncio.wait_for(self.ws.close(), 6)
            except (TimeoutError, OSError):
                if self.ws.transport:
                    self.ws.transport.abort()


class Worker:
    def __init__(self, settings: Settings, store: Store, base: Path,
                 stop: threading.Event, session_factory=BrokerSession):
        self.settings, self.store, self.base, self.stop = settings, store, base, stop
        self.session_factory = session_factory
        self.cache_path = base / "history-cache.json"

    async def pause(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not self.stop.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(min(0.5, max(0, deadline - time.monotonic())))

    async def wait_for_auth(self, previous: str) -> None:
        self.store.set_connection("AUTH_REQUIRED")
        trigger = self.base / "need_auth.trigger"
        if self.settings.bas_trigger and not trigger.exists():
            await asyncio.to_thread(trigger.write_text, "need_auth", encoding="utf-8")
        deadline = time.monotonic() + self.settings.auth_wait_timeout
        LOG.warning("AUTH_REQUIRED: update ssid.txt locally; no credentials are served over HTTP")
        while not self.stop.is_set():
            candidate = await asyncio.to_thread(read_secret, self.base / "ssid.txt")
            if candidate and candidate != previous:
                try:
                    parse_ssid(candidate)
                    return
                except AuthRequired:
                    pass
            if time.monotonic() >= deadline:
                LOG.warning("Authentication wait expired; remaining offline until ssid.txt changes")
                deadline = time.monotonic() + self.settings.auth_wait_timeout
            await self.pause(2)

    async def poll(self, session: BrokerSession) -> None:
        due = {s: 0.0 for s in self.store.pairs}
        failures = {s: 0 for s in due}
        while not self.stop.is_set():
            symbol = min(due, key=due.get)
            await self.pause(max(0, due[symbol] - time.monotonic()))
            if self.stop.is_set():
                return
            started = time.monotonic()
            try:
                await session.snapshot(symbol)
                failures[symbol] = 0
                due[symbol] = time.monotonic() + self.settings.history_interval
            except TimeoutError:
                failures[symbol] += 1
                self.store.mark_error(symbol, "HISTORY_TIMEOUT")
                delay = min(300, self.settings.history_interval * 2 ** min(failures[symbol], 3))
                due[symbol] = time.monotonic() + delay
                LOG.warning("History timeout: %s; retry in %.0fs", symbol, delay)
            await self.pause(max(0, self.settings.request_spacing - (time.monotonic() - started)))

    async def watch(self, session: BrokerSession, ssid: str) -> None:
        started = time.monotonic()
        last_live = started
        startup_grace = max(180, len(self.store.pairs) * (self.settings.request_timeout + self.settings.request_spacing))
        last_check = 0
        while not self.stop.is_set():
            if session.auth_error:
                raise AuthRequired("Broker rejected authentication")
            now = time.monotonic()
            if now - last_check >= 2:
                current = await asyncio.to_thread(read_secret, self.base / "ssid.txt")
                if current != ssid:
                    raise ConnectionError("Credentials file changed")
                last_check = now
            if any(p["status"] == "LIVE" for p in self.store.pairs_payload()["data"]):
                last_live = now
            if now - started > startup_grace and now - last_live > self.settings.stale_after:
                raise TimeoutError("No fresh market data")
            await self.pause(0.5)

    async def save_cache(self) -> None:
        try:
            await asyncio.to_thread(self.store.save, self.cache_path)
        except (OSError, ValueError) as error:
            safe_failure("History cache write failed", error)

    async def persist(self) -> None:
        while not self.stop.is_set():
            await self.pause(30)
            await self.save_cache()

    async def run(self) -> None:
        with self.store.lock:
            self.store.worker_alive = True
        failures = 0
        persistence = asyncio.create_task(self.persist(), name="history-cache")
        try:
            while not self.stop.is_set():
                session, tasks, denied, wait_auth = None, [], False, False
                ssid = ""
                started = time.monotonic()
                try:
                    ssid = await asyncio.to_thread(read_secret, self.base / "ssid.txt")
                    parse_ssid(ssid)
                    self.store.set_connection("CONNECTING")
                    session = self.session_factory(self.settings, self.store, ssid)
                    await asyncio.wait_for(session.open(), self.settings.connect_timeout * 2)
                    self.store.begin_session()
                    LOG.info("Authenticated; session epoch=%s", self.store.epoch)
                    tasks = [asyncio.create_task(self.poll(session)),
                             asyncio.create_task(self.watch(session, ssid)),
                             asyncio.create_task(session.keepalive()), session.receiver]
                    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        await task
                    if not self.stop.is_set():
                        raise ConnectionError("Session task stopped")
                except AuthRequired:
                    wait_auth = True
                except InvalidStatusCode as error:
                    denied = error.status_code in (401, 403, 429)
                    LOG.warning("WebSocket HTTP status=%d; reconnect postponed", error.status_code)
                except Exception as error:
                    safe_failure("Session failed", error)
                    if isinstance(session, BrokerSession):
                        session.log_diagnostics("session_failed")
                finally:
                    self.store.set_connection("DISCONNECTED")
                    for task in tasks:
                        task.cancel()
                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)
                    if session:
                        try:
                            await session.close()
                        except Exception as error:
                            safe_failure("Connection cleanup failed", error)
                    await self.save_cache()
                if self.stop.is_set():
                    break
                if wait_auth:
                    await self.wait_for_auth(ssid)
                    continue
                failures = 0 if time.monotonic() - started >= 300 else min(failures + 1, 6)
                delay = 300 if denied else min(60, 2 ** failures) + random.uniform(0, 1)
                self.store.set_connection("ACCESS_DENIED" if denied else "DISCONNECTED")
                LOG.info("Reconnect on the same network in %.1fs", delay)
                await self.pause(delay)
        finally:
            self.stop.set()
            # Let an in-flight atomic write finish before the final save.
            await persistence
            await self.save_cache()
            self.store.set_connection("STOPPED")
            with self.store.lock:
                self.store.worker_alive = False


def create_app(store: Store, token: str = "") -> Flask:
    app = Flask(__name__)
    app.config.update(MAX_CONTENT_LENGTH=1024, JSON_SORT_KEYS=False)

    @app.before_request
    def authorize():
        if token:
            supplied = request.headers.get("Authorization", "")
            if not hmac.compare_digest(supplied.encode(), ("Bearer " + token).encode()):
                return jsonify(error="UNAUTHORIZED"), 401

    @app.after_request
    def headers(response):
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.get("/v1/pairs")
    def pairs():
        return jsonify(store.pairs_payload())

    @app.get("/v1/klines")
    def klines():
        symbol = request.args.get("symbol", "")
        symbol = symbol.replace("-OTCp", "_otc").replace("-OTC", "_otc")
        if symbol not in store.pairs:
            return jsonify(error="UNKNOWN_SYMBOL"), 404
        if request.args.get("interval", "1m") != "1m":
            return jsonify(error="ONLY_M1_SUPPORTED"), 400
        try:
            limit = int(request.args.get("limit", "2"))
            since = int(request.args.get("since", "0"))
            if not 1 <= limit <= MAX_BARS or not 0 <= since <= 9_000_000_000_000:
                raise ValueError
        except ValueError:
            return jsonify(error="INVALID_RANGE"), 400
        return jsonify(store.klines(symbol, limit, since))

    @app.get("/server_status")
    @app.get("/healthz")
    def health():
        payload = store.pairs_payload()
        payload["worker_alive"] = store.worker_alive
        payload["active"] = sum(p["status"] == "LIVE" for p in payload["data"])
        payload["total"] = len(payload["data"])
        return jsonify(payload), 200 if store.worker_alive else 503

    @app.get("/readyz")
    def ready():
        payload = store.pairs_payload()
        return jsonify(payload), 200 if payload["status"] == "READY" and store.worker_alive else 503

    return app


def main() -> None:
    os.umask(0o077)
    handler = RotatingFileHandler(BASE / "bridge.log", maxBytes=2_000_000, backupCount=5, encoding="utf-8")
    logging.basicConfig(level=logging.INFO, handlers=[handler, logging.StreamHandler()],
                        format="%(asctime)s %(levelname)s %(message)s")
    # Never enable transport DEBUG: WebSocket frames include the auth packet.
    logging.getLogger("bridge.transport").setLevel(logging.CRITICAL)
    try:
        settings = Settings.load(BASE / "bridge.json")
        token = read_secret(BASE / "api_token.txt")
        if settings.host not in ("127.0.0.1", "::1", "localhost") and len(token) < 32:
            raise ValueError("Non-loopback binding requires an API token of at least 32 characters")
        store = Store(settings, load_pairs(BASE / "pairs.json"))
    except Exception as error:
        safe_failure("Invalid configuration; check bridge.json, pairs.json and api_token.txt", error)
        raise SystemExit(2) from None
    store.restore(BASE / "history-cache.json")
    stop = threading.Event()
    worker = Worker(settings, store, BASE, stop)

    def run_worker():
        try:
            asyncio.run(worker.run())
        except Exception as error:
            safe_failure("Worker exited; healthz is unhealthy", error)
            store.set_connection("WORKER_FAILED")
            with store.lock:
                store.worker_alive = False

    server = create_server(create_app(store, token), host=settings.host, port=settings.port,
                           threads=4, channel_timeout=30)
    thread = threading.Thread(target=run_worker, name="pocketoption-worker", daemon=True)
    thread.start()

    def shutdown(signum, frame):
        stop.set()
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    LOG.info("Bridge %s; HTTP listening on %s:%s; configured pairs=%d",
             BRIDGE_VERSION, settings.host, settings.port, len(store.pairs))
    try:
        server.run()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.close()
        thread.join(timeout=settings.connect_timeout * 2 + 20)
        if thread.is_alive():
            LOG.error("Worker did not stop within the shutdown deadline")


if __name__ == "__main__":
    main()
