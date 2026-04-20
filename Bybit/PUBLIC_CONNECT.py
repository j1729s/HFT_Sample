#!/usr/bin/env python3
import asyncio
import os
import time
import logging
from logging.handlers import RotatingFileHandler
from collections import deque
from dataclasses import dataclass
from typing import Optional, Tuple

import orjson
import websockets
from websockets.exceptions import ConnectionClosed


# ----------------------------
# Logging (unique per run via user-provided run_tag)
# ----------------------------
def make_logger(symbol: str, stream_name: str, run_tag: str, log_dir: str = "logs") -> logging.Logger:
    """
    Creates a unique logger + file per run so multiple bots don't collide.
    Uses user-provided run_tag (NOT pid).

    Example run_tag: "mm_test_01", "prod_sg_1", "botA"
    """
    os.makedirs(log_dir, exist_ok=True)

    ts = time.strftime("%Y%m%d-%H%M%S")
    log_path = os.path.join(log_dir, f"{run_tag}_{ts}_{symbol}_{stream_name}.log")

    logger = logging.getLogger(f"bybit.{run_tag}.{symbol}.{stream_name}.{ts}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = RotatingFileHandler(log_path, maxBytes=25_000_000, backupCount=3)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.info(f"logger_init path={log_path}")
    return logger


# ----------------------------
# Keying (fastest-first gate)
# ----------------------------
@dataclass(frozen=True, slots=True)
class OBKey:
    ts: int
    seq: int


def newer(a: OBKey, b: OBKey) -> bool:
    # lexicographic compare: (ts, seq)
    return (a.ts > b.ts) or (a.ts == b.ts and a.seq > b.seq)


def extract_key(msg: dict) -> Optional[OBKey]:
    ts = msg.get("ts")
    data = msg.get("data") or {}
    seq = data.get("seq")

    if ts is None or seq is None:
        return None

    try:
        return OBKey(int(ts), int(seq))  # If timestamp has arbitrary jumps change to OBKey(int(seq), int(ts))
    except (TypeError, ValueError):
        return None


# ----------------------------
# Ingest core (shared by ticker/orderbook)
# ----------------------------
class BybitOBIngest:
    """
    3 connections -> one shared FIFO -> consumer drains FIFO -> fastest-first by (ts, seq).
    Control messages (subscribed/ack/pong/etc.) are logged and dropped.
    """

    def __init__(
        self,
        *,
        symbol: str,
        market: str,
        topic: str,
        stream_name: str,
        logger: logging.Logger,
        n_conns: int = 3,
        stagger_s: float = 3,
        q_maxlen: int = 20_000,
    ):
        self.symbol = symbol
        self.market = market
        self.topic = topic
        self.stream_name = stream_name
        self.log = logger

        self.url = f"wss://stream.bybit.com/v5/public/{market}"

        # Shared FIFO
        self.q = deque(maxlen=q_maxlen)

        # "Atomic flag" pattern: wake consumer only when queue becomes non-empty
        self.queue_nonempty = False
        self.wake = asyncio.Event()

        self.n_conns = n_conns
        self.stagger_s = stagger_s

        self.last_key: Optional[OBKey] = None
        self.state = None

        self._sub_msg_bytes = orjson.dumps(
            {
                "req_id": f"{stream_name}_{symbol}",
                "op": "subscribe",
                "args": [topic],
            }
        )
        self._ping_bytes = orjson.dumps({"op": "ping"})

    # ----- message classification -----
    @staticmethod
    def _is_control_msg(msg: dict) -> bool:
        """
        Bybit sends control/ack messages that are not market-data updates.
        We drop them from queue but log for visibility.
        """
        if "op" in msg:
            return True  # ping/pong/subscribe etc.
        if "success" in msg and "ret_msg" in msg:
            return True
        if msg.get("type") == "subscribed":
            return True
        return False

    def _log_control(self, conn_id: int, msg: dict) -> None:
        op = msg.get("op")
        typ = msg.get("type")
        success = msg.get("success")
        ret = msg.get("ret_msg")
        self.log.info(f"conn={conn_id} control op={op} type={typ} success={success} ret_msg={ret}")

    # ----- keepalive -----
    async def _keepalive(self, ws, conn_id: int, interval: float = 20.0):
        while True:
            await asyncio.sleep(interval)
            try:
                await ws.send(self._ping_bytes)
            except Exception as e:
                self.log.warning(f"conn={conn_id} ping_failed err={type(e).__name__}:{e}")
                return

    # ----- producer (one ws connection) -----
    async def _conn_loop(self, conn_id: int):
        backoff = 0.25

        while True:
            try:
                self.log.info(f"conn={conn_id} connecting url={self.url} topic={self.topic}")
                async with websockets.connect(
                    self.url,
                    ping_interval=None,   # app ping
                    compression=None,
                    max_queue=64,
                    close_timeout=2,
                ) as ws:
                    await ws.send(self._sub_msg_bytes)
                    self.log.info(f"conn={conn_id} subscribed")

                    # reset immediately after a healthy subscribe
                    backoff = 0.25

                    ka = asyncio.create_task(self._keepalive(ws, conn_id))
                    try:
                        async for raw in ws:
                            t_ms = time.time_ns() // 1_000_000

                            if isinstance(raw, str):
                                raw = raw.encode()

                            try:
                                msg = orjson.loads(raw)
                            except Exception as e:
                                self.log.warning(f"conn={conn_id} json_decode_failed err={type(e).__name__}:{e}")
                                continue

                            # Drop + log control messages
                            if self._is_control_msg(msg):
                                self._log_control(conn_id, msg)
                                continue

                            # Filter: only our topic
                            if msg.get("topic") != self.topic:
                                continue

                            # Append to FIFO
                            self.q.append((t_ms, conn_id, msg))

                            # Wake consumer only on empty->non-empty transition
                            if not self.queue_nonempty:
                                self.queue_nonempty = True
                                self.wake.set()

                    finally:
                        ka.cancel()
                        await asyncio.gather(ka, return_exceptions=True)

            except (ConnectionClosed, ConnectionResetError, TimeoutError, OSError) as e:
                self.log.warning(f"conn={conn_id} ws_error err={type(e).__name__}:{e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 2.0)
                self.log.info("Restarting!!")

            except Exception as e:
                self.log.exception(f"conn={conn_id} ws_error err={type(e).__name__}:{e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 2.0)
                self.log.info("Restarting!!")

    # ----- gate (fastest-first) -----
    def _accept(self, msg: dict) -> bool:
        k = extract_key(msg)
        if k is None:
            self.log.warning("missing_key dropped")
            return False

        if self.last_key is None or newer(k, self.last_key):
            self.last_key = k
            return True
        return False

    @staticmethod
    def _top_of_book(msg: dict) -> Tuple[Optional[list], Optional[list]]:
        data = msg.get("data") or {}
        b = data.get("b") or []
        a = data.get("a") or []
        return (b[0] if b else None, a[0] if a else None)

    # ----- consumer (drains queue FIFO) -----
    async def consume_and_update_and_print_state(self):
        while True:
            await self.wake.wait()
            self.wake.clear()

            while self.q:
                t_ms, conn_id, msg = self.q.popleft()

                try:
                    if not self._accept(msg):
                        continue

                    ok = self.state.update(msg)
                    if not ok:
                        continue

                    # ---------- Comment out in Production ---------- #
                    ticker = self.state.get_ticker()
                    if ticker is not None:
                        bb, ba = ticker
                        print(f"[STATE] t_ms={t_ms} bb={bb} ba={ba}")
                    # ----------------------------------------------- #

                except Exception as e:
                    self.log.exception(f"consumer_error conn={conn_id} err={type(e).__name__}:{e}")
                    continue

            self.queue_nonempty = False

    async def run(self):
        tasks = [asyncio.create_task(self.consume_and_update_and_print_state())]

        for i in range(self.n_conns):

            async def delayed_start(ix=i):
                await asyncio.sleep(ix * self.stagger_s)
                await self._conn_loop(ix)

            tasks.append(asyncio.create_task(delayed_start()))

        await asyncio.gather(*tasks)


# ----------------------------
# Public entry points (no depth choice)
# ----------------------------
async def run_ticker(symbol: str, run_tag: str, market: str = "linear"):
    """
    Uses orderbook.1.{symbol} (Bybit L1: snapshot-only for linear/inverse/spot).
    """
    from .BYBIT_STATE import TickerState

    stream_name = "orderbook1"
    log = make_logger(symbol, stream_name, run_tag)
    topic = f"orderbook.1.{symbol}"
    ingest = BybitOBIngest(
        symbol=symbol,
        market=market,
        topic=topic,
        stream_name=stream_name,
        logger=log,
    )
    ingest.state = TickerState(price_scale=100, qty_scale=1_000)
    await ingest.run()


async def run_orderbook(symbol: str, run_tag: str, market: str = "linear"):
    """
    Uses orderbook.50.{symbol} (Bybit L50: snapshot + delta).
    """
    from .BYBIT_STATE import OrderBookTopN

    stream_name = "orderbook50"
    log = make_logger(symbol, stream_name, run_tag)
    topic = f"orderbook.50.{symbol}"
    ingest = BybitOBIngest(
        symbol=symbol,
        market=market,
        topic=topic,
        stream_name=stream_name,
        logger=log,
    )
    ingest.state = OrderBookTopN(N=50, price_scale=100, qty_scale=1_000)
    await ingest.run()


if __name__ == "__main__":
    # Test one at a time
    # asyncio.run(run_orderbook("BTCUSDT", run_tag="mm_test_01", market="linear"))
    asyncio.run(run_ticker("BTCUSDT", run_tag="mm_test_01", market="linear"))
