#!/usr/bin/env python3
import asyncio
import os
import time
import logging
from logging.handlers import RotatingFileHandler
from collections import deque
from dataclasses import dataclass
from typing import Optional

import orjson
import websockets
from websockets.exceptions import ConnectionClosed


def make_logger(symbol: str, stream_name: str, run_tag: str, log_dir: str = "logs") -> logging.Logger:
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


@dataclass(frozen=True, slots=True)
class OBKey:
    ts: int
    seq: int


def newer(a: OBKey, b: OBKey) -> bool:
    return (a.ts > b.ts) or (a.ts == b.ts and a.seq > b.seq)


def extract_key(msg: dict) -> Optional[OBKey]:
    ts = msg.get("ts")
    data = msg.get("data") or {}
    seq = data.get("seq")

    if ts is None or seq is None:
        return None

    try:
        return OBKey(int(ts), int(seq))
    except (TypeError, ValueError):
        return None


class BybitOBIngest:
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
        latency_csv_path: Optional[str] = None,
        latency_flush_every: int = 256,
        latency_sample_every: int = 1,
    ):
        self.symbol = symbol
        self.market = market
        self.topic = topic
        self.stream_name = stream_name
        self.log = logger

        self.url = f"wss://stream.bybit.com/v5/public/{market}"

        self.q = deque(maxlen=q_maxlen)
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

        self.latency_csv_path = latency_csv_path
        self.latency_flush_every = max(1, latency_flush_every)
        self.latency_sample_every = max(1, latency_sample_every)

        self._lat_fh = None
        self._lat_buf = []
        self._lat_seen = 0

        if self.latency_csv_path is not None:
            lat_dir = os.path.dirname(self.latency_csv_path)
            if lat_dir:
                os.makedirs(lat_dir, exist_ok=True)

            new_file = not os.path.exists(self.latency_csv_path) or os.path.getsize(self.latency_csv_path) == 0
            self._lat_fh = open(self.latency_csv_path, "a", buffering=1)

            if new_file:
                self._lat_fh.write(
                    "recv_wall_ms,recv_mono_ns,apply_start_wall_ms,apply_start_mono_ns,apply_end_wall_ms,apply_end_mono_ns,"
                    "ts_ms,cts_ms,conn_id,u,seq,"
                    "exch_cts_to_recv_ms,exch_cts_to_apply_ms,exch_ts_to_recv_ms,exch_ts_to_apply_ms,"
                    "queue_delay_us,state_update_us,local_e2e_us\n"
                )

    @staticmethod
    def _is_control_msg(msg: dict) -> bool:
        if "op" in msg:
            return True
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

    async def _keepalive(self, ws, conn_id: int, interval: float = 20.0):
        while True:
            await asyncio.sleep(interval)
            try:
                await ws.send(self._ping_bytes)
            except Exception as e:
                self.log.warning(f"conn={conn_id} ping_failed err={type(e).__name__}:{e}")
                return

    def _flush_latency_rows(self) -> None:
        if self._lat_fh is None or not self._lat_buf:
            return

        self._lat_fh.writelines(self._lat_buf)
        self._lat_buf.clear()
        self._lat_fh.flush()

    def _record_latency_row(
        self,
        recv_wall_ms: int,
        recv_mono_ns: int,
        apply_start_wall_ms: int,
        apply_start_mono_ns: int,
        apply_end_wall_ms: int,
        apply_end_mono_ns: int,
        conn_id: int,
        msg: dict,
    ) -> None:
        if self._lat_fh is None:
            return

        self._lat_seen += 1
        if self._lat_seen % self.latency_sample_every != 0:
            return

        try:
            ts_ms = int(msg.get("ts") or 0)
            cts_ms = int(msg.get("cts") or 0)
            data = msg.get("data") or {}
            u = int(data.get("u") or 0)
            seq = int(data.get("seq") or 0)
        except (TypeError, ValueError):
            return

        exch_cts_to_recv_ms = recv_wall_ms - cts_ms if cts_ms > 0 else ""
        exch_cts_to_apply_ms = apply_end_wall_ms - cts_ms if cts_ms > 0 else ""
        exch_ts_to_recv_ms = recv_wall_ms - ts_ms if ts_ms > 0 else ""
        exch_ts_to_apply_ms = apply_end_wall_ms - ts_ms if ts_ms > 0 else ""

        queue_delay_us = (apply_start_mono_ns - recv_mono_ns) // 1_000
        state_update_us = (apply_end_mono_ns - apply_start_mono_ns) // 1_000
        local_e2e_us = (apply_end_mono_ns - recv_mono_ns) // 1_000

        self._lat_buf.append(
            f"{recv_wall_ms},{recv_mono_ns},{apply_start_wall_ms},{apply_start_mono_ns},{apply_end_wall_ms},{apply_end_mono_ns},"
            f"{ts_ms},{cts_ms},{conn_id},{u},{seq},"
            f"{exch_cts_to_recv_ms},{exch_cts_to_apply_ms},{exch_ts_to_recv_ms},{exch_ts_to_apply_ms},"
            f"{queue_delay_us},{state_update_us},{local_e2e_us}\n"
        )

        if len(self._lat_buf) >= self.latency_flush_every:
            self._flush_latency_rows()

    def _close_latency_file(self) -> None:
        if self._lat_fh is None:
            return

        self._flush_latency_rows()
        self._lat_fh.close()
        self._lat_fh = None

    async def _conn_loop(self, conn_id: int):
        backoff = 0.25

        while True:
            try:
                self.log.info(f"conn={conn_id} connecting url={self.url} topic={self.topic}")
                async with websockets.connect(
                    self.url,
                    ping_interval=None,
                    compression=None,
                    max_queue=64,
                    close_timeout=2,
                ) as ws:
                    await ws.send(self._sub_msg_bytes)
                    self.log.info(f"conn={conn_id} subscribed")

                    backoff = 0.25

                    ka = asyncio.create_task(self._keepalive(ws, conn_id))
                    try:
                        async for raw in ws:
                            recv_wall_ms = time.time_ns() // 1_000_000
                            recv_mono_ns = time.monotonic_ns()

                            if isinstance(raw, str):
                                raw = raw.encode()

                            try:
                                msg = orjson.loads(raw)
                            except Exception as e:
                                self.log.warning(f"conn={conn_id} json_decode_failed err={type(e).__name__}:{e}")
                                continue

                            if self._is_control_msg(msg):
                                self._log_control(conn_id, msg)
                                continue

                            if msg.get("topic") != self.topic:
                                continue

                            self.q.append((recv_wall_ms, recv_mono_ns, conn_id, msg))

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

    def _accept(self, msg: dict) -> bool:
        k = extract_key(msg)
        if k is None:
            self.log.warning("missing_key dropped")
            return False

        if self.last_key is None or newer(k, self.last_key):
            self.last_key = k
            return True

        return False

    async def consume_and_update_and_print_state(self):
        while True:
            await self.wake.wait()
            self.wake.clear()

            while self.q:
                recv_wall_ms, recv_mono_ns, conn_id, msg = self.q.popleft()

                try:
                    if not self._accept(msg):
                        continue

                    apply_start_wall_ms = time.time_ns() // 1_000_000
                    apply_start_mono_ns = time.monotonic_ns()

                    ok = self.state.update(msg)

                    apply_end_mono_ns = time.monotonic_ns()
                    apply_end_wall_ms = time.time_ns() // 1_000_000

                    if not ok:
                        continue

                    self._record_latency_row(
                        recv_wall_ms=recv_wall_ms,
                        recv_mono_ns=recv_mono_ns,
                        apply_start_wall_ms=apply_start_wall_ms,
                        apply_start_mono_ns=apply_start_mono_ns,
                        apply_end_wall_ms=apply_end_wall_ms,
                        apply_end_mono_ns=apply_end_mono_ns,
                        conn_id=conn_id,
                        msg=msg,
                    )

                    """
                    ticker = self.state.get_ticker()
                    if ticker is not None:
                        bb, ba = ticker
                        print(f"[STATE] recv_wall_ms={recv_wall_ms} bb={bb} ba={ba}")
                    """

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

        try:
            await asyncio.gather(*tasks)
        finally:
            self._close_latency_file()


async def run_ticker(symbol: str, run_tag: str, market: str = "linear"):
    from BYBIT_STATE import TickerState

    stream_name = "orderbook1"
    log = make_logger(symbol, stream_name, run_tag)
    topic = f"orderbook.1.{symbol}"

    ingest = BybitOBIngest(
        symbol=symbol,
        market=market,
        topic=topic,
        stream_name=stream_name,
        logger=log,
        latency_csv_path=f"latency/{run_tag}_{symbol}_{stream_name}.csv",
        latency_flush_every=256,
        latency_sample_every=10,
    )
    ingest.state = TickerState(price_scale=100, qty_scale=1_000)
    await ingest.run()


async def run_orderbook(symbol: str, run_tag: str, market: str = "linear"):
    from BYBIT_STATE import OrderBookTopN

    stream_name = "orderbook50"
    log = make_logger(symbol, stream_name, run_tag)
    topic = f"orderbook.50.{symbol}"

    ingest = BybitOBIngest(
        symbol=symbol,
        market=market,
        topic=topic,
        stream_name=stream_name,
        logger=log,
        latency_csv_path=f"latency/{run_tag}_{symbol}_{stream_name}.csv",
        latency_flush_every=256,
        latency_sample_every=1,
    )
    ingest.state = OrderBookTopN(N=50, price_scale=100, qty_scale=1_000)
    await ingest.run()


if __name__ == "__main__":
    # asyncio.run(run_orderbook("BTCUSDT", run_tag="lat_test_02", market="linear"))
    asyncio.run(run_ticker("BTCUSDT", run_tag="lat_test_02", market="linear"))