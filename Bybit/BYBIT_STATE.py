# bybit_state.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, List


def to_int_scaled(x: str, scale: int) -> int:
    """
    Convert numeric string to scaled int.
    Example: "123.45" with scale=100 -> 12345
    """
    return int(float(x) * scale)


@dataclass(slots=True)
class BestLevel:
    p: int  # scaled price
    q: int  # scaled quantity


class TickerState:
    """
    Processor for Bybit orderbook.1.{symbol} (snapshot-only).
    Keeps only current best bid/ask as scaled ints.
    """

    __slots__ = ("price_scale", "qty_scale", "bb", "ba", "ts", "cts")

    def __init__(self, price_scale: int, qty_scale: int):
        self.price_scale = price_scale
        self.qty_scale = qty_scale
        self.bb: Optional[BestLevel] = None
        self.ba: Optional[BestLevel] = None
        self.ts: int = 0
        self.cts: int = 0

    def update(self, msg: dict) -> None:
        self.ts = int(msg.get("ts") or 0)
        self.cts = int(msg.get("cts") or 0)

        data = msg.get("data") or {}
        b = data.get("b") or []
        a = data.get("a") or []

        if b:
            p_s, q_s = b[0]
            self.bb = BestLevel(
                p=to_int_scaled(p_s, self.price_scale),
                q=to_int_scaled(q_s, self.qty_scale),
            )

        if a:
            p_s, q_s = a[0]
            self.ba = BestLevel(
                p=to_int_scaled(p_s, self.price_scale),
                q=to_int_scaled(q_s, self.qty_scale),
            )

    def get_ticker(self) -> tuple[int, int] | None:
        if not self.bb and not self.ba:
            return None
        return self.bb.p, self.ba.p

    def best_bid(self) -> tuple[int, int] | None:
        if not self.bb and not self.ba:
            return None
        return self.bb.p, self.bb.q

    def best_ask(self) -> tuple[int, int] | None:
        if not self.bb and not self.ba:
            return None
        return self.ba.p, self.ba.q


class OrderBookTopN:
    """
    Processor for Bybit orderbook.50.{symbol} (snapshot + delta).
    Maintains TOP N levels only, per side, using dicts keyed by scaled price ints.

    bids: price_i -> qty_i, keep highest N prices
    asks: price_i -> qty_i, keep lowest  N prices
    """

    __slots__ = (
        "N",
        "price_scale",
        "qty_scale",
        "bids",
        "asks",
        "best_bid_p",
        "best_ask_p",
        "ts",
        "cts",
        "has_snapshot",
    )

    def __init__(self, N: int, price_scale: int, qty_scale: int):
        self.N = N
        self.price_scale = price_scale
        self.qty_scale = qty_scale

        self.bids: Dict[int, int] = {}
        self.asks: Dict[int, int] = {}
        self.best_bid_p: Optional[int] = None
        self.best_ask_p: Optional[int] = None

        self.ts: int = 0
        self.cts: int = 0
        self.has_snapshot: bool = False

    # ---------- internal helpers ----------
    def _recompute_best_bid(self) -> None:
        self.best_bid_p = max(self.bids) if self.bids else None

    def _recompute_best_ask(self) -> None:
        self.best_ask_p = min(self.asks) if self.asks else None

    def _trim(self) -> None:
        """
        Enforce top-N cap.
        N is small (50), so sorting here is OK.
        """
        if len(self.bids) > self.N:
            excess = len(self.bids) - self.N
            for p in sorted(self.bids)[:excess]:  # remove lowest bids
                del self.bids[p]
            self._recompute_best_bid()

        if len(self.asks) > self.N:
            excess = len(self.asks) - self.N
            for p in sorted(self.asks, reverse=True)[:excess]:  # remove highest asks
                del self.asks[p]
            self._recompute_best_ask()

    def _apply_side(self, side: Dict[int, int], updates: List[List[str]], is_bid: bool) -> None:
        best_removed = False

        for p_s, q_s in updates:
            p = to_int_scaled(p_s, self.price_scale)
            q = to_int_scaled(q_s, self.qty_scale)

            if q > 0:
                side[p] = q
                if is_bid:
                    if self.best_bid_p is None or p > self.best_bid_p:
                        self.best_bid_p = p
                else:
                    if self.best_ask_p is None or p < self.best_ask_p:
                        self.best_ask_p = p
            else:
                if p in side:
                    del side[p]
                    if is_bid and self.best_bid_p == p:
                        best_removed = True
                    if (not is_bid) and self.best_ask_p == p:
                        best_removed = True

        if best_removed:
            if is_bid:
                self._recompute_best_bid()
            else:
                self._recompute_best_ask()

    # ---------- public update ----------
    def update(self, msg: dict) -> None:
        self.ts = int(msg.get("ts") or 0)
        self.cts = int(msg.get("cts") or 0)

        typ = msg.get("type")  # "snapshot" or "delta"
        data = msg.get("data") or {}

        if typ == "snapshot":
            # overwrite
            self.bids.clear()
            self.asks.clear()
            self.best_bid_p = None
            self.best_ask_p = None

            for p_s, q_s in (data.get("b") or []):
                p = to_int_scaled(p_s, self.price_scale)
                q = to_int_scaled(q_s, self.qty_scale)
                if q > 0:
                    self.bids[p] = q

            for p_s, q_s in (data.get("a") or []):
                p = to_int_scaled(p_s, self.price_scale)
                q = to_int_scaled(q_s, self.qty_scale)
                if q > 0:
                    self.asks[p] = q

            self._trim()
            self._recompute_best_bid()
            self._recompute_best_ask()
            self.has_snapshot = True
            return

        # delta
        if not self.has_snapshot:
            return  # ignore deltas until first snapshot

        self._apply_side(self.bids, data.get("b") or [], is_bid=True)
        self._apply_side(self.asks, data.get("a") or [], is_bid=False)
        self._trim()

    # ---------- reads ----------
    def get_ticker(self) -> tuple[int, int] | None:
        if not self.best_bid_p and not self.best_ask_p:
            return None
        return self.best_bid_p, self.best_ask_p

    def get_ob(self) -> tuple[dict[int, int], dict[int, int]] | None:
        if not self.best_bid_p and not self.best_ask_p:
            return None
        return self.bids, self.asks
