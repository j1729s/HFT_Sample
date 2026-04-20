# bybit_state.py
from __future__ import annotations

from typing import Optional


def _scale_digits(scale: int) -> int:
    if scale < 1:
        raise ValueError("scale must be >= 1")

    n = scale
    digits = 0
    while n > 1 and n % 10 == 0:
        n //= 10
        digits += 1

    if n != 1:
        raise ValueError("scale must be a power of 10")

    return digits


def to_int_scaled(x: str, scale: int, digits: int) -> int:
    if not x:
        return 0

    if "e" in x or "E" in x:
        return int(float(x) * scale)

    neg = x[0] == "-"
    if neg:
        x = x[1:]

    dot = x.find(".")
    if dot == -1:
        out = int(x) * scale
        return -out if neg else out

    whole = x[:dot]
    frac = x[dot + 1:]

    if len(frac) >= digits:
        frac = frac[:digits]
    else:
        frac = frac + ("0" * (digits - len(frac)))

    out = (int(whole) if whole else 0) * scale + (int(frac) if frac else 0)
    return -out if neg else out


class TickerState:
    __slots__ = (
        "price_scale",
        "qty_scale",
        "price_digits",
        "qty_digits",
        "bb_p",
        "bb_q",
        "ba_p",
        "ba_q",
        "has_bid",
        "has_ask",
        "ts",
        "cts",
        "seq",
        "version",
        "n_updates",
        "n_malformed",
    )

    def __init__(self, price_scale: int, qty_scale: int):
        self.price_scale = price_scale
        self.qty_scale = qty_scale
        self.price_digits = _scale_digits(price_scale)
        self.qty_digits = _scale_digits(qty_scale)

        self.bb_p = 0
        self.bb_q = 0
        self.ba_p = 0
        self.ba_q = 0
        self.has_bid = False
        self.has_ask = False

        self.ts = 0
        self.cts = 0
        self.seq = 0
        self.version = 0

        self.n_updates = 0
        self.n_malformed = 0

    def update(self, msg: dict) -> bool:
        self.n_updates += 1

        try:
            self.ts = int(msg.get("ts") or 0)
            self.cts = int(msg.get("cts") or 0)

            data = msg.get("data")
            if not isinstance(data, dict):
                self.n_malformed += 1
                return False

            seq = data.get("seq")
            self.seq = int(seq) if seq is not None else 0

            b = data.get("b")
            if b:
                lvl = b[0]
                self.bb_p = to_int_scaled(lvl[0], self.price_scale, self.price_digits)
                self.bb_q = to_int_scaled(lvl[1], self.qty_scale, self.qty_digits)
                self.has_bid = True

            a = data.get("a")
            if a:
                lvl = a[0]
                self.ba_p = to_int_scaled(lvl[0], self.price_scale, self.price_digits)
                self.ba_q = to_int_scaled(lvl[1], self.qty_scale, self.qty_digits)
                self.has_ask = True

            if not self.has_bid and not self.has_ask:
                self.n_malformed += 1
                return False

            self.version += 1
            return True

        except (TypeError, ValueError, IndexError):
            self.n_malformed += 1
            return False

    def get_ticker(self) -> tuple[int, int] | None:
        if not self.has_bid or not self.has_ask:
            return None
        return self.bb_p, self.ba_p

    def best_bid(self) -> tuple[int, int] | None:
        if not self.has_bid:
            return None
        return self.bb_p, self.bb_q

    def best_ask(self) -> tuple[int, int] | None:
        if not self.has_ask:
            return None
        return self.ba_p, self.ba_q

    def stats(self) -> dict:
        return {
            "ts": self.ts,
            "cts": self.cts,
            "seq": self.seq,
            "version": self.version,
            "n_updates": self.n_updates,
            "n_malformed": self.n_malformed,
            "has_bid": self.has_bid,
            "has_ask": self.has_ask,
        }


class OrderBookTopN:
    __slots__ = (
        "N",
        "price_scale",
        "qty_scale",
        "price_digits",
        "qty_digits",
        "bids",
        "asks",
        "best_bid_p",
        "best_ask_p",
        "ts",
        "cts",
        "seq",
        "has_snapshot",
        "version",
        "n_updates",
        "n_snapshots",
        "n_deltas",
        "n_ignored_before_snapshot",
        "n_malformed",
    )

    def __init__(self, N: int, price_scale: int, qty_scale: int):
        self.N = N
        self.price_scale = price_scale
        self.qty_scale = qty_scale
        self.price_digits = _scale_digits(price_scale)
        self.qty_digits = _scale_digits(qty_scale)

        self.bids: dict[int, int] = {}
        self.asks: dict[int, int] = {}
        self.best_bid_p: Optional[int] = None
        self.best_ask_p: Optional[int] = None

        self.ts = 0
        self.cts = 0
        self.seq = 0
        self.has_snapshot = False
        self.version = 0

        self.n_updates = 0
        self.n_snapshots = 0
        self.n_deltas = 0
        self.n_ignored_before_snapshot = 0
        self.n_malformed = 0

    def _recompute_best_bid(self) -> None:
        self.best_bid_p = max(self.bids) if self.bids else None

    def _recompute_best_ask(self) -> None:
        self.best_ask_p = min(self.asks) if self.asks else None

    def _trim_bids(self) -> None:
        if len(self.bids) <= self.N:
            return

        excess = len(self.bids) - self.N
        for p in sorted(self.bids)[:excess]:
            del self.bids[p]

    def _trim_asks(self) -> None:
        if len(self.asks) <= self.N:
            return

        excess = len(self.asks) - self.N
        for p in sorted(self.asks, reverse=True)[:excess]:
            del self.asks[p]

    def _apply_side(self, side: dict[int, int], updates, is_bid: bool) -> None:
        best_removed = False
        price_scale = self.price_scale
        qty_scale = self.qty_scale
        price_digits = self.price_digits
        qty_digits = self.qty_digits

        for lvl in updates:
            try:
                p = to_int_scaled(lvl[0], price_scale, price_digits)
                q = to_int_scaled(lvl[1], qty_scale, qty_digits)
            except (TypeError, ValueError, IndexError):
                self.n_malformed += 1
                continue

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
                    if is_bid:
                        if self.best_bid_p == p:
                            best_removed = True
                    else:
                        if self.best_ask_p == p:
                            best_removed = True

        if best_removed:
            if is_bid:
                self._recompute_best_bid()
            else:
                self._recompute_best_ask()

    def update(self, msg: dict) -> bool:
        self.n_updates += 1

        try:
            self.ts = int(msg.get("ts") or 0)
            self.cts = int(msg.get("cts") or 0)

            data = msg.get("data")
            if not isinstance(data, dict):
                self.n_malformed += 1
                return False

            seq = data.get("seq")
            self.seq = int(seq) if seq is not None else 0

            typ = msg.get("type")

            if typ == "snapshot":
                self.n_snapshots += 1

                self.bids.clear()
                self.asks.clear()
                self.best_bid_p = None
                self.best_ask_p = None

                price_scale = self.price_scale
                qty_scale = self.qty_scale
                price_digits = self.price_digits
                qty_digits = self.qty_digits

                for lvl in (data.get("b") or ()):
                    try:
                        p = to_int_scaled(lvl[0], price_scale, price_digits)
                        q = to_int_scaled(lvl[1], qty_scale, qty_digits)
                    except (TypeError, ValueError, IndexError):
                        self.n_malformed += 1
                        continue
                    if q > 0:
                        self.bids[p] = q

                for lvl in (data.get("a") or ()):
                    try:
                        p = to_int_scaled(lvl[0], price_scale, price_digits)
                        q = to_int_scaled(lvl[1], qty_scale, qty_digits)
                    except (TypeError, ValueError, IndexError):
                        self.n_malformed += 1
                        continue
                    if q > 0:
                        self.asks[p] = q

                self._trim_bids()
                self._trim_asks()
                self._recompute_best_bid()
                self._recompute_best_ask()
                self.has_snapshot = True
                self.version += 1
                return True

            if typ == "delta":
                self.n_deltas += 1

                if not self.has_snapshot:
                    self.n_ignored_before_snapshot += 1
                    return False

                self._apply_side(self.bids, data.get("b") or (), is_bid=True)
                self._apply_side(self.asks, data.get("a") or (), is_bid=False)

                if len(self.bids) > self.N:
                    self._trim_bids()

                if len(self.asks) > self.N:
                    self._trim_asks()

                self.version += 1
                return True

            self.n_malformed += 1
            return False

        except (TypeError, ValueError):
            self.n_malformed += 1
            return False

    def get_ticker(self) -> tuple[int, int] | None:
        if self.best_bid_p is None or self.best_ask_p is None:
            return None
        return self.best_bid_p, self.best_ask_p

    def best_bid(self) -> tuple[int, int] | None:
        p = self.best_bid_p
        if p is None:
            return None
        return p, self.bids[p]

    def best_ask(self) -> tuple[int, int] | None:
        p = self.best_ask_p
        if p is None:
            return None
        return p, self.asks[p]

    def get_ob(self) -> tuple[dict[int, int], dict[int, int]] | None:
        if self.best_bid_p is None or self.best_ask_p is None:
            return None
        return self.bids, self.asks

    def stats(self) -> dict:
        return {
            "ts": self.ts,
            "cts": self.cts,
            "seq": self.seq,
            "version": self.version,
            "has_snapshot": self.has_snapshot,
            "n_updates": self.n_updates,
            "n_snapshots": self.n_snapshots,
            "n_deltas": self.n_deltas,
            "n_ignored_before_snapshot": self.n_ignored_before_snapshot,
            "n_malformed": self.n_malformed,
            "best_bid_p": self.best_bid_p,
            "best_ask_p": self.best_ask_p,
            "bid_levels": len(self.bids),
            "ask_levels": len(self.asks),
        }
