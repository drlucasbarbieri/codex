#!/usr/bin/env python3
"""Mercado sintético para testar o pipeline (não serve para calibrar parâmetros).

O mercado é um passeio aleatório simétrico, SEM vantagem estatística embutida:
qualquer estratégia deve perder aproximadamente os custos (corretagem + slippage).
Lucro consistente fora da amostra aqui indica bug ou overfitting.
"""
from __future__ import annotations

import argparse
import random
import threading
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Iterator, Optional

from ofi_vpin_strategy import BRT
from profitdll_feed import (
    BOOK_SIDE_ASK, BOOK_SIDE_BID, CONNECTION_ACTIVATE_VALID, CONNECTION_STATE_LOGIN,
    CONNECTION_STATE_MARKET_DATA, CONNECTION_STATE_MARKET_LOGIN, LOGIN_CONNECTED, MARKET_CONNECTED,
    NL_OK, EventBuilder,
)

TICK = 5.0


def market_stream(day: date, seed: int, start: time = time(9, 0), end: time = time(17, 10),
                  rate_hz: float = 4.0, price0: float = 176900.0) -> Iterator[tuple]:
    """Gera itens no formato da fila da DLL: ("book", lado, preço, qtd, ts) e
    ("trade", preço, qtd, tipo, data_profit, flag_edição, ts)."""
    rng = random.Random(seed)
    t = datetime.combine(day, start, tzinfo=BRT)
    stop = datetime.combine(day, end, tzinfo=BRT)
    bid = round(price0 / TICK) * TICK
    bid_size, ask_size = rng.randint(20, 150), rng.randint(20, 150)
    yield ("book", BOOK_SIDE_BID, bid, bid_size, t)
    yield ("book", BOOK_SIDE_ASK, bid + TICK, ask_size, t)
    while True:
        t += timedelta(seconds=rng.expovariate(rate_hz))
        if t >= stop:
            return
        ask = bid + TICK
        if rng.random() < 0.45:
            side = 1 if rng.random() < 0.5 else -1
            qty = min(1 + int(rng.expovariate(0.25)), 200)
            price = ask if side > 0 else bid
            stamp = t.strftime("%d/%m/%Y %H:%M:%S.") + f"{t.microsecond // 1000:03d}"
            yield ("trade", price, qty, 2 if side > 0 else 3, stamp, "\x00", t)
            if side > 0:
                ask_size -= qty
                if ask_size <= 0:
                    bid += TICK
                    bid_size, ask_size = rng.randint(5, 60), rng.randint(20, 150)
                    yield ("book", BOOK_SIDE_BID, bid, bid_size, t)
                yield ("book", BOOK_SIDE_ASK, bid + TICK, max(ask_size, 1), t)
            else:
                bid_size -= qty
                if bid_size <= 0:
                    bid -= TICK
                    bid_size, ask_size = rng.randint(20, 150), rng.randint(5, 60)
                    yield ("book", BOOK_SIDE_ASK, bid + TICK, ask_size, t)
                yield ("book", BOOK_SIDE_BID, bid, max(bid_size, 1), t)
        elif rng.random() < 0.5:
            bid_size = max(1, bid_size + rng.randint(-15, 15))
            yield ("book", BOOK_SIDE_BID, bid, bid_size, t)
        else:
            ask_size = max(1, ask_size + rng.randint(-15, 15))
            yield ("book", BOOK_SIDE_ASK, ask, ask_size, t)


def to_replay_events(items: Iterator[tuple]) -> Iterator[dict]:
    builder = EventBuilder()
    for item in items:
        if item[0] == "book":
            raw = builder.on_book(item[1], item[2], item[3], item[-1])
        else:
            raw = builder.on_trade(item[1], item[2], item[3], item[4], item[5], item[-1])
        if raw is not None:
            yield raw


def business_days(first: date, count: int) -> list[date]:
    days, d = [], first
    while len(days) < count:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def write_sessions(out_dir: Path, first: date, count: int, seed: int, rate_hz: float) -> list[Path]:
    import json

    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for i, day in enumerate(business_days(first, count)):
        path = out_dir / f"SINTETICO_{day.isoformat()}.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for raw in to_replay_events(market_stream(day, seed + i, rate_hz=rate_hz)):
                handle.write(json.dumps(raw) + "\n")
        paths.append(path)
    return paths


class SimulatedProfitDLL:
    """Imita a ProfitDLL chamando os mesmos callbacks ctypes, numa thread própria,
    com relógio simulado (o pregão sintético roda acelerado)."""

    def __init__(self, day: date, end: time = time(17, 10), seed: int = 7, rate_hz: float = 4.0):
        self.day = day
        self.end = end
        self.seed = seed
        self.rate_hz = rate_hz
        self.callbacks: Optional[tuple] = None
        self.finished = False
        self._stop = threading.Event()
        self._clock_ns = int(datetime.combine(day, time(8, 59), tzinfo=BRT).timestamp() * 1e9)
        self._thread: Optional[threading.Thread] = None

    def clock_ns(self) -> int:
        return self._clock_ns

    def DLLInitializeMarketLogin(self, key, user, password, state, trade, daily, price_book, offer_book,
                                 history, progress, tiny_book) -> int:
        self.callbacks = (state, trade, tiny_book)
        state(CONNECTION_STATE_MARKET_LOGIN, CONNECTION_ACTIVATE_VALID)
        state(CONNECTION_STATE_LOGIN, LOGIN_CONNECTED)
        state(CONNECTION_STATE_MARKET_DATA, MARKET_CONNECTED)
        return NL_OK

    def SubscribeTicker(self, ticker: str, exchange: str) -> int:
        self._thread = threading.Thread(target=self._run, args=(ticker, exchange), daemon=True)
        self._thread.start()
        return NL_OK

    def _run(self, ticker: str, exchange: str) -> None:
        _, trade_cb, book_cb = self.callbacks
        # Usa a struct declarada no próprio tipo do callback: o feed pode ter sido
        # carregado como __main__, e aí `profitdll_feed.TAssetID` seria outra classe.
        asset = type(book_cb)._argtypes_[0](ticker, exchange, 0)
        try:
            for item in market_stream(self.day, self.seed, end=self.end, rate_hz=self.rate_hz):
                if self._stop.is_set():
                    break
                self._clock_ns = int(item[-1].timestamp() * 1e9)
                if item[0] == "book":
                    book_cb(asset, item[2], item[3], item[1])
                else:
                    trade_cb(asset, item[4], 0, item[1], item[1] * item[2] * 0.2, item[2], 0, 0, item[3], item[5])
            self._clock_ns = int(datetime.combine(self.day, self.end, tzinfo=BRT).timestamp() * 1e9)
        finally:
            self.finished = True

    def UnsubscribeTicker(self, ticker: str, exchange: str) -> int:
        self._stop.set()
        return NL_OK

    def DLLFinalize(self) -> int:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        return NL_OK


def main() -> int:
    parser = argparse.ArgumentParser(description="Gera pregões sintéticos em JSONL (formato do replay).")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--sessions", type=int, default=8)
    parser.add_argument("--first-day", type=date.fromisoformat, default=date(2026, 9, 1))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rate-hz", type=float, default=4.0)
    args = parser.parse_args()
    for path in write_sessions(args.out, args.first_day, args.sessions, args.seed, args.rate_hz):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
