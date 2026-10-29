#!/usr/bin/env python3
"""Feed ao vivo da ProfitDLL (Nelogica) para a estratégia OFI+VPIN em modo virtual.

Segurança:
- Usa `DLLInitializeMarketLogin`: login SOMENTE de market data. Esta sessão da DLL
  não tem como enviar ordens; toda execução é feita pelo PaperBroker.
- Credenciais vêm das variáveis PROFITDLL_KEY / PROFITDLL_USER / PROFITDLL_PASSWORD
  ou de prompt oculto; nunca são gravadas nem impressas.

Regras da DLL respeitadas:
- Callbacks rodam na thread da DLL: só copiam os dados para uma fila. Nenhuma função
  da DLL é chamada dentro de callback; todo o processamento roda na thread principal.
- As referências aos callbacks ctypes ficam vivas durante toda a sessão. Se o Python
  coletar um callback, a DLL chama memória liberada e o processo cai sem traceback.
- Python e DLL precisam ter a mesma arquitetura (Python 64-bit -> ProfitDLL64.dll).

Todo evento é gravado em JSONL no formato do replay, então o pregão gravado vira
dado de backtest para `ofi_vpin_strategy.py --replay` e `optimize.py`.
"""
from __future__ import annotations

import argparse
import ctypes
import getpass
import json
import os
import queue
import struct
import sys
import threading
import time as _time
from ctypes import POINTER, Structure, c_double, c_int, c_int32, c_longlong, c_uint, c_wchar, c_wchar_p
from dataclasses import asdict
from datetime import datetime, time
from pathlib import Path
from typing import Callable, Optional

from ofi_vpin_strategy import BRT, Event, Strategy, load_config, summarize

FUNCTYPE = getattr(ctypes, "WINFUNCTYPE", ctypes.CFUNCTYPE)

NL_OK = 0
NL_ERRORS = {
    -2147483647: "NL_INTERNAL_ERROR",
    -2147483646: "NL_NOT_INITIALIZED",
    -2147483645: "NL_INVALID_ARGS",
    -2147483644: "NL_WAITING_SERVER",
    -2147483643: "NL_NO_LOGIN",
    -2147483642: "NL_NO_LICENSE",
}

CONNECTION_STATE_LOGIN = 0
CONNECTION_STATE_ROTEAMENTO = 1
CONNECTION_STATE_MARKET_DATA = 2
CONNECTION_STATE_MARKET_LOGIN = 3
LOGIN_CONNECTED = 0
MARKET_CONNECTED = 4
CONNECTION_ACTIVATE_VALID = 0

# nTradeType do callback de negócios: 2 = agressão compradora, 3 = agressão vendedora.
TRADE_TYPE_AGGRESSOR = {2: 1, 3: -1}
BOOK_SIDE_BID = 0
BOOK_SIDE_ASK = 1


class TAssetID(Structure):
    _fields_ = [("ticker", c_wchar_p), ("bolsa", c_wchar_p), ("feed", c_int)]


StateCallback = FUNCTYPE(None, c_int32, c_int32)
TradeCallback = FUNCTYPE(None, TAssetID, c_wchar_p, c_uint, c_double, c_double, c_int, c_int, c_int, c_int, c_wchar)
DailyCallback = FUNCTYPE(None, TAssetID, c_wchar_p, *([c_double] * 10), *([c_int] * 7))
PriceBookCallback = FUNCTYPE(None, TAssetID, c_int, c_int, c_int, c_int, c_int, c_double, POINTER(c_int), POINTER(c_int))
OfferBookCallback = FUNCTYPE(
    None, TAssetID, c_int, c_int, c_int, c_int, c_int, c_longlong, c_double,
    c_int, c_int, c_int, c_int, c_int, c_wchar_p, POINTER(c_int), POINTER(c_int),
)
HistoryTradeCallback = FUNCTYPE(None, TAssetID, c_wchar_p, c_uint, c_double, c_double, c_int, c_int, c_int, c_int)
ProgressCallback = FUNCTYPE(None, TAssetID, c_int)
TinyBookCallback = FUNCTYPE(None, TAssetID, c_double, c_int, c_int)


def describe_result(code: int) -> str:
    return NL_ERRORS.get(code, str(code))


def parse_profit_date(value: str) -> Optional[str]:
    for fmt in ("%d/%m/%Y %H:%M:%S.%f", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=BRT).isoformat(timespec="milliseconds")
        except (TypeError, ValueError):
            continue
    return None


def is_trade_edit(flag: str) -> bool:
    return bool(flag) and flag not in ("\x00", "0", "F", "N")


class EventBuilder:
    """Combina topo do book (TinyBook) e negócios num stream de eventos do replay."""

    def __init__(self) -> None:
        self.bid: Optional[float] = None
        self.ask: Optional[float] = None
        self.bid_size = 0.0
        self.ask_size = 0.0
        self.last_price: Optional[float] = None
        self.trades = 0
        self.edits_skipped = 0

    def ready(self) -> bool:
        return self.bid is not None and self.ask is not None and 0 < self.bid < self.ask

    def on_book(self, side: int, price: float, qty: int, recv: datetime) -> Optional[dict]:
        if price <= 0:
            return None
        if side == BOOK_SIDE_BID:
            self.bid, self.bid_size = price, float(qty)
        elif side == BOOK_SIDE_ASK:
            self.ask, self.ask_size = price, float(qty)
        else:
            return None
        return self._event(recv, 0.0, 0)

    def on_trade(self, price: float, qty: int, trade_type: int, exchange_date: str, edit_flag: str,
                 recv: datetime) -> Optional[dict]:
        if is_trade_edit(edit_flag):
            self.edits_skipped += 1
            return None
        self.trades += 1
        self.last_price = price
        event = self._event(recv, float(qty), TRADE_TYPE_AGGRESSOR.get(trade_type, 0))
        if event is not None:
            event["trade_type"] = trade_type
            event["exchange_time"] = parse_profit_date(exchange_date)
        return event

    def _event(self, recv: datetime, volume: float, aggressor: int) -> Optional[dict]:
        if not self.ready():
            return None
        mid = (self.bid + self.ask) / 2.0
        return {
            "timestamp": recv.isoformat(timespec="milliseconds"),
            "session": recv.date().isoformat(),
            "bid": self.bid,
            "ask": self.ask,
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "last_price": self.last_price if self.last_price is not None else mid,
            "last_volume": volume,
            "aggressor_side": aggressor,
        }


def load_dll(path: Path):
    if os.name != "nt":
        raise SystemExit("A ProfitDLL só roda no Windows. Use --simulate para testar o pipeline em outro sistema.")
    path = path.resolve()
    if not path.exists():
        raise SystemExit(f"DLL não encontrada: {path}")
    bits = struct.calcsize("P") * 8
    if (bits == 64) != ("64" in path.name):
        raise SystemExit(f"Python {bits}-bit não carrega {path.name}: use ProfitDLL64.dll com Python 64-bit.")
    # Dependências da DLL (ex.: libs de criptografia) ficam na mesma pasta.
    os.add_dll_directory(str(path.parent))
    return ctypes.WinDLL(str(path))


class ProfitDLLFeed:
    def __init__(self, dll, ticker: str, exchange: str, clock: Callable[[], int] = _time.time_ns):
        self.dll = dll
        self.ticker = ticker.upper()
        self.exchange = exchange
        self.clock = clock
        self.queue: queue.SimpleQueue = queue.SimpleQueue()
        self.activated = threading.Event()
        self.activation_failed = threading.Event()
        self.market_connected = threading.Event()
        self.callbacks = self._build_callbacks()
        if isinstance(dll, ctypes.CDLL):
            self._declare_signatures()

    def _build_callbacks(self) -> dict:
        q, ticker, clock = self.queue, self.ticker, self.clock

        @StateCallback
        def on_state(kind, result):
            if kind == CONNECTION_STATE_MARKET_LOGIN:
                if result == CONNECTION_ACTIVATE_VALID:
                    self.activated.set()
                else:
                    self.activation_failed.set()
            elif kind == CONNECTION_STATE_MARKET_DATA:
                if result == MARKET_CONNECTED:
                    self.market_connected.set()
                else:
                    self.market_connected.clear()
            q.put(("state", kind, result, clock()))

        @TradeCallback
        def on_trade(asset, date, number, price, volume, qty, buy_agent, sell_agent, trade_type, edit):
            if asset.ticker == ticker:
                q.put(("trade", price, qty, trade_type, date, edit, clock()))

        @TinyBookCallback
        def on_tiny_book(asset, price, qty, side):
            if asset.ticker == ticker:
                q.put(("book", side, price, qty, clock()))

        return {
            "state": on_state,
            "trade": on_trade,
            "daily": DailyCallback(lambda *args: None),
            "price_book": PriceBookCallback(lambda *args: None),
            "offer_book": OfferBookCallback(lambda *args: None),
            "history": HistoryTradeCallback(lambda *args: None),
            "progress": ProgressCallback(lambda *args: None),
            "tiny_book": on_tiny_book,
        }

    def _declare_signatures(self) -> None:
        dll = self.dll
        dll.DLLInitializeMarketLogin.restype = c_int32
        dll.DLLInitializeMarketLogin.argtypes = [
            c_wchar_p, c_wchar_p, c_wchar_p, StateCallback, TradeCallback, DailyCallback,
            PriceBookCallback, OfferBookCallback, HistoryTradeCallback, ProgressCallback, TinyBookCallback,
        ]
        for name in ("SubscribeTicker", "UnsubscribeTicker"):
            getattr(dll, name).restype = c_int32
            getattr(dll, name).argtypes = [c_wchar_p, c_wchar_p]
        dll.DLLFinalize.restype = c_int32
        dll.DLLFinalize.argtypes = []

    def initialize(self, key: str, user: str, password: str) -> None:
        cb = self.callbacks
        result = self.dll.DLLInitializeMarketLogin(
            key, user, password, cb["state"], cb["trade"], cb["daily"], cb["price_book"],
            cb["offer_book"], cb["history"], cb["progress"], cb["tiny_book"],
        )
        if result != NL_OK:
            raise RuntimeError(f"DLLInitializeMarketLogin falhou: {describe_result(result)}")

    def wait_connected(self, timeout_s: float) -> None:
        deadline = _time.monotonic() + timeout_s
        while not (self.activated.is_set() and self.market_connected.is_set()):
            if self.activation_failed.is_set():
                raise RuntimeError("Chave de ativação da ProfitDLL inválida.")
            if _time.monotonic() > deadline:
                raise TimeoutError("Sem conexão de market data dentro do prazo. Confira login, chave e internet.")
            _time.sleep(0.1)

    def subscribe(self) -> None:
        result = self.dll.SubscribeTicker(self.ticker, self.exchange)
        if result != NL_OK:
            raise RuntimeError(f"SubscribeTicker({self.ticker}, {self.exchange}) falhou: {describe_result(result)}")

    def close(self) -> None:
        try:
            self.dll.UnsubscribeTicker(self.ticker, self.exchange)
        finally:
            self.dll.DLLFinalize()

    def source_finished(self) -> bool:
        return bool(getattr(self.dll, "finished", False))


class Recorder:
    def __init__(self, directory: Path, ticker: str):
        self.directory = directory
        self.ticker = ticker
        self.session: Optional[str] = None
        self.handle = None
        directory.mkdir(parents=True, exist_ok=True)

    def write(self, raw: dict) -> None:
        if raw["session"] != self.session:
            self.close()
            self.session = raw["session"]
            path = self.directory / f"{self.ticker}_{self.session}.jsonl"
            self.handle = path.open("a", encoding="utf-8", buffering=1)
        self.handle.write(json.dumps(raw, ensure_ascii=False) + "\n")

    def close(self) -> None:
        if self.handle is not None:
            self.handle.close()
            self.handle = None


def ns_to_brt(ns: int) -> datetime:
    return datetime.fromtimestamp(ns / 1e9, BRT)


def run_live(feed: ProfitDLLFeed, strategy: Optional[Strategy], recorder: Recorder, until: time,
             status_every_s: float = 30.0, warn_lag_ms: Optional[float] = 500.0) -> EventBuilder:
    builder = EventBuilder()
    last_status = _time.monotonic()
    events = 0
    while True:
        try:
            item = feed.queue.get(timeout=1.0)
        except queue.Empty:
            item = None

        if item is None:
            now = ns_to_brt(feed.clock())
            if strategy is not None:
                strategy.on_clock(now)
            if now.time() >= until or feed.source_finished():
                break
            continue

        kind = item[0]
        now = ns_to_brt(item[-1])
        if kind == "state":
            print(f"[{now:%H:%M:%S}] estado DLL: tipo={item[1]} resultado={item[2]}", flush=True)
            continue
        if kind == "book":
            raw = builder.on_book(item[1], item[2], item[3], now)
        else:
            raw = builder.on_trade(item[1], item[2], item[3], item[4], item[5], now)
        if raw is None:
            continue
        recorder.write(raw)
        events += 1
        if strategy is not None:
            before = len(strategy.broker.trades)
            had_position = strategy.broker.position is not None
            strategy.on_event(Event.from_dict(raw))
            report_paper_activity(strategy, before, had_position)
        if now.time() >= until:
            break

        if _time.monotonic() - last_status >= status_every_s:
            last_status = _time.monotonic()
            lag_ms = (_time.time_ns() - item[-1]) / 1e6
            print_status(now, builder, strategy, events, lag_ms, feed.queue.qsize())
            if warn_lag_ms is not None and lag_ms > warn_lag_ms:
                print(f"  AVISO: processamento {lag_ms:.0f} ms atrás do feed.", flush=True)
    return builder


def report_paper_activity(strategy: Strategy, trades_before: int, had_position: bool) -> None:
    broker = strategy.broker
    for trade in broker.trades[trades_before:]:
        print(f"  [VIRTUAL] saída {trade.exit_reason}: {trade.exit_price:.0f} pnl R$ {trade.pnl_brl:.2f} "
              f"(acumulado R$ {broker.realized_pnl:.2f})", flush=True)
    p = broker.position
    if p is not None and (not had_position or len(broker.trades) > trades_before):
        side = "COMPRA" if p.side == 1 else "VENDA"
        print(f"  [VIRTUAL] {side} {p.qty} @ {p.entry_price:.0f} stop {p.stop_price:.0f} "
              f"alvo {p.target_price:.0f} | {p.reason}", flush=True)


def print_status(now: datetime, builder: EventBuilder, strategy: Optional[Strategy], events: int,
                 lag_ms: float, backlog: int) -> None:
    line = f"[{now:%H:%M:%S}] bid {builder.bid} ask {builder.ask} | eventos {events} | negócios {builder.trades}"
    if builder.edits_skipped:
        line += f" (editados ignorados {builder.edits_skipped})"
    if strategy is not None:
        vpin = strategy.vpin.last_vpin
        line += f" | VPIN {'aquecendo' if vpin is None else f'{vpin:.3f}'}"
        line += f" | pos {'zerado' if strategy.broker.position is None else strategy.broker.position.side}"
        line += f" | pnl R$ {strategy.broker.realized_pnl:.2f}"
    line += f" | atraso {lag_ms:.0f} ms | fila {backlog}"
    print(line, flush=True)


def read_credentials() -> tuple[str, str, str]:
    key = os.environ.get("PROFITDLL_KEY") or getpass.getpass("Chave de ativação ProfitDLL: ")
    user = os.environ.get("PROFITDLL_USER") or input("Usuário Nelogica: ")
    password = os.environ.get("PROFITDLL_PASSWORD") or getpass.getpass("Senha: ")
    return key, user, password


def main() -> int:
    parser = argparse.ArgumentParser(description="ProfitDLL -> gravação JSONL + estratégia OFI/VPIN virtual.")
    parser.add_argument("--ticker", required=True, help="ex.: WINZ26")
    parser.add_argument("--exchange", default="F", help="bolsa na ProfitDLL (F = BMF/futuros)")
    parser.add_argument("--dll", type=Path, default=Path(__file__).with_name("ProfitDLL64.dll"))
    parser.add_argument("--config", type=Path, help="JSON com parâmetros da estratégia")
    parser.add_argument("--record-dir", type=Path, default=Path("dados"))
    parser.add_argument("--record-only", action="store_true", help="só grava os dados, sem estratégia")
    parser.add_argument("--until", default="17:05", help="horário (BRT) para encerrar")
    parser.add_argument("--connect-timeout", type=float, default=60.0)
    parser.add_argument("--simulate", action="store_true",
                        help="usa uma DLL simulada com mercado sintético (testa o pipeline em qualquer SO)")
    args = parser.parse_args()

    until = time.fromisoformat(args.until)
    if args.simulate:
        from synthetic import SimulatedProfitDLL

        dll = SimulatedProfitDLL(day=datetime.now(BRT).date(), end=until)
        feed = ProfitDLLFeed(dll, args.ticker, args.exchange, clock=dll.clock_ns)
        credentials = ("simulado", "simulado", "simulado")
    else:
        dll = load_dll(args.dll)
        feed = ProfitDLLFeed(dll, args.ticker, args.exchange)
        credentials = read_credentials()

    strategy = None if args.record_only else Strategy(load_config(args.config))
    recorder = Recorder(args.record_dir, feed.ticker)
    try:
        feed.initialize(*credentials)
        print("Aguardando conexão de market data...", flush=True)
        feed.wait_connected(args.connect_timeout)
        feed.subscribe()
        print(f"Assinado {feed.ticker} ({args.exchange}). Gravando em {args.record_dir}. Ctrl+C encerra.", flush=True)
        run_live(feed, strategy, recorder, until, warn_lag_ms=None if args.simulate else 500.0)
    except KeyboardInterrupt:
        print("Encerrando por Ctrl+C...", flush=True)
    finally:
        recorder.close()
        feed.close()

    if strategy is not None:
        strategy.finish()
        summary = summarize(strategy.broker.trades)
        stamp = datetime.now(BRT).strftime("%Y-%m-%d_%H%M%S")
        out = args.record_dir / f"virtual_{feed.ticker}_{stamp}.json"
        out.write_text(json.dumps({
            "paper_only": True,
            "summary": summary,
            "trades": [asdict(t) for t in strategy.broker.trades],
            "config": strategy.cfg.to_dict(),
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        print(f"Resultado virtual salvo em {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
