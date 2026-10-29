#!/usr/bin/env python3
"""Estratégia de pesquisa/paper trading baseada em OFI e VPIN.

Características:
- Apenas replay JSONL e PaperBroker; não contém roteamento de ordens reais.
- Processamento causal por evento e decisão em microjanelas.
- Stops obrigatórios, take profit, limite diário, limite de contratos e zeragem 17:00.
- Dados ao vivo vêm de `profitdll_feed.py` (ProfitDLL, login só de market data).

Formato mínimo de cada linha JSON:
{
  "timestamp": "2026-08-06T09:00:00.123-03:00",
  "session": "2026-08-06",
  "bid": 176900,
  "ask": 176905,
  "bid_size": 100,
  "ask_size": 80,
  "last_price": 176900,
  "last_volume": 2,
  "aggressor_side": -1,
  "macd_hist": 1.2,
  "t3": 176850
}

`aggressor_side`: +1 compra iniciada, -1 venda iniciada, 0 desconhecido
(com `infer_aggressor`, o desconhecido é classificado pela regra de cotação).
Campos extras (ex.: `exchange_time`) são ignorados.
"""
from __future__ import annotations

import argparse
import bisect
import json
import math
import sys
from collections import Counter, deque
from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Iterable, Iterator, Optional
from zoneinfo import ZoneInfo

BRT = ZoneInfo("America/Sao_Paulo")
TIME_FIELDS = ("force_flatten_at", "no_new_entries_after")


@dataclass(slots=True)
class Config:
    symbol: str = "WIN"
    tick_size: float = 5.0
    point_value_brl: float = 0.20
    decision_interval_ms: int = 1000
    volume_bucket: float = 500.0
    vpin_L: int = 50
    min_ofi_z: float = 1.5
    absorption_delta_z: float = 0.5
    min_vpin: float = 0.45
    max_vpin: float = 0.95
    # "absolute": min/max_vpin comparados ao VPIN bruto.
    # "percentile": min/max_vpin (0..1) comparados ao percentil do VPIN atual
    # entre os últimos `vpin_rank_window` valores (independe do tamanho do bucket).
    vpin_threshold_mode: str = "absolute"
    vpin_rank_window: int = 500
    lateral_ofi_z: float = 0.75
    max_spread_ticks: int = 3
    min_stop_ticks: int = 80
    volatility_stop_multiplier: float = 3.0
    slippage_ticks: int = 2
    rr: float = 2.0
    max_risk_brl_per_trade: float = 300.0
    max_contracts: int = 1
    fee_brl_per_contract: float = 1.50  # ida e volta
    daily_loss_limit_brl: float = 600.0
    max_trades_per_session: int = 4
    max_feed_staleness_ms: int = 1500
    force_flatten_at: time = time(17, 0)
    no_new_entries_after: time = time(16, 45)
    require_trend_filters: bool = False
    infer_aggressor: bool = True
    target_requires_trade_through: bool = True

    @classmethod
    def from_dict(cls, overrides: dict) -> "Config":
        known = {f.name for f in fields(cls)}
        unknown = sorted(set(overrides) - known)
        if unknown:
            raise ValueError(f"Parâmetros de configuração desconhecidos: {unknown}")
        values = {
            name: time.fromisoformat(value) if name in TIME_FIELDS and isinstance(value, str) else value
            for name, value in overrides.items()
        }
        return replace(cls(), **values)

    def to_dict(self) -> dict:
        return {k: v.isoformat(timespec="minutes") if isinstance(v, time) else v for k, v in asdict(self).items()}


@dataclass(slots=True)
class Event:
    timestamp: datetime
    session: str
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    last_price: float
    last_volume: float
    aggressor_side: int
    macd_hist: Optional[float] = None
    t3: Optional[float] = None

    @classmethod
    def from_dict(cls, raw: dict) -> "Event":
        ts = parse_timestamp(raw["timestamp"])
        return cls(
            timestamp=ts,
            session=str(raw.get("session", ts.date().isoformat())),
            bid=float(raw["bid"]),
            ask=float(raw["ask"]),
            bid_size=float(raw["bid_size"]),
            ask_size=float(raw["ask_size"]),
            last_price=float(raw.get("last_price", (float(raw["bid"]) + float(raw["ask"])) / 2)),
            last_volume=float(raw.get("last_volume", 0.0)),
            aggressor_side=int(raw.get("aggressor_side", 0)),
            macd_hist=None if raw.get("macd_hist") is None else float(raw["macd_hist"]),
            t3=None if raw.get("t3") is None else float(raw["t3"]),
        )


def parse_timestamp(value: str) -> datetime:
    ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=BRT)
    return ts.astimezone(BRT)


def sign(value: float) -> int:
    return 1 if value > 0 else -1 if value < 0 else 0


class EWMAZ:
    __slots__ = ("alpha", "mean", "var", "n")

    def __init__(self, span: int = 100):
        self.alpha = 2.0 / (span + 1.0)
        self.mean = 0.0
        self.var = 0.0
        self.n = 0

    def z_before_update(self, x: float) -> float:
        if self.n < 5:
            return 0.0
        std = math.sqrt(max(self.var, 1e-12))
        return (x - self.mean) / std

    def update(self, x: float) -> None:
        if self.n == 0:
            self.mean = x
            self.var = 0.0
            self.n = 1
            return
        delta = x - self.mean
        self.mean += self.alpha * delta
        self.var = (1.0 - self.alpha) * (self.var + self.alpha * delta * delta)
        self.n += 1


class OnlineOFI:
    __slots__ = ("prev_bid", "prev_ask", "prev_bid_size", "prev_ask_size", "prev_session")

    def __init__(self):
        self.prev_bid = self.prev_ask = None
        self.prev_bid_size = self.prev_ask_size = None
        self.prev_session = None

    def update(self, event: Event) -> float:
        if self.prev_session != event.session or self.prev_bid is None:
            self.prev_bid = event.bid
            self.prev_ask = event.ask
            self.prev_bid_size = event.bid_size
            self.prev_ask_size = event.ask_size
            self.prev_session = event.session
            return 0.0

        if event.bid > self.prev_bid:
            e_bid = event.bid_size
        elif event.bid == self.prev_bid:
            e_bid = event.bid_size - self.prev_bid_size
        else:
            e_bid = -self.prev_bid_size

        if event.ask < self.prev_ask:
            e_ask = event.ask_size
        elif event.ask == self.prev_ask:
            e_ask = event.ask_size - self.prev_ask_size
        else:
            e_ask = -self.prev_ask_size

        self.prev_bid = event.bid
        self.prev_ask = event.ask
        self.prev_bid_size = event.bid_size
        self.prev_ask_size = event.ask_size
        return e_bid - e_ask


class OnlineVPIN:
    __slots__ = (
        "volume_bucket", "L", "bucket_volume", "bucket_buy", "bucket_sell",
        "history", "history_sum", "history_count", "history_pos", "last_vpin",
        "session", "rank_window", "recent", "recent_sorted", "last_rank",
    )

    def __init__(self, volume_bucket: float, L: int, rank_window: int = 500):
        if volume_bucket <= 0 or L <= 0 or rank_window <= 0:
            raise ValueError("volume_bucket, L e rank_window devem ser positivos")
        self.volume_bucket = volume_bucket
        self.L = L
        self.rank_window = rank_window
        self._reset(None)

    def _reset(self, session: Optional[str]) -> None:
        self.bucket_volume = 0.0
        self.bucket_buy = 0.0
        self.bucket_sell = 0.0
        self.history = [0.0] * self.L
        self.history_sum = 0.0
        self.history_count = 0
        self.history_pos = 0
        self.last_vpin: Optional[float] = None
        self.session = session
        self.recent: deque = deque()
        self.recent_sorted: list[float] = []
        self.last_rank: Optional[float] = None

    def _record(self, vpin: float) -> None:
        # Percentil do valor novo contra os anteriores (causal: não inclui ele mesmo).
        if len(self.recent_sorted) >= 20:
            self.last_rank = bisect.bisect_left(self.recent_sorted, vpin) / len(self.recent_sorted)
        self.recent.append(vpin)
        bisect.insort(self.recent_sorted, vpin)
        if len(self.recent) > self.rank_window:
            old = self.recent.popleft()
            del self.recent_sorted[bisect.bisect_left(self.recent_sorted, old)]

    def update(self, session: str, volume: float, side: int) -> Optional[float]:
        if self.session != session:
            self._reset(session)
        remaining = max(0.0, volume)
        if remaining == 0.0 or side == 0:
            return self.last_vpin

        while remaining > 1e-12:
            capacity = self.volume_bucket - self.bucket_volume
            take = min(remaining, capacity)
            if side > 0:
                self.bucket_buy += take
            else:
                self.bucket_sell += take
            self.bucket_volume += take
            remaining -= take

            if self.bucket_volume >= self.volume_bucket - 1e-12:
                imbalance = abs(self.bucket_buy - self.bucket_sell)
                if self.history_count == self.L:
                    self.history_sum -= self.history[self.history_pos]
                else:
                    self.history_count += 1
                self.history[self.history_pos] = imbalance
                self.history_sum += imbalance
                self.history_pos = (self.history_pos + 1) % self.L
                if self.history_count == self.L:
                    self.last_vpin = self.history_sum / (self.L * self.volume_bucket)
                    self._record(self.last_vpin)
                self.bucket_volume = 0.0
                self.bucket_buy = 0.0
                self.bucket_sell = 0.0
        return self.last_vpin


@dataclass(slots=True)
class Features:
    timestamp: datetime
    mid: float
    spread_ticks: float
    ofi: float
    delta: float
    volume: float
    ofi_z: float
    delta_z: float
    vpin: Optional[float]
    vpin_rank: Optional[float]
    vol_ticks: float
    macd_hist: Optional[float]
    t3: Optional[float]


@dataclass(slots=True)
class Position:
    side: int
    qty: int
    entry_price: float
    stop_price: float
    target_price: float
    entry_time: datetime
    session: str
    reason: str


@dataclass(slots=True)
class Trade:
    side: int
    qty: int
    entry_price: float
    exit_price: float
    entry_time: str
    exit_time: str
    session: str
    pnl_brl: float
    exit_reason: str
    signal_reason: str


class PaperBroker:
    """Execução simulada.

    Saídas a mercado (stop, zeragem, fim de dados) pagam slippage e, se o preço
    saltar o stop, saem pelo preço negociado. O alvo é ordem limitada: sem
    slippage e, com `target_requires_trade_through`, só executa quando o mercado
    negocia um tick além do alvo (fila do book).
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.position: Optional[Position] = None
        self.trades: list[Trade] = []
        self.realized_pnl = 0.0

    def _pnl(self, side: int, qty: int, entry: float, exit_: float) -> float:
        gross = (exit_ - entry) * side * qty * self.cfg.point_value_brl
        return gross - self.cfg.fee_brl_per_contract * qty

    def mark_and_manage(self, event: Event) -> None:
        p = self.position
        if p is None:
            return
        cfg = self.cfg
        if event.timestamp.astimezone(BRT).time() >= cfg.force_flatten_at:
            self.close(event.last_price, event.timestamp, "force_flatten", market=True)
            return
        px = event.last_price
        through = cfg.tick_size if cfg.target_requires_trade_through else 0.0
        if p.side == 1:
            if px <= p.stop_price:
                self.close(min(px, p.stop_price), event.timestamp, "stop", market=True)
            elif px >= p.target_price + through:
                self.close(p.target_price, event.timestamp, "target", market=False)
        else:
            if px >= p.stop_price:
                self.close(max(px, p.stop_price), event.timestamp, "stop", market=True)
            elif px <= p.target_price - through:
                self.close(p.target_price, event.timestamp, "target", market=False)

    def open(self, side: int, price: float, qty: int, stop_ticks: int, reason: str, ts: datetime, session: str) -> None:
        if self.position is not None:
            return
        adverse = self.cfg.slippage_ticks * self.cfg.tick_size
        fill = price + adverse if side == 1 else price - adverse
        stop_distance = stop_ticks * self.cfg.tick_size
        target_distance = stop_distance * self.cfg.rr
        if side == 1:
            stop, target = fill - stop_distance, fill + target_distance
        else:
            stop, target = fill + stop_distance, fill - target_distance
        self.position = Position(side, qty, fill, stop, target, ts, session, reason)

    def close(self, price: float, ts: datetime, reason: str, market: bool = True) -> None:
        if self.position is None:
            return
        p = self.position
        adverse = self.cfg.slippage_ticks * self.cfg.tick_size if market else 0.0
        fill = price - adverse if p.side == 1 else price + adverse
        pnl = self._pnl(p.side, p.qty, p.entry_price, fill)
        self.realized_pnl += pnl
        self.trades.append(Trade(
            p.side, p.qty, p.entry_price, fill, p.entry_time.isoformat(), ts.isoformat(),
            p.session, pnl, reason, p.reason,
        ))
        self.position = None


class Strategy:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.ofi = OnlineOFI()
        if cfg.vpin_threshold_mode not in ("absolute", "percentile"):
            raise ValueError("vpin_threshold_mode deve ser 'absolute' ou 'percentile'")
        self.vpin = OnlineVPIN(cfg.volume_bucket, cfg.vpin_L, cfg.vpin_rank_window)
        self.ofi_stats = EWMAZ(span=100)
        self.delta_stats = EWMAZ(span=100)
        self.move_stats = EWMAZ(span=100)
        self.broker = PaperBroker(cfg)
        self.session: Optional[str] = None
        self.session_start_pnl = 0.0
        self.trades_today = 0
        self.funnel: Counter = Counter()
        self.last_event: Optional[Event] = None
        self.window_start: Optional[datetime] = None
        self.window_ofi = 0.0
        self.window_delta = 0.0
        self.window_volume = 0.0
        self.window_spread_ticks = 0.0
        self.window_mid_open = 0.0
        self.window_mid = 0.0
        self.window_macd: Optional[float] = None
        self.window_t3: Optional[float] = None

    def _start_session(self, event: Event) -> None:
        # Nunca carrega posição de um pregão para o outro.
        if self.broker.position is not None and self.last_event is not None:
            last = self.last_event
            self.broker.close(last.last_price, last.timestamp, "session_end", market=True)
        self.session = event.session
        self.session_start_pnl = self.broker.realized_pnl
        self.trades_today = 0
        self.window_start = None

    def _reset_window(self, event: Event) -> None:
        mid = (event.bid + event.ask) / 2.0
        self.window_start = event.timestamp
        self.window_ofi = 0.0
        self.window_delta = 0.0
        self.window_volume = 0.0
        self.window_spread_ticks = 0.0
        self.window_mid_open = mid
        self.window_mid = mid
        self.window_macd = event.macd_hist
        self.window_t3 = event.t3

    def _aggressor(self, event: Event) -> int:
        side = sign(event.aggressor_side)
        if side != 0 or not self.cfg.infer_aggressor or event.last_volume <= 0:
            return side
        if event.last_price >= event.ask:
            return 1
        if event.last_price <= event.bid:
            return -1
        return 0

    def _session_allowed(self, ts: datetime) -> bool:
        local = ts.astimezone(BRT).time()
        return time(9, 0) <= local < self.cfg.force_flatten_at

    def _stop_ticks(self, f: Features) -> int:
        cfg = self.cfg
        return math.ceil(max(cfg.min_stop_ticks, cfg.volatility_stop_multiplier * f.vol_ticks) + cfg.slippage_ticks)

    def _size_position(self, stop_ticks: int) -> int:
        cfg = self.cfg
        risk = stop_ticks * cfg.tick_size * cfg.point_value_brl
        risk += cfg.fee_brl_per_contract + 2.0 * cfg.slippage_ticks * cfg.tick_size * cfg.point_value_brl
        if risk <= 0:
            return 0
        qty = math.floor(cfg.max_risk_brl_per_trade / risk)
        return max(0, min(cfg.max_contracts, qty))

    def _finalize_window(self, event: Event) -> None:
        if self.window_start is None or self.window_volume <= 0:
            return
        ofi_z = self.ofi_stats.z_before_update(self.window_ofi)
        delta_z = self.delta_stats.z_before_update(self.window_delta)
        self.ofi_stats.update(self.window_ofi)
        self.delta_stats.update(self.window_delta)
        self.move_stats.update(abs(self.window_mid - self.window_mid_open) / self.cfg.tick_size)
        f = Features(
            timestamp=self.window_start,
            mid=self.window_mid,
            spread_ticks=self.window_spread_ticks,
            ofi=self.window_ofi,
            delta=self.window_delta,
            volume=self.window_volume,
            ofi_z=ofi_z,
            delta_z=delta_z,
            vpin=self.vpin.last_vpin,
            vpin_rank=self.vpin.last_rank,
            vol_ticks=max(1.0, self.move_stats.mean),
            macd_hist=self.window_macd,
            t3=self.window_t3,
        )
        self._evaluate(f, event)

    def _evaluate(self, f: Features, event: Event) -> None:
        reject = self._rejection(f, event)
        self.funnel[reject or "entrada"] += 1
        if reject is not None:
            return
        buy, _ = self._absorption(f)
        side = 1 if buy else -1
        stop_ticks = self._stop_ticks(f)
        qty = self._size_position(stop_ticks)
        vpin_pct = "-" if f.vpin_rank is None else f"{f.vpin_rank:.2f}"
        reason = (f"OFI_z={f.ofi_z:.2f};delta_z={f.delta_z:.2f};VPIN={f.vpin:.3f};VPIN_pct={vpin_pct};"
                  f"spread={f.spread_ticks:.1f}")
        price = event.ask if side == 1 else event.bid
        self.broker.open(side, price, qty, stop_ticks, reason, event.timestamp, event.session)
        self.trades_today += 1

    def _absorption(self, f: Features) -> tuple[bool, bool]:
        cfg = self.cfg
        # Absorção compradora: agressão vendedora/delta negativo, mas OFI positivo.
        buy = f.ofi_z >= cfg.min_ofi_z and f.delta_z <= -cfg.absorption_delta_z
        # Absorção vendedora: agressão compradora/delta positivo, mas OFI negativo.
        sell = f.ofi_z <= -cfg.min_ofi_z and f.delta_z >= cfg.absorption_delta_z
        return buy, sell

    def _rejection(self, f: Features, event: Event) -> Optional[str]:
        """Motivo do primeiro filtro que barra a entrada, ou None se ela deve ocorrer."""
        cfg = self.cfg
        if not self._session_allowed(event.timestamp):
            return "fora_do_horario"
        if self.broker.realized_pnl - self.session_start_pnl <= -cfg.daily_loss_limit_brl:
            return "limite_diario"
        if self.broker.position is not None:
            return "posicionado"
        if self.trades_today >= cfg.max_trades_per_session:
            return "max_operacoes"
        if event.timestamp.astimezone(BRT).time() >= cfg.no_new_entries_after:
            return "apos_ultima_entrada"
        if f.spread_ticks > cfg.max_spread_ticks:
            return "spread"
        if abs(f.ofi_z) < cfg.lateral_ofi_z:
            return "ofi_lateral"
        vpin = f.vpin if cfg.vpin_threshold_mode == "absolute" else f.vpin_rank
        if vpin is None:
            return "vpin_aquecendo"
        if not (cfg.min_vpin <= vpin <= cfg.max_vpin):
            return "vpin_fora_da_faixa"

        trend_long = True
        trend_short = True
        if cfg.require_trend_filters:
            trend_long = f.macd_hist is not None and f.t3 is not None and f.macd_hist >= 0 and f.mid >= f.t3
            trend_short = f.macd_hist is not None and f.t3 is not None and f.macd_hist <= 0 and f.mid <= f.t3

        buy, sell = self._absorption(f)
        if not buy and not sell:
            return "sem_absorcao"
        if (buy and not trend_long) or (sell and not trend_short):
            return "tendencia"
        if self._size_position(self._stop_ticks(f)) <= 0:
            return "risco_por_operacao"
        return None

    def on_event(self, event: Event) -> None:
        cfg = self.cfg
        if event.session != self.session:
            self._start_session(event)
        elif self.last_event is not None:
            gap_ms = (event.timestamp - self.last_event.timestamp).total_seconds() * 1000.0
            if gap_ms > cfg.max_feed_staleness_ms:
                # Lacuna no feed: a janela em curso não representa o fluxo recente.
                self.window_start = None
        self.broker.mark_and_manage(event)

        aggressor = self._aggressor(event)
        ofi_value = self.ofi.update(event)
        self.vpin.update(event.session, event.last_volume, aggressor)

        if self.window_start is None:
            self._reset_window(event)
        elif (event.timestamp - self.window_start).total_seconds() * 1000.0 >= cfg.decision_interval_ms:
            self._finalize_window(event)
            self._reset_window(event)

        self.window_ofi += ofi_value
        self.window_delta += max(0.0, event.last_volume) * aggressor
        self.window_volume += max(0.0, event.last_volume)
        self.window_spread_ticks = max(self.window_spread_ticks, (event.ask - event.bid) / cfg.tick_size)
        self.window_mid = (event.bid + event.ask) / 2.0
        self.window_macd = event.macd_hist
        self.window_t3 = event.t3
        self.last_event = event

    def on_clock(self, now: datetime) -> None:
        """Zera pelo relógio quando o feed para no horário de zeragem (modo ao vivo)."""
        if self.broker.position is None or self.last_event is None:
            return
        if now.astimezone(BRT).time() >= self.cfg.force_flatten_at:
            self.broker.close(self.last_event.last_price, now, "force_flatten_clock", market=True)

    def finish(self) -> None:
        """Fim dos dados: encerra posição aberta; nunca abre operação nova."""
        last = self.last_event
        if last is not None and self.broker.position is not None:
            self.broker.close(last.last_price, last.timestamp, "end_of_data", market=True)


def expand_paths(paths: Iterable[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(sorted(path.glob("*.jsonl")))
        else:
            files.append(path)
    if not files:
        raise FileNotFoundError("nenhum arquivo .jsonl encontrado")
    return files


def load_jsonl(path: Path) -> Iterator[Event]:
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                yield Event.from_dict(json.loads(line))
            except Exception as exc:
                raise ValueError(f"{path}: linha inválida {line_no}: {exc}") from exc


def iter_events(paths: Iterable[Path]) -> Iterator[Event]:
    for path in expand_paths(paths):
        yield from load_jsonl(path)


def summarize(trades: list[Trade]) -> dict:
    pnls = [t.pnl_brl for t in trades]
    n = len(pnls)
    gross_win = sum(x for x in pnls if x > 0)
    gross_loss = -sum(x for x in pnls if x <= 0)
    equity = peak = max_dd = 0.0
    for x in pnls:
        equity += x
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    by_session: dict[str, float] = {}
    for t in trades:
        by_session[t.session] = by_session.get(t.session, 0.0) + t.pnl_brl
    holding = [
        (datetime.fromisoformat(t.exit_time) - datetime.fromisoformat(t.entry_time)).total_seconds()
        for t in trades
    ]
    return {
        "trades": n,
        "net_pnl_brl": round(sum(pnls), 2),
        "gross_profit_brl": round(gross_win, 2),
        "gross_loss_brl": round(gross_loss, 2),
        "profit_factor": round(gross_win / gross_loss, 3) if gross_loss > 0 else None,
        "win_rate": round(sum(1 for x in pnls if x > 0) / n, 4) if n else None,
        "expectancy_brl": round(sum(pnls) / n, 2) if n else None,
        "max_drawdown_brl": round(max_dd, 2),
        "sessions_with_trades": len(by_session),
        "positive_sessions": sum(1 for v in by_session.values() if v > 0),
        "worst_session_brl": round(min(by_session.values()), 2) if by_session else None,
        "avg_holding_s": round(sum(holding) / n, 1) if n else None,
        "exit_reasons": dict(Counter(t.exit_reason for t in trades)),
    }


def run_events(events: Iterable[Event], cfg: Config) -> tuple[Strategy, int]:
    strategy = Strategy(cfg)
    count = 0
    for event in events:
        strategy.on_event(event)
        count += 1
    strategy.finish()
    return strategy, count


def run_replay(paths: list[Path], cfg: Config) -> dict:
    strategy, count = run_events(iter_events(paths), cfg)
    return {
        "events": count,
        "symbol": cfg.symbol,
        "paper_only": True,
        "summary": summarize(strategy.broker.trades),
        "filter_funnel": dict(strategy.funnel.most_common()),
        "trades": [asdict(t) for t in strategy.broker.trades],
        "config": cfg.to_dict(),
    }


def load_config(path: Optional[Path]) -> Config:
    if path is None:
        return Config()
    return Config.from_dict(json.loads(path.read_text(encoding="utf-8")))


def self_test() -> dict:
    """Teste de engenharia com eventos determinísticos; não é backtest."""
    cfg = Config(volume_bucket=2, vpin_L=2, decision_interval_ms=1000, max_risk_brl_per_trade=300)
    base = datetime(2026, 8, 6, 9, 0, tzinfo=BRT)
    events = []
    for i in range(20):
        ts = base + timedelta(milliseconds=i * 100)
        mid = 176900 + (i // 5) * 5
        events.append(Event(ts, "2026-08-06", mid, mid + 5, 100 + i, 100, mid, 1, -1, 1.0, mid - 10))
    strategy, count = run_events(events, cfg)
    assert strategy.broker.position is None, "posição aberta após finish()"
    return {"self_test": "ok", "events": count, "paper_trades": len(strategy.broker.trades)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay/backtest da estratégia OFI+VPIN (paper only).")
    parser.add_argument("--replay", type=Path, nargs="+", help="arquivos .jsonl ou pastas com .jsonl")
    parser.add_argument("--config", type=Path, help="JSON com parâmetros que substituem o padrão")
    parser.add_argument("--output", type=Path, help="grava o resultado completo (com trades) em JSON")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        print(json.dumps(self_test(), ensure_ascii=False, indent=2))
        return 0
    if not args.replay:
        parser.error("use --self-test ou forneça --replay arquivo.jsonl")
    result = run_replay(args.replay, load_config(args.config))
    if args.output:
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({k: result[k] for k in ("events", "summary", "filter_funnel")}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
