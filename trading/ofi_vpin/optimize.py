#!/usr/bin/env python3
"""Backtest em grade + walk-forward para a estratégia OFI+VPIN.

Para cada combinação da grade roda o replay completo (todas as sessões). Como a
estratégia é causal, o resultado de cada sessão só depende de dados até ela, então
o walk-forward é feito escolhendo, a cada sessão de teste, a combinação com melhor
objetivo nas `train_sessions` anteriores e registrando o resultado dela no teste.

O número que importa é o resultado FORA da amostra (walk-forward), não o ranking.
"""
from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import sys
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Optional

from ofi_vpin_strategy import Config, iter_events, load_config, run_events, summarize

DEFAULT_GRID = {
    "min_ofi_z": [1.0, 1.5, 2.0],
    "absorption_delta_z": [0.0, 0.5, 1.0],
    "min_stop_ticks": [20, 40, 80],
    "rr": [1.0, 2.0],
}

_EVENTS: list = []


def _load(paths: list[str]) -> None:
    global _EVENTS
    _EVENTS = list(iter_events([Path(p) for p in paths]))


def _run(base: dict, params: dict) -> dict:
    cfg = Config.from_dict({**base, **params})
    strategy, _ = run_events(_EVENTS, cfg)
    trades = strategy.broker.trades
    by_session: dict[str, list[float]] = defaultdict(list)
    for t in trades:
        by_session[t.session].append(t.pnl_brl)
    return {"params": params, "summary": summarize(trades), "session_pnls": dict(by_session)}


def objective(result: dict, sessions: list[str], min_trades: int, dd_penalty: float) -> float:
    pnls = [x for s in sessions for x in result["session_pnls"].get(s, [])]
    if len(pnls) < min_trades:
        return -math.inf
    equity = peak = max_dd = 0.0
    for x in pnls:
        equity += x
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return equity - dd_penalty * max_dd


def walk_forward(results: list[dict], sessions: list[str], train_n: int, min_trades: int,
                 dd_penalty: float) -> list[dict]:
    folds = []
    for k in range(train_n, len(sessions)):
        train, test = sessions[k - train_n:k], sessions[k]
        scored = [(objective(r, train, min_trades, dd_penalty), i) for i, r in enumerate(results)]
        score, best = max(scored)
        if score == -math.inf:
            folds.append({"test_session": test, "params": None, "train_score": None, "test_pnl_brl": 0.0,
                          "test_trades": 0})
            continue
        chosen = results[best]
        test_pnls = chosen["session_pnls"].get(test, [])
        folds.append({
            "test_session": test,
            "params": chosen["params"],
            "train_score": round(score, 2),
            "test_pnl_brl": round(sum(test_pnls), 2),
            "test_trades": len(test_pnls),
        })
    return folds


def diagnostics(results: list[dict], folds: list[dict], grid: dict, base_cfg: Config,
                baseline: Optional[dict]) -> list[str]:
    notes = []
    oos = sum(f["test_pnl_brl"] for f in folds)
    oos_trades = sum(f["test_trades"] for f in folds)
    best_is = max(results, key=lambda r: r["summary"]["net_pnl_brl"])
    if folds:
        notes.append(f"Walk-forward: R$ {oos:.2f} em {oos_trades} operações fora da amostra "
                     f"({len(folds)} sessões de teste).")
        if oos <= 0:
            notes.append("Sem vantagem fora da amostra: NÃO operar nem refinar a grade; o problema é o sinal, "
                         "não o ajuste fino. Melhor in-sample (R$ "
                         f"{best_is['summary']['net_pnl_brl']:.2f}) é provavelmente overfitting.")
    if baseline is not None and baseline["trades"]:
        notes.append(f"Configuração atual (sem otimizar) no período todo: R$ {baseline['net_pnl_brl']:.2f} "
                     f"em {baseline['trades']} operações, PF {baseline['profit_factor']}.")

    cfg = base_cfg
    slip_brl = cfg.slippage_ticks * cfg.tick_size * cfg.point_value_brl
    round_trip = cfg.fee_brl_per_contract + 2 * slip_brl
    notes.append(f"Custo por contrato ida+volta (corretagem + slippage nos dois lados): R$ {round_trip:.2f} "
                 f"= {round_trip / (cfg.tick_size * cfg.point_value_brl):.1f} ticks. O sinal precisa prever "
                 "movimento maior que isso no horizonte da operação.")

    reasons = Counter()
    for r in results:
        reasons.update(r["summary"]["exit_reasons"])
    total = sum(reasons.values())
    if total:
        slow = sum(v for k, v in reasons.items() if k in ("force_flatten", "force_flatten_clock",
                                                          "session_end", "end_of_data"))
        if slow / total > 0.3:
            notes.append(f"{slow / total:.0%} das saídas foram por zeragem/fim do pregão, não por stop/alvo: "
                         "stop e alvo largos demais para um sinal de 1 s. Teste stops menores.")

    chosen = [f["params"] for f in folds if f["params"]]
    for name, values in grid.items():
        if len(values) < 2 or not chosen:
            continue
        counts = Counter(p[name] for p in chosen)
        top, n = counts.most_common(1)[0]
        stability = n / len(chosen)
        edge = top in (min(values), max(values)) and isinstance(top, (int, float))
        msg = f"{name}: escolhido {top} em {stability:.0%} dos folds"
        if stability < 0.5:
            msg += " (instável: parâmetro sem efeito real ou sobreajuste)"
        if edge:
            msg += " (na borda da grade: amplie a faixa nessa direção)"
        notes.append(msg)
    return notes


def main() -> int:
    parser = argparse.ArgumentParser(description="Grade + walk-forward para OFI/VPIN (paper only).")
    parser.add_argument("--replay", type=Path, nargs="+", required=True, help="arquivos .jsonl ou pastas")
    parser.add_argument("--config", type=Path, help="parâmetros base (fora da grade)")
    parser.add_argument("--grid", type=Path, help='JSON {"parametro": [valores...]}')
    parser.add_argument("--train-sessions", type=int, default=5)
    parser.add_argument("--min-trades", type=int, default=5, help="mínimo de operações no treino")
    parser.add_argument("--dd-penalty", type=float, default=0.5, help="objetivo = PnL - k * drawdown")
    parser.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 1))
    parser.add_argument("--output", type=Path, default=Path("otimizacao.json"))
    args = parser.parse_args()

    base_cfg = load_config(args.config)
    base = json.loads(args.config.read_text(encoding="utf-8")) if args.config else {}
    grid = json.loads(args.grid.read_text(encoding="utf-8")) if args.grid else DEFAULT_GRID
    Config.from_dict({k: v[0] for k, v in grid.items()})  # valida nomes antes de rodar
    combos = [dict(zip(grid, values)) for values in itertools.product(*grid.values())]
    paths = [str(p) for p in args.replay]

    _load(paths)
    sessions = sorted({e.session for e in _EVENTS})
    print(f"{len(_EVENTS)} eventos, {len(sessions)} sessões, {len(combos)} combinações, "
          f"{args.workers} processos.", flush=True)
    if len(sessions) <= args.train_sessions:
        print("AVISO: poucas sessões para walk-forward; grave mais pregões.", flush=True)

    baseline = _run(base, {})["summary"]
    if args.workers > 1:
        with ProcessPoolExecutor(args.workers, initializer=_load, initargs=(paths,)) as pool:
            results = list(pool.map(_run, itertools.repeat(base), combos))
    else:
        results = [_run(base, c) for c in combos]

    folds = walk_forward(results, sessions, args.train_sessions, args.min_trades, args.dd_penalty)
    notes = diagnostics(results, folds, grid, base_cfg, baseline)
    ranking = sorted(results, key=lambda r: objective(r, sessions, args.min_trades, args.dd_penalty),
                     reverse=True)
    report = {
        "paper_only": True,
        "sessions": sessions,
        "grid": grid,
        "baseline_summary": baseline,
        "walk_forward": {
            "train_sessions": args.train_sessions,
            "oos_pnl_brl": round(sum(f["test_pnl_brl"] for f in folds), 2),
            "oos_trades": sum(f["test_trades"] for f in folds),
            "folds": folds,
        },
        "in_sample_top10": [{"params": r["params"], "summary": r["summary"]} for r in ranking[:10]],
        "diagnostics": notes,
    }
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print("\nTop 5 in-sample (NÃO use isto sozinho para escolher parâmetros):")
    for r in ranking[:5]:
        s = r["summary"]
        print(f"  {r['params']} -> R$ {s['net_pnl_brl']:.2f} | {s['trades']} ops | PF {s['profit_factor']} "
              f"| DD R$ {s['max_drawdown_brl']:.2f}")
    print("\nWalk-forward (fora da amostra):")
    for f in folds:
        print(f"  {f['test_session']}: R$ {f['test_pnl_brl']:.2f} ({f['test_trades']} ops) com {f['params']}")
    print("\nDiagnóstico:")
    for note in notes:
        print(f"  - {note}")
    print(f"\nRelatório completo: {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
