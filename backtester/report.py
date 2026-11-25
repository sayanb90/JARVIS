"""
What a backtest made, and how (SCRUM-217): the trades, the P&L per day, and a summary with the numbers a strategy is judged on.

  trades_frame(res)   one row per round trip: contract, lots, prices, reason for the exit, gross, costs, net, conviction score and scale, planned risk
  daily_frame(res)    one row per trading day the backtest saw (days without a trade at 0): trades, gross, costs, net, cumulative net, drawdown
  summary(res)        totals, win rate, average win and loss, profit factor, Sharpe (daily, x sqrt 250), max drawdown, worst day against the budget,
                      exits by reason, and the same numbers per conviction tier (does a bigger score really earn more?)
  write(res, folder)  trades.csv, daily.csv, skipped.csv, summary.json
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict
from pathlib import Path

import numpy as np
import pandas as pd

from backtester.engine import Result

COLS = ["day", "symbol", "kind", "direction", "lots", "lot_size", "qty", "decided_at", "entry_at", "entry_price", "exit_at", "exit_price",
        "exit_reason", "gross", "costs", "net", "score", "size_scale", "planned_risk"]


def trades_frame(res: Result) -> pd.DataFrame:
    rows = [{**{k: getattr(t, k) for k in COLS if k not in ("qty", "gross", "net")}, "qty": t.qty, "gross": t.gross, "net": t.net} for t in res.trades]
    return pd.DataFrame(rows, columns=COLS)


def daily_frame(res: Result) -> pd.DataFrame:
    tr = trades_frame(res)
    days = pd.DataFrame({"day": sorted(set(res.days))})
    if len(tr):
        g = tr.groupby("day").agg(trades=("net", "size"), gross=("gross", "sum"), costs=("costs", "sum"), net=("net", "sum")).reset_index()
        days = days.merge(g, on="day", how="left")
    for c in ("trades", "gross", "costs", "net"):
        days[c] = days[c].fillna(0) if c in days.columns else 0.0
    days["cum_net"] = days["net"].cumsum()
    days["drawdown"] = days["cum_net"] - days["cum_net"].cummax().clip(lower=0)
    return days


def _stats(tr: pd.DataFrame) -> dict:
    if not len(tr):
        return {"trades": 0}
    wins, losses = tr[tr["net"] > 0]["net"], tr[tr["net"] <= 0]["net"]
    return {"trades": int(len(tr)), "net": round(float(tr["net"].sum()), 2), "gross": round(float(tr["gross"].sum()), 2),
            "costs": round(float(tr["costs"].sum()), 2), "win_rate": round(float(len(wins) / len(tr)), 4),
            "avg_win": round(float(wins.mean()), 2) if len(wins) else 0.0, "avg_loss": round(float(losses.mean()), 2) if len(losses) else 0.0,
            "profit_factor": round(float(wins.sum() / -losses.sum()), 3) if len(losses) and losses.sum() < 0 else None,
            "avg_net": round(float(tr["net"].mean()), 2)}


def summary(res: Result) -> dict:
    tr, dl = trades_frame(res), daily_frame(res)
    s = res.strategy
    out = {"strategy": s.name, "underlying": s.underlying, "broker": s.broker, "days": int(len(dl)),
           "first_day": str(dl["day"].min()) if len(dl) else None, "last_day": str(dl["day"].max()) if len(dl) else None, **_stats(tr)}
    if len(dl):
        sd = float(dl["net"].std(ddof=1)) if len(dl) > 1 else 0.0
        out["sharpe_daily_annualised"] = round(float(dl["net"].mean() / sd * math.sqrt(250)), 3) if sd > 0 else None
        out["max_drawdown"] = round(float(dl["drawdown"].min()), 2)
        out["worst_day"] = round(float(dl["net"].min()), 2)
        out["days_over_budget"] = int((dl["net"] < -s.risk.daily_loss_budget).sum())          # gaps through stops: the real loss passed the plan
        out["days_traded"] = int((dl["trades"] > 0).sum())
    if len(tr):
        out["exits"] = {k: int(v) for k, v in tr["exit_reason"].value_counts().items()}
        out["costs_share_of_gross"] = round(float(tr["costs"].sum() / abs(tr["gross"].sum())), 4) if tr["gross"].sum() else None
        out["by_tier"] = {f"{scale:g}": _stats(g) for scale, g in tr.groupby("size_scale")}
    out["skipped"] = len(res.skipped)
    if res.skipped:
        out["skipped_reasons"] = pd.Series([x["reason"].split(",")[0] for x in res.skipped]).value_counts().to_dict()
    return out


def write(res: Result, folder) -> Path:
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    trades_frame(res).to_csv(folder / "trades.csv", index=False)
    daily_frame(res).to_csv(folder / "daily.csv", index=False)
    pd.DataFrame(res.skipped).to_csv(folder / "skipped.csv", index=False)
    (folder / "summary.json").write_text(json.dumps(summary(res), indent=1, default=str))
    return folder
