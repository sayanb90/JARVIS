"""
The backtester (SCRUM-217): replays a rule-based strategy over historical minutes, with real F&O costs and a rupee risk budget.

  backtester/spec.py      the strategy file (TOML): instrument, entry and exit rules on signal columns, risk budget, conviction tiers, fills
  backtester/data.py      signals as known minute by minute (the store's 1-minute layers, as-of their known_at), index/future/option minute bars
  backtester/engine.py    the simulation: decide at a bar's close, fill at the next bar's open, stops on bar high/low, sizing from the budget
  backtester/report.py    trades, daily P&L, summary (and per conviction tier)
  python -m backtester run --config backtester/examples/fut_noise_breakout.toml --from 2024-11-01 --to 2026-11-20

Separate from the paper trader (trading/): the backtester runs on history, the paper trader on the live session.  A strategy that passes validation
in the backtester is what the paper trader should run later (a follow-up ticket gives both one rule evaluator).
"""
