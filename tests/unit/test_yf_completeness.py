"""Four-year completeness per symbol (SCRUM-163) and the backfill it drives: gaps are retried weekly, failures after 20 hours, a day the job did
not run is caught up, and a later fetch that fills a gap clears it."""
from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from data_layer import yf_completeness as cmp, yf_fundamentals as yf
from tests.unit.test_yf_fundamentals import FakeTicker, NOW, PERIODS, good_balance, good_cashflow, good_income, go, stored, table
from tests.unit.test_yf_pillars import ENDS, period

TODAY = date(2026, 11, 10)


@pytest.fixture(autouse=True)
def _reset():
    FakeTicker.BOOK, FakeTicker.CALLS = {}, []
FIN = {"sector": "Financial Services"}


def periods(ends=ENDS, **kw):
    return [period(e, **kw) for e in ends]


def test_four_consecutive_complete_years_are_complete():
    s = cmp.symbol_status(periods(), {}, TODAY)
    assert (s["status"], s["periods"], s["latest_fy"], s["reason"]) == ("complete", 4, "2025-12-31", "")


def test_three_complete_years_are_short_and_a_missing_item_names_the_year_and_the_item():
    assert cmp.symbol_status(periods(ENDS[1:]), {}, TODAY)["status"] == "short"
    ps = periods()
    ps[1].pop("InvestedCapital")
    ps[2].pop("FreeCashFlow")
    s = cmp.symbol_status(ps, {}, TODAY)
    assert s["status"] == "incomplete" and s["missing"] == "2023:invested_capital;2024:fcf" and "2023:invested_capital" in s["reason"]


def test_a_year_missing_in_the_middle_is_a_gap_and_an_old_latest_year_is_stale():
    ends = ["2021-12-31", "2023-12-31", "2024-12-31", "2025-12-31"]
    s = cmp.symbol_status(periods(ends), {}, TODAY)
    assert s["status"] == "gap" and "2021-12-31..2023-12-31" in s["reason"]
    old = cmp.symbol_status(periods(["2020-12-31", "2021-12-31", "2022-12-31", "2023-03-31"]), {}, TODAY)
    assert old["status"] == "stale" and old["age_days"] > 548


def test_a_financial_company_is_not_asked_for_ebit_or_invested_capital():
    ps = periods()
    for p in ps:
        p.pop("EBIT")
        p.pop("InvestedCapital")
    assert cmp.symbol_status(ps, FIN, TODAY)["status"] == "complete"
    assert cmp.symbol_status(ps, {"sector": "Industrials"}, TODAY)["status"] == "incomplete"


def test_the_fallback_chains_count_as_present():
    ps = periods(LongTermDebt=None, LongTermDebtAndCapitalLeaseObligation=100.0, FreeCashFlow=None, OperatingCashFlow=50.0, CapitalExpenditure=-10.0)
    assert cmp.symbol_status(ps, {}, TODAY)["status"] == "complete"
    assert cmp.symbol_status([], {}, TODAY)["status"] == "no_data"


def test_the_check_covers_the_whole_universe_and_writes_the_table(tmp_path):
    FakeTicker.BOOK = {"BAD": {"income": pd.DataFrame()}, "SHORT": {"income": table({"TotalRevenue": [1, 2, 3, 4], "NetIncome": [1, 1, 1, 1]}, PERIODS[:2]).iloc[:, :2]}}
    go(tmp_path, {"GOOD": "US", "BAD": "US", "SHORT": "UK"})
    df = cmp.check({"GOOD": "US", "BAD": "US", "SHORT": "UK", "NEVER": "JP"}, tmp_path, TODAY.replace(year=2026), write=True)
    st = df.set_index("symbol")["status"]
    assert st["NEVER"] == "pending" and st["BAD"] == "fetch_failed" and st["GOOD"] in ("complete", "incomplete")
    assert "income: empty" in df.set_index("symbol").loc["BAD", "reason"]
    assert (tmp_path / "yf_fundamentals" / "completeness.parquet").exists()
    sm = cmp.summary(df)
    assert sm["symbols"] == 4 and sm["by_status"]["pending"] == 1 and sm["by_market"]["JP"]["complete_share"] is None


# ---------------------------------------------------------------------------------------------- what the daily job backfills
def log_row(sym, when, ok=True):
    return [{"symbol": sym, "part": p, "fetched_at": when.isoformat(timespec="seconds"), "ok": ok, "error_class": "" if ok else "empty",
             "rows": 5 if ok else 0, "attempts": 1, "latency_s": 0.1, "message": ""} for p in ("income", "balance", "cashflow")]


def test_a_symbol_with_gaps_is_retried_weekly_not_daily_and_a_failed_one_after_20_hours():
    lg = pd.DataFrame(log_row("GAPPY", NOW - timedelta(days=2)) + log_row("OLDGAP", NOW - timedelta(days=8)) + log_row("FINE", NOW - timedelta(days=2))
                      + log_row("FAILED", NOW - timedelta(hours=5), ok=False) + log_row("FAILED2", NOW - timedelta(hours=25), ok=False))
    syms = {s: "US" for s in ("GAPPY", "OLDGAP", "FINE", "FAILED", "FAILED2", "NEW")}
    due = yf.select_due(syms, lg, NOW, refresh_days=30, gap_symbols={"GAPPY", "OLDGAP"})
    assert due == ["NEW", "FAILED2", "OLDGAP"]                                    # never fetched, failed (long enough ago), gap (a week ago)


def test_a_backlog_after_missed_days_is_caught_up_within_the_cap():
    syms = {f"S{i}": "US" for i in range(30)}
    lg = pd.DataFrame([r for s in list(syms)[:10] for r in log_row(s, NOW - timedelta(days=60))])           # ten are two months old
    assert len(yf.select_due(syms, lg, NOW, 30, limit=5)) == 5                                              # no catch-up asked: the daily slice
    assert len(yf.select_due(syms, lg, NOW, 30, limit=5, catch_up=50)) == 30                               # 20 never fetched + 10 late: all of them
    assert len(yf.select_due(syms, lg, NOW, 30, limit=5, catch_up=12)) == 12                               # but never more than the cap
    assert len(yf.select_due(syms, lg, NOW, 30, limit=40, catch_up=50)) == 30                              # a limit above the backlog changes nothing


def test_a_run_that_was_skipped_is_made_up_by_the_next_one(tmp_path):
    syms = {f"S{i}": "US" for i in range(6)}
    go(tmp_path, syms, limit=2, now=NOW)                                          # day 1: 2 of 6
    # days 2 to 4 never ran; the next run (limit 2, catch-up 10) takes everything still never fetched, not just 2
    s = go(tmp_path, syms, limit=2, catch_up=10, now=NOW + timedelta(days=4))
    assert s["symbols"] == 4 and sorted({c[0] for c in FakeTicker.CALLS}) == sorted(syms)


def test_a_later_fetch_that_fills_a_gap_clears_it(tmp_path):
    thin = good_income()
    thin = thin.drop(columns=[PERIODS[1]])                                        # the 2024 column is missing: a gap in the sequence
    FakeTicker.BOOK = {"GAPPY": {"income": thin, "balance": good_balance().drop(columns=[PERIODS[1]]), "cashflow": good_cashflow().drop(columns=[PERIODS[1]])}}
    go(tmp_path, {"GAPPY": "US"})
    assert cmp.check({"GAPPY": "US"}, tmp_path, date(2026, 11, 11)).iloc[0]["status"] == "gap"
    assert "GAPPY" in cmp.gap_symbols(tmp_path, date(2026, 11, 11))
    n = len(FakeTicker.CALLS)
    go(tmp_path, {"GAPPY": "US"}, now=NOW + timedelta(days=2))                    # too early: not retried
    assert len(FakeTicker.CALLS) == n
    FakeTicker.BOOK = {}                                                          # Yahoo now has the year
    s = go(tmp_path, {"GAPPY": "US"}, now=NOW + timedelta(days=8))
    assert s["symbols"] == 1 and len(FakeTicker.CALLS) > n
    assert cmp.check({"GAPPY": "US"}, tmp_path, date(2026, 11, 19)).iloc[0]["status"] == "complete"
    assert "GAPPY" not in cmp.gap_symbols(tmp_path, date(2026, 11, 19))


def test_check_command_line_reports_each_market_and_exits_nonzero_below_the_threshold(tmp_path, monkeypatch, capsys):
    FakeTicker.BOOK = {"BAD": {"income": pd.DataFrame()}}
    go(tmp_path, {"GOOD": "US", "BAD": "UK"})
    real_check = cmp.check
    monkeypatch.setattr(yf, "store_dir", lambda lake_dir=None: tmp_path / "yf_fundamentals")
    monkeypatch.setattr(yf, "universe_symbols", lambda markets=None: {"GOOD": "US", "BAD": "UK", "NEW": "JP"})
    monkeypatch.setattr(cmp, "check", lambda symbols, lake_dir=None, today=None, write=True: real_check(symbols, tmp_path, date(2026, 11, 11), write))
    assert yf.main(["check", "--min-complete", "0.9"]) == 1                      # UK has none complete
    out = capsys.readouterr().out
    assert "3 symbols, by status" in out and "UK" in out and "fetch_failed" in out
    assert yf.main(["check", "--min-complete", "0.0", "--market", "US"]) == 0
