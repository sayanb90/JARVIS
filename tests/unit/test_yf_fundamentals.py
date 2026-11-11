"""Yahoo fundamentals (SCRUM-161): parsing, the failure classes, storage and restatements, what is due, and never overwriting good data with
empty data.  A fake Ticker stands in for yfinance; nothing touches the network."""
from __future__ import annotations

import gzip
import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from data_layer import yf_fundamentals as yf

PERIODS = [pd.Timestamp(d) for d in ("2025-12-31", "2024-12-31", "2023-12-31", "2022-12-31")]
NOW = datetime(2026, 11, 11, 12, 0)


def table(rows: dict, periods=PERIODS):
    return pd.DataFrame({p: [rows[i][k] for i in rows] for k, p in enumerate(periods)}, index=list(rows))


def good_income(scale=1.0):
    return table({"TotalRevenue": [400, 380, 350, 300], "NetIncome": [50, 45, 40, 30], "EBIT": [70, 65, 60, 50], "TaxRateForCalcs": [.2, .2, .21, .2],
                  "DilutedAverageShares": [95, 98, 100, 102], "SomethingWeDoNotKeep": [1, 2, 3, 4]})


def good_balance():
    return table({"InvestedCapital": [300, 290, 280, 270], "LongTermDebt": [80, 85, 90, 95], "OrdinarySharesNumber": [95, 98, 100, 102],
                  "TotalAssets": [700, 690, 650, 600]})


def good_cashflow():
    return table({"FreeCashFlow": [60, 55, 50, 40], "OperatingCashFlow": [90, 85, 80, 70], "CapitalExpenditure": [-30, -30, -30, -30]})


class FakeTicker:
    """A Ticker whose answers are set per symbol in BOOK: a value, or an exception instance to raise."""
    BOOK: dict = {}
    CALLS: list = []

    def __init__(self, symbol):
        self.s = symbol
        self.b = self.BOOK.get(symbol, {})

    def _get(self, key, default):
        FakeTicker.CALLS.append((self.s, key))
        v = self.b.get(key, default)
        if isinstance(v, BaseException):
            raise v
        return v

    def get_income_stmt(self, freq="yearly"):
        return self._get("income", good_income())

    def get_balance_sheet(self, freq="yearly"):
        return self._get("balance", good_balance())

    def get_cashflow(self, freq="yearly"):
        return self._get("cashflow", good_cashflow())

    def get_info(self):
        return self._get("info", {"currency": "USD", "financialCurrency": "USD", "marketCap": 5e9, "sharesOutstanding": 95.0, "sector": "Technology",
                                  "quoteType": "EQUITY", "longName": f"{self.s} Inc", "exchange": "NMS", "ignored": [1, 2]})

    def get_dividends(self):
        return self._get("dividends", pd.Series([0.5, 0.55], index=pd.to_datetime(["2025-06-01", "2025-12-01"])))

    def get_splits(self):
        return self._get("splits", pd.Series(dtype=float))


@pytest.fixture(autouse=True)
def reset():
    FakeTicker.BOOK, FakeTicker.CALLS = {}, []


def go(tmp_path, symbols, **kw):
    kw.setdefault("now", NOW)
    return yf.run(symbols, tmp_path, FakeTicker, pause_s=0, sleep=lambda s: None, **kw)


def stored(tmp_path, name):
    return pd.read_parquet(yf.store_dir(tmp_path) / f"{name}.parquet")


# ---------------------------------------------------------------------------------------------- parsing
def test_only_the_items_the_pillars_use_are_kept_with_their_period_ends():
    rows = yf.parse_statement(good_income(), "income")
    assert {r["item"] for r in rows} == {"TotalRevenue", "NetIncome", "EBIT", "TaxRateForCalcs", "DilutedAverageShares"}
    rev = {r["period_end"]: r["value"] for r in rows if r["item"] == "TotalRevenue"}
    assert rev == {"2025-12-31": 400.0, "2024-12-31": 380.0, "2023-12-31": 350.0, "2022-12-31": 300.0}


def test_missing_values_are_skipped_and_a_shape_change_is_a_parse_error():
    t = good_income()
    t.loc["NetIncome", PERIODS[3]] = np.nan
    assert len([r for r in yf.parse_statement(t, "income") if r["item"] == "NetIncome"]) == 3
    with pytest.raises(yf.ParseError, match="row labels"):
        yf.parse_statement(pd.DataFrame([[1, 2]], index=[0], columns=PERIODS[:2]), "income")
    with pytest.raises(yf.ParseError, match="column labels"):
        yf.parse_statement(pd.DataFrame([[1, 2]], index=["NetIncome"], columns=["a", "b"]), "income")
    with pytest.raises(yf.ParseError, match="expected a table"):
        yf.parse_statement({"NetIncome": 1}, "income")


# ---------------------------------------------------------------------------------------------- a normal symbol
def test_a_complete_symbol_stores_values_profile_actions_log_and_the_whole_raw_payload(tmp_path):
    s = go(tmp_path, {"AAPL": "US"})
    assert s["symbols"] == 1 and s["complete"] == 1 and s["partial"] == 0 and s["new_versions"] == len(stored(tmp_path, "versions"))
    v = stored(tmp_path, "versions")
    assert set(v["statement"]) == {"income", "balance", "cashflow"} and set(v["currency"]) == {"USD"}
    assert (v["first_seen"] == "2026-11-11").all()
    p = stored(tmp_path, "profile").iloc[0]
    assert (p["symbol"], p["market"], p["currency"], p["financial_currency"], p["sector"]) == ("AAPL", "US", "USD", "USD", "Technology")
    a = stored(tmp_path, "actions")
    assert list(a["kind"]) == ["dividend", "dividend"] and list(a["value"]) == [0.5, 0.55]
    lg = stored(tmp_path, "fetch_log")
    assert sorted(lg["part"]) == sorted(yf.PARTS) and lg["ok"].all()
    raw = json.loads(gzip.open(yf.store_dir(tmp_path) / "raw" / "date=2026-11-11" / "AAPL.json.gz", "rt").read())
    assert "SomethingWeDoNotKeep" in raw["income"]["index"] and raw["info"]["currency"] == "USD" and "ignored" not in raw["info"]


# ---------------------------------------------------------------------------------------------- failures
def test_each_failure_has_its_own_class_and_a_partial_symbol_is_not_complete(tmp_path):
    FakeTicker.BOOK = {
        "EMPTY": {"income": pd.DataFrame()},
        "ODD": {"balance": pd.DataFrame([[1, 2]], index=[0, 1][:1], columns=PERIODS[:2])},
        "BOOM": {"cashflow": RuntimeError("connection reset")},
        "RENAMED": {"income": table({"RevenueTotal": [1, 2, 3, 4]})},
    }
    s = go(tmp_path, {k: "US" for k in FakeTicker.BOOK})
    assert s["symbols"] == 4 and s["complete"] == 0 and s["partial"] == 4
    lg = stored(tmp_path, "fetch_log").set_index(["symbol", "part"])["error_class"]
    assert lg[("EMPTY", "income")] == yf.EMPTY and lg[("ODD", "balance")] == yf.PARSE
    assert lg[("BOOM", "cashflow")] == yf.EXCEPTION and lg[("RENAMED", "income")] == yf.PARSE
    msg = stored(tmp_path, "fetch_log").set_index(["symbol", "part"])["message"]
    assert "none of the expected items" in msg[("RENAMED", "income")] and "connection reset" in msg[("BOOM", "cashflow")]


def test_a_throttled_call_is_retried_with_growing_pauses_and_then_classified(tmp_path):
    answers = [RuntimeError("HTTP Error 429: Too Many Requests"), RuntimeError("429"), good_income()]
    pauses = []

    class Flaky(FakeTicker):
        def get_income_stmt(self, freq="yearly"):
            v = answers.pop(0)
            if isinstance(v, BaseException):
                raise v
            return v
    res = yf.fetch_symbol("X", Flaky, sleep=pauses.append)
    assert res["parts"]["income"].ok and res["parts"]["income"].attempts == 3 and pauses == [5.0, 10.0]
    always = lambda s: type("T", (FakeTicker,), {"get_income_stmt": lambda self, freq="yearly": (_ for _ in ()).throw(RuntimeError("429 rate limit"))})(s)
    res = yf.fetch_symbol("X", always, sleep=lambda s: None)
    assert res["parts"]["income"].cls == yf.THROTTLED and res["parts"]["income"].attempts == 3


def test_a_company_that_never_paid_a_dividend_or_split_is_not_a_failure(tmp_path):
    FakeTicker.BOOK = {"NODIV": {"dividends": pd.Series(dtype=float)}}
    s = go(tmp_path, {"NODIV": "US"})
    assert s["complete"] == 1 and stored(tmp_path, "fetch_log")["ok"].all()


def test_a_failed_answer_never_overwrites_stored_values(tmp_path):
    go(tmp_path, {"AAPL": "US"})
    before = stored(tmp_path, "versions").sort_values(yf._KEY).reset_index(drop=True)
    FakeTicker.BOOK = {"AAPL": {"income": pd.DataFrame(), "balance": RuntimeError("blocked"), "info": {}}}
    s = go(tmp_path, {"AAPL": "US"}, now=NOW + timedelta(days=40))
    after = stored(tmp_path, "versions")
    assert s["partial"] == 1
    assert len(after) == len(before)
    cash = after[after["statement"] == "cashflow"]
    assert (cash["last_seen"] == "2026-12-21").all()                           # only the part that answered moved
    assert (after[after["statement"] == "income"]["last_seen"] == "2026-11-11").all()
    assert stored(tmp_path, "profile").iloc[0]["sector"] == "Technology"       # an empty info did not blank the profile


def test_a_restated_value_is_a_new_version_and_an_unchanged_one_only_moves_last_seen(tmp_path):
    go(tmp_path, {"AAPL": "US"})
    n0 = len(stored(tmp_path, "versions"))
    changed = good_income()
    changed.loc["NetIncome", PERIODS[0]] = 55.0
    FakeTicker.BOOK = {"AAPL": {"income": changed}}
    s = go(tmp_path, {"AAPL": "US"}, now=NOW + timedelta(days=31))
    v = stored(tmp_path, "versions")
    assert s["new_versions"] == 1 and len(v) == n0 + 1
    ni = v[(v["item"] == "NetIncome") & (v["period_end"] == "2025-12-31")].sort_values("first_seen")
    assert list(ni["value"]) == [50.0, 55.0] and list(ni["first_seen"]) == ["2026-11-11", "2026-12-12"]
    assert list(ni["last_seen"]) == ["2026-11-11", "2026-12-12"]                  # the old value keeps its last sighting
    assert (v[v["item"] == "TotalRevenue"]["last_seen"] == "2026-12-12").all()


# ---------------------------------------------------------------------------------------------- what is due
def test_due_order_is_never_fetched_then_failed_then_oldest_and_a_fresh_symbol_waits(tmp_path):
    FakeTicker.BOOK = {"BAD": {"income": pd.DataFrame()}}
    go(tmp_path, {"OLD": "US", "BAD": "US", "FRESH": "US"}, now=NOW - timedelta(days=45))
    go(tmp_path, {"FRESH": "US"}, now=NOW - timedelta(days=1))                    # fetched yesterday
    lg = stored(tmp_path, "fetch_log")
    syms = {"NEW": "US", "OLD": "US", "BAD": "US", "FRESH": "US"}
    assert yf.select_due(syms, lg, NOW, refresh_days=30) == ["NEW", "BAD", "OLD"]
    assert yf.select_due(syms, lg, NOW, refresh_days=30, limit=2) == ["NEW", "BAD"]
    # the failed one is not hammered: tried 5 hours ago, it waits
    recent = yf.select_due({"BAD": "US"}, lg, NOW - timedelta(days=45) + timedelta(hours=5), refresh_days=30)
    assert recent == []


def test_a_limit_runs_a_slice_and_the_next_run_continues_with_the_rest(tmp_path):
    syms = {f"S{i}": "US" for i in range(5)}
    assert go(tmp_path, syms, limit=2)["symbols"] == 2
    assert go(tmp_path, syms, limit=2)["symbols"] == 2
    assert go(tmp_path, syms, limit=2)["symbols"] == 1
    assert go(tmp_path, syms, limit=2)["symbols"] == 0
    assert sorted({c[0] for c in FakeTicker.CALLS}) == sorted(syms)


def test_stop_keeps_the_work_done_and_status_reports_the_failures(tmp_path):
    import threading
    stop = threading.Event()
    FakeTicker.BOOK = {"B": {"income": pd.DataFrame()}}

    class Stopper(FakeTicker):
        def get_info(self):
            if self.s == "A":
                stop.set()
            return super().get_info()
    s = yf.run({"A": "US", "B": "US"}, tmp_path, Stopper, pause_s=0, now=NOW, sleep=lambda s: None, stop=stop)
    assert s["stopped"] and s["symbols"] == 1 and len(stored(tmp_path, "versions")) > 0
    st = yf.status(tmp_path)
    assert st["symbols_with_values"] == 1 and st["last_run_day"] == "2026-11-11"
    go(tmp_path, {"B": "US"})
    assert yf.status(tmp_path)["last_day_classes"] == {"empty": 1}


def test_command_line_with_symbols_and_status(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(yf, "store_dir", lambda lake_dir=None: tmp_path / "yf")
    monkeypatch.setattr("yfinance.Ticker", FakeTicker)
    monkeypatch.setenv("JARVIS_YF_FUND_PAUSE_S", "0")
    assert yf.main(["run", "--symbols", "AAPL,MSFT"]) == 0
    out = capsys.readouterr().out
    assert "2 symbols (2 complete, 0 partial)" in out and "problems by class: {}" in out
    assert yf.main(["status"]) == 0 and '"symbols_with_values": 2' in capsys.readouterr().out
