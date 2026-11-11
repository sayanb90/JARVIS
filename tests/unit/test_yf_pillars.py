"""Pillar inputs from Yahoo statements (SCRUM-162): the fallback chains, ROIC, currency conversion, financials, and the scorer on the result."""
from __future__ import annotations

from datetime import date, datetime

import duckdb
import pandas as pd
import pytest

from data_layer import fx, yf_pillars as pil
from data_layer.query import Lake

ENDS = ["2022-12-31", "2023-12-31", "2024-12-31", "2025-12-31"]
DAY = date(2026, 11, 10)


def period(end, **kw):
    base = {"period_end": end, "TotalRevenue": 1000.0, "NetIncome": 100.0, "EBIT": 150.0, "TaxRateForCalcs": 0.2, "InvestedCapital": 800.0,
            "FreeCashFlow": 90.0, "OrdinarySharesNumber": 100.0, "LongTermDebt": 200.0, "TotalDebt": 250.0, "CurrentDebt": 50.0}
    base.update(kw)
    return {k: v for k, v in base.items() if v is not None}


def four(**kw):
    return [period(e, **kw) for e in ENDS]


def rates(**usd):
    rows = [{"date": date(2026, 11, 9), "currency": c, "usd_per_unit": v} for c, v in usd.items()]
    return fx.Rates.from_frame(pd.DataFrame(rows))


PROF = {"currency": "USD", "financial_currency": "USD", "sector": "Industrials", "market": "US"}


def derive(periods, profile=PROF, r=None):
    return pil.derive_symbol("X", periods, profile, r if r is not None else rates(), DAY)


# ---------------------------------------------------------------------------------------------- ROIC
def test_roic_is_nopat_over_average_invested_capital_and_the_latest_year_alone_has_no_prior():
    ps = four()
    ps[2]["InvestedCapital"], ps[3]["InvestedCapital"] = 700.0, 900.0              # average of the last two years: 800
    res = derive(ps)
    last = res["periods"][-1]
    assert last["roic"] == pytest.approx(150 * 0.8 / 800)
    assert res["periods"][0]["roic"] == pytest.approx(150 * 0.8 / 800)               # the first year uses its own capital
    assert res["metrics"]["latest_roic"] == pytest.approx(0.15) and res["metrics"]["periods_used"] == 4


def test_roic_ex_cash_removes_cash_and_is_not_defined_when_nothing_is_left():
    res = derive(four(CashAndCashEquivalents=200.0))
    assert res["metrics"]["roic_ex_cash"] == pytest.approx(150 * 0.8 / 600)
    assert derive(four(CashAndCashEquivalents=900.0))["metrics"]["roic_ex_cash"] is None


def test_tax_rate_falls_back_to_the_ratio_of_the_provision_and_then_to_a_default():
    assert derive(four(TaxRateForCalcs=None, PretaxIncome=200.0, TaxProvision=50.0))["periods"][-1]["tax"] == 0.25
    assert derive(four(TaxRateForCalcs=None, PretaxIncome=200.0, TaxProvision=40.0))["periods"][-1]["src"]["tax"] == "TaxProvision/PretaxIncome"
    assert derive(four(TaxRateForCalcs=None))["periods"][-1]["src"]["tax"] == "default"
    assert derive(four(TaxRateForCalcs=0.9))["periods"][-1]["src"]["tax"] == "default"        # an absurd rate is not used


def test_ebit_and_invested_capital_have_fallbacks_and_a_negative_capital_gives_no_roic():
    res = derive(four(EBIT=None, OperatingIncome=120.0))
    assert res["periods"][-1]["ebit"] == 120.0 and "ebit=OperatingIncome" in res["metrics"]["flags"]
    res = derive(four(EBIT=None, PretaxIncome=100.0, InterestExpense=-20.0))
    assert res["periods"][-1]["ebit"] == 120.0
    res = derive(four(InvestedCapital=None, StockholdersEquity=500.0, MinorityInterest=10.0))
    assert res["periods"][-1]["invested_capital"] == 760.0 and "ic=TotalDebt+Equity" in res["metrics"]["flags"]
    res = derive(four(InvestedCapital=-50.0))
    assert res["metrics"]["latest_roic"] is None and "roic=missing_inputs" in res["metrics"]["flags"]


def test_a_financial_company_has_roic_not_applicable_and_is_flagged():
    res = derive(four(), {**PROF, "sector": "Financial Services"})
    assert res["metrics"]["latest_roic"] is None and res["metrics"]["financial"] is True and "roic=na_financial" in res["metrics"]["flags"]


# ---------------------------------------------------------------------------------------------- the chains
def test_long_term_debt_chain_from_the_real_cases():
    assert derive(four())["periods"][-1]["ltd"] == 200.0                                          # LongTermDebt
    nestle = derive(four(LongTermDebt=None, LongTermDebtAndCapitalLeaseObligation=180.0))
    assert nestle["periods"][-1]["ltd"] == 180.0 and "ltd=LongTermDebtAndCapitalLeaseObligation" in nestle["metrics"]["flags"]
    split = derive(four(LongTermDebt=None, TotalDebt=300.0, CurrentDebt=75.0))
    assert split["periods"][-1]["ltd"] == 225.0 and "ltd=TotalDebt-CurrentDebt" in split["metrics"]["flags"]
    whole = derive(four(LongTermDebt=None, TotalDebt=300.0, CurrentDebt=None))
    assert whole["periods"][-1]["ltd"] == 300.0                                                    # the strict, higher figure
    none = derive(four(LongTermDebt=None, TotalDebt=None, CurrentDebt=None))
    assert none["metrics"]["latest_long_term_debt"] is None


def test_free_cash_flow_and_share_count_fall_back():
    res = derive(four(FreeCashFlow=None, OperatingCashFlow=120.0, CapitalExpenditure=-30.0))
    assert res["periods"][-1]["fcf"] == 90.0 and "fcf=OperatingCashFlow-CapitalExpenditure" in res["metrics"]["flags"]
    assert derive(four(FreeCashFlow=None, OperatingCashFlow=120.0, CapitalExpenditure=30.0))["periods"][-1]["fcf"] == 90.0     # a positive capex sign
    res = derive(four(OrdinarySharesNumber=None, DilutedAverageShares=95.0))
    assert res["periods"][-1]["shares"] == 95.0


def test_fewer_than_three_statements_leave_the_multi_year_values_empty():
    res = derive(four()[:2])
    m = res["metrics"]
    assert m["periods_used"] == 2 and m["avg_net_income_5y"] is None and m["avg_fcf_5y"] is None and "periods=2" in m["flags"]
    assert derive(four()[:3])["metrics"]["avg_net_income_5y"] == 100.0


def test_only_the_latest_four_years_count():
    ps = [period("2021-12-31", NetIncome=1000.0)] + four()
    assert derive(ps)["metrics"]["avg_net_income_5y"] == 100.0


# ---------------------------------------------------------------------------------------------- currency
def test_dollar_statements_against_a_pence_price_are_converted_into_pence():
    prof = {**PROF, "currency": "GBp", "financial_currency": "USD", "market": "UK"}
    m = derive(four(), prof, rates(GBP=1.25))["metrics"]
    assert m["fx_rate"] == pytest.approx(80.0)                                                  # 1 USD = 0.8 GBP = 80 pence
    assert m["avg_net_income_5y"] == pytest.approx(100 * 80) and m["latest_long_term_debt"] == pytest.approx(200 * 80)
    assert m["latest_roic"] == pytest.approx(0.15) and m["latest_shares_outstanding"] == 100.0   # ratios and counts are not converted


def test_yuan_statements_against_a_hong_kong_price():
    prof = {**PROF, "currency": "HKD", "financial_currency": "CNY", "market": "HK"}
    m = derive(four(), prof, rates(CNY=0.14, HKD=0.128))["metrics"]
    assert m["fx_rate"] == pytest.approx(0.14 / 0.128) and m["avg_fcf_5y"] == pytest.approx(90 * 0.14 / 0.128)


def test_a_missing_rate_leaves_converted_values_empty_never_converts_with_a_guess():
    prof = {**PROF, "currency": "KRW", "financial_currency": "USD"}
    m = derive(four(), prof, rates(USD=1.0))["metrics"]                                           # no KRW rate
    assert m["fx_missing"] is True and m["avg_net_income_5y"] is None and m["latest_long_term_debt"] is None and "fx_missing" in m["flags"]
    assert derive(four(), prof, None)["metrics"]["fx_missing"] is True                           # no rates file at all


def test_a_profile_without_currencies_is_assumed_to_report_in_its_price_currency_and_nan_is_not_a_string():
    m = derive(four(), {"currency": "USD", "financial_currency": float("nan"), "sector": float("nan")})["metrics"]
    assert m["report_currency"] == "USD" and m["fx_rate"] == 1.0 and "ccy=assumed_same" in m["flags"]
    m = derive(four(), {"currency": float("nan"), "financial_currency": float("nan")})["metrics"]
    assert m["fx_missing"] is True                                                               # no currency known at all


# ---------------------------------------------------------------------------------------------- the build and the scorer
def seed(tmp_path, symbols):
    """versions and profiles for {symbol: (periods, profile)} under tmp_path, plus daily prices, and the FX rates of the day."""
    rows, profs = [], []
    for sym, (ps, prof) in symbols.items():
        for p in ps:
            for k, v in p.items():
                if k != "period_end":
                    rows.append({"symbol": sym, "statement": "x", "item": k, "period_end": p["period_end"], "value": v, "currency": "",
                                 "first_seen": "2026-11-01", "last_seen": "2026-11-01"})
        profs.append({"symbol": sym, "financial_currency": prof.get("financial_currency"), "currency": prof.get("currency"),
                      "sector": prof.get("sector"), "market": prof.get("market"), "fetched_at": "2026-11-01"})
    base = tmp_path / "yf_fundamentals"
    base.mkdir(parents=True)
    pd.DataFrame(rows).to_parquet(base / "versions.parquet", index=False)
    pd.DataFrame(profs).to_parquet(base / "profile.parquet", index=False)


def prices(tmp_path, px):
    d = tmp_path / "ohlc" / "year=2026"
    d.mkdir(parents=True)
    pd.DataFrame([{"date": date(2026, 11, 9), "symbol": s, "market": "X", "open": p, "high": p, "low": p, "close": p, "volume": 1}
                  for s, p in px.items()]).to_parquet(d / "ohlc.parquet", index=False)


def growing(**kw):
    """Four years that pass every pillar: revenue, net income and FCF grow, shares fall, debt is small, ROIC is high."""
    out = []
    for i, e in enumerate(ENDS):
        out.append(period(e, TotalRevenue=1000 + 100 * i, NetIncome=100 + 20 * i, FreeCashFlow=100 + 20 * i, OrdinarySharesNumber=100 - 2 * i,
                          EBIT=200.0, InvestedCapital=800.0, LongTermDebt=100.0, **kw))
    return out


def test_build_writes_the_tables_and_the_scorer_scores_a_us_stock_a_pence_stock_and_a_bank(tmp_path):
    uk = {**PROF, "currency": "GBp", "financial_currency": "USD", "market": "UK"}
    bank = {**PROF, "sector": "Financial Services"}
    seed(tmp_path, {"GOOD": (growing(), PROF), "UKCO": (growing(), uk), "BANK": (growing(), bank), "THIN": (growing()[:2], PROF)})
    prices(tmp_path, {"GOOD": 10.0, "UKCO": 1000.0, "BANK": 10.0, "THIN": 10.0})
    fx.refresh(tmp_path, currencies=["GBP"], now=datetime(2026, 11, 9, 1, 0),
               download=lambda t, **k: {x: pd.DataFrame({"Close": [1.25]}, index=pd.to_datetime(["2026-11-09"])) for x in t})
    s = pil.build(tmp_path, day=DAY)
    assert s["symbols"] == 4 and s["financial"] == 1 and s["short"] == 1 and s["scoreable"] == 3
    q = Lake(tmp_path)
    assert set(q.sql("SELECT DISTINCT symbol FROM income_statement")["symbol"]) == {"GOOD", "UKCO", "BANK", "THIN"}
    assert len(q.sql("SELECT * FROM cash_flow WHERE symbol = 'GOOD'")) == 4
    sc = q.score_all().set_index("symbol")
    assert sc.loc["GOOD", "score"] == 8 and sc.loc["GOOD", "max_score"] == 8
    assert sc.loc["UKCO", "score"] == 8                                                          # pence price against dollar earnings, converted
    assert sc.loc["BANK", "score"] == 7 and sc.loc["BANK", "max_score"] == 7                     # ROIC does not apply to a bank
    assert sc.loc["THIN", "score"] < 8 and sc.loc["THIN", "periods"] == 2
    # the pence stock would fail P/E if the earnings were left in dollars: 1000 * 100 shares / 140 avg NI = 714 per unit
    assert q.pillar_metrics().set_index("symbol").loc["UKCO", "avg_net_income_5y"] == pytest.approx(130 * 80)


def test_a_restated_value_is_used_and_a_second_build_changes_nothing(tmp_path):
    seed(tmp_path, {"GOOD": (growing(), PROF)})
    prices(tmp_path, {"GOOD": 10.0})
    pil.build(tmp_path, day=DAY, rates=rates())
    before = Lake(tmp_path).pillar_metrics().iloc[0].to_dict()
    pil.build(tmp_path, day=DAY, rates=rates())
    q = Lake(tmp_path)
    assert len(q.pillar_metrics()) == 1 and len(q.sql("SELECT * FROM ratios")) == 4
    assert q.pillar_metrics().iloc[0].to_dict() == before


def test_command_line(tmp_path, monkeypatch, capsys):
    seed(tmp_path, {"GOOD": (growing(), PROF)})
    monkeypatch.setattr(pil.lake_mod, "LAKE_DIR", tmp_path)
    monkeypatch.setattr(pil.yfc, "store_dir", lambda lake_dir=None: tmp_path / "yf_fundamentals")
    monkeypatch.setattr(pil.fx_mod, "load_rates", lambda lake_dir=None: rates())
    assert pil.main(["build"]) == 0
    assert "1 symbols, 1 scoreable" in capsys.readouterr().out
