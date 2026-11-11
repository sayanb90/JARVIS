"""
Pillar inputs from the Yahoo statements (SCRUM-162): ROIC, long-term debt, free cash flow, share counts, in the price's currency.

  python -m data_layer.yf_pillars build        # lake/yf_fundamentals/versions.parquet -> the tables Lake.score_ticker reads

What it does, per symbol, from up to the last four annual statements Yahoo gives (the window the owner accepted on 2026-11-11):

  * net income, revenue, EBIT, free cash flow, share count, long-term debt, invested capital: each falls back along a chain, because
    Yahoo does not label every company the same.  The real data showed it: Nestlé, TCS and Moutai have no "LongTermDebt" line but
    do have "LongTermDebtAndCapitalLeaseObligation" and "TotalDebt"; banks have no EBIT.  The source used is recorded in `flags`.
  * ROIC = NOPAT / invested capital, with NOPAT = EBIT x (1 - tax rate) and the invested capital averaged over this and the prior
    year (the latest year's alone when there is no prior).  Yahoo's "Invested Capital" is used; a variant with cash removed is stored
    next to it (roic_ex_cash).  Banks, insurers and other financial-sector companies get ROIC "not applicable": their debt is
    operating, so the pillar does not apply and the score is out of 7 for them (see Lake.score_all).
  * currency: statements are in the reporting currency (profile.financial_currency) and the price in the listing currency
    (profile.currency, which can be pence).  Earnings, cash flow and debt are converted into the price's own unit with the latest
    stored FX rate before the scorer divides market cap by them.  Hong Kong listings of mainland companies (HKD price, RMB statements)
    and London listings that report in dollars are the common cases.  A missing rate leaves the converted values empty and sets
    fx_missing; nothing is ever converted with a guessed rate.
  * averages and growth need at least MIN_PERIODS (3) annual statements; fewer leaves the pillar unscored (never "passed").

Tables written (upsert, replaced atomically), all in lake/fundamentals/, the ones Lake.score_ticker reads:
  income_statement (revenue, net_income, ebit), balance_sheet, cash_flow (free_cash_flow), ratios (roic, shares_outstanding): one row
  per symbol and fiscal year end, in the reporting currency (growth comparisons do not need a conversion);
  pillar_metrics: one row per symbol in the price's unit, with periods_used, financial, report_currency, price_currency, fx_rate,
  fx_missing, roic_ex_cash and the flags.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from . import fx as fx_mod
from . import lake as lake_mod
from . import yf_fundamentals as yfc

log = logging.getLogger(__name__)

WINDOW = 4                  # annual statements used: the latest four
MIN_PERIODS = 3             # fewer than this and the multi-year pillars are not scored
DEFAULT_TAX = 0.25
FINANCIAL_SECTORS = ("Financial Services", "Financial")


def _s(x) -> str:
    """A profile text field: pandas hands a missing one over as NaN, which must not be mistaken for a string."""
    return x.strip() if isinstance(x, str) else ""


def _v(d: dict, *keys: str):
    """The first present value among `keys` and the key it came from."""
    for k in keys:
        x = d.get(k)
        if x is not None and not pd.isna(x):
            return float(x), k
    return None, None


def derive_period(p: dict) -> dict:
    """One fiscal year's figures from a dict of Yahoo items (plus the prior year's invested capital added by the caller)."""
    f: dict = {"period_end": p["period_end"], "src": {}}
    f["revenue"], _ = _v(p, "TotalRevenue")
    f["net_income"], f["src"]["ni"] = _v(p, "NetIncome", "NetIncomeCommonStockholders", "NetIncomeContinuousOperations")
    ebit, src = _v(p, "EBIT", "OperatingIncome")
    if ebit is None and p.get("PretaxIncome") is not None and p.get("InterestExpense") is not None \
            and not pd.isna(p["PretaxIncome"]) and not pd.isna(p["InterestExpense"]):
        ebit, src = float(p["PretaxIncome"]) + abs(float(p["InterestExpense"])), "PretaxIncome+InterestExpense"
    f["ebit"], f["src"]["ebit"] = ebit, src
    fcf, src = _v(p, "FreeCashFlow")
    if fcf is None:
        ocf, capex = _v(p, "OperatingCashFlow")[0], _v(p, "CapitalExpenditure")[0]
        if ocf is not None and capex is not None:
            fcf, src = ocf - abs(capex), "OperatingCashFlow-CapitalExpenditure"        # Yahoo's capex is negative; the sign is not trusted
    f["fcf"], f["src"]["fcf"] = fcf, src
    f["shares"], f["src"]["shares"] = _v(p, "OrdinarySharesNumber", "ShareIssued", "DilutedAverageShares", "BasicAverageShares")
    total_debt, _ = _v(p, "TotalDebt")
    ltd, src = _v(p, "LongTermDebt", "LongTermDebtAndCapitalLeaseObligation")
    if ltd is None and total_debt is not None:
        cur, _ = _v(p, "CurrentDebt")
        ltd, src = (total_debt - cur, "TotalDebt-CurrentDebt") if cur is not None else (total_debt, "TotalDebt")
    f["ltd"], f["src"]["ltd"] = ltd, src
    ic, src = _v(p, "InvestedCapital")
    if ic is None and total_debt is not None:
        eq, _ = _v(p, "StockholdersEquity", "CommonStockEquity")
        if eq is not None:
            ic, src = total_debt + eq + (_v(p, "MinorityInterest")[0] or 0.0), "TotalDebt+Equity"
    f["invested_capital"], f["src"]["ic"] = ic, src
    f["cash"], _ = _v(p, "CashCashEquivalentsAndShortTermInvestments", "CashAndCashEquivalents")
    f["total_assets"], _ = _v(p, "TotalAssets")
    f["total_liabilities"], _ = _v(p, "TotalLiabilitiesNetMinorityInterest")
    f["equity"], _ = _v(p, "StockholdersEquity", "CommonStockEquity")
    tax, src = _v(p, "TaxRateForCalcs")
    if tax is None or not 0.0 <= tax <= 0.6:
        pre, prov = _v(p, "PretaxIncome")[0], _v(p, "TaxProvision")[0]
        tax, src = ((prov / pre, "TaxProvision/PretaxIncome") if pre and pre > 0 and prov is not None and 0.0 <= prov / pre <= 0.6
                    else (DEFAULT_TAX, "default"))
    f["tax"], f["src"]["tax"] = tax, src
    return f


def derive_symbol(symbol: str, periods: list[dict], profile: dict, rates: Optional[fx_mod.Rates], day: date) -> dict:
    """Pillar inputs for one symbol: {"periods": [derived rows], "metrics": {...pillar_metrics row...}}."""
    rows = [derive_period(p) for p in sorted(periods, key=lambda p: p["period_end"])]
    flags: list[str] = []
    for prev, cur in zip([None] + rows[:-1], rows):
        cur["prior_ic"] = prev["invested_capital"] if prev else None
        ic, prior = cur["invested_capital"], cur["prior_ic"]
        avg_ic = (ic + prior) / 2 if ic is not None and prior is not None else ic
        nopat = cur["ebit"] * (1 - cur["tax"]) if cur["ebit"] is not None else None
        cur["roic"] = nopat / avg_ic if nopat is not None and avg_ic is not None and avg_ic > 0 else None
        ic_x = avg_ic - cur["cash"] if avg_ic is not None and cur["cash"] is not None else None
        cur["roic_ex_cash"] = nopat / ic_x if nopat is not None and ic_x is not None and ic_x > 0 else None
    window = rows[-WINDOW:]
    latest = window[-1] if window else None
    sector = _s(profile.get("sector"))
    financial = sector in FINANCIAL_SECTORS
    price_ccy = _s(profile.get("currency"))
    report_ccy = _s(profile.get("financial_currency"))
    if not report_ccy:
        report_ccy = price_ccy
        flags.append("ccy=assumed_same")
    rate = None
    if price_ccy and report_ccy:
        rate = rates.convert(1.0, report_ccy, price_ccy, day) if rates is not None else None
    fx_missing = rate is None

    def conv(x):
        return None if x is None or rate is None else x * rate

    def mean(key):
        vals = [r[key] for r in window if r[key] is not None]
        return sum(vals) / len(vals) if len(vals) >= MIN_PERIODS else None

    n_periods = len([r for r in window if r["revenue"] is not None])
    if latest is None:
        m = {"symbol": symbol, "periods_used": 0}
    else:
        roic = None if financial else latest["roic"]
        roic_x = None if financial else latest["roic_ex_cash"]
        if financial:
            flags.append("roic=na_financial")
        elif roic is None:
            flags.append("roic=missing_inputs")
        elif abs(roic) > 5:
            flags.append("roic=extreme")
        for key, name in (("ltd", "ltd"), ("tax", "tax"), ("ebit", "ebit"), ("fcf", "fcf"), ("shares", "shares"), ("ic", "ic")):
            s = latest["src"].get(key)
            if s and s not in ("LongTermDebt", "TaxRateForCalcs", "EBIT", "FreeCashFlow", "OrdinarySharesNumber", "InvestedCapital"):
                flags.append(f"{name}={s}")
        if fx_missing:
            flags.append("fx_missing")
        if n_periods < MIN_PERIODS:
            flags.append(f"periods={n_periods}")
        m = {
            "symbol": symbol,
            "avg_net_income_5y": conv(mean("net_income")), "avg_fcf_5y": conv(mean("fcf")),
            "latest_revenue": conv(latest["revenue"]), "latest_ebit": conv(latest["ebit"]),
            "latest_total_assets": conv(latest["total_assets"]), "latest_total_liabilities": conv(latest["total_liabilities"]),
            "latest_long_term_debt": conv(latest["ltd"]), "latest_total_equity": conv(latest["equity"]),
            "latest_roic": roic, "latest_shares_outstanding": latest["shares"],
            "roic_ex_cash": roic_x, "periods_used": n_periods, "financial": bool(financial),
            "report_currency": report_ccy, "price_currency": price_ccy, "fx_rate": rate, "fx_missing": bool(fx_missing),
            "fy_end": latest["period_end"], "flags": ",".join(flags),
        }
    return {"periods": rows, "metrics": m}


# ---------------------------------------------------------------------------------------------- the build
def load_periods(lake_dir: Path | None = None) -> dict[str, list[dict]]:
    """{symbol: [one dict of Yahoo items per fiscal year end]}, from the version table (the newest value of each item and period)."""
    p = yfc.store_dir(lake_dir) / "versions.parquet"
    if not p.exists():
        return {}
    v = pd.read_parquet(p)
    v = v.sort_values("first_seen").drop_duplicates(["symbol", "item", "period_end"], keep="last")           # a restated value wins
    out: dict[str, list[dict]] = {}
    for (sym, pe), g in v.groupby(["symbol", "period_end"]):
        d = {"period_end": pe}
        d.update(dict(zip(g["item"], g["value"])))
        out.setdefault(sym, []).append(d)
    return out


def load_profiles(lake_dir: Path | None = None) -> dict[str, dict]:
    p = yfc.store_dir(lake_dir) / "profile.parquet"
    if not p.exists():
        return {}
    return {r["symbol"]: r for r in pd.read_parquet(p).to_dict("records")}


def _table_rows(symbol: str, market: str, rows: list[dict], currency: str) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {k: [] for k in ("income_statement", "balance_sheet", "cash_flow", "ratios")}
    for r in rows:
        base = {"date": r["period_end"], "symbol": symbol, "market": market, "currency": currency}
        out["income_statement"].append({**base, "revenue": r["revenue"], "net_income": r["net_income"], "ebit": r["ebit"]})
        out["balance_sheet"].append({**base, "total_assets": r["total_assets"], "total_liabilities": r["total_liabilities"],
                                     "long_term_debt": r["ltd"], "total_equity": r["equity"]})
        out["cash_flow"].append({**base, "free_cash_flow": r["fcf"]})
        out["ratios"].append({**base, "roic": r["roic"], "shares_outstanding": r["shares"]})
    return out


def build(lake_dir: Path | None = None, rates: Optional[fx_mod.Rates] = None, day: Optional[date] = None,
          periods: Optional[dict] = None, profiles: Optional[dict] = None) -> dict:
    """Derive the pillar inputs of every stored symbol and write the tables.  Returns a summary."""
    lake_dir = lake_dir or lake_mod.LAKE_DIR
    day = day or datetime.now(timezone.utc).date()
    periods = load_periods(lake_dir) if periods is None else periods
    profiles = load_profiles(lake_dir) if profiles is None else profiles
    rates = fx_mod.load_rates(lake_dir) if rates is None else rates
    tables: dict[str, list[dict]] = {k: [] for k in ("income_statement", "balance_sheet", "cash_flow", "ratios")}
    metrics: list[dict] = []
    summary = {"symbols": 0, "scoreable": 0, "fx_missing": 0, "financial": 0, "roic_missing": 0, "short": 0}
    for sym, plist in sorted(periods.items()):
        prof = profiles.get(sym, {})
        res = derive_symbol(sym, plist, prof, rates, day)
        market = _s(prof.get("market"))
        for k, rows in _table_rows(sym, market, res["periods"], _s(prof.get("financial_currency")) or _s(prof.get("currency"))).items():
            tables[k] += rows
        m = {**res["metrics"], "market": market}
        metrics.append(m)
        summary["symbols"] += 1
        summary["fx_missing"] += int(bool(m.get("fx_missing")))
        summary["financial"] += int(bool(m.get("financial")))
        summary["roic_missing"] += int("roic=missing_inputs" in m.get("flags", ""))
        summary["short"] += int(m.get("periods_used", 0) < MIN_PERIODS)
        summary["scoreable"] += int(m.get("periods_used", 0) >= MIN_PERIODS and not m.get("fx_missing"))
    if not metrics:
        return summary
    base = lake_dir / "fundamentals"
    keys = {"income_statement": ["symbol", "date"], "balance_sheet": ["symbol", "date"], "cash_flow": ["symbol", "date"],
            "ratios": ["symbol", "date"], "pillar_metrics": ["symbol"]}
    for name, rows in {**tables, "pillar_metrics": metrics}.items():
        df = pd.DataFrame(rows)
        if "date" in df:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        con = duckdb.connect()                                         # one connection per table: _upsert leaves its last result open
        con.register("new_rows", df)
        lake_mod._upsert(con, base / f"{name}.parquet", "SELECT * FROM new_rows", keys[name], keys[name])
        con.close()
    return summary


# ---------------------------------------------------------------------------------------------- command line
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Pillar inputs from the Yahoo statements")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("build", help="derive ROIC, debt, FCF, shares in the price's currency and write the tables the scorer reads")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    s = build()
    print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} yahoo pillars: {s['symbols']} symbols, {s['scoreable']} scoreable "
          f"({s['short']} with fewer than {MIN_PERIODS} statements, {s['fx_missing']} without an FX rate, {s['roic_missing']} without ROIC "
          f"inputs, {s['financial']} financials with ROIC not applicable)")
    return 0 if s["symbols"] else 1


if __name__ == "__main__":
    sys.exit(main())
