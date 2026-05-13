"""
FMP (Financial Modeling Prep) data extraction layer for JARVIS.

Pulls 5 years of fundamental + OHLC data for the Nifty 50 universe and
writes kdb+-compatible CSVs partitioned for the 32-bit (4 GB) engine.

Rate limits respected:
  Free tier  : ~250 requests / day, burst 5 req/s → RATE_LIMIT_DELAY = 0.5 s
  Starter tier: 300 req/min                       → RATE_LIMIT_DELAY = 0.2 s

Usage:
    python -m data_layer.fmp_client            # full run
    python -m data_layer.fmp_client --dry-run  # print plan, no API calls
"""

from __future__ import annotations

import csv
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Nifty 50 universe  (FMP uses Yahoo-style .NS suffix for NSE-listed stocks)
# ---------------------------------------------------------------------------
NIFTY_50: list[str] = [
    "ADANIENT.NS",  "ADANIPORTS.NS", "APOLLOHOSP.NS", "ASIANPAINT.NS",
    "AXISBANK.NS",  "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS",
    "BPCL.NS",      "BHARTIARTL.NS", "BRITANNIA.NS",  "CIPLA.NS",
    "COALINDIA.NS", "DIVISLAB.NS",   "DRREDDY.NS",    "EICHERMOT.NS",
    "GRASIM.NS",    "HCLTECH.NS",    "HDFCBANK.NS",   "HDFCLIFE.NS",
    "HEROMOTOCO.NS","HINDALCO.NS",   "HINDUNILVR.NS", "ICICIBANK.NS",
    "INDUSINDBK.NS","INFY.NS",       "ITC.NS",        "JSWSTEEL.NS",
    "KOTAKBANK.NS", "LT.NS",         "M&M.NS",        "MARUTI.NS",
    "NESTLEIND.NS", "NTPC.NS",       "ONGC.NS",       "POWERGRID.NS",
    "RELIANCE.NS",  "SBILIFE.NS",    "SBIN.NS",       "SHRIRAMFIN.NS",
    "SUNPHARMA.NS", "TATACONSUM.NS", "TATAMOTORS.NS", "TATASTEEL.NS",
    "TCS.NS",       "TECHM.NS",      "TITAN.NS",      "ULTRACEMCO.NS",
    "WIPRO.NS",     "ZOMATO.NS",
]

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL         = "https://financialmodelingprep.com/api/v3"
YEARS_BACK       = 5
RATE_LIMIT_DELAY = float(os.getenv("FMP_RATE_LIMIT_DELAY", "0.5"))  # seconds between calls
MAX_RETRIES      = 3
RETRY_BACKOFF    = 2.0   # exponential base

LOG_DIR  = Path("logs")
DATA_DIR = Path("data")
FUND_DIR = DATA_DIR / "fundamentals"
OHLC_DIR = DATA_DIR / "ohlc"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "fmp_client.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain dataclasses (one row per ticker per fiscal year)
# ---------------------------------------------------------------------------
@dataclass
class IncomeRow:
    date: str
    symbol: str
    revenue: Optional[float]
    net_income: Optional[float]
    ebit: Optional[float]


@dataclass
class BalanceRow:
    date: str
    symbol: str
    total_assets: Optional[float]
    total_liabilities: Optional[float]
    long_term_debt: Optional[float]
    total_equity: Optional[float]


@dataclass
class CashFlowRow:
    date: str
    symbol: str
    free_cash_flow: Optional[float]


@dataclass
class RatiosRow:
    date: str
    symbol: str
    roic: Optional[float]
    shares_outstanding: Optional[float]


@dataclass
class OHLCRow:
    date: str
    symbol: str
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[int]


@dataclass
class PillarMetrics:
    """Pre-calculated aggregates that feed the 8 Pillar scoring model."""
    symbol: str
    avg_net_income_5y: Optional[float]
    avg_fcf_5y: Optional[float]
    latest_revenue: Optional[float]
    latest_ebit: Optional[float]
    latest_total_assets: Optional[float]
    latest_total_liabilities: Optional[float]
    latest_long_term_debt: Optional[float]
    latest_total_equity: Optional[float]
    latest_roic: Optional[float]
    latest_shares_outstanding: Optional[float]


# ---------------------------------------------------------------------------
# Rate-limited HTTP session
# ---------------------------------------------------------------------------
class FMPSession:
    """Thin wrapper around requests.Session with rate limiting + retry."""

    def __init__(self, api_key: str, delay: float = RATE_LIMIT_DELAY) -> None:
        self._key   = api_key
        self._delay = delay
        self._last  = 0.0
        self._sess  = requests.Session()
        self._sess.headers.update({"Accept": "application/json"})

    def get(self, endpoint: str, **params: Any) -> Any:
        """GET an FMP endpoint, returning parsed JSON.  Retries on 429/5xx."""
        params["apikey"] = self._key
        url = f"{BASE_URL}/{endpoint}"

        for attempt in range(1, MAX_RETRIES + 1):
            # respect inter-call gap
            gap = self._delay - (time.monotonic() - self._last)
            if gap > 0:
                time.sleep(gap)

            try:
                resp = self._sess.get(url, params=params, timeout=30)
                self._last = time.monotonic()

                if resp.status_code == 429:
                    wait = RETRY_BACKOFF ** attempt
                    log.warning("Rate-limited by FMP — sleeping %.1f s (attempt %d)", wait, attempt)
                    time.sleep(wait)
                    continue

                if resp.status_code >= 500:
                    wait = RETRY_BACKOFF ** attempt
                    log.warning("FMP server error %d — sleeping %.1f s (attempt %d)",
                                resp.status_code, wait, attempt)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp.json()

            except requests.RequestException as exc:
                wait = RETRY_BACKOFF ** attempt
                log.error("Request error on attempt %d: %s — retry in %.1f s", attempt, exc, wait)
                time.sleep(wait)

        log.error("Exhausted retries for %s", url)
        return []


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------
class FMPExtractor:
    """Pulls fundamental + OHLC data for a list of tickers."""

    def __init__(self, api_key: str) -> None:
        self._sess     = FMPSession(api_key)
        self._from_dt  = (date.today() - timedelta(days=365 * YEARS_BACK)).isoformat()
        self._to_dt    = date.today().isoformat()

    # ------------------------------------------------------------------
    # Fundamental endpoints
    # ------------------------------------------------------------------
    def income_statement(self, ticker: str) -> list[IncomeRow]:
        data = self._sess.get(
            f"income-statement/{ticker}",
            period="annual", limit=YEARS_BACK,
        )
        rows = []
        for rec in (data if isinstance(data, list) else []):
            rows.append(IncomeRow(
                date       = _kdb_date(rec.get("date", "")),
                symbol     = ticker,
                revenue    = _f(rec.get("revenue")),
                net_income = _f(rec.get("netIncome")),
                ebit       = _f(rec.get("ebitda")),  # FMP: operatingIncome ≈ EBIT
            ))
        return rows

    def balance_sheet(self, ticker: str) -> list[BalanceRow]:
        data = self._sess.get(
            f"balance-sheet-statement/{ticker}",
            period="annual", limit=YEARS_BACK,
        )
        rows = []
        for rec in (data if isinstance(data, list) else []):
            rows.append(BalanceRow(
                date              = _kdb_date(rec.get("date", "")),
                symbol            = ticker,
                total_assets      = _f(rec.get("totalAssets")),
                total_liabilities = _f(rec.get("totalLiabilities")),
                long_term_debt    = _f(rec.get("longTermDebt")),
                total_equity      = _f(rec.get("totalStockholdersEquity")),
            ))
        return rows

    def cash_flow(self, ticker: str) -> list[CashFlowRow]:
        data = self._sess.get(
            f"cash-flow-statement/{ticker}",
            period="annual", limit=YEARS_BACK,
        )
        rows = []
        for rec in (data if isinstance(data, list) else []):
            rows.append(CashFlowRow(
                date           = _kdb_date(rec.get("date", "")),
                symbol         = ticker,
                free_cash_flow = _f(rec.get("freeCashFlow")),
            ))
        return rows

    def ratios(self, ticker: str) -> list[RatiosRow]:
        data = self._sess.get(
            f"ratios/{ticker}",
            period="annual", limit=YEARS_BACK,
        )
        rows = []
        for rec in (data if isinstance(data, list) else []):
            rows.append(RatiosRow(
                date               = _kdb_date(rec.get("date", "")),
                symbol             = ticker,
                roic               = _f(rec.get("returnOnCapitalEmployed")),
                shares_outstanding = _f(rec.get("weightedAverageSharesDiluted")),
            ))
        return rows

    # ------------------------------------------------------------------
    # OHLC
    # ------------------------------------------------------------------
    def ohlc(self, ticker: str) -> list[OHLCRow]:
        data = self._sess.get(
            f"historical-price-full/{ticker}",
            **{"from": self._from_dt, "to": self._to_dt},
        )
        historical = data.get("historical", []) if isinstance(data, dict) else []
        rows = []
        for rec in historical:
            rows.append(OHLCRow(
                date   = _kdb_date(rec.get("date", "")),
                symbol = ticker,
                open   = _f(rec.get("open")),
                high   = _f(rec.get("high")),
                low    = _f(rec.get("low")),
                close  = _f(rec.get("close")),
                volume = _int(rec.get("volume")),
            ))
        return rows

    # ------------------------------------------------------------------
    # Full ticker fetch
    # ------------------------------------------------------------------
    def fetch_ticker(self, ticker: str) -> dict[str, list]:
        log.info("Fetching %s …", ticker)
        return {
            "income":    self.income_statement(ticker),
            "balance":   self.balance_sheet(ticker),
            "cash_flow": self.cash_flow(ticker),
            "ratios":    self.ratios(ticker),
            "ohlc":      self.ohlc(ticker),
        }


# ---------------------------------------------------------------------------
# Pillar metric calculations
# ---------------------------------------------------------------------------
def compute_pillar_metrics(
    income_rows:    list[IncomeRow],
    balance_rows:   list[BalanceRow],
    cash_flow_rows: list[CashFlowRow],
    ratios_rows:    list[RatiosRow],
    symbol: str,
) -> PillarMetrics:
    """
    Pre-aggregate per-ticker values consumed by the 8 Pillar model:

    Pillar 1  → P/E < 22.5        uses avg_net_income_5y
    Pillar 3  → Buyback proxy     uses latest_shares_outstanding (trend vs prior years)
    Pillar 8  → P/FCF < 22.5      uses avg_fcf_5y
    """
    net_incomes = [r.net_income for r in income_rows if r.net_income is not None]
    fcfs        = [r.free_cash_flow for r in cash_flow_rows if r.free_cash_flow is not None]

    latest_income  = income_rows[0]  if income_rows  else None
    latest_balance = balance_rows[0] if balance_rows else None
    latest_ratios  = ratios_rows[0]  if ratios_rows  else None

    return PillarMetrics(
        symbol                  = symbol,
        avg_net_income_5y       = _avg(net_incomes),
        avg_fcf_5y              = _avg(fcfs),
        latest_revenue          = latest_income.revenue          if latest_income  else None,
        latest_ebit             = latest_income.ebit             if latest_income  else None,
        latest_total_assets     = latest_balance.total_assets     if latest_balance else None,
        latest_total_liabilities= latest_balance.total_liabilities if latest_balance else None,
        latest_long_term_debt   = latest_balance.long_term_debt   if latest_balance else None,
        latest_total_equity     = latest_balance.total_equity     if latest_balance else None,
        latest_roic             = latest_ratios.roic              if latest_ratios  else None,
        latest_shares_outstanding = latest_ratios.shares_outstanding if latest_ratios else None,
    )


# ---------------------------------------------------------------------------
# CSV writers — kdb+-compatible format
#   • Dates  : YYYY.MM.DD  (kdb+ date literal)
#   • Floats : plain decimal, empty string for null
#   • Symbols: bare string (kdb+ reads unquoted symbol columns)
# ---------------------------------------------------------------------------
def write_fundamentals(
    all_income:    list[IncomeRow],
    all_balance:   list[BalanceRow],
    all_cash_flow: list[CashFlowRow],
    all_ratios:    list[RatiosRow],
) -> None:
    FUND_DIR.mkdir(parents=True, exist_ok=True)

    _write_csv(
        FUND_DIR / "income_statement.csv",
        ["date", "symbol", "revenue", "net_income", "ebit"],
        ([r.date, r.symbol, r.revenue, r.net_income, r.ebit] for r in all_income),
    )
    _write_csv(
        FUND_DIR / "balance_sheet.csv",
        ["date", "symbol", "total_assets", "total_liabilities",
         "long_term_debt", "total_equity"],
        ([r.date, r.symbol, r.total_assets, r.total_liabilities,
          r.long_term_debt, r.total_equity] for r in all_balance),
    )
    _write_csv(
        FUND_DIR / "cash_flow.csv",
        ["date", "symbol", "free_cash_flow"],
        ([r.date, r.symbol, r.free_cash_flow] for r in all_cash_flow),
    )
    _write_csv(
        FUND_DIR / "ratios.csv",
        ["date", "symbol", "roic", "shares_outstanding"],
        ([r.date, r.symbol, r.roic, r.shares_outstanding] for r in all_ratios),
    )
    log.info("Fundamentals CSVs written to %s", FUND_DIR)


def write_ohlc(all_ohlc: list[OHLCRow]) -> None:
    """
    Writes one CSV per calendar year — mirrors the kdb+ date-partition layout
    and keeps individual file sizes well within the 4 GB engine limit.
    """
    OHLC_DIR.mkdir(parents=True, exist_ok=True)

    by_year: dict[str, list[OHLCRow]] = {}
    for row in all_ohlc:
        year = row.date[:4]  # YYYY from YYYY.MM.DD
        by_year.setdefault(year, []).append(row)

    for year, rows in sorted(by_year.items()):
        _write_csv(
            OHLC_DIR / f"ohlc_{year}.csv",
            ["date", "symbol", "open", "high", "low", "close", "volume"],
            ([r.date, r.symbol, r.open, r.high, r.low, r.close, r.volume]
             for r in rows),
        )
    log.info("OHLC CSVs written to %s (%d year-partitions)", OHLC_DIR, len(by_year))


def write_pillar_metrics(metrics: list[PillarMetrics]) -> None:
    FUND_DIR.mkdir(parents=True, exist_ok=True)
    _write_csv(
        FUND_DIR / "pillar_metrics.csv",
        [
            "symbol", "avg_net_income_5y", "avg_fcf_5y",
            "latest_revenue", "latest_ebit",
            "latest_total_assets", "latest_total_liabilities",
            "latest_long_term_debt", "latest_total_equity",
            "latest_roic", "latest_shares_outstanding",
        ],
        (
            [
                m.symbol, m.avg_net_income_5y, m.avg_fcf_5y,
                m.latest_revenue, m.latest_ebit,
                m.latest_total_assets, m.latest_total_liabilities,
                m.latest_long_term_debt, m.latest_total_equity,
                m.latest_roic, m.latest_shares_outstanding,
            ]
            for m in metrics
        ),
    )
    log.info("Pillar metrics written to %s", FUND_DIR / "pillar_metrics.csv")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _f(val: Any) -> Optional[float]:
    """Coerce to float, returning None on null/empty."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _int(val: Any) -> Optional[int]:
    f = _f(val)
    return int(f) if f is not None else None


def _avg(values: list[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def _kdb_date(iso: str) -> str:
    """Convert YYYY-MM-DD → YYYY.MM.DD for kdb+ date literals."""
    return iso.replace("-", ".") if iso else ""


def _write_csv(path: Path, headers: list[str], rows: Iterator) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows:
            # kdb+ treats empty string as null for numeric columns
            writer.writerow(["" if v is None else v for v in row])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def run(tickers: list[str] = NIFTY_50, dry_run: bool = False) -> None:
    api_key = os.getenv("FMP_API_KEY", "")
    if not api_key and not dry_run:
        log.error("FMP_API_KEY not set.  Copy .env.example → .env and add your key.")
        sys.exit(1)

    LOG_DIR.mkdir(exist_ok=True)

    if dry_run:
        log.info("DRY RUN — would fetch %d tickers × 5 endpoints = %d API calls",
                 len(tickers), len(tickers) * 5)
        log.info("Estimated time at %.1f s/call: %.0f s (~%.0f min)",
                 RATE_LIMIT_DELAY,
                 len(tickers) * 5 * RATE_LIMIT_DELAY,
                 len(tickers) * 5 * RATE_LIMIT_DELAY / 60)
        return

    extractor = FMPExtractor(api_key)

    all_income: list[IncomeRow]       = []
    all_balance: list[BalanceRow]     = []
    all_cash_flow: list[CashFlowRow]  = []
    all_ratios: list[RatiosRow]       = []
    all_ohlc: list[OHLCRow]           = []
    all_pillars: list[PillarMetrics]  = []

    total = len(tickers)
    for idx, ticker in enumerate(tickers, 1):
        log.info("[%d/%d] Processing %s", idx, total, ticker)
        try:
            data = extractor.fetch_ticker(ticker)
        except Exception as exc:
            log.error("Skipping %s — unexpected error: %s", ticker, exc)
            continue

        all_income.extend(data["income"])
        all_balance.extend(data["balance"])
        all_cash_flow.extend(data["cash_flow"])
        all_ratios.extend(data["ratios"])
        all_ohlc.extend(data["ohlc"])

        pillars = compute_pillar_metrics(
            data["income"], data["balance"], data["cash_flow"], data["ratios"],
            ticker,
        )
        all_pillars.append(pillars)
        log.info(
            "  %s → 5y avg NI=%.2f Cr  5y avg FCF=%.2f Cr",
            ticker,
            (pillars.avg_net_income_5y or 0) / 1e7,   # raw INR → Crore
            (pillars.avg_fcf_5y or 0) / 1e7,
        )

    log.info("Writing CSVs …")
    write_fundamentals(all_income, all_balance, all_cash_flow, all_ratios)
    write_ohlc(all_ohlc)
    write_pillar_metrics(all_pillars)

    log.info(
        "Done.  %d tickers | %d income rows | %d OHLC rows",
        total, len(all_income), len(all_ohlc),
    )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="JARVIS FMP data extractor")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without API calls")
    parser.add_argument(
        "--tickers", nargs="*", default=None,
        help="Override ticker list (e.g. RELIANCE.NS TCS.NS)",
    )
    args = parser.parse_args()
    run(tickers=args.tickers or NIFTY_50, dry_run=args.dry_run)
