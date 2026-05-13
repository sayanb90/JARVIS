"""
FMP (Financial Modeling Prep) data extraction layer for JARVIS.

Pulls 5 years of fundamental + OHLC data for Nifty 50 (Indian, market="IN")
and S&P 500 (US, market="US") universes, writing kdb+-compatible CSVs.
Each row carries a `market` tag so both universes share the same KDB+ tables.

Identifiers:
  Indian stocks : Yahoo-Finance/RIC style  (e.g. RELIANCE.NS, INFY.NS)
  US stocks     : Plain exchange ticker    (e.g. AAPL, MSFT)
Both are stored in the `symbol` column; `market` distinguishes origin.

Rate limits respected:
  Free tier   : ~250 requests / day  → FMP_RATE_LIMIT_DELAY = 0.5 s
  Starter tier: 300 req / min        → FMP_RATE_LIMIT_DELAY = 0.2 s

Usage:
    python -m data_layer.fmp_client                 # Indian stocks (default)
    python -m data_layer.fmp_client --market US     # S&P 500
    python -m data_layer.fmp_client --market ALL    # both universes
    python -m data_layer.fmp_client --dry-run       # print plan, no API calls
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

# S&P 500 constituents are fetched live from the FMP API via
# FMPExtractor.fetch_sp500_constituents().  The list below is a minimal
# fallback used only when --dry-run is requested without an API key.
_SP500_FALLBACK: list[str] = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "BRK-B", "TSLA",
    "UNH",  "LLY",  "JPM",  "XOM",  "V",     "AVGO", "PG",    "MA",
    "HD",   "COST", "MRK",  "CVX",
]

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL           = "https://financialmodelingprep.com/api/v3"
YEARS_BACK         = 5
RATE_LIMIT_DELAY   = float(os.getenv("FMP_RATE_LIMIT_DELAY", "0.5"))
MAX_RETRIES        = 3
RETRY_BACKOFF      = 2.0
# Number of calendar days to look back when running in daily (incremental) mode.
# Set to 5 to safely capture Mon after a long weekend.
DAILY_OHLC_DAYS    = int(os.getenv("FMP_DAILY_OHLC_DAYS", "5"))

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
# Domain dataclasses
# Each row carries `market` ("IN" or "US") so both universes share one table.
# ---------------------------------------------------------------------------
@dataclass
class IncomeRow:
    date: str
    symbol: str
    market: str
    revenue: Optional[float]
    net_income: Optional[float]
    ebit: Optional[float]


@dataclass
class BalanceRow:
    date: str
    symbol: str
    market: str
    total_assets: Optional[float]
    total_liabilities: Optional[float]
    long_term_debt: Optional[float]
    total_equity: Optional[float]


@dataclass
class CashFlowRow:
    date: str
    symbol: str
    market: str
    free_cash_flow: Optional[float]


@dataclass
class RatiosRow:
    date: str
    symbol: str
    market: str
    roic: Optional[float]
    shares_outstanding: Optional[float]


@dataclass
class OHLCRow:
    date: str
    symbol: str
    market: str
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[int]


@dataclass
class PillarMetrics:
    """Pre-calculated aggregates that feed the 8 Pillar scoring model."""
    symbol: str
    market: str
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
        """GET an FMP endpoint, returning parsed JSON. Retries on 429/5xx."""
        params["apikey"] = self._key
        url = f"{BASE_URL}/{endpoint}"

        for attempt in range(1, MAX_RETRIES + 1):
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
        self._sess    = FMPSession(api_key)
        self._from_dt = (date.today() - timedelta(days=365 * YEARS_BACK)).isoformat()
        self._to_dt   = date.today().isoformat()

    # ------------------------------------------------------------------
    # S&P 500 constituent list  (fetched from FMP; refreshes on each run)
    # ------------------------------------------------------------------
    def fetch_sp500_constituents(self) -> list[str]:
        """Return current S&P 500 ticker list from FMP's /sp500_constituent."""
        log.info("Fetching S&P 500 constituent list from FMP …")
        data = self._sess.get("sp500_constituent")
        if not isinstance(data, list) or not data:
            log.warning("Failed to fetch S&P 500 constituents — using fallback list")
            return list(_SP500_FALLBACK)
        tickers = [rec["symbol"] for rec in data if rec.get("symbol")]
        log.info("S&P 500: %d constituents loaded", len(tickers))
        return tickers

    # ------------------------------------------------------------------
    # Fundamental endpoints  (each method accepts a market tag)
    # ------------------------------------------------------------------
    def income_statement(self, ticker: str, market: str) -> list[IncomeRow]:
        data = self._sess.get(
            f"income-statement/{ticker}",
            period="annual", limit=YEARS_BACK,
        )
        rows = []
        for rec in (data if isinstance(data, list) else []):
            rows.append(IncomeRow(
                date       = _kdb_date(rec.get("date", "")),
                symbol     = ticker,
                market     = market,
                revenue    = _f(rec.get("revenue")),
                net_income = _f(rec.get("netIncome")),
                ebit       = _f(rec.get("ebitda")),
            ))
        return rows

    def balance_sheet(self, ticker: str, market: str) -> list[BalanceRow]:
        data = self._sess.get(
            f"balance-sheet-statement/{ticker}",
            period="annual", limit=YEARS_BACK,
        )
        rows = []
        for rec in (data if isinstance(data, list) else []):
            rows.append(BalanceRow(
                date              = _kdb_date(rec.get("date", "")),
                symbol            = ticker,
                market            = market,
                total_assets      = _f(rec.get("totalAssets")),
                total_liabilities = _f(rec.get("totalLiabilities")),
                long_term_debt    = _f(rec.get("longTermDebt")),
                total_equity      = _f(rec.get("totalStockholdersEquity")),
            ))
        return rows

    def cash_flow(self, ticker: str, market: str) -> list[CashFlowRow]:
        data = self._sess.get(
            f"cash-flow-statement/{ticker}",
            period="annual", limit=YEARS_BACK,
        )
        rows = []
        for rec in (data if isinstance(data, list) else []):
            rows.append(CashFlowRow(
                date           = _kdb_date(rec.get("date", "")),
                symbol         = ticker,
                market         = market,
                free_cash_flow = _f(rec.get("freeCashFlow")),
            ))
        return rows

    def ratios(self, ticker: str, market: str) -> list[RatiosRow]:
        data = self._sess.get(
            f"ratios/{ticker}",
            period="annual", limit=YEARS_BACK,
        )
        rows = []
        for rec in (data if isinstance(data, list) else []):
            rows.append(RatiosRow(
                date               = _kdb_date(rec.get("date", "")),
                symbol             = ticker,
                market             = market,
                roic               = _f(rec.get("roic")),  # true ROIC (NOPAT/InvestedCapital)
                shares_outstanding = _f(rec.get("weightedAverageSharesDiluted")),
            ))
        return rows

    def ohlc(self, ticker: str, market: str) -> list[OHLCRow]:
        """Full historical OHLC — YEARS_BACK years. Used in full-refresh mode."""
        data = self._sess.get(
            f"historical-price-full/{ticker}",
            **{"from": self._from_dt, "to": self._to_dt},
        )
        return self._parse_ohlc(data, ticker, market)

    def ohlc_incremental(self, ticker: str, market: str,
                         days_back: int = DAILY_OHLC_DAYS) -> list[OHLCRow]:
        """Recent OHLC only — used in daily (incremental) mode to minimise API calls."""
        from_dt = (date.today() - timedelta(days=days_back)).isoformat()
        data = self._sess.get(
            f"historical-price-full/{ticker}",
            **{"from": from_dt, "to": self._to_dt},
        )
        return self._parse_ohlc(data, ticker, market)

    def _parse_ohlc(self, data: Any, ticker: str, market: str) -> list[OHLCRow]:
        historical = data.get("historical", []) if isinstance(data, dict) else []
        rows = []
        for rec in historical:
            rows.append(OHLCRow(
                date   = _kdb_date(rec.get("date", "")),
                symbol = ticker,
                market = market,
                open   = _f(rec.get("open")),
                high   = _f(rec.get("high")),
                low    = _f(rec.get("low")),
                close  = _f(rec.get("close")),
                volume = _int(rec.get("volume")),
            ))
        return rows

    def fetch_ticker(self, ticker: str, market: str) -> dict[str, list]:
        """Full refresh — all 5 endpoints."""
        log.info("Fetching %s [%s] …", ticker, market)
        return {
            "income":    self.income_statement(ticker, market),
            "balance":   self.balance_sheet(ticker, market),
            "cash_flow": self.cash_flow(ticker, market),
            "ratios":    self.ratios(ticker, market),
            "ohlc":      self.ohlc(ticker, market),
        }

    def fetch_ticker_ohlc(self, ticker: str, market: str,
                          days_back: int = DAILY_OHLC_DAYS) -> list[OHLCRow]:
        """OHLC only — used in daily incremental mode (1 API call per ticker)."""
        log.debug("OHLC [%s] %s …", market, ticker)
        return self.ohlc_incremental(ticker, market, days_back)


# ---------------------------------------------------------------------------
# Pillar metric calculations
# ---------------------------------------------------------------------------
def compute_pillar_metrics(
    income_rows:    list[IncomeRow],
    balance_rows:   list[BalanceRow],
    cash_flow_rows: list[CashFlowRow],
    ratios_rows:    list[RatiosRow],
    symbol: str,
    market: str,
) -> PillarMetrics:
    """
    Pre-aggregate per-ticker values consumed by the 8 Pillar model.

    Pillar 1  → P/E < 22.5        uses avg_net_income_5y
    Pillar 3  → Buyback proxy     uses latest_shares_outstanding trend
    Pillar 8  → P/FCF < 22.5      uses avg_fcf_5y
    """
    net_incomes = [r.net_income for r in income_rows if r.net_income is not None]
    fcfs        = [r.free_cash_flow for r in cash_flow_rows if r.free_cash_flow is not None]

    latest_income  = income_rows[0]  if income_rows  else None
    latest_balance = balance_rows[0] if balance_rows else None
    latest_ratios  = ratios_rows[0]  if ratios_rows  else None

    return PillarMetrics(
        symbol                   = symbol,
        market                   = market,
        avg_net_income_5y        = _avg(net_incomes),
        avg_fcf_5y               = _avg(fcfs),
        latest_revenue           = latest_income.revenue           if latest_income  else None,
        latest_ebit              = latest_income.ebit              if latest_income  else None,
        latest_total_assets      = latest_balance.total_assets      if latest_balance else None,
        latest_total_liabilities = latest_balance.total_liabilities if latest_balance else None,
        latest_long_term_debt    = latest_balance.long_term_debt    if latest_balance else None,
        latest_total_equity      = latest_balance.total_equity      if latest_balance else None,
        latest_roic              = latest_ratios.roic               if latest_ratios  else None,
        latest_shares_outstanding= latest_ratios.shares_outstanding if latest_ratios  else None,
    )


# ---------------------------------------------------------------------------
# CSV writers — kdb+-compatible format
#
# Column order (fundamentals): date, symbol, market, <floats>
# Column order (ohlc):         date, symbol, market, open, high, low, close, volume
# Column order (pillar):       symbol, market, <floats>
#
# Files are written per-market so each universe can be refreshed independently:
#   data/fundamentals/income_statement_in.csv   (Indian stocks)
#   data/fundamentals/income_statement_us.csv   (S&P 500)
#   data/ohlc/ohlc_YYYY_in.csv
#   data/ohlc/ohlc_YYYY_us.csv
# The KDB+ loader merges both files into unified splayed tables.
# ---------------------------------------------------------------------------
def write_fundamentals(
    all_income:    list[IncomeRow],
    all_balance:   list[BalanceRow],
    all_cash_flow: list[CashFlowRow],
    all_ratios:    list[RatiosRow],
    market: str,
) -> None:
    FUND_DIR.mkdir(parents=True, exist_ok=True)
    suffix = market.lower()

    _write_csv(
        FUND_DIR / f"income_statement_{suffix}.csv",
        ["date", "symbol", "market", "revenue", "net_income", "ebit"],
        ([r.date, r.symbol, r.market, r.revenue, r.net_income, r.ebit]
         for r in all_income),
    )
    _write_csv(
        FUND_DIR / f"balance_sheet_{suffix}.csv",
        ["date", "symbol", "market", "total_assets", "total_liabilities",
         "long_term_debt", "total_equity"],
        ([r.date, r.symbol, r.market, r.total_assets, r.total_liabilities,
          r.long_term_debt, r.total_equity] for r in all_balance),
    )
    _write_csv(
        FUND_DIR / f"cash_flow_{suffix}.csv",
        ["date", "symbol", "market", "free_cash_flow"],
        ([r.date, r.symbol, r.market, r.free_cash_flow] for r in all_cash_flow),
    )
    _write_csv(
        FUND_DIR / f"ratios_{suffix}.csv",
        ["date", "symbol", "market", "roic", "shares_outstanding"],
        ([r.date, r.symbol, r.market, r.roic, r.shares_outstanding]
         for r in all_ratios),
    )
    log.info("Fundamentals CSVs written to %s (market=%s)", FUND_DIR, market)


def write_ohlc(all_ohlc: list[OHLCRow], market: str) -> None:
    """
    Writes one CSV per calendar year per market, mirroring kdb+ partition layout.
    Files: data/ohlc/ohlc_YYYY_in.csv, data/ohlc/ohlc_YYYY_us.csv
    """
    OHLC_DIR.mkdir(parents=True, exist_ok=True)
    suffix = market.lower()

    by_year: dict[str, list[OHLCRow]] = {}
    for row in all_ohlc:
        year = row.date[:4]
        by_year.setdefault(year, []).append(row)

    for year, rows in sorted(by_year.items()):
        _write_csv(
            OHLC_DIR / f"ohlc_{year}_{suffix}.csv",
            ["date", "symbol", "market", "open", "high", "low", "close", "volume"],
            ([r.date, r.symbol, r.market, r.open, r.high, r.low, r.close, r.volume]
             for r in rows),
        )
    log.info("OHLC CSVs written to %s (%d year-partitions, market=%s)",
             OHLC_DIR, len(by_year), market)


def write_ohlc_daily(all_ohlc: list[OHLCRow], market: str) -> None:
    """
    Write incremental OHLC to a flat file for daily appends.

    Unlike write_ohlc() which splits by year, this writes a single file:
      data/ohlc/ohlc_daily_<market>.csv
    The KDB+ appendOhlcDaily[] loader reads this and appends to the correct
    year partitions, deduplicating on date+symbol.
    """
    OHLC_DIR.mkdir(parents=True, exist_ok=True)
    suffix = market.lower()
    _write_csv(
        OHLC_DIR / f"ohlc_daily_{suffix}.csv",
        ["date", "symbol", "market", "open", "high", "low", "close", "volume"],
        ([r.date, r.symbol, r.market, r.open, r.high, r.low, r.close, r.volume]
         for r in all_ohlc),
    )
    log.info("Daily OHLC written to data/ohlc/ohlc_daily_%s.csv (%d rows)", suffix, len(all_ohlc))


def write_pillar_metrics(metrics: list[PillarMetrics], market: str) -> None:
    FUND_DIR.mkdir(parents=True, exist_ok=True)
    suffix = market.lower()
    _write_csv(
        FUND_DIR / f"pillar_metrics_{suffix}.csv",
        [
            "symbol", "market",
            "avg_net_income_5y", "avg_fcf_5y",
            "latest_revenue", "latest_ebit",
            "latest_total_assets", "latest_total_liabilities",
            "latest_long_term_debt", "latest_total_equity",
            "latest_roic", "latest_shares_outstanding",
        ],
        (
            [
                m.symbol, m.market,
                m.avg_net_income_5y, m.avg_fcf_5y,
                m.latest_revenue, m.latest_ebit,
                m.latest_total_assets, m.latest_total_liabilities,
                m.latest_long_term_debt, m.latest_total_equity,
                m.latest_roic, m.latest_shares_outstanding,
            ]
            for m in metrics
        ),
    )
    log.info("Pillar metrics written to %s (market=%s)", FUND_DIR, market)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _f(val: Any) -> Optional[float]:
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
    return iso.replace("-", ".") if iso else ""


def _write_csv(path: Path, headers: list[str], rows: Iterator) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(["" if v is None else v for v in row])


# ---------------------------------------------------------------------------
# Core run functions
# ---------------------------------------------------------------------------
def _resolve_universe(market: str, api_key: str, dry_run: bool,
                      tickers: list[str] | None) -> list[str]:
    if tickers:
        return tickers
    if market == "IN":
        return NIFTY_50
    # US — fetch live or fallback
    if dry_run:
        log.info("DRY RUN — using %d-ticker S&P 500 fallback list", len(_SP500_FALLBACK))
        return list(_SP500_FALLBACK)
    return FMPExtractor(api_key).fetch_sp500_constituents()


def run(
    tickers: list[str] | None = None,
    market: str = "IN",
    dry_run: bool = False,
) -> None:
    """
    Full refresh — fundamentals + 5-year OHLC for all tickers in a market.
    Run weekly/quarterly; fundamentals are annual data that changes rarely.

    API calls: ~5 per ticker  (income, balance, cashflow, ratios, ohlc)
    At 550 tickers: ~2,750 calls — ~9 min on FMP Starter (300 req/min)
    At 1,000 tickers: ~5,000 calls — ~17 min on FMP Starter

    Args:
        tickers : Override ticker list.
        market  : "IN" | "US" | "ALL"
        dry_run : Print plan without API calls.
    """
    if market == "ALL":
        run(tickers=tickers, market="IN", dry_run=dry_run)
        run(tickers=tickers, market="US", dry_run=dry_run)
        return

    if market not in ("IN", "US"):
        log.error("Invalid --market '%s'. Choose IN, US, or ALL.", market)
        sys.exit(1)

    api_key = os.getenv("FMP_API_KEY", "")
    if not api_key and not dry_run:
        log.error("FMP_API_KEY not set. Copy .env.example → .env and add your key.")
        sys.exit(1)

    LOG_DIR.mkdir(exist_ok=True)
    universe = _resolve_universe(market, api_key, dry_run, tickers)

    if dry_run:
        calls = len(universe) * 5
        log.info("DRY RUN [%s] — %d tickers × 5 endpoints = %d API calls", market, len(universe), calls)
        log.info("Estimated time at %.1f s/call: %.0f s (~%.0f min)",
                 RATE_LIMIT_DELAY, calls * RATE_LIMIT_DELAY, calls * RATE_LIMIT_DELAY / 60)
        return

    extractor = FMPExtractor(api_key)

    all_income:    list[IncomeRow]     = []
    all_balance:   list[BalanceRow]    = []
    all_cash_flow: list[CashFlowRow]   = []
    all_ratios:    list[RatiosRow]     = []
    all_ohlc:      list[OHLCRow]       = []
    all_pillars:   list[PillarMetrics] = []

    total = len(universe)
    for idx, ticker in enumerate(universe, 1):
        log.info("[%d/%d] %s [%s]", idx, total, ticker, market)
        try:
            data = extractor.fetch_ticker(ticker, market)
        except Exception as exc:
            log.error("Skipping %s — %s", ticker, exc)
            continue

        all_income.extend(data["income"])
        all_balance.extend(data["balance"])
        all_cash_flow.extend(data["cash_flow"])
        all_ratios.extend(data["ratios"])
        all_ohlc.extend(data["ohlc"])

        pillars = compute_pillar_metrics(
            data["income"], data["balance"], data["cash_flow"], data["ratios"],
            ticker, market,
        )
        all_pillars.append(pillars)
        log.info("  %s → 5y avg NI=%.2f M  5y avg FCF=%.2f M",
                 ticker, (pillars.avg_net_income_5y or 0) / 1e6,
                 (pillars.avg_fcf_5y or 0) / 1e6)

    write_fundamentals(all_income, all_balance, all_cash_flow, all_ratios, market)
    write_ohlc(all_ohlc, market)
    write_pillar_metrics(all_pillars, market)
    log.info("Done [%s]. %d tickers | %d income rows | %d OHLC rows",
             market, total, len(all_income), len(all_ohlc))


def run_daily(
    tickers: list[str] | None = None,
    market: str = "ALL",
    days_back: int = DAILY_OHLC_DAYS,
    dry_run: bool = False,
) -> None:
    """
    Daily incremental OHLC only — 1 API call per ticker, no fundamentals.

    Run every trading day via cron/scheduler.  After this completes, run
    the KDB+ loader in daily mode to append the new rows:
      q data_layer/kdb_loader.q -mode daily

    API calls: 1 per ticker
    At 550 tickers: ~550 calls — ~2 min on FMP Starter
    At 1,000 tickers: ~1,000 calls — ~3 min on FMP Starter

    Args:
        tickers  : Override ticker list.
        market   : "IN" | "US" | "ALL"
        days_back: Calendar days to look back (default 5 — covers Mon after long weekend).
        dry_run  : Print plan without API calls.
    """
    if market == "ALL":
        run_daily(tickers=tickers, market="IN", days_back=days_back, dry_run=dry_run)
        run_daily(tickers=tickers, market="US", days_back=days_back, dry_run=dry_run)
        return

    if market not in ("IN", "US"):
        log.error("Invalid --market '%s'. Choose IN, US, or ALL.", market)
        sys.exit(1)

    api_key = os.getenv("FMP_API_KEY", "")
    if not api_key and not dry_run:
        log.error("FMP_API_KEY not set.")
        sys.exit(1)

    LOG_DIR.mkdir(exist_ok=True)
    universe = _resolve_universe(market, api_key, dry_run, tickers)

    if dry_run:
        log.info("DRY RUN daily [%s] — %d tickers × 1 OHLC call = %d API calls "
                 "(last %d days)",
                 market, len(universe), len(universe), days_back)
        log.info("Estimated time at %.1f s/call: %.0f s (~%.0f min)",
                 RATE_LIMIT_DELAY, len(universe) * RATE_LIMIT_DELAY,
                 len(universe) * RATE_LIMIT_DELAY / 60)
        return

    extractor = FMPExtractor(api_key)
    all_ohlc: list[OHLCRow] = []
    total = len(universe)

    for idx, ticker in enumerate(universe, 1):
        try:
            rows = extractor.fetch_ticker_ohlc(ticker, market, days_back)
            all_ohlc.extend(rows)
            if idx % 50 == 0:
                log.info("[%d/%d] daily OHLC — %d rows so far", idx, total, len(all_ohlc))
        except Exception as exc:
            log.error("Skipping %s — %s", ticker, exc)

    write_ohlc_daily(all_ohlc, market)
    log.info("Daily done [%s]. %d tickers | %d OHLC rows (last %d days)",
             market, total, len(all_ohlc), days_back)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="JARVIS FMP data extractor")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without API calls")
    parser.add_argument("--market", default="IN",
                        choices=["IN", "US", "ALL"],
                        help="Market universe: IN (Nifty 50), US (S&P 500), ALL (both)")
    parser.add_argument("--mode", default="full",
                        choices=["full", "daily"],
                        help=(
                            "full  = fetch fundamentals + 5yr OHLC (run weekly/quarterly); "
                            "daily = incremental OHLC only, last --days-back days (run every trading day)"
                        ))
    parser.add_argument("--days-back", type=int, default=DAILY_OHLC_DAYS,
                        help=f"Days to look back in daily mode (default: {DAILY_OHLC_DAYS})")
    parser.add_argument("--tickers", nargs="*", default=None,
                        help="Override ticker list (e.g. RELIANCE.NS TCS.NS)")
    args = parser.parse_args()

    if args.mode == "daily":
        run_daily(tickers=args.tickers, market=args.market,
                  days_back=args.days_back, dry_run=args.dry_run)
    else:
        run(tickers=args.tickers, market=args.market, dry_run=args.dry_run)
