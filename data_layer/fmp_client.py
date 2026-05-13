"""
FMP (Financial Modeling Prep) data extraction layer for JARVIS — fundamentals only.

Pulls 5 years of annual fundamental data (income statement, balance sheet,
cash flow, ratios) for Nifty 50 (market="IN") and S&P 500 (market="US").

OHLC price data is handled separately by yf_client.py (Yahoo Finance, free).

Identifiers:
  Indian stocks : Yahoo-Finance/RIC style  (e.g. RELIANCE.NS, INFY.NS)
  US stocks     : Plain exchange ticker    (e.g. AAPL, MSFT)
Both stored in `symbol`; `market` column distinguishes origin.

FMP free tier: 250 requests/day.
  250 stocks × 4 fundamental endpoints = 1,000 calls → spread over 4 days.
  Run monthly; annual data rarely changes more frequently.

Usage:
    python -m data_layer.fmp_client --market IN     # Nifty 50 fundamentals
    python -m data_layer.fmp_client --market US     # S&P 500 fundamentals
    python -m data_layer.fmp_client --market ALL    # both
    python -m data_layer.fmp_client --dry-run       # print plan, no API calls

    # Batch mode: run a slice of tickers (to stay within 250-call daily limit)
    python -m data_layer.fmp_client --market US --slice 0 62   # first 63 tickers
    python -m data_layer.fmp_client --market US --slice 63 125  # next 63
"""

from __future__ import annotations

import csv
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
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

# Minimal fallback used only when --dry-run is passed without an API key.
# The live list is fetched from FMP's /sp500_constituent endpoint.
_SP500_FALLBACK: list[str] = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "BRK-B", "TSLA",
    "UNH",  "LLY",  "JPM",  "XOM",  "V",     "AVGO", "PG",    "MA",
    "HD",   "COST", "MRK",  "CVX",
]

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL         = "https://financialmodelingprep.com/api/v3"
YEARS_BACK       = 5
RATE_LIMIT_DELAY = float(os.getenv("FMP_RATE_LIMIT_DELAY", "0.5"))
MAX_RETRIES      = 3
RETRY_BACKOFF    = 2.0

LOG_DIR  = Path("logs")
DATA_DIR = Path("data")
FUND_DIR = DATA_DIR / "fundamentals"

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
# Domain dataclasses — one row per ticker per fiscal year
# Each carries `market` so both universes share the same KDB+ tables.
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
                    log.warning("FMP server error %d — sleeping %.1f s", resp.status_code, wait)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp.json()

            except requests.RequestException as exc:
                wait = RETRY_BACKOFF ** attempt
                log.error("Request error attempt %d: %s — retry in %.1f s", attempt, exc, wait)
                time.sleep(wait)

        log.error("Exhausted retries for %s", url)
        return []


# ---------------------------------------------------------------------------
# Extractor — fundamentals only
# ---------------------------------------------------------------------------
class FMPExtractor:
    """Pulls fundamental data for a list of tickers. OHLC → use yf_client.py."""

    def __init__(self, api_key: str) -> None:
        self._sess = FMPSession(api_key)

    def fetch_sp500_constituents(self) -> list[str]:
        """Return current S&P 500 ticker list from FMP's /sp500_constituent (1 API call)."""
        log.info("Fetching S&P 500 constituent list from FMP …")
        data = self._sess.get("sp500_constituent")
        if not isinstance(data, list) or not data:
            log.warning("Failed to fetch S&P 500 constituents — using fallback list")
            return list(_SP500_FALLBACK)
        tickers = [rec["symbol"] for rec in data if rec.get("symbol")]
        log.info("S&P 500: %d constituents loaded", len(tickers))
        return tickers

    def income_statement(self, ticker: str, market: str) -> list[IncomeRow]:
        data = self._sess.get(f"income-statement/{ticker}", period="annual", limit=YEARS_BACK)
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
        data = self._sess.get(f"balance-sheet-statement/{ticker}", period="annual", limit=YEARS_BACK)
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
        data = self._sess.get(f"cash-flow-statement/{ticker}", period="annual", limit=YEARS_BACK)
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
        data = self._sess.get(f"ratios/{ticker}", period="annual", limit=YEARS_BACK)
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

    def fetch_ticker(self, ticker: str, market: str) -> dict[str, list]:
        """Fetch all 4 fundamental datasets for one ticker (4 API calls)."""
        log.info("Fetching fundamentals: %s [%s]", ticker, market)
        return {
            "income":    self.income_statement(ticker, market),
            "balance":   self.balance_sheet(ticker, market),
            "cash_flow": self.cash_flow(ticker, market),
            "ratios":    self.ratios(ticker, market),
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
    market: str,
) -> PillarMetrics:
    """
    Pre-aggregate per-ticker values consumed by the 8 Pillar scoring model.

    Pillar 1 → 5yr avg P/E < 22.5       uses avg_net_income_5y
    Pillar 6 → LT Debt / avg FCF < 5    uses latest_long_term_debt + avg_fcf_5y
    Pillar 8 → 5yr avg P/FCF < 20       uses avg_fcf_5y
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
# Column order: date, symbol, market, <floats>
# Files are market-specific so each universe can be refreshed independently:
#   data/fundamentals/income_statement_in.csv
#   data/fundamentals/income_statement_us.csv
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


def write_pillar_metrics(metrics: list[PillarMetrics], market: str) -> None:
    FUND_DIR.mkdir(parents=True, exist_ok=True)
    _write_csv(
        FUND_DIR / f"pillar_metrics_{market.lower()}.csv",
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
    log.info("Pillar metrics written (market=%s)", market)


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
# Core run function — fundamentals only, no OHLC
# ---------------------------------------------------------------------------
def _resolve_universe(market: str, api_key: str, dry_run: bool,
                      tickers: list[str] | None) -> list[str]:
    if tickers:
        return tickers
    if market == "IN":
        return NIFTY_50
    if dry_run:
        return list(_SP500_FALLBACK)
    return FMPExtractor(api_key).fetch_sp500_constituents()


def run(
    tickers: list[str] | None = None,
    market: str = "IN",
    dry_run: bool = False,
    slice_start: int | None = None,
    slice_end: int | None = None,
) -> None:
    """
    Fetch and persist fundamentals for a market universe.

    OHLC is NOT fetched here — use yf_client.py for that.

    FMP free tier: 250 calls/day, 4 calls/ticker.
    At 250 stocks, a full refresh takes 1,000 calls → spread over 4 days using
    --slice to batch:  --slice 0 62  / --slice 63 125  / --slice 126 188  / --slice 189 249

    Args:
        tickers     : Override ticker list.
        market      : "IN" | "US" | "ALL"
        dry_run     : Print plan without API calls.
        slice_start : First ticker index (inclusive) for batched runs.
        slice_end   : Last ticker index (inclusive) for batched runs.
    """
    if market == "ALL":
        run(tickers=tickers, market="IN", dry_run=dry_run,
            slice_start=slice_start, slice_end=slice_end)
        run(tickers=tickers, market="US", dry_run=dry_run,
            slice_start=slice_start, slice_end=slice_end)
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

    # Apply slice for batched daily runs
    if slice_start is not None or slice_end is not None:
        s = slice_start or 0
        e = (slice_end + 1) if slice_end is not None else len(universe)
        universe = universe[s:e]
        log.info("Slice [%d:%d] → %d tickers", s, e - 1, len(universe))

    if dry_run:
        calls = len(universe) * 4
        log.info("DRY RUN [%s] — %d tickers × 4 endpoints = %d FMP calls", market, len(universe), calls)
        log.info("Free tier (250/day): need %d days for full refresh", -(-calls // 250))
        log.info("Estimated time at %.1f s/call: %.0f s (~%.0f min)",
                 RATE_LIMIT_DELAY, calls * RATE_LIMIT_DELAY, calls * RATE_LIMIT_DELAY / 60)
        return

    extractor = FMPExtractor(api_key)

    all_income:    list[IncomeRow]     = []
    all_balance:   list[BalanceRow]    = []
    all_cash_flow: list[CashFlowRow]   = []
    all_ratios:    list[RatiosRow]     = []
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

        pillars = compute_pillar_metrics(
            data["income"], data["balance"], data["cash_flow"], data["ratios"],
            ticker, market,
        )
        all_pillars.append(pillars)

    write_fundamentals(all_income, all_balance, all_cash_flow, all_ratios, market)
    write_pillar_metrics(all_pillars, market)
    log.info("Done [%s]. %d tickers | %d income rows", market, total, len(all_income))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="JARVIS FMP fundamentals extractor (OHLC → use yf_client.py)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without API calls")
    parser.add_argument("--market", default="IN", choices=["IN", "US", "ALL"],
                        help="Market universe: IN (Nifty 50), US (S&P 500), ALL (both)")
    parser.add_argument("--slice", nargs=2, type=int, metavar=("START", "END"),
                        help=(
                            "Run only tickers[START:END+1] to stay within 250 FMP calls/day. "
                            "Example: --slice 0 62  (first 63 tickers)"
                        ))
    parser.add_argument("--tickers", nargs="*", default=None,
                        help="Override ticker list")
    args = parser.parse_args()

    slice_start, slice_end = (args.slice if args.slice else (None, None))
    run(tickers=args.tickers, market=args.market, dry_run=args.dry_run,
        slice_start=slice_start, slice_end=slice_end)
