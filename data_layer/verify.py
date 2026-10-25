"""
Data checks with explicit PASS / WARN / FAIL lines, for go-live testing (runbook section 14).

  python -m data_layer.verify ohlc [--years 5]
  python -m data_layer.verify bars --date 2026-10-26 [--interval 1m]

Exit code is 1 if any FAIL.  WARN means look at it, not necessarily wrong.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta

import pandas as pd

from .fmp_client import read_universe
from .lake import BARS_MARKETS, MARKETS
from .query import Lake


class Report:
    def __init__(self) -> None:
        self.fails = 0

    def line(self, level: str, msg: str) -> None:
        if level == "FAIL":
            self.fails += 1
        print(f"[{level:4s}] {msg}")


def verify_ohlc(lake: Lake, years: int = 5, today: date | None = None) -> Report:
    r, today = Report(), today or date.today()
    if "ohlc" not in lake.tables():
        r.line("FAIL", "no ohlc table in the lake (run the loader)")
        return r
    for m in MARKETS:
        uni = {u["symbol"] for u in read_universe(m)}
        df = lake.sql("SELECT symbol, date, open, high, low, close, volume FROM ohlc WHERE market = ?", [m])
        print(f"\n== {m}: {len(uni)} in universe, {df.symbol.nunique()} with data, {len(df)} rows ==")
        if not uni:
            continue
        missing = sorted(uni - set(df.symbol))
        r.line("FAIL" if missing else "PASS", f"{m} symbols with no data: {missing or 'none'}")
        if df.empty:
            continue
        df["date"] = pd.to_datetime(df["date"])
        g = df.groupby("symbol")["date"].agg(["min", "max", "count"])
        want = pd.Timestamp(today - timedelta(days=365 * years - 30))
        short = g[g["min"] > want]
        r.line("WARN" if len(short) else "PASS",
               f"{m} history starting after {want.date()} (young listing or fetch gap): "
               f"{sorted(short.index)[:10] or 'none'}")
        stale = g[g["max"] < pd.Timestamp(today - timedelta(days=7))]
        r.line("WARN" if len(stale) else "PASS", f"{m} last bar older than 7 days: {sorted(stale.index)[:10] or 'none'}")
        dup = int(df.duplicated(["symbol", "date"]).sum())
        r.line("FAIL" if dup else "PASS", f"{m} duplicate (symbol, date) rows: {dup}")
        bad = df[(df.close.isna()) | (df.close <= 0) | (df.high < df.low)]
        r.line("FAIL" if len(bad) else "PASS", f"{m} null/zero close or high<low rows: {len(bad)}")
        df = df.sort_values(["symbol", "date"])
        gap = df.groupby("symbol")["date"].diff().dt.days
        big = df.loc[gap > 10, "symbol"].unique()
        r.line("WARN" if len(big) else "PASS", f"{m} symbols with a >10 day gap inside the history: {sorted(big)[:10] or 'none'}")
        jump = (df.groupby("symbol")["close"].pct_change().abs() > 0.4)
        js = df.loc[jump, ["symbol", "date"]]
        r.line("WARN" if len(js) else "PASS",
               f"{m} one-day moves over 40% (check for unadjusted splits or bad prints): {len(js)}"
               + (f", e.g. {js.head(3).values.tolist()}" if len(js) else ""))
    return r


def verify_bars(lake: Lake, day: date, interval: str = "1m", expected_bars: int | None = None) -> Report:
    r = Report()
    view = f"bars_{interval}"
    if view not in lake.tables():
        r.line("FAIL", f"no {view} table in the lake")
        return r
    exp = expected_bars or {"1m": 375, "5m": 75}.get(interval, 0)
    df = lake.sql(f"SELECT * FROM {view} WHERE CAST(ts AS DATE) = CAST(? AS DATE)", [str(day)])
    uni = {u["symbol"] for u in read_universe(BARS_MARKETS[0])}
    print(f"\n== bars {interval} {day}: {len(df)} rows, {df.symbol.nunique()} symbols ==")
    r.line("FAIL" if df.empty else "PASS", f"rows present for {day}")
    if df.empty:
        return r
    r.line("FAIL" if df.duplicated(["symbol", "ts"]).any() else "PASS",
           f"duplicate (symbol, ts): {int(df.duplicated(['symbol', 'ts']).sum())} (run compact_day if >0 before the first compaction)")
    first, last = df.ts.min(), df.ts.max()
    r.line("PASS" if (first.hour, first.minute) == (3, 45) else "WARN", f"first bar {first} UTC (expect 03:45 = 09:15 IST)")
    r.line("PASS" if (last.hour, last.minute) >= (9, 55) else "WARN", f"last bar {last} UTC (expect about 09:59 = 15:29 IST)")
    sane = df[(df.low > df.open) | (df.low > df.close) | (df.high < df.open) | (df.high < df.close)]
    r.line("FAIL" if len(sane) else "PASS", f"bars where open/close fall outside low-high: {len(sane)}")
    per = df.groupby("symbol").size()
    if exp:
        thin = per[per < exp * 0.5]
        r.line("WARN" if len(thin) else "PASS", f"symbols with under 50% of {exp} bars (illiquid or feed gaps): {sorted(thin.index)[:10] or 'none'}")
    print(f"      bars per symbol: min {per.min()}, median {int(per.median())}, max {per.max()}")
    absent = sorted(uni - set(df.symbol))
    r.line("WARN" if absent else "PASS", f"universe symbols with no bars (no ISIN, not subscribed, or no trades): {absent[:10] or 'none'}")
    if "ohlc" in lake.tables():
        d = lake.sql("SELECT symbol, close dclose, volume dvol FROM ohlc WHERE date = CAST(? AS DATE) AND market = 'IN'", [str(day)])
        agg = df.sort_values("ts").groupby("symbol").agg(bclose=("close", "last"), bvol=("volume", "sum")).reset_index()
        j = agg.merge(d, on="symbol")
        if j.empty:
            r.line("WARN", "no Yahoo daily row for that date yet to cross-check against (load daily OHLC first)")
        else:
            j["close_diff"] = (j.bclose / j.dclose - 1).abs()
            off = j[j.close_diff > 0.005]
            r.line("WARN" if len(off) else "PASS", f"last-bar close vs Yahoo daily close differs >0.5%: {off.symbol.tolist()[:10] or 'none'}")
            j["vol_ratio"] = j.bvol / j.dvol
            print(f"      volume ratio (bars / Yahoo daily): median {j.vol_ratio.median():.2f} "
                  "(about 1 expected; far from 1 means volume deltas or units are off)")
    return r


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    o = sub.add_parser("ohlc"); o.add_argument("--years", type=int, default=5)
    b = sub.add_parser("bars"); b.add_argument("--date", type=date.fromisoformat, required=True)
    b.add_argument("--interval", default="1m")
    a = ap.parse_args()
    lake = Lake()
    rep = verify_ohlc(lake, a.years) if a.cmd == "ohlc" else verify_bars(lake, a.date, a.interval)
    print(f"\n{'FAILED: ' + str(rep.fails) + ' check(s)' if rep.fails else 'no FAIL checks'}")
    sys.exit(1 if rep.fails else 0)


if __name__ == "__main__":
    main()
