"""
Data checks with explicit PASS / WARN / FAIL lines, for go-live testing (runbook section 14).

  python -m data_layer.verify ohlc [--years 5]
  python -m data_layer.verify bars --date 2026-10-26 [--interval 1s]
  python -m data_layer.verify compare-rest --date 2026-10-26      # live 1s bars rolled up vs Upstox 1-minute candles
  python -m data_layer.verify fo --date 2026-10-26                # futures and options bars: open interest, books, coverage

Exit code is 1 if any FAIL.  WARN means look at it, not necessarily wrong.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, time, timedelta

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


SESSION_SECONDS = 22_500          # 09:15-15:30 IST


def verify_fo(lake: Lake, day: date, contracts: pd.DataFrame | None = None, interval: str = "1s") -> Report:
    """
    Checks for futures and options bars on `day`.  `contracts` is the selected-contract table (data/fo_universe.csv);
    without it every symbol that carries open interest is treated as a derivative.
    """
    r = Report()
    view = f"bars_{interval}"
    if view not in lake.tables():
        r.line("FAIL", f"no {view} table in the lake")
        return r
    df = lake.sql(f"SELECT * FROM {view} WHERE CAST(ts AS DATE) = CAST(? AS DATE)", [str(day)])
    if df.empty:
        r.line("FAIL", f"no bars at all for {day}")
        return r
    if contracts is not None:
        df = df[df.symbol.isin(set(contracts["symbol"]))]
    elif "oi" in df.columns:
        df = df[df.groupby("symbol")["oi"].transform(lambda x: x.notna().any())]
    print(f"\n== futures/options bars {interval} {day}: {len(df)} rows, {df.symbol.nunique()} contracts ==")
    r.line("FAIL" if df.empty else "PASS", f"derivative bars present for {day}")
    if df.empty:
        return r
    r.line("FAIL" if df.duplicated(["symbol", "ts"]).any() else "PASS",
           f"duplicate (symbol, ts): {int(df.duplicated(['symbol', 'ts']).sum())}")
    if contracts is not None:
        absent = sorted(set(contracts["symbol"]) - set(df.symbol))
        r.line("WARN" if absent else "PASS",
               f"selected contracts with no bars (far strikes that never traded are normal): {len(absent)}"
               + (f", e.g. {absent[:5]}" if absent else ""))
    if "oi" not in df.columns or df["oi"].isna().all():
        r.line("FAIL", "no open interest on any bar: the feed mode may not carry it, or the field is not decoded")
        return r
    cover = df.groupby("symbol")["oi"].apply(lambda x: x.notna().mean())
    low = sorted(cover[cover < 0.9].index)
    r.line("WARN" if low else "PASS", f"contracts with open interest on under 90% of their bars: {low[:8] or 'none'}")
    print(f"      open interest coverage per contract: min {cover.min():.0%}, median {cover.median():.0%}")
    neg = df[df["oi"] < 0]
    r.line("FAIL" if len(neg) else "PASS", f"negative open interest rows: {len(neg)}")
    fut = df.sort_values("ts").groupby("symbol").agg(first=("oi", "first"), last=("oi", "last"))
    flat = sorted(fut[fut["first"] == fut["last"]].index)
    r.line("WARN" if len(flat) > len(fut) * 0.9 else "PASS",
           f"contracts whose open interest never changed all day: {len(flat)} of {len(fut)} "
           "(on liquid contracts it should move; most flat means OI is not being refreshed)")
    book = df[["bid", "ask"]].notna().all(axis=1)
    r.line("WARN" if book.mean() < 0.5 else "PASS", f"bars with a book snapshot: {book.mean():.0%}")
    crossed = df[(df.ask < df.bid) & book]
    r.line("WARN" if len(crossed) else "PASS", f"crossed books: {len(crossed)}")
    sane = df[(df.low > df.open) | (df.low > df.close) | (df.high < df.open) | (df.high < df.close)]
    r.line("FAIL" if len(sane) else "PASS", f"bars where open/close fall outside low-high: {len(sane)}")
    return r


def verify_bars(lake: Lake, day: date, interval: str = "1s", expected_bars: int | None = None) -> Report:
    r = Report()
    view = f"bars_{interval}"
    if view not in lake.tables():
        r.line("FAIL", f"no {view} table in the lake")
        return r
    fine = interval.endswith("s")                      # sub-minute bars: a quiet second simply has no row
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
    if fine:
        r.line("PASS" if first.time() < time(3, 45, 30) else "WARN", f"first bar {first} UTC (expect within 30 s of 03:45 = 09:15 IST)")
        r.line("PASS" if last.time() >= time(9, 59, 0) else "WARN", f"last bar {last} UTC (expect in the final minute, before 10:00 = 15:30 IST)")
    else:
        r.line("PASS" if (first.hour, first.minute) == (3, 45) else "WARN", f"first bar {first} UTC (expect 03:45 = 09:15 IST)")
        r.line("PASS" if (last.hour, last.minute) >= (9, 55) else "WARN", f"last bar {last} UTC (expect about 09:59 = 15:29 IST)")
    sane = df[(df.low > df.open) | (df.low > df.close) | (df.high < df.open) | (df.high < df.close)]
    r.line("FAIL" if len(sane) else "PASS", f"bars where open/close fall outside low-high: {len(sane)}")
    per = df.groupby("symbol").size()
    if fine:
        ordered = df.sort_values(["symbol", "ts"])
        gap = ordered.groupby("symbol")["ts"].diff().dt.total_seconds()
        long_quiet = sorted(ordered.loc[gap > 300, "symbol"].unique())
        r.line("WARN" if long_quiet else "PASS", f"symbols silent for more than 5 minutes inside the session (illiquid or feed gaps): {long_quiet[:10] or 'none'}")
        print(f"      active seconds per symbol: min {per.min()}, median {int(per.median())}, max {per.max()} of {SESSION_SECONDS}")
    elif exp:
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


def verify_against_rest(lake: Lake, day: date, base: str = "1s") -> Report:
    """
    Independent check of the live feed: roll the base bars up to 1 minute and compare them with the
    minute candles pulled from Upstox's REST API (bars_1m rows tagged src='rest').  Small differences are
    expected (the feed sends snapshots, the candle API uses exchange trades), large or systematic ones are not.
    """
    r = Report()
    tables = lake.tables()
    if f"bars_{base}" not in tables or "bars_1m" not in tables:
        r.line("FAIL", f"need bars_{base} (live) and bars_1m (run data_layer.upstox.candles for that day)")
        return r
    live = lake.rollup(60, start=str(day), end=str(day + timedelta(days=1)), base=base)
    rest = lake.sql("SELECT symbol, ts, open, high, low, close, volume FROM bars_1m WHERE src = 'rest' "
                    "AND CAST(ts AS DATE) = CAST(? AS DATE)", [str(day)])
    print(f"\n== live {base} rolled up to 1m vs REST 1m candles, {day}: {len(live)} live minutes, {len(rest)} REST candles ==")
    if live.empty or rest.empty:
        r.line("FAIL", "one side has no rows for that day")
        return r
    j = rest.merge(live, on=["symbol", "ts"], suffixes=("_rest", "_live"))
    cover = len(j) / len(rest)
    r.line("PASS" if cover >= 0.95 else ("WARN" if cover >= 0.5 else "FAIL"),
           f"REST minutes that also exist live: {cover:.1%} ({len(j)} of {len(rest)})")
    if j.empty:
        return r
    cd = (j.close_live / j.close_rest - 1).abs()
    ok_close = (cd <= 0.002).mean()
    r.line("PASS" if ok_close >= 0.99 else "WARN", f"minutes whose close agrees within 0.2%: {ok_close:.1%} (worst {cd.max():.2%})")
    hl = ((j.high_live - j.high_rest).abs() / j.high_rest <= 0.002).mean()
    r.line("PASS" if hl >= 0.95 else "WARN", f"minutes whose high agrees within 0.2%: {hl:.1%}")
    vr = (j.volume_live.sum() / j.volume_rest.sum()) if j.volume_rest.sum() else float("nan")
    r.line("PASS" if 0.9 <= vr <= 1.1 else "WARN", f"total volume live / REST: {vr:.3f} (expect about 1; the first tick after connect is a baseline)")
    per_sym = j.groupby("symbol").apply(lambda g: g.volume_live.sum() / max(g.volume_rest.sum(), 1))
    off = sorted(per_sym[(per_sym < 0.8) | (per_sym > 1.2)].index)
    r.line("WARN" if off else "PASS", f"symbols with volume more than 20% off: {off[:10] or 'none'}")
    return r


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    o = sub.add_parser("ohlc"); o.add_argument("--years", type=int, default=5)
    b = sub.add_parser("bars"); b.add_argument("--date", type=date.fromisoformat, required=True)
    b.add_argument("--interval", default="1s")
    c = sub.add_parser("compare-rest"); c.add_argument("--date", type=date.fromisoformat, required=True)
    f = sub.add_parser("fo"); f.add_argument("--date", type=date.fromisoformat, required=True)
    f.add_argument("--interval", default="1s")
    f.add_argument("--all", action="store_true", help="every symbol with open interest, not only data/fo_universe.csv")
    a = ap.parse_args()
    lake = Lake()
    if a.cmd == "fo":
        from .upstox.fo_instruments import read_universe as read_fo
        rep = verify_fo(lake, a.date, None if a.all else read_fo(day=a.date), a.interval)
    else:
        rep = (verify_ohlc(lake, a.years) if a.cmd == "ohlc" else
               verify_against_rest(lake, a.date) if a.cmd == "compare-rest" else verify_bars(lake, a.date, a.interval))
    print(f"\n{'FAILED: ' + str(rep.fails) + ' check(s)' if rep.fails else 'no FAIL checks'}")
    sys.exit(1 if rep.fails else 0)


if __name__ == "__main__":
    main()
