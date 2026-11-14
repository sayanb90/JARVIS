"""
Data-health check for every signal input (SCRUM-189).  The live-only signals can only be computed from ticks we recorded, and a day the
feed missed cannot be fetched later, so this fails loudly the same evening.

  python -m signals health [--lake LAKE] [--date YYYY-MM-DD]      # exit 1 on any FAIL; WARN does not fail

Checks for one session (default: the latest weekday with data), per underlying where it applies:
  index_minutes      NIFTY 50, NIFTY BANK and India VIX 1-minute bars                          FAIL below MIN_MINUTES
  index_ticks        live ticks of the same index keys (NSE_INDEX:...)                          WARN when absent (backfill bars still work)
  futures            live ticks of at least two futures per underlying (front and next month)   FAIL below 1, WARN below 2
  option_ticks       distinct option contracts with ticks per underlying                        FAIL below MIN_OPTION_CONTRACTS
  option_greeks      share of option ticks with IV, delta, gamma and OI present                 FAIL below MIN_FIELD_SHARE
  quotes             share of option ticks with bid, ask and sizes                              FAIL below MIN_FIELD_SHARE
  extended_fields    share of ticks carrying last-trade quantity, total buy and sell quantity   WARN below MIN_FIELD_SHARE
  contracts          the contract table lists live option expiries on or after the day          FAIL when none
  participant        NSE participant OI for the day (published in the evening)                  WARN when missing
  eod_fo             F&O bhavcopy for the day (SCRUM-184), when that table exists               WARN when missing
  index_weights      index weights snapshot exists (SCRUM-183)                                  WARN when missing
  events             the event calendar loads                                                    FAIL on a bad file
A day the NSE calendar says was a holiday reports SKIP and passes.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from signals.lakeio import _day_files, _q

MIN_MINUTES = 370
MIN_OPTION_CONTRACTS = 200
MIN_FIELD_SHARE = 0.5
UNDERLYINGS = ("NIFTY", "BANKNIFTY")
INDEX_KEYS = ("NSE_INDEX:Nifty 50", "NSE_INDEX:Nifty Bank", "NSE_INDEX:India VIX")


@dataclass
class Check:
    name: str
    status: str            # OK, WARN, FAIL, SKIP
    detail: str

    def __str__(self) -> str:
        return f"{self.status:4} {self.name:18} {self.detail}"


def _files(lake: Path, kind: str, day: date) -> list[str]:
    if kind == "bars":
        return _day_files(lake / "bars" / "interval=1m", day, day)
    base = lake / "ticks"
    return _day_files(base / f"segment={kind}", day, day) + (_day_files(base, day, day) if kind == "fo" else [])


def _cols(con, files: list[str]) -> set[str]:
    return {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_q(files)}, union_by_name=true)").fetchall()}


def check_day(lake: Path, day: date) -> list[Check]:
    lake = Path(lake)
    out: list[Check] = []
    try:
        from data_layer import market_calendar
        st = market_calendar.status(day, "NSE", cal=market_calendar.load(lake))
        if not st.expects_data:
            return [Check("calendar", "SKIP", f"{day}: {st}")]
    except Exception:                                                      # noqa: BLE001 -- no calendar: check anyway
        pass
    con = duckdb.connect()
    bars = _files(lake, "bars", day)
    if bars:
        got = dict(con.execute(f"SELECT symbol, count(DISTINCT ts) FROM read_parquet({_q(bars)}, union_by_name=true) WHERE symbol IN "
                               f"({', '.join(repr(k) for k in INDEX_KEYS)}) GROUP BY 1").fetchall())
    else:
        got = {}
    for k in INDEX_KEYS:
        n = int(got.get(k, 0))
        out.append(Check("index_minutes", "OK" if n >= MIN_MINUTES else "FAIL", f"{k}: {n} minutes"))
    eq = _files(lake, "eq", day)
    n_idx = con.execute(f"SELECT count(*) FROM read_parquet({_q(eq)}, union_by_name=true) WHERE symbol LIKE 'NSE_INDEX:%'").fetchone()[0] if eq else 0
    out.append(Check("index_ticks", "OK" if n_idx else "WARN", f"{n_idx:,} index ticks"))
    fo = _files(lake, "fo", day)
    if not fo:
        out.append(Check("option_ticks", "FAIL", "no F&O tick files for the day"))
    else:
        cols = _cols(con, fo)
        src = f"read_parquet({_q(fo)}, union_by_name=true)"
        for u in UNDERLYINGS:
            nf = con.execute(f"SELECT count(DISTINCT symbol) FROM {src} WHERE regexp_matches(symbol, ?)", [f"^{u}_FUT_"]).fetchone()[0]
            out.append(Check("futures", "OK" if nf >= 2 else ("WARN" if nf == 1 else "FAIL"), f"{u}: {nf} futures"))
            no = con.execute(f"SELECT count(DISTINCT symbol) FROM {src} WHERE regexp_matches(symbol, ?)", [f"^{u}_[0-9.]+_(CE|PE)_"]).fetchone()[0]
            out.append(Check("option_ticks", "OK" if no >= MIN_OPTION_CONTRACTS else "FAIL", f"{u}: {no} option contracts"))
        opt = f"FROM {src} WHERE regexp_matches(symbol, '_(CE|PE)_')"
        for name, fields, level in (("option_greeks", ("iv", "delta", "gamma", "oi"), "FAIL"), ("quotes", ("bid", "ask", "bid_qty", "ask_qty"), "FAIL"),
                                    ("extended_fields", ("ltq", "tbq", "tsq"), "WARN")):
            for f in fields:
                if f not in cols:
                    out.append(Check(name, level, f"{f}: column missing"))
                    continue
                share = con.execute(f"SELECT avg(CASE WHEN {f} IS NOT NULL THEN 1.0 ELSE 0 END) {opt}").fetchone()[0] or 0.0
                out.append(Check(name, "OK" if share >= MIN_FIELD_SHARE else level, f"{f}: {share:.0%} of option ticks"))
    tables = [p for p in (lake / "instruments" / "fo.parquet", lake / "instruments" / "fo_expired.parquet") if p.exists()]
    if tables:
        c = pd.concat([pd.read_parquet(p, columns=["instrument_type", "expiry"]) for p in tables], ignore_index=True)
        live = c[(pd.to_datetime(c["expiry"]).dt.date >= day) & c["instrument_type"].isin(["CE", "PE"])]
        out.append(Check("contracts", "OK" if len(live) else "FAIL", f"{len(live):,} option contracts expiring on or after the day"))
    else:
        out.append(Check("contracts", "FAIL", "no contract table in lake/instruments"))
    pp = lake / "positioning" / "participant_oi.parquet"
    if pp.exists():
        have = day in set(pd.to_datetime(pd.read_parquet(pp, columns=["date"])["date"]).dt.date)
        out.append(Check("participant", "OK" if have else "WARN", "present" if have else "not yet published or not fetched"))
    else:
        out.append(Check("participant", "WARN", "table missing (SCRUM-175 not run yet)"))
    eod = lake / "eod_fo"
    if eod.is_dir():
        files = [str(f) for f in eod.rglob("*.parquet")]
        have = bool(files) and con.execute(f"SELECT count(*) FROM read_parquet({_q(files)}, union_by_name=true) WHERE CAST(date AS DATE) = ?",
                                           [day]).fetchone()[0] > 0
        out.append(Check("eod_fo", "OK" if have else "WARN", "present" if have else "missing for the day"))
    w = lake / "reference" / "index_weights.parquet"
    out.append(Check("index_weights", "OK" if w.exists() else "WARN", "present" if w.exists() else "missing (SCRUM-183 not run yet)"))
    try:
        from data_layer import market_events
        n = len(market_events.load())
        out.append(Check("events", "OK", f"{n} events"))
    except Exception as exc:                                               # noqa: BLE001
        out.append(Check("events", "FAIL", str(exc)[:120]))
    return out


def latest_day(lake: Path) -> Optional[date]:
    base = Path(lake) / "bars" / "interval=1m"
    days = sorted(d.name[5:] for d in base.glob("date=*")) if base.is_dir() else []
    return date.fromisoformat(days[-1]) if days else None


def main(argv: Optional[list[str]] = None) -> int:
    from data_layer.lake import LAKE_DIR
    ap = argparse.ArgumentParser(prog="python -m signals health")
    ap.add_argument("--lake", type=Path, default=LAKE_DIR)
    ap.add_argument("--date", type=date.fromisoformat)
    a = ap.parse_args(argv)
    day = a.date or latest_day(a.lake)
    if day is None:
        print("FAIL no 1-minute bars in the lake")
        return 1
    checks = check_day(a.lake, day)
    print(f"signal inputs for {day}:")
    for c in checks:
        print(" ", c)
    fails = [c for c in checks if c.status == "FAIL"]
    print(f"{'FAIL' if fails else 'OK'}: {len(fails)} failed, {sum(c.status == 'WARN' for c in checks)} warnings")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
