"""
Which time should the calculations use?  A read-only analysis of the three time columns of a day's ticks (SCRUM-128).

  python -m data_layer.time_diag                      # today (UTC); report to logs/time-diag-<day>.md and the screen
  python -m data_layer.time_diag --day 2026-11-04 --skip-repeats

The columns (all naive UTC):
  ts   the exchange's last-trade time (ltpc.ltt).  It does not move when only a quote, a Greek or the open interest changes.
  fts  when Upstox generated the message (FeedResponse.currentTs): the candidate event time for every row.
  rts  when this process received the message, on this server's clock: orders rows exactly, and measures delay.

What it measures, per segment (equity, F&O):
  1. delay: rts minus fts.  Near zero and never negative means the two clocks agree and fts is a real event time; negative values
     mean the server clock runs behind Upstox's; a long tail means delivery delays.  By half hour, to show drift.
  2. resolution and uniqueness: how many values sit on whole seconds, and how many rows share a value with another row of the
     same instrument (a key that repeats cannot order events).
  3. trade time against message time: for rows that carry a new trade (the cumulative volume went up) how far ts lags fts; for
     the other rows how stale ts is.  And how many rows would fall into a different 1, 5 or 60 second bar if bars used ts.
  4. order: in arrival order, how often fts or ts go backwards.
  5. session bounds: rows timed outside 03:45 to 10:00 UTC.
  6. repeats: rows identical in every column except rts and fts (the like-for-like version of the 2026-11-03 duplicate count).
Then a verdict, computed from the numbers, on which time to use for what.

Nothing in the lake is changed.  Working memory spills to a temporary folder (not the lake) and is removed afterwards.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

from . import lake as lake_mod

FO = "regexp_matches(symbol, '_(CE|PE|FUT)_')"
OPEN_UTC, CLOSE_UTC = "03:45:00", "10:00:00"
WIDTHS = (1, 5, 60)
MEMORY = "3GB"


def _source(folder: Path) -> Optional[str]:
    files = sorted(folder.glob("*.parquet")) if folder.is_dir() else []
    if not files:
        return None
    names = ", ".join("'" + str(f).replace("'", "''") + "'" for f in files)
    return f"read_parquet([{names}], union_by_name=true, hive_partitioning=false)"


def _pct(x) -> Optional[float]:
    return None if x is None else round(float(x) * 100, 3)


def _num(x, digits: int = 1) -> Optional[float]:
    return None if x is None else round(float(x), digits)


def analyse(day: date, lake_dir: Path, tmp_dir: Path, skip_repeats: bool = False) -> dict:
    """The measurements as a dict (see the module text).  {'error': ...} when the day cannot be analysed."""
    folder = lake_dir / "ticks" / f"date={day}"
    src = _source(folder)
    if src is None:
        return {"error": f"no tick files in {folder}"}
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit='{MEMORY}'")
        con.execute(f"SET temp_directory='{tmp_dir}'")
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
        if not {"ts", "fts", "rts"} <= cols:
            return {"error": f"{day} has no fts/rts columns (they exist from 2026-11-04); columns present: {sorted(cols & {'ts', 'fts', 'rts'})}"}
        con.execute(f"""CREATE VIEW t AS SELECT symbol, ts, fts, rts, ltp, vol_today,
                           CASE WHEN {FO} THEN 'fo' ELSE 'equity' END AS seg,
                           epoch_ms(rts) - epoch_ms(fts) AS lat_ms,
                           epoch_ms(fts) - epoch_ms(ts) AS stale_ms
                        FROM {src} WHERE fts IS NOT NULL AND rts IS NOT NULL""")
        con.execute("""CREATE TABLE w AS SELECT seg, symbol, ts, fts, rts, lat_ms, stale_ms,
                          vol_today - lag(vol_today) OVER p AS dvol,
                          epoch_ms(fts) - epoch_ms(lag(fts) OVER p) AS dfts,
                          epoch_ms(ts) - epoch_ms(lag(ts) OVER p) AS dts
                       FROM t WINDOW p AS (PARTITION BY symbol ORDER BY rts, fts, ts)""")
        out: dict = {"day": str(day)}
        out["rows"] = {r[0]: r[1] for r in con.execute("SELECT seg, count(*) FROM w GROUP BY 1 ORDER BY 1").fetchall()}
        segs = list(out["rows"])

        # 1. delay
        out["delay"] = {}
        for seg in segs:
            r = con.execute("""SELECT median(lat_ms), quantile_cont(lat_ms, 0.9), quantile_cont(lat_ms, 0.99), quantile_cont(lat_ms, 0.999),
                                      min(lat_ms), max(lat_ms), avg((lat_ms < 0)::INT), avg((lat_ms < -50)::INT), avg((lat_ms > 1000)::INT)
                               FROM w WHERE seg = ?""", [seg]).fetchone()
            out["delay"][seg] = dict(zip(("median_ms", "p90_ms", "p99_ms", "p999_ms", "min_ms", "max_ms", "negative", "negative_over_50ms", "over_1s"),
                                         [_num(x, 1) for x in r[:6]] + [_pct(x) for x in r[6:]]))
        out["delay_by_half_hour"] = [dict(zip(("half_hour", "rows", "median_ms", "p99_ms", "negative_pct"), (str(r[0])[11:16], r[1], _num(r[2]), _num(r[3]), _pct(r[4]))))
                                     for r in con.execute("""SELECT date_trunc('hour', fts) + to_minutes(30 * CAST(floor(minute(fts) / 30) AS INT)) AS h, count(*),
                                                                    median(lat_ms), quantile_cont(lat_ms, 0.99), avg((lat_ms < 0)::INT)
                                                             FROM w GROUP BY 1 ORDER BY 1""").fetchall()]
        out["worst_delays"] = [dict(zip(("symbol", "fts", "delay_ms"), (r[0], str(r[1]), r[2])))
                               for r in con.execute("SELECT symbol, fts, lat_ms FROM w ORDER BY lat_ms DESC LIMIT 5").fetchall()]

        # 2. resolution and uniqueness
        out["columns"] = {}
        for col in ("ts", "fts", "rts"):
            res = {}
            for seg in segs:
                whole, = con.execute(f"SELECT avg((epoch_ms({col}) % 1000 = 0)::INT) FROM w WHERE seg = ? AND {col} IS NOT NULL", [seg]).fetchone()
                shared, = con.execute(f"""SELECT coalesce(sum(c) FILTER (WHERE c > 1), 0) / sum(c)
                                          FROM (SELECT count(*) AS c FROM w WHERE seg = ? AND {col} IS NOT NULL GROUP BY symbol, {col})""", [seg]).fetchone()
                res[seg] = {"on_whole_seconds_pct": _pct(whole), "rows_sharing_a_value_pct": _pct(shared)}
            out["columns"][col] = res

        # 3. trade time against message time
        out["trade_vs_message"] = {}
        for seg in segs:
            fresh = con.execute("""SELECT count(*), median(stale_ms), quantile_cont(stale_ms, 0.99), avg((stale_ms < 0)::INT), avg((stale_ms <= 100)::INT),
                                          avg((stale_ms <= 1000)::INT), avg((stale_ms > 5000)::INT)
                                   FROM w WHERE seg = ? AND dvol > 0 AND ts IS NOT NULL""", [seg]).fetchone()
            other = con.execute("""SELECT count(*), median(stale_ms), quantile_cont(stale_ms, 0.9), avg((stale_ms > 1000)::INT)
                                   FROM w WHERE seg = ? AND coalesce(dvol, 0) <= 0 AND ts IS NOT NULL""", [seg]).fetchone()
            bars = {}
            for width in WIDTHS:
                b = lambda c: f"floor((epoch_ms({c}) - epoch_ms(date_trunc('day', {c})) - 13500000) / {width * 1000})"
                bars[f"{width}s"], = con.execute(f"SELECT avg(({b('ts')} <> {b('fts')})::INT) FROM w WHERE seg = ? AND dvol > 0 AND ts IS NOT NULL", [seg]).fetchone()
            out["trade_vs_message"][seg] = {
                "new_trade_rows": fresh[0], "fts_minus_ts_median_ms": _num(fresh[1]), "fts_minus_ts_p99_ms": _num(fresh[2]),
                "ts_after_fts_pct": _pct(fresh[3]), "within_100ms_pct": _pct(fresh[4]), "within_1s_pct": _pct(fresh[5]), "over_5s_pct": _pct(fresh[6]),
                "other_rows": other[0], "stale_ts_median_ms": _num(other[1]), "stale_ts_p90_ms": _num(other[2]), "stale_ts_over_1s_pct": _pct(other[3]),
                "new_trade_rows_in_a_different_bar_pct": {k: _pct(v) for k, v in bars.items()}}

        # 4. order (arrival order within an instrument)
        out["order"] = {}
        for seg in segs:
            r = con.execute("""SELECT avg((dfts < 0)::INT), coalesce(min(dfts) FILTER (WHERE dfts < 0), 0), avg((dfts = 0)::INT),
                                      avg((dts < 0)::INT), coalesce(min(dts) FILTER (WHERE dts < 0), 0)
                               FROM w WHERE seg = ?""", [seg]).fetchone()
            out["order"][seg] = {"fts_goes_backwards_pct": _pct(r[0]), "largest_fts_step_back_ms": r[1], "fts_equal_to_previous_pct": _pct(r[2]),
                                 "ts_goes_backwards_pct": _pct(r[3]), "largest_ts_step_back_ms": r[4]}

        # 5. session bounds
        out["bounds"] = {}
        for col in ("ts", "fts", "rts"):
            r = con.execute(f"""SELECT min({col}), max({col}), count(*) FILTER (WHERE CAST({col} AS TIME) < TIME '{OPEN_UTC}'),
                                       count(*) FILTER (WHERE CAST({col} AS TIME) > TIME '{CLOSE_UTC}') FROM w WHERE {col} IS NOT NULL""").fetchone()
            out["bounds"][col] = {"min": str(r[0]), "max": str(r[1]), "before_open": r[2], "after_close": r[3]}

        # 6. repeats, like for like
        if not skip_repeats:
            out["repeats"] = {}
            for seg in segs:
                total, distinct = con.execute(f"""SELECT count(*), (SELECT count(*) FROM (SELECT DISTINCT * EXCLUDE (rts, fts) FROM {src}
                                                                                          WHERE fts IS NOT NULL AND ({FO}) = ?))
                                                  FROM {src} WHERE fts IS NOT NULL AND ({FO}) = ?""", [seg == "fo", seg == "fo"]).fetchone()
                out["repeats"][seg] = {"rows": total, "repeated_pct": _pct((total - distinct) / total) if total else None}
        out["verdict"] = verdict(out)
        return out
    finally:
        con.close()


def verdict(m: dict) -> list[str]:
    """Plain statements computed from the measurements; each says what it is based on."""
    lines: list[str] = []
    segs = list(m["rows"])
    worst_neg = max((m["delay"][s]["negative_over_50ms"] or 0) for s in segs)
    worst_p99 = max((m["delay"][s]["p99_ms"] or 0) for s in segs)
    fts_share = max((m["columns"]["fts"][s]["rows_sharing_a_value_pct"] or 0) for s in segs)
    ts_share = max((m["columns"]["ts"][s]["rows_sharing_a_value_pct"] or 0) for s in segs)
    fts_back = max((m["order"][s]["fts_goes_backwards_pct"] or 0) for s in segs)
    ts_back = max((m["order"][s]["ts_goes_backwards_pct"] or 0) for s in segs)
    ok_clock = worst_neg < 0.1 and worst_p99 < 1000
    ok_key = fts_share < 0.1
    ok_order = fts_back < 0.1
    lines.append(f"fts as the event time of every row: {'YES' if ok_clock and ok_key and ok_order else 'CHECK'}. Based on: delay rts-fts p99 {worst_p99} ms "
                 f"(worst segment), {worst_neg} % of rows more than 50 ms negative, {fts_share} % of rows sharing an fts with another row of the same "
                 f"instrument, {fts_back} % of rows where fts goes backwards in arrival order.")
    lines.append(f"ts as an event time: {'NO' if ts_share > 5 else 'MAYBE'} for rows that are not trades. {ts_share} % of rows share their ts with another row "
                 f"of the same instrument and {ts_back} % go backwards in arrival order.")
    for s in segs:
        t = m["trade_vs_message"][s]
        if t["new_trade_rows"]:
            lines.append(f"{s}: for the {t['new_trade_rows']:,} rows that carry a new trade, ts lags fts by a median of {t['fts_minus_ts_median_ms']} ms "
                         f"(99th percentile {t['fts_minus_ts_p99_ms']} ms); {t['within_1s_pct']} % within 1 s, {t['over_5s_pct']} % over 5 s; "
                         f"using ts instead of fts would move {t['new_trade_rows_in_a_different_bar_pct']['1s']} % of them to another 1-second bar, "
                         f"{t['new_trade_rows_in_a_different_bar_pct']['60s']} % to another 1-minute bar.")
        lines.append(f"{s}: on the other {t['other_rows']:,} rows ts is stale by a median of {t['stale_ts_median_ms']} ms; {t['stale_ts_over_1s_pct']} % are "
                     f"more than 1 s old.")
    lines.append("Recommended use, if the first line says YES: (1) event time of any row, bar, Greek, open-interest or quote update = fts; "
                 "(2) the time of the last trade = ts, used only as 'time since last trade' (or on rows with a new trade, when it is the exchange's own "
                 "trade time); (3) row order and tie-breaking = rts, then fts, then ts; (4) rts minus fts is a feed-health signal: alert when its median "
                 "or 99th percentile moves. If the first line says CHECK, read the numbers it quotes before relying on fts.")
    return lines


def render(m: dict) -> str:
    if "error" in m:
        return f"# Time columns {m.get('day', '')}\n\n{m['error']}\n"

    def table(headers: list[str], rows: list[list]) -> list[str]:
        return ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)] + ["| " + " | ".join("" if c is None else str(c) for c in r) + " |" for r in rows]

    segs = list(m["rows"])
    out = [f"# Time columns of the ticks of {m['day']}", "", "## Verdict (read this first)", "", *[f"- {x}" for x in m["verdict"]], "",
           "## Rows", "", *table(["segment", "rows with fts and rts"], [[s, f"{n:,}"] for s, n in m["rows"].items()]), "",
           "## 1. Delay: rts minus fts (ms)", "",
           *table(["segment", "median", "p90", "p99", "p99.9", "min", "max", "% negative", "% below -50 ms", "% above 1 s"],
                  [[s, *[m["delay"][s][k] for k in ("median_ms", "p90_ms", "p99_ms", "p999_ms", "min_ms", "max_ms", "negative", "negative_over_50ms", "over_1s")]] for s in segs]),
           "", "By half hour (all segments):", "",
           *table(["from (UTC)", "rows", "median ms", "p99 ms", "% negative"], [[r["half_hour"], f"{r['rows']:,}", r["median_ms"], r["p99_ms"], r["negative_pct"]] for r in m["delay_by_half_hour"]]),
           "", "Worst five delays:", "", *table(["symbol", "fts", "delay ms"], [[r["symbol"], r["fts"], r["delay_ms"]] for r in m["worst_delays"]]), "",
           "## 2. Resolution and uniqueness", ""]
    for col in ("ts", "fts", "rts"):
        out += [f"**{col}**", "", *table(["segment", "% on whole seconds", "% of rows sharing a value with another row of the same instrument"],
                                         [[s, m["columns"][col][s]["on_whole_seconds_pct"], m["columns"][col][s]["rows_sharing_a_value_pct"]] for s in segs]), ""]
    out += ["## 3. Trade time against message time", ""]
    for s in segs:
        t = m["trade_vs_message"][s]
        out += [f"**{s}**", "", *table(["measure", "value"], [
            ["rows with a new trade", f"{t['new_trade_rows']:,}"], ["fts minus ts, median ms", t["fts_minus_ts_median_ms"]], ["fts minus ts, p99 ms", t["fts_minus_ts_p99_ms"]],
            ["% where ts is after fts", t["ts_after_fts_pct"]], ["% within 100 ms", t["within_100ms_pct"]], ["% within 1 s", t["within_1s_pct"]], ["% over 5 s", t["over_5s_pct"]],
            ["rows without a new trade", f"{t['other_rows']:,}"], ["their ts staleness, median ms", t["stale_ts_median_ms"]], ["their ts staleness, p90 ms", t["stale_ts_p90_ms"]],
            ["% of them with ts more than 1 s old", t["stale_ts_over_1s_pct"]],
            *[[f"new-trade rows in a different {k} bar if ts were used, %", v] for k, v in t["new_trade_rows_in_a_different_bar_pct"].items()]]), ""]
    out += ["## 4. Order within an instrument, in arrival order", "",
            *table(["segment", "% fts backwards", "largest fts step back ms", "% fts equal to previous", "% ts backwards", "largest ts step back ms"],
                   [[s, *[m["order"][s][k] for k in ("fts_goes_backwards_pct", "largest_fts_step_back_ms", "fts_equal_to_previous_pct", "ts_goes_backwards_pct", "largest_ts_step_back_ms")]] for s in segs]),
            "", "## 5. Session bounds (03:45 to 10:00 UTC)", "",
            *table(["column", "min", "max", "rows before 03:45", "rows after 10:00"], [[c, v["min"], v["max"], f"{v['before_open']:,}", f"{v['after_close']:,}"] for c, v in m["bounds"].items()]), ""]
    if "repeats" in m:
        out += ["## 6. Repeats: rows identical in every column except rts and fts", "",
                *table(["segment", "rows", "% repeated"], [[s, f"{v['rows']:,}", v["repeated_pct"]] for s, v in m["repeats"].items()]),
                "", "(2026-11-03, before the new columns existed: equity 11.1 %, F&O 4.3 %.)", ""]
    return "\n".join(out)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Read-only analysis of the ts, fts and rts columns of a day's ticks")
    ap.add_argument("--day", type=date.fromisoformat, default=None, help="UTC day (default: today)")
    ap.add_argument("--lake", type=Path, default=lake_mod.LAKE_DIR)
    ap.add_argument("--logs", type=Path, default=lake_mod.ROOT / "logs")
    ap.add_argument("--skip-repeats", action="store_true", help="skip the repeat count (the slowest query)")
    ap.add_argument("--out", type=Path, default=None)
    a = ap.parse_args(argv)
    day = a.day or datetime.now(timezone.utc).date()
    tmp = Path(tempfile.mkdtemp(prefix="jarvis-time-diag-"))
    try:
        result = analyse(day, a.lake, tmp, a.skip_repeats)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    report = render(result)
    out = a.out or a.logs / f"time-diag-{day:%Y%m%d}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report)
    print(report)
    print(f"saved to {out}")
    return 1 if "error" in result else 0


if __name__ == "__main__":
    sys.exit(main())
