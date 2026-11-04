"""
JARVIS query layer — DuckDB over the Parquet lake.

  from data_layer.query import Lake
  lake = Lake()
  lake.latest_prices("IN")
  lake.ohlc("INFY.NS", "2024-01-01", "2024-12-31")
  lake.score_all("IN").head(10)

Every method returns a pandas DataFrame (or a scalar).  Tables are exposed as
views, so ad-hoc SQL works too:  lake.sql("SELECT ... FROM ohlc ...").
Missing tables (e.g. no bars collected yet) simply do not exist as views.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from . import tick_store
from .lake import LAKE_DIR

PE_MAX, ROIC_MIN, LT_DEBT_FCF_MAX, PFCF_MAX = 22.5, 0.09, 5.0, 20.0
TRADING_DAYS = 250
FO_REGEX = tick_store.FO_REGEX                  # lake symbols of futures and options
VIEWER_MEMORY_GB = 2.0                         # DuckDB memory per query connection; beyond it sorts and joins spill to disk (SCRUM-129)

_NULLABLE = {"oi": "DOUBLE", "bid": "DOUBLE", "ask": "DOUBLE", "bid_qty": "DOUBLE", "ask_qty": "DOUBLE", "vol_today": "DOUBLE"}   # as the feed writes them


def _rp(src) -> str:
    """read_parquet over a pattern or a list of files / patterns; columns are matched by name, a folder name never becomes a column."""
    items = [src] if isinstance(src, (str, Path)) else list(src)
    names = ", ".join("'" + str(x).replace("'", "''") + "'" for x in items)
    return f"read_parquet([{names}], union_by_name=true, hive_partitioning=false)"


def _pad(cols: set[str]) -> str:
    """SELECT-list additions that give the columns a file may lack (a stock file has no open interest) as NULL."""
    return "".join(f", CAST(NULL AS {t}) AS {c}" for c, t in _NULLABLE.items() if c not in cols)


_CACHE: dict = {}                              # results per (lake, day, file fingerprint): a finished day never changes, a live day does
_CACHE_MAX = 12


def _cached(key: tuple, compute):
    if key not in _CACHE:
        if len(_CACHE) >= _CACHE_MAX:
            _CACHE.pop(next(iter(_CACHE)))             # oldest first
        _CACHE[key] = compute()
    return _CACHE[key]


def _memory_limit() -> str:
    try:
        gb = float(os.environ.get("JARVIS_VIEWER_MEMORY_GB") or VIEWER_MEMORY_GB)
    except ValueError:
        gb = VIEWER_MEMORY_GB
    return f"{max(gb, 0.25):g}GB"


class Lake:
    def __init__(self, lake_dir: Path | str | None = None) -> None:
        self.dir = Path(lake_dir) if lake_dir else LAKE_DIR
        self.con = duckdb.connect()
        self.con.execute(f"SET memory_limit='{_memory_limit()}'")
        self.con.execute(f"SET temp_directory='{Path(tempfile.gettempdir()) / 'jarvis-viewer-spill'}'")
        for name in ("income_statement", "balance_sheet", "cash_flow", "ratios",
                     "pillar_metrics", "fund_meta"):
            f = self.dir / "fundamentals" / f"{name}.parquet"
            if f.exists():
                self.con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{f}')")
        if any((self.dir / "ohlc").glob("year=*/ohlc.parquet")):
            self.con.execute(
                f"CREATE VIEW ohlc AS SELECT * FROM "
                f"read_parquet('{self.dir}/ohlc/year=*/ohlc.parquet', hive_partitioning=false)")
        bar_ivs = sorted(p.name.split("=", 1)[1] for p in (self.dir / "bars").glob("interval=*"))
        for iv in bar_ivs:
            if iv == "1s":
                continue                                             # handled below: may be ticks-derived too
            self.con.execute(
                f"CREATE VIEW bars_{iv} AS SELECT * FROM "
                f"read_parquet('{self.dir}/bars/interval={iv}/date=*/*.parquet', union_by_name=true)")
        tick_patterns = [pat for pat, probe in ((f"{self.dir}/ticks/date=*/*.parquet", "date=*/*.parquet"),
                                                (f"{self.dir}/ticks/segment=*/date=*/*.parquet", "segment=*/date=*/*.parquet"))
                         if any((self.dir / "ticks").glob(probe))]            # the older layout and the segment layout (SCRUM-133)
        has_ticks = bool(tick_patterns)
        self.has_ticks = has_ticks
        if "1s" in bar_ivs or has_ticks:
            parts = []
            legacy_1s_dates = set()
            if "1s" in bar_ivs:
                legacy_1s_dates = {p.name.split("=", 1)[1] for p in (self.dir / "bars" / "interval=1s").glob("date=*")
                                   if any(p.glob("*.parquet"))}
                parts.append(f"SELECT * FROM read_parquet("
                            f"'{self.dir}/bars/interval=1s/date=*/*.parquet', union_by_name=true)")
            if has_ticks:
                parts.append(self._ticks_bars_sql(tick_patterns, exclude_dates=legacy_1s_dates or None))
            self.con.execute(f"CREATE VIEW bars_1s AS {' UNION ALL BY NAME '.join(parts)}")

    # -- raw ticks -> bar-shaped rows (SCRUM-77: the live feed stores ticks, not 1-second bars) -----
    @staticmethod
    def _ticks_bars_sql(pattern, width_s: int = 1, exclude_dates: set[str] | None = None, where: str | None = None) -> str:
        """
        SQL (no trailing semicolon, usable as a CTE or subquery) that aggregates raw ticks matched by
        `pattern` into bar-shaped rows at `width_s` seconds, aligned to the 03:45 UTC session open —
        the same shape BarAggregator used to produce live.  Volume is the per-tick delta of the
        cumulative `vol_today` field (first tick of a symbol's day only sets the baseline, matching
        BarAggregator.add's semantics); bid/ask/bid_qty/ask_qty/oi are the last value seen in the bucket.
        """
        try:
            cols = {r[0] for r in duckdb.connect().execute(f"DESCRIBE SELECT * FROM {_rp(pattern)}").fetchall()}
        except Exception:                                            # noqa: BLE001 -- no readable file: behave as before
            cols = {"ts"}
        ev, od = Lake._time_exprs(cols)
        conds = ["ltp IS NOT NULL"]                                  # a quote-only row (no trade yet today) makes no price bar
        if where:
            conds.append(f"({where})")                               # e.g. equities only: cut the rows before the windows run
        if exclude_dates:
            date_list = ", ".join("DATE '{}'".format(d) for d in sorted(exclude_dates))
            conds.append(f"CAST({ev} AS DATE) NOT IN ({date_list})")
        return f"""(
            WITH raw AS (SELECT *, {ev} AS _ev, {od} AS _ord{_pad(cols)} FROM {_rp(pattern)} WHERE {' AND '.join(conds)}),
                 t AS (SELECT *, lag(vol_today) OVER (PARTITION BY symbol, CAST(_ev AS DATE) ORDER BY _ord) AS prev_vol
                       FROM raw),
                 d AS (SELECT *, CASE WHEN prev_vol IS NULL OR vol_today < prev_vol THEN 0
                                       ELSE vol_today - prev_vol END AS vol
                       FROM t),
                 b AS (SELECT *, date_trunc('day', _ev) + to_seconds(13500 + CAST(floor(
                             (epoch(_ev - date_trunc('day', _ev)) - 13500) / {int(width_s)}) AS BIGINT) * {int(width_s)}
                           ) AS bucket
                       FROM d)
            SELECT symbol, 'IN' AS market, bucket AS ts,
                   arg_min(ltp, _ord) AS open, max(ltp) AS high, min(ltp) AS low, arg_max(ltp, _ord) AS close,
                   sum(vol)::BIGINT AS volume,
                   sum(ltp * vol) FILTER (WHERE vol > 0) / nullif(sum(vol) FILTER (WHERE vol > 0), 0) AS vwap,
                   count(*)::BIGINT AS n_ticks,
                   arg_max(bid, _ord) FILTER (WHERE bid IS NOT NULL) AS bid,
                   arg_max(ask, _ord) FILTER (WHERE ask IS NOT NULL) AS ask,
                   arg_max(bid_qty, _ord) FILTER (WHERE bid_qty IS NOT NULL) AS bid_qty,
                   arg_max(ask_qty, _ord) FILTER (WHERE ask_qty IS NOT NULL) AS ask_qty,
                   'live' AS src,
                   arg_max(oi, _ord) FILTER (WHERE oi IS NOT NULL) AS oi
            FROM b GROUP BY symbol, bucket
        )"""

    @staticmethod
    def _time_exprs(cols: set[str]) -> tuple[str, str]:
        """(event time, arrival order) as SQL for files with these columns.  Event time is the message time `fts` when the files have it,
        else the last-trade time `ts`; order is arrival time `rts`, else `fts`, else `ts`.  Files written before SCRUM-116 have neither
        extra column and read exactly as before."""
        ev = "coalesce(fts, ts)" if "fts" in cols else "ts"
        return ev, tick_store.order_expr([c for c in ("rts", "fts", "ts") if c in cols])

    # -- generic ------------------------------------------------------------
    def sql(self, query: str, params: list | None = None) -> pd.DataFrame:
        return self.con.execute(query, params or []).df()

    def tables(self) -> list[str]:
        return [r[0] for r in self.con.execute("SHOW TABLES").fetchall()]

    # -- universe -----------------------------------------------------------
    def symbols(self, market: str | None = None) -> list[str]:
        q = "SELECT symbol FROM pillar_metrics" + (" WHERE market = ?" if market else "") + " ORDER BY symbol"
        return [r[0] for r in self.con.execute(q, [market] if market else []).fetchall()]

    def markets(self) -> list[str]:
        return [r[0] for r in self.con.execute(
            "SELECT DISTINCT market FROM pillar_metrics ORDER BY 1").fetchall()]

    # -- prices -------------------------------------------------------------
    def latest_prices(self, market: str | None = None) -> pd.DataFrame:
        """Most recent close per symbol: symbol, market, date, close."""
        where = "WHERE market = ?" if market else ""
        return self.sql(
            f"SELECT symbol, market, date, close FROM ("
            f"SELECT *, row_number() OVER (PARTITION BY symbol ORDER BY date DESC) rn "
            f"FROM ohlc {where}) WHERE rn = 1 ORDER BY symbol",
            [market] if market else [])

    def latest_price(self, symbol: str) -> float | None:
        r = self.con.execute(
            "SELECT close FROM ohlc WHERE symbol = ? ORDER BY date DESC LIMIT 1", [symbol]).fetchone()
        return r[0] if r else None

    def ohlc(self, symbol: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
        return self.sql(
            "SELECT * FROM ohlc WHERE symbol = ? AND date >= COALESCE(CAST(? AS DATE), DATE '1900-01-01') "
            "AND date <= COALESCE(CAST(? AS DATE), DATE '2999-12-31') ORDER BY date",
            [symbol, start, end])

    def ohlc_market(self, market: str, start: str, end: str) -> pd.DataFrame:
        return self.sql("SELECT * FROM ohlc WHERE market = ? AND date BETWEEN CAST(? AS DATE) "
                        "AND CAST(? AS DATE) ORDER BY symbol, date", [market, start, end])

    def bars(self, symbol: str, start: str, end: str, interval: str = "1s") -> pd.DataFrame:
        """Intraday bars between two UTC timestamps/dates (inclusive of start, exclusive of end)."""
        return self.sql(f"SELECT * FROM bars_{interval} WHERE symbol = ? AND ts >= CAST(? AS TIMESTAMP) "
                        f"AND ts < CAST(? AS TIMESTAMP) ORDER BY ts", [symbol, start, end])

    def rollup(self, width_s: int, symbol: str | None = None, start: str | None = None, end: str | None = None,
               base: str = "1s", frame: pd.DataFrame | None = None) -> pd.DataFrame:
        """
        Coarser bars (`width_s` seconds: 60 = 1 minute, 300 = 5 minutes, ...) built from the base bars and
        aligned to the 09:15 IST open (03:45 UTC), so they match bars built tick by tick at that width.
        Open/close are the first/last base bar, volume is summed, VWAP is volume-weighted, L1 is the last seen.
        Reads the `bars_<base>` view unless `frame` (base bars you already hold) is given.  The view can still
        contain duplicate rows for a day that has not been compacted (a retried flush), which would be double
        counted; pass a de-duplicated `frame` in that case, as `day_bars` does.
        """
        if width_s <= 0:
            raise ValueError("width_s must be positive")
        where, params = ["1=1"], []
        if symbol:
            where.append("symbol = ?"); params.append(symbol)
        if start:
            where.append("ts >= CAST(? AS TIMESTAMP)"); params.append(start)
        if end:
            where.append("ts < CAST(? AS TIMESTAMP)"); params.append(end)
        last = lambda c: f"arg_max({c}, ts) FILTER (WHERE {c} IS NOT NULL) AS {c}"  # noqa: E731
        src = f"bars_{base}"
        if frame is not None:
            self.con.register("_rollup_src", frame)
            src = "_rollup_src"
        cols = {r[0] for r in self.con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
        oi_col = f", {last('oi')}" if "oi" in cols else ""                      # futures and options carry open interest
        return self.sql(f"""
            SELECT symbol, market, bucket AS ts,
                   arg_min(open, ts) AS open, max(high) AS high, min(low) AS low, arg_max(close, ts) AS close,
                   sum(volume) AS volume,
                   sum(vwap * volume) FILTER (WHERE vwap IS NOT NULL) / nullif(sum(volume) FILTER (WHERE vwap IS NOT NULL), 0) AS vwap,
                   sum(n_ticks) AS n_ticks, {last('bid')}, {last('ask')}, {last('bid_qty')}, {last('ask_qty')}{oi_col}
            FROM (SELECT *, date_trunc('day', ts) + to_seconds(13500 + CAST(floor((epoch(ts - date_trunc('day', ts)) - 13500) / {int(width_s)}) AS BIGINT) * {int(width_s)}) AS bucket
                  FROM {src} WHERE {' AND '.join(where)})
            GROUP BY symbol, market, bucket ORDER BY symbol, bucket""", params)

    # -- partition discovery (used by the webapp dropdowns) ------------------
    def _tick_files(self, day: str, segment: str | None = None) -> list[Path]:
        """The day's tick files in either layout; with `segment` ("eq" or "fo") the other segment's folder is left out (the older
        layout's single folder holds both, so a reader still filters by symbol)."""
        return tick_store.day_files(self.dir, day, segment)

    def bar_days(self, interval: str = "1s") -> list[str]:
        """Dates (YYYY-MM-DD) that have bars at this interval, newest first."""
        base = self.dir / "bars" / f"interval={interval}"
        days = {p.name.split("=", 1)[1] for p in base.glob("date=*") if any(p.glob("*.parquet"))}
        if interval == "1s":
            days |= tick_store.tick_days(self.dir)
        return sorted(days, reverse=True)

    def fingerprint(self, day: str) -> tuple:
        """Identity of the files a day's results are computed from (name, modification time, size): a cached result is reused only while
        this is unchanged, so a live day recomputes after each flush and a finished day never does."""
        out = []
        for sub, files in (("ticks", self._tick_files(day)), ("greeks", sorted((self.dir / "greeks" / f"date={day}").glob("*.parquet")))):
            for f in files:
                st = f.stat()
                out.append((sub, f"{f.parent.parent.name}/{f.name}", st.st_mtime_ns, st.st_size))
        folder = self.dir / "bars" / "interval=1s" / f"date={day}"
        for f in sorted(folder.glob("*.parquet")):
            st = f.stat()
            out.append(("bars", f.name, st.st_mtime_ns, st.st_size))
        return tuple(out)

    def day_symbol_set(self, day: str, interval: str = "1s") -> set[str]:
        """Symbols that have bars on `day`, without building the bars: for ticks, the symbols with at least one traded row (a quote-only
        row makes no bar).  Reads two columns of the day's file, once per file fingerprint."""
        files = self._tick_files(day)
        if interval == "1s" and files:
            def compute():
                return set(self.sql(f"SELECT DISTINCT symbol FROM {_rp(files)} WHERE ltp IS NOT NULL")["symbol"])
            return _cached(("symbols", str(self.dir), day, self.fingerprint(day)), compute)
        return set(self.bar_symbols(day, interval)["symbol"])

    def bar_symbols(self, day: str, interval: str = "1s") -> pd.DataFrame:
        """Symbols with bars on `day`: columns symbol, bars.  Reads only that day's folder."""
        files = self._tick_files(day)
        if interval == "1s" and files:
            return self.sql(f"SELECT symbol, count(*)::BIGINT AS bars FROM "
                            f"{self._ticks_bars_sql(files)} AS b GROUP BY symbol ORDER BY symbol")
        folder = self.dir / "bars" / f"interval={interval}" / f"date={day}"
        if not any(folder.glob("*.parquet")):
            return pd.DataFrame({"symbol": [], "bars": []})
        return self.sql(f"SELECT symbol, count(*)::BIGINT AS bars FROM (SELECT DISTINCT symbol, ts FROM "
                        f"read_parquet('{folder}/*.parquet', union_by_name=true)) GROUP BY 1 ORDER BY 1")

    def day_bars(self, day: str, symbol: str, interval: str = "1s", width_s: int = 1) -> pd.DataFrame:
        """One instrument's bars for one day at `width_s` (rolled up from the base bars when coarser)."""
        files = self._tick_files(day, tick_store.segment_of(symbol))          # its own segment's file (and the older layout's single file)
        if interval == "1s" and files:
            df = self.sql(f"SELECT * FROM {self._ticks_bars_sql(files, where='symbol = ?')} AS b ORDER BY ts", [symbol])    # filter before the windows run
            return self.rollup(width_s, symbol, frame=df) if width_s > 1 and not df.empty else df
        folder = self.dir / "bars" / f"interval={interval}" / f"date={day}"
        if not any(folder.glob("*.parquet")):
            return pd.DataFrame()
        df = self.sql(f"SELECT * FROM read_parquet('{folder}/*.parquet', union_by_name=true, filename=true) "
                      f"WHERE symbol = ? ORDER BY ts, filename", [symbol])
        df = df.drop_duplicates("ts", keep="last").drop(columns="filename").reset_index(drop=True)   # newest segment wins
        return self.rollup(width_s, symbol, frame=df) if width_s > 1 and not df.empty else df

    def day_ticks(self, day: str, symbol: str) -> pd.DataFrame:
        """One instrument's raw rows for one day, no aggregation, in the shape of a one-second bar so the same charts read it
        (SCRUM-131): ts is the event time (fts, else ts); open = high = low = close = the last traded price known at that row (carried
        over a quote-only row); volume is the rise of the cumulative volume since the previous row (the first row only sets the baseline);
        bid, ask and their sizes and open interest are the row's own.  Rows before the first trade of the day are left out.  Empty when the
        day has no raw ticks (a lake of legacy bars cannot show ticks)."""
        files = self._tick_files(day, tick_store.segment_of(symbol))
        if not files:
            return pd.DataFrame()
        src = _rp(files)
        cols = {r[0] for r in self.con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
        ev, od = self._time_exprs(cols)
        return self.sql(f"""
            WITH raw AS (SELECT symbol, ltp, vol_today, bid, ask, bid_qty, ask_qty, oi, {ev} AS _ev, {od} AS _ord
                         FROM (SELECT *{_pad(cols)} FROM {src}) WHERE symbol = ?),
                 t AS (SELECT *, last_value(ltp IGNORE NULLS) OVER (ORDER BY _ord) AS px FROM raw),
                 u AS (SELECT *, lag(vol_today) OVER (ORDER BY _ord) AS prev FROM t WHERE px IS NOT NULL)    -- as the bars: the first row is the baseline
            SELECT symbol, 'IN' AS market, _ev AS ts, px AS open, px AS high, px AS low, px AS close,
                   CASE WHEN prev IS NULL OR vol_today < prev THEN 0 ELSE vol_today - prev END::BIGINT AS volume,
                   px AS vwap, 1::BIGINT AS n_ticks, bid, ask, bid_qty, ask_qty, 'live' AS src, oi
            FROM u ORDER BY _ord""", [symbol])

    # -- futures and options (runbook section 17) ---------------------------
    def fo_symbols(self) -> set[str]:
        """Every symbol ever in the F&O instrument table (any day, including expired contracts), used to
        separate derivatives from equities when a view wants one or the other.  {} if nothing imported yet."""
        fo_path = self.dir / "instruments" / "fo.parquet"
        if not fo_path.exists():
            return set()
        return set(pd.read_parquet(fo_path, columns=["symbol"])["symbol"])

    def fo_underlyings(self, day: str, interval: str = "1s") -> list[str]:
        """Underlyings with derivative bars on `day` (joins that day's bar symbols against the
        instrument table written by `fo_instruments import`); [] if nothing was ever imported."""
        fo_path = self.dir / "instruments" / "fo.parquet"
        if not fo_path.exists():
            return []
        syms = self.day_symbol_set(day, interval)
        if not syms:
            return []
        inst = pd.read_parquet(fo_path, columns=["symbol", "underlying"])
        return sorted(inst[inst["symbol"].isin(syms)]["underlying"].unique().tolist())

    def _tick_stats(self, files: list[Path], cond: str) -> pd.DataFrame:
        """Per-symbol day stats straight from the ticks (no bar frames): bars (distinct 1-second buckets with a trade), volume (the per-tick
        rise of the cumulative volume, the first tick only sets the baseline, a fall counts as zero), last close, first and last open
        interest.  The same numbers the bars give, from one pass over the matching rows (SCRUM-130: this took 7 s through the bars)."""
        src = _rp(files)
        cols = {r[0] for r in self.con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
        ev, od = self._time_exprs(cols)
        df = self.sql(f"""
            WITH raw AS (SELECT symbol, ltp, vol_today, oi, {ev} AS _ev, {od} AS _ord
                         FROM (SELECT *{_pad(cols)} FROM {src})
                         WHERE ltp IS NOT NULL AND ({cond})),
                 t AS (SELECT *, lag(vol_today) OVER (PARTITION BY symbol, CAST(_ev AS DATE) ORDER BY _ord) AS prev,
                              CAST(floor(epoch(_ev - date_trunc('day', _ev)) - 13500) AS BIGINT) AS bk FROM raw),
                 f AS (SELECT symbol, min(bk) FILTER (WHERE oi IS NOT NULL) AS b0 FROM t GROUP BY symbol)
            SELECT t.symbol, count(DISTINCT bk)::BIGINT AS bars,
                   coalesce(sum(CASE WHEN prev IS NULL OR vol_today < prev THEN 0 ELSE vol_today - prev END), 0)::BIGINT AS volume,
                   arg_max(ltp, _ord) AS close,
                   arg_max(oi, _ord) FILTER (WHERE oi IS NOT NULL) AS oi_last,
                   arg_max(oi, _ord) FILTER (WHERE oi IS NOT NULL AND bk = f.b0) AS oi_first    -- the last reading inside the first bar that has one
            FROM t JOIN f ON t.symbol = f.symbol GROUP BY t.symbol, f.b0""")
        df["oi_change"] = df["oi_last"] - df["oi_first"]
        return df

    def _fo_day_stats(self, day: str, symbols: list[str], interval: str = "1s") -> pd.DataFrame:
        """Per-contract stats for `day`: bars, volume, last close, and open interest first/last/change
        (None when the lake has no `oi` column at all, e.g. before any derivative bars were collected)."""
        cols = ["symbol", "bars", "volume", "close", "oi_last", "oi_first", "oi_change"]
        if not symbols:
            return pd.DataFrame(columns=cols)
        fo_files = self._tick_files(day, "fo")                       # the futures and options file (the older layout's single file also holds stocks)
        if interval == "1s" and fo_files:
            allfo = _cached(("fostats", str(self.dir), day, self.fingerprint(day)),
                            lambda: self._tick_stats(fo_files, f"regexp_matches(symbol, '{FO_REGEX}')"))
            have = allfo[allfo["symbol"].isin(symbols)]
            missing = [x for x in symbols if x not in set(allfo["symbol"])]
            if missing:                                              # a symbol that does not look like a contract: compute it on its own
                names = ", ".join("'" + x.replace("'", "''") + "'" for x in missing)
                have = pd.concat([have, self._tick_stats(self._tick_files(day), f"symbol IN ({names})")], ignore_index=True)
            return have.reset_index(drop=True)
        folder = self.dir / "bars" / f"interval={interval}" / f"date={day}"
        if not any(folder.glob("*.parquet")):
            return pd.DataFrame(columns=cols)
        pattern = f"{folder}/*.parquet"
        have = {r[0] for r in self.con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{pattern}', union_by_name=true)").fetchall()}
        oi_sql = (", arg_max(oi, ts) FILTER (WHERE oi IS NOT NULL) AS oi_last, "
                 "arg_min(oi, ts) FILTER (WHERE oi IS NOT NULL) AS oi_first") if "oi" in have \
            else ", NULL AS oi_last, NULL AS oi_first"
        self.con.register("_fo_wanted", pd.DataFrame({"symbol": symbols}))
        try:
            df = self.sql(f"""
                WITH raw AS (
                  SELECT * FROM read_parquet('{pattern}', union_by_name=true, filename=true)
                  WHERE symbol IN (SELECT symbol FROM _fo_wanted)
                  QUALIFY row_number() OVER (PARTITION BY symbol, ts ORDER BY filename DESC) = 1)
                SELECT symbol, count(*)::BIGINT AS bars, coalesce(sum(volume), 0)::BIGINT AS volume,
                       arg_max(close, ts) AS close{oi_sql}
                FROM raw GROUP BY symbol""")
        finally:
            self.con.unregister("_fo_wanted")
        df["oi_change"] = df["oi_last"] - df["oi_first"] if "oi" in have else None
        return df

    # -- option Greeks (stored on raw ticks only; absent on days collected before they were stored) -------
    GREEKS = ("iv", "delta", "theta", "gamma", "vega", "rho")
    CORE_GREEKS = ("iv", "delta", "gamma")        # what the production feed stores (JARVIS_GREEKS=core); theta, vega, rho are computed

    def _greek_source(self, day: str) -> str | None:
        """SQL source (a read_parquet expression) for the day's rows that carry Greeks, or None.  Reads the raw tick
        files and, if present, `greeks/date=<day>/*.parquet` (Greeks-only rows: ts, symbol and the six columns;
        the demo lake keeps its synthetic Greeks there so its bars stay untouched)."""
        files = [*self._tick_files(day, "fo"), *sorted((self.dir / "greeks" / f"date={day}").glob("*.parquet"))]      # only options carry Greeks
        if not files:
            return None
        src = _rp(files)
        cols = {r[0] for r in self.con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
        return src if set(self.CORE_GREEKS) <= cols else None

    def _greek_exprs(self, src: str) -> tuple[str, str, str]:
        """(the last-value-per-group expression for each Greek, event time, arrival order).  A Greek the files do not carry (theta, vega
        and rho under JARVIS_GREEKS=core) is NULL here and computed afterwards."""
        cols = {r[0] for r in self.con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
        ev, od = self._time_exprs(cols)
        exprs = ", ".join(f"arg_max({g}, _ord) FILTER (WHERE {g} IS NOT NULL) AS {g}" if g in cols else f"CAST(NULL AS DOUBLE) AS {g}"
                          for g in self.GREEKS)
        return exprs, ev, od

    def _underlying_price(self, day: str, symbol: str, ts: pd.Series, width_s: int) -> pd.Series | None:
        """Spot implied by the nearest live future of this option's underlying at each time in `ts` (sorted ascending):
        the last bar at or before it, less the future's carry (greeks_calc.spot_from_future).  None when the symbol is not
        an option or that future has no bars on the day."""
        from .greeks_calc import parse_future, parse_option, spot_from_future
        meta = parse_option(symbol)
        if meta is None:
            return None
        today = datetime.strptime(day, "%Y-%m-%d").date()
        cands = sorted((f[1], sym) for sym in self.fo_symbols() if (f := parse_future(sym)) and f[0] == meta[0] and f[1] >= today)
        for fexp, sym in cands:
            bars = self.day_bars(day, sym, "1s", width_s)
            if bars.empty:
                continue
            bars = bars[["ts", "close"]].dropna().sort_values("ts")
            left = pd.DataFrame({"ts": pd.to_datetime(ts).to_numpy()})
            merged = pd.merge_asof(left, bars, on="ts", direction="backward")
            return pd.Series(spot_from_future(merged["close"], left["ts"], fexp), index=merged.index)
        return None

    def _with_computed(self, df: pd.DataFrame, day: str, symbol: str, width_s: int) -> pd.DataFrame:
        """Fill the Greeks the feed did not store (theta, vega, rho under the core setting) from IV and the underlying."""
        from .greeks_calc import add_computed
        df.attrs["computed"] = []
        if df.empty or not any(df[g].isna().all() for g in ("theta", "vega", "rho")):
            return df
        under = self._underlying_price(day, symbol, df["ts"], width_s)
        if under is None:
            return df
        out = add_computed(df.sort_values("ts").reset_index(drop=True), symbol, under)
        keep = [g for g in out.attrs.get("computed", []) if out[g].notna().any()]
        out.attrs["computed"] = keep
        return out

    def greeks_series(self, day: str, symbol: str, width_s: int = 1) -> pd.DataFrame:
        """IV, delta, theta, gamma, vega, rho of one option, last value per bar of `width_s` seconds (bars aligned
        to the 03:45 UTC open like every other series).  Empty when the day has no Greeks."""
        src = self._greek_source(day)
        if src is None:
            return pd.DataFrame(columns=["ts", *self.GREEKS])
        last, ev, od = self._greek_exprs(src)
        if width_s == 0:                                                   # ticks: every row that carries a Greek, as it arrived
            have = {r[0] for r in self.con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
            sel = ", ".join(g if g in have else f"CAST(NULL AS DOUBLE) AS {g}" for g in self.GREEKS)
            df = self.con.execute(f"""
                SELECT _ev AS ts, {sel} FROM (SELECT *, {ev} AS _ev, {od} AS _ord FROM {src} WHERE symbol = ?)
                WHERE {' OR '.join(f"{g} IS NOT NULL" for g in self.CORE_GREEKS if g in have)} ORDER BY _ord""", [symbol]).df()
        else:
            df = self.con.execute(f"""
                WITH t AS (SELECT *, {ev} AS _ev, {od} AS _ord FROM {src} WHERE symbol = ?),
                     b AS (SELECT *, date_trunc('day', _ev) + to_seconds(13500 + CAST(floor(
                                  (epoch(_ev - date_trunc('day', _ev)) - 13500) / {int(width_s)}) AS BIGINT) * {int(width_s)}
                                ) AS bucket FROM t)
                SELECT bucket AS ts, {last} FROM b GROUP BY bucket ORDER BY bucket""", [symbol]).df()
        from .greeks_calc import iv_to_percent
        df = self._with_computed(df, day, symbol, max(width_s, 1))          # the underlying is read at one-second bars in ticks mode
        computed = df.attrs.get("computed", [])
        df["iv"] = iv_to_percent(df["iv"])                                  # stored as delivered (a fraction); shown as a percent
        df.attrs["computed"] = computed
        return df

    def greeks_last(self, day: str, symbols: list[str]) -> dict[str, dict]:
        """{symbol: {iv, delta, ...}} -- the last non-null value of each Greek that day, for chain columns."""
        key = ("greeks_last", str(self.dir), day, self.fingerprint(day), tuple(sorted(symbols)))
        if key in _CACHE:
            return _CACHE[key]
        out = self._greeks_last(day, symbols)
        _CACHE[key] = out
        if len(_CACHE) > _CACHE_MAX:
            _CACHE.pop(next(iter(_CACHE)))
        return out

    def _greeks_last(self, day: str, symbols: list[str]) -> dict[str, dict]:
        src = self._greek_source(day)
        if src is None or not symbols:
            return {}
        last, ev, od = self._greek_exprs(src)
        self.con.register("_g_wanted", pd.DataFrame({"symbol": symbols}))
        try:
            df = self.con.execute(f"""
                SELECT symbol, {last} FROM (SELECT *, {ev} AS _ev, {od} AS _ord FROM {src})
                WHERE symbol IN (SELECT symbol FROM _g_wanted) GROUP BY symbol""").df()
        finally:
            self.con.unregister("_g_wanted")
        from .greeks_calc import iv_to_percent
        out = {r["symbol"]: {g: (None if pd.isna(r[g]) else float(r[g])) for g in self.GREEKS} for r in df.to_dict("records")}
        for v in out.values():
            if v["iv"] is not None:
                v["iv"] = float(iv_to_percent(v["iv"]))                     # stored as delivered (a fraction); shown as a percent
        return out

    def fo_chain(self, day: str, underlying: str, expiry: str | None = None,
                interval: str = "1s") -> dict:
        """
        Futures + option chain snapshot for one underlying on `day`.  `expiry` picks which option
        expiry's chain to return; defaults to the nearest one with bars that day.  Spot is a proxy:
        the nearest future's last close (no index LTP is collected).  {} if nothing to show.
        """
        fo_path = self.dir / "instruments" / "fo.parquet"
        if not fo_path.exists():
            return {}
        inst = pd.read_parquet(fo_path)
        inst = inst[inst["underlying"] == underlying]
        syms = self.day_symbol_set(day, interval)
        if not syms or inst.empty:
            return {}
        inst = inst[inst["symbol"].isin(syms)]
        if inst.empty:
            return {}
        stats = self._fo_day_stats(day, inst["symbol"].tolist(), interval)
        inst = inst.merge(stats, on="symbol", how="left")

        fut = inst[inst["instrument_type"] == "FUT"].sort_values("expiry")
        opts = inst[inst["instrument_type"].isin(["CE", "PE"])]
        expiries = sorted(opts["expiry"].astype(str).unique().tolist())
        if expiry not in expiries:
            expiry = expiries[0] if expiries else None

        def num(v):
            return None if pd.isna(v) else (int(v) if float(v).is_integer() else float(v))

        fut_rows = [{"symbol": r.symbol, "expiry": str(r.expiry), "close": num(r.close),
                    "oi": num(r.oi_last), "oi_change": num(r.oi_change), "volume": num(r.volume),
                    "bars": num(r.bars)} for r in fut.itertuples()]
        spot = fut_rows[0]["close"] if fut_rows else None

        chain, pcr, atm, total_call_oi, total_put_oi = [], None, None, 0, 0
        if expiry:
            exp_opts = opts[opts["expiry"].astype(str) == expiry]
            ce = exp_opts[exp_opts["instrument_type"] == "CE"].set_index("strike")
            pe = exp_opts[exp_opts["instrument_type"] == "PE"].set_index("strike")
            strikes = sorted(set(ce.index) | set(pe.index))
            atm = min(strikes, key=lambda k: abs(k - spot)) if spot and strikes else None
            gk = self.greeks_last(day, exp_opts["symbol"].tolist())
            side = lambda row: {"symbol": row.symbol, "close": num(row.close), "oi": num(row.oi_last),  # noqa: E731
                                "oi_change": num(row.oi_change), "volume": num(row.volume),
                                "iv": (gk.get(row.symbol) or {}).get("iv"),
                                "delta": (gk.get(row.symbol) or {}).get("delta")} if row is not None else {}
            for k in strikes:
                c = ce.loc[k] if k in ce.index else None
                p = pe.loc[k] if k in pe.index else None
                chain.append({"strike": num(k), **{f"ce_{kk}": vv for kk, vv in side(c).items()},
                              **{f"pe_{kk}": vv for kk, vv in side(p).items()}})
            total_call_oi = int(ce["oi_last"].dropna().sum())
            total_put_oi = int(pe["oi_last"].dropna().sum())
            pcr = round(total_put_oi / total_call_oi, 3) if total_call_oi else None

        lots = inst["lot_size"].dropna() if "lot_size" in inst.columns else pd.Series(dtype=float)
        return {"underlying": underlying, "date": day, "expiry": expiry, "expiries": expiries,
                "lot_size": num(lots.iloc[0]) if len(lots) else None,
                "spot": spot, "atm_strike": num(atm) if atm is not None else None,
                "futures": fut_rows, "chain": chain, "pcr": pcr,
                "total_call_oi": total_call_oi or None, "total_put_oi": total_put_oi or None}

    def day_overview(self, day: str, interval: str = "1s", equities_only: bool = False) -> pd.DataFrame:
        """
        One row per instrument for `day`: bars, volume, vwap, open/high/low/close, return %, average and
        median spread in bps, realised volatility % (annualised), average book sizes.  `equities_only` leaves the futures and options
        out before any bar is built (they are most of the rows, and the Market tab does not show them).
        """
        files = self._tick_files(day, "eq" if equities_only else None)         # only the stocks' file: the options are not even opened
        if interval == "1s" and files:
            where = f"NOT regexp_matches(symbol, '{FO_REGEX}')" if equities_only else None
            raw_cte = f"raw AS {self._ticks_bars_sql(files, where=where)}"
        else:
            folder = self.dir / "bars" / f"interval={interval}" / f"date={day}"
            if not any(folder.glob("*.parquet")):
                return pd.DataFrame()
            raw_cte = (f"raw AS (SELECT * FROM read_parquet('{folder}/*.parquet', union_by_name=true, filename=true) "
                      f"QUALIFY row_number() OVER (PARTITION BY symbol, ts ORDER BY filename DESC) = 1)")
        return self.sql(f"""
            WITH {raw_cte},
            r AS (
              SELECT *, CASE WHEN close > 0 AND lag(close) OVER w > 0 THEN ln(close / lag(close) OVER w) END AS lr,
                     CASE WHEN bid > 0 AND ask >= bid THEN (ask - bid) / ((ask + bid) / 2) * 10000 END AS sbps
              FROM raw WINDOW w AS (PARTITION BY symbol ORDER BY ts))
            SELECT symbol, count(*)::BIGINT AS bars, coalesce(sum(volume), 0)::BIGINT AS volume,
                   sum(vwap * volume) FILTER (WHERE vwap IS NOT NULL AND volume > 0)
                     / nullif(sum(volume) FILTER (WHERE vwap IS NOT NULL AND volume > 0), 0) AS vwap,
                   arg_min(open, ts) AS open, max(high) AS high, min(low) AS low, arg_max(close, ts) AS close,
                   (arg_max(close, ts) / arg_min(open, ts) - 1) * 100 AS return_pct,
                   avg(sbps) AS avg_spread_bps, median(sbps) AS median_spread_bps,
                   avg(bid_qty) AS avg_bid_qty, avg(ask_qty) AS avg_ask_qty,
                   sqrt(coalesce(sum(lr * lr), 0) * {TRADING_DAYS}) * 100 AS realised_vol_pct
            FROM r GROUP BY symbol ORDER BY symbol""")

    # -- fundamentals -------------------------------------------------------
    def fundamentals(self, symbol: str) -> dict[str, pd.DataFrame]:
        return {t: self.sql(f"SELECT * FROM {t} WHERE symbol = ? ORDER BY date DESC", [symbol])
                for t in ("income_statement", "balance_sheet", "cash_flow", "ratios")}

    def pillar_metrics(self, market: str | None = None) -> pd.DataFrame:
        return self.sql("SELECT * FROM pillar_metrics" + (" WHERE market = ?" if market else "")
                        + " ORDER BY symbol", [market] if market else [])

    # -- scoring ------------------------------------------------------------
    def score_ticker(self, symbol: str, price: float) -> int:
        """
        8-pillar score, 0-8.  "Growth"/"decrease" pillars compare the newest
        annual row with the oldest one available (ordered by date, explicitly).
        """
        m = self.sql("SELECT * FROM pillar_metrics WHERE symbol = ?", [symbol])
        if m.empty or price is None:
            return 0
        m = m.iloc[0]

        def num(col):
            v = m[col]
            return None if pd.isna(v) else float(v)

        def grew(table: str, col: str, decreasing: bool = False) -> bool:
            s = self.sql(f"SELECT {col} v FROM {table} WHERE symbol = ? AND {col} IS NOT NULL "
                         f"ORDER BY date", [symbol])["v"]
            if len(s) < 2:
                return False
            return bool(s.iloc[-1] < s.iloc[0]) if decreasing else bool(s.iloc[-1] > s.iloc[0])

        shares, ni, fcf = num("latest_shares_outstanding"), num("avg_net_income_5y"), num("avg_fcf_5y")
        ltd, roic = num("latest_long_term_debt"), num("latest_roic")
        mkt_cap = price * shares if shares else None

        pillars = [
            bool(mkt_cap and ni and ni > 0 and mkt_cap / ni < PE_MAX),                 # 1 avg P/E
            bool(roic is not None and roic > ROIC_MIN),                                  # 2 ROIC
            grew("income_statement", "revenue"),                                         # 3 revenue growth
            grew("income_statement", "net_income"),                                      # 4 net income growth
            grew("ratios", "shares_outstanding", decreasing=True),                       # 5 shares falling
            bool(fcf and fcf > 0 and ltd is not None and ltd / fcf < LT_DEBT_FCF_MAX),   # 6 LT debt / FCF
            grew("cash_flow", "free_cash_flow"),                                         # 7 FCF growth
            bool(mkt_cap and fcf and fcf > 0 and mkt_cap / fcf < PFCF_MAX),              # 8 avg P/FCF
        ]
        return sum(pillars)

    def score_all(self, market: str | None = None) -> pd.DataFrame:
        """symbol, market, price, score — sorted by score desc (symbols with no price score 0)."""
        pm = self.pillar_metrics(market)[["symbol", "market"]]
        px = self.latest_prices(market)[["symbol", "close"]].rename(columns={"close": "price"})
        df = pm.merge(px, on="symbol", how="left")
        df["score"] = [self.score_ticker(s, p) if pd.notna(p) else 0
                       for s, p in zip(df["symbol"], df["price"])]
        return df.sort_values(["score", "symbol"], ascending=[False, True]).reset_index(drop=True)

    def top_by_score(self, market: str | None, n: int = 10) -> pd.DataFrame:
        return self.score_all(market).head(n)
