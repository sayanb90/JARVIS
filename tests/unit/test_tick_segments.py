"""The tick lake by segment (SCRUM-133): lake/ticks/segment=eq|fo/date=DAY, one folder per feed, stocks without option columns, the older
single-folder layout still readable, and the migration that converts it."""
from __future__ import annotations

import threading
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
import pytest

from data_layer import tick_store
from data_layer.query import Lake
from data_layer.ticks import Tick
from tests.helpers import tick_file, tick_folder

DAY = date(2026, 11, 4)
D = str(DAY)
T0 = datetime(2026, 11, 4, 4, 0, 0)
OPT, FUT, STK = "NIFTY_25000_CE_09_NOV_26", "NIFTY_FUT_23_NOV_26", "A.NS"


def stock(i, px=100.0, **kw):
    t = T0 + timedelta(seconds=i)
    return Tick(t, STK, px + i * 0.1, 10 * (i + 1), 99.9, 100, 100.1, 50, None, delta=kw.pop("delta", 0.0), gamma=kw.pop("gamma", 0.0), fts=t, rts=t, **kw)


def option(i, sym=OPT):
    t = T0 + timedelta(seconds=i)
    return Tick(t, sym, 120.0 + i, 7 * (i + 1), 119.9, 75, 120.1, 75, 1000 + i, iv=0.15, delta=0.5, gamma=0.0005, fts=t, rts=t)


def cols(f) -> set[str]:
    return set(duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{f}')").df()["column_name"])


def mixed(n=20):
    return [x for i in range(n) for x in (stock(i), option(i), option(i, FUT))]


# ---------------------------------------------------------------------------------------------- writing
def test_each_tick_goes_to_its_segment_and_nothing_to_the_older_folder(tmp_path):
    assert tick_store.write_ticks(mixed(), tmp_path) == 60
    eq, fo = (list(tick_folder(tmp_path, D, s).glob("*.parquet")) for s in ("eq", "fo"))
    assert len(eq) == 1 and len(fo) == 1 and not (tmp_path / "ticks" / f"date={D}").exists()
    count = lambda f, sql="": duckdb.sql(f"SELECT count(*) FROM read_parquet('{f}') {sql}").fetchall()[0][0]    # noqa: E731
    assert count(eq[0]) == 20 and count(fo[0]) == 40 and count(eq[0], f"WHERE symbol = '{STK}'") == 20
    assert [tick_store.segment_of(s) for s in (STK, OPT, FUT, "OPT_CE", "SBIN.NS")] == ["eq", "fo", "fo", "eq", "eq"]


def test_a_stock_file_leaves_out_the_option_columns_but_keeps_real_ones(tmp_path):
    tick_store.write_ticks(mixed(), tmp_path)
    assert not cols(next(tick_folder(tmp_path, D, "eq").glob("*.parquet"))) & {"iv", "delta", "gamma", "theta", "vega", "rho", "oi"}
    assert {"iv", "delta", "gamma", "theta", "vega", "rho", "oi"} <= cols(next(tick_folder(tmp_path, D, "fo").glob("*.parquet")))
    other = tmp_path / "b"
    tick_store.write_ticks([stock(0, delta=0.3, gamma=0.001)], other)                    # a real value is never dropped
    assert {"delta", "gamma"} <= cols(next(tick_folder(other, D, "eq").glob("*.parquet")))


# ---------------------------------------------------------------------------------------------- compacting
def test_each_feed_compacts_only_its_own_folder_and_two_at_once_need_no_shared_lock(tmp_path):
    for _ in range(3):
        tick_store.write_ticks(mixed(5), tmp_path)
    assert tick_store.compact_ticks_day(DAY, tmp_path, "eq") == 15
    assert [p.name for p in tick_folder(tmp_path, D, "eq").iterdir()] == ["ticks.parquet"]
    assert len(list(tick_folder(tmp_path, D, "fo").glob("seg-*.parquet"))) == 3          # the F&O folder is untouched
    out, errors = {}, []

    def run(seg):
        try:
            out[seg] = tick_store.compact_ticks_day(DAY, tmp_path, seg)
        except Exception as exc:                                                        # noqa: BLE001
            errors.append(exc)
    threads = [threading.Thread(target=run, args=(s,)) for s in ("eq", "fo")]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors and out == {"eq": 15, "fo": 30}
    assert tick_store.compact_ticks_day(DAY, tmp_path) == 45                            # no segment: every folder of the day


# ---------------------------------------------------------------------------------------------- reading
def test_the_lake_reads_the_segment_layout_like_the_old_one(tmp_path):
    tick_store.write_ticks(mixed(), tmp_path)
    tick_store.compact_ticks_day(DAY, tmp_path)
    q = Lake(tmp_path)
    assert q.bar_days() == [D] and q.has_ticks
    assert sorted(q.bar_symbols(D)["symbol"]) == sorted([STK, OPT, FUT])
    assert len(q.day_bars(D, STK)) == 20 and len(q.day_bars(D, OPT)) == 20            # a stock file has no open interest column: still works
    assert q.day_bars(D, STK)["oi"].isna().all() and q.day_bars(D, OPT)["oi"].notna().all()
    assert len(q.day_ticks(D, STK)) == 20 and len(q.day_ticks(D, OPT)) == 20
    assert list(q.day_overview(D, equities_only=True)["symbol"]) == [STK]
    st = q._fo_day_stats(D, [OPT, FUT]).set_index("symbol")
    assert st.loc[OPT, "bars"] == 20 and st.loc[OPT, "oi_last"] == 1019
    assert q.day_symbol_set(D) == {STK, OPT, FUT}
    assert set(q.greeks_series(D, OPT, 1).columns) >= {"iv", "delta", "gamma"} and len(q.greeks_series(D, OPT, 0)) == 20
    before = q.fingerprint(D)
    tick_store.write_ticks([option(30)], tmp_path)
    assert Lake(tmp_path).fingerprint(D) != before                                      # a new file changes the cache key


# ---------------------------------------------------------------------------------------------- migrating
def old_layout_day(lake):
    """What 2026-11-03 and 2026-11-04 look like on the VPS: one compacted file, stocks with constant zero delta and gamma."""
    rows = []
    for i in range(30):
        t = T0 + timedelta(seconds=i)
        rows.append({"ts": t, "symbol": STK, "ltp": 100.0 + i * 0.1, "vol_today": 10 * (i + 1), "bid": 99.9, "bid_qty": 100, "ask": 100.1, "ask_qty": 50,
                     "oi": None, "iv": None, "delta": 0.0, "gamma": 0.0, "theta": None, "vega": None, "rho": None, "fts": t, "rts": t})
        for sym in (OPT, FUT):
            rows.append({"ts": t, "symbol": sym, "ltp": 120.0 + i, "vol_today": 7 * (i + 1), "bid": 119.9, "bid_qty": 75, "ask": 120.1, "ask_qty": 75,
                         "oi": 1000 + i, "iv": 0.15, "delta": 0.5, "gamma": 0.0005, "theta": None, "vega": None, "rho": None, "fts": t, "rts": t})
    folder = lake / "ticks" / f"date={D}"
    folder.mkdir(parents=True)
    pd.DataFrame(rows).sort_values(["symbol", "rts"]).to_parquet(folder / "ticks.parquet")
    return len(rows)


def test_splitting_a_day_keeps_every_row_and_every_answer(tmp_path):
    n = old_layout_day(tmp_path)
    before = Lake(tmp_path)
    ref = {"overview": before.day_overview(D).set_index("symbol"), "bars": before.day_bars(D, OPT, "1s", 5),
           "stats": before._fo_day_stats(D, [OPT, FUT]).set_index("symbol"), "ticks": before.day_ticks(D, STK)}
    r = tick_store.split_ticks_day(DAY, tmp_path)
    assert r["rows"] == n == sum(v["rows"] for v in r["segments"].values()) and r["segments"]["eq"]["rows"] == 30 and r["segments"]["fo"]["rows"] == 60
    assert {"iv", "delta", "gamma", "theta", "vega", "rho", "oi"} <= set(r["segments"]["eq"]["dropped"]) and not r["segments"]["fo"]["dropped"]
    assert not cols(tick_file(tmp_path, D, "eq")) & {"iv", "delta", "gamma", "oi"} and "oi" in cols(tick_file(tmp_path, D, "fo"))
    assert not (tmp_path / "ticks" / f"date={D}").exists() and (tmp_path / "_pre_split" / f"date={D}" / "ticks.parquet").exists()      # moved, not deleted
    after = Lake(tmp_path)
    pd.testing.assert_frame_equal(after.day_overview(D).set_index("symbol"), ref["overview"])
    pd.testing.assert_frame_equal(after.day_bars(D, OPT, "1s", 5), ref["bars"])
    pd.testing.assert_frame_equal(after._fo_day_stats(D, [OPT, FUT]).set_index("symbol"), ref["stats"])
    pd.testing.assert_frame_equal(after.day_ticks(D, STK), ref["ticks"])
    assert tick_store.split_ticks_day(DAY, tmp_path)["skipped"]                         # a second run finds nothing to do


def test_splitting_refuses_what_it_cannot_do_safely(tmp_path):
    old_layout_day(tmp_path)
    (tmp_path / "ticks" / f"date={D}" / "seg-1.parquet").write_bytes(b"x")
    with pytest.raises(RuntimeError, match="uncompacted"):
        tick_store.split_ticks_day(DAY, tmp_path)
    (tmp_path / "ticks" / f"date={D}" / "seg-1.parquet").unlink()
    tick_store.write_ticks([stock(40)], tmp_path)                                       # a segment folder already exists for the day
    with pytest.raises(RuntimeError, match="already has segment folders"):
        tick_store.split_ticks_day(DAY, tmp_path)
    assert (tmp_path / "ticks" / f"date={D}" / "ticks.parquet").exists()                # the original was not touched


def test_the_split_command_prints_sizes_and_converts_every_old_day(tmp_path, capsys):
    old_layout_day(tmp_path)
    assert tick_store.main(["split", "--lake", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "90 rows" in out and "eq:" in out and "fo:" in out and "checked row for row" in out
    assert tick_store.main(["split", "--lake", str(tmp_path)]) == 0 and "nothing to split" in capsys.readouterr().out
