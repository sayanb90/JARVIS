"""Tick -> bar aggregation edge cases and Parquet segment storage."""
from datetime import date, datetime, timedelta

import duckdb
import pytest

from data_layer.ticks import BarAggregator, Tick

D = (2026, 10, 23)


def tk(h, m, s, px, vtt=None, sym="A.NS", **kw):
    return Tick(datetime(*D, h, m, s), sym, px, vtt, **kw)


def test_rejects_non_positive_interval():
    with pytest.raises(ValueError):
        BarAggregator(0)


@pytest.mark.parametrize("h,m,kept", [(3, 44, False), (3, 45, True), (9, 59, True), (10, 0, False), (10, 1, False)])
def test_session_boundaries_are_open_closed(h, m, kept):
    ag = BarAggregator(60)
    ag.add(tk(h, m, 0, 100.0, 1))
    assert bool(ag.flush_all()) is kept


def test_symbols_are_independent():
    ag = BarAggregator(60)
    ag.add(tk(3, 45, 1, 10, 1, "A.NS")); ag.add(tk(3, 45, 2, 20, 1, "B.NS"))
    out = ag.add(tk(3, 46, 1, 11, 2, "A.NS"))
    assert [b.symbol for b in out] == ["A.NS"]                 # B's bar is still open
    assert sorted(b.symbol for b in ag.flush_all()) == ["A.NS", "B.NS"]


def test_volume_baseline_reset_and_new_day():
    ag = BarAggregator(60)
    bars = []
    bars += ag.add(tk(3, 45, 0, 10, 1000))                      # baseline
    bars += ag.add(tk(3, 45, 10, 10, 900))                      # cumulative went DOWN (feed reset): delta 0, not negative
    bars += ag.add(tk(3, 45, 20, 10, 950))                      # +50 after the reset
    bars += ag.add(tk(3, 46, 0, 10, 960))
    assert bars[0].volume == 50
    next_day = Tick(datetime(2026, 10, 26, 3, 45, 5), "A.NS", 11, 5)
    ag.add(next_day)                                            # new date: baseline again, not a 5 - 960 delta
    assert ag.add(Tick(datetime(2026, 10, 26, 3, 46, 5), "A.NS", 11, 9))[0].volume == 0


def test_no_volume_info_gives_zero_volume_and_vwap_equals_close():
    ag = BarAggregator(60)
    ag.add(tk(3, 45, 0, 10.0)); ag.add(tk(3, 45, 30, 12.0))
    (b,) = ag.flush_all()
    assert (b.volume, b.vwap, b.n_ticks, b.close) == (0, 12.0, 2, 12.0)


def test_l1_only_updates_when_present_and_keeps_last_seen():
    ag = BarAggregator(60)
    ag.add(tk(3, 45, 0, 10, 1, bid=9.9, bid_qty=4, ask=10.1, ask_qty=6))
    ag.add(tk(3, 45, 30, 10.05, 2))                              # tick without L1 must not erase it
    (b,) = ag.flush_all()
    assert (b.bid, b.ask, b.bid_qty, b.ask_qty) == (9.9, 10.1, 4, 6)


def test_flush_only_closes_finished_buckets_and_is_idempotent():
    ag = BarAggregator(300)
    ag.add(tk(3, 45, 0, 10, 1, "A.NS")); ag.add(tk(3, 50, 0, 11, 2, "B.NS"))
    assert ag.flush(datetime(*D, 3, 49, 59)) == []
    assert [b.ts.minute for b in ag.flush(datetime(*D, 3, 50, 0))] == [45]          # A's bucket ended at 03:50
    assert [b.ts.minute for b in ag.flush(datetime(*D, 3, 55, 0))] == [50]
    assert ag.flush(datetime(*D, 4, 0, 0)) == []


def test_gap_in_ticks_produces_no_forward_fill():
    ag = BarAggregator(60)
    ag.add(tk(3, 45, 0, 10, 1))
    out = ag.add(tk(3, 50, 0, 11, 2))
    assert len(out) == 1 and out[0].ts.minute == 45             # minutes 46-49 simply do not exist


def test_market_label_is_configurable():
    ag = BarAggregator(60, market="IN")
    ag.add(tk(3, 45, 0, 10)); assert ag.flush_all()[0].market == "IN"


# -- tick_store ------------------------------------------------------------------------------------
def bars_for(*minutes, sym="A.NS", base=10.0):
    ag = BarAggregator(60)
    out = []
    for i, m in enumerate(minutes):
        out += ag.add(tk(3, m, 0, base + i, 100 * (i + 1), sym))
    return out + ag.flush_all()


def test_write_bars_empty_and_multi_day_split(world):
    assert world.tick_store.write_bars([], "1m") == 0
    d1 = bars_for(45, 46)
    ag = BarAggregator(60)
    ag.add(Tick(datetime(2026, 10, 26, 3, 45), "A.NS", 5, 1)); d2 = ag.flush_all()
    assert world.tick_store.write_bars(d1 + d2, "1m") == 3
    days = sorted(p.name for p in (world.lakedir / "bars/interval=1m").iterdir())
    assert days == ["date=2026-10-23", "date=2026-10-26"]


def test_compact_without_files_and_idempotent_second_run(world):
    assert world.tick_store.compact_day("1m", date(2026, 10, 23)) == 0
    world.tick_store.write_bars(bars_for(45, 46), "1m")
    assert world.tick_store.compact_day("1m", date(2026, 10, 23)) == 2
    assert world.tick_store.compact_day("1m", date(2026, 10, 23)) == 2         # no segments left: still 2, no dupes
    folder = world.lakedir / "bars/interval=1m/date=2026-10-23"
    assert [p.name for p in folder.iterdir()] == ["bars.parquet"]


def test_compact_merges_new_segments_into_existing_file_newest_wins(world):
    world.tick_store.write_bars(bars_for(45, 46, base=10), "1m")
    world.tick_store.compact_day("1m", date(2026, 10, 23))
    world.tick_store.write_bars(bars_for(46, 47, base=500), "1m")              # overlaps 03:46, adds 03:47
    n = world.tick_store.compact_day("1m", date(2026, 10, 23))
    rows = duckdb.sql(f"SELECT ts, close FROM read_parquet('{world.lakedir}/bars/interval=1m/date=2026-10-23/bars.parquet') ORDER BY ts").fetchall()
    assert n == 3 and [r[1] for r in rows] == [10.0, 500.0, 501.0]


def test_write_ticks_keeps_l1_columns(world):
    t = [tk(3, 45, 1, 10, 5, bid=9.9, bid_qty=3, ask=10.1, ask_qty=4, oi=7)]
    assert world.tick_store.write_ticks(t) == 1
    (row,) = duckdb.sql(f"SELECT bid, bid_qty, ask, ask_qty, oi FROM read_parquet('{world.lakedir}/ticks/date=2026-10-23/*.parquet')").fetchall()
    assert row == (9.9, 3, 10.1, 4, 7)
    assert world.tick_store.write_ticks([]) == 0


def test_segments_are_never_half_written(world):
    world.tick_store.write_bars(bars_for(45), "1m")
    assert not list(world.lakedir.rglob("*.tmp"))
