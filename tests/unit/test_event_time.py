"""Event time (SCRUM-116): every tick carries the message time (fts) and our arrival time (rts) next to the last-trade time (ts);
quote-only rows keep quotes and Greeks of instruments that have not traded; readers use event time and read older files as before."""
from __future__ import annotations

from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
import pytest

from data_layer import tick_store
from data_layer.query import Lake
from data_layer.tick_policy import TickPolicy
from data_layer.ticks import Tick
from data_layer.upstox import decode, feed
from data_layer.upstox.proto import MarketDataFeedV3_pb2 as pb
from tests.helpers import ms

KEY = "NSE_EQ|INE000A01010"
D = (2026, 11, 3)


def frame(current=None, ltt=None, ltp=100.0, bid=99.9, key=KEY, kind="full") -> bytes:
    r = pb.FeedResponse(type=pb.live_feed)
    if current is not None:
        r.currentTs = current
    if kind == "full":
        m = r.feeds[key].fullFeed.marketFF
        m.ltpc.ltp, m.ltpc.ltt, m.ltpc.cp = ltp, ltt or 0, 98.0
        m.vtt = 1000
        q = m.marketLevel.bidAskQuote.add()
        q.bidP, q.bidQ, q.askP, q.askQ = bid, 5, bid + 0.2, 6
    elif kind == "index":
        i = r.feeds[key].fullFeed.indexFF
        i.ltpc.ltp, i.ltpc.ltt = ltp, ltt or 0
    elif kind == "greeks":
        g = r.feeds[key].firstLevelWithGreeks
        g.ltpc.ltp, g.ltpc.ltt = ltp, ltt or 0
        g.optionGreeks.delta = 0.5
        g.iv = 0.2
    else:
        r.feeds[key].ltpc.ltp, r.feeds[key].ltpc.ltt = ltp, ltt or 0
    return r.SerializeToString()


def ticks(data: bytes, **kw) -> list[Tick]:
    return list(decode.ticks_from_response(decode.parse_frame(data), {KEY: "A.NS"}, **kw))


def test_every_feed_type_carries_the_message_time_next_to_the_last_trade_time():
    for kind in ("full", "index", "greeks", "ltpc"):
        t = ticks(frame(current=ms(4, 0, 30, D), ltt=ms(4, 0, 1, D), kind=kind))[0]
        assert t.ts == datetime(2026, 11, 3, 4, 0, 1) and t.fts == datetime(2026, 11, 3, 4, 0, 30), kind     # trade time and event time differ
    assert ticks(frame(ltt=ms(4, 0, 1, D)))[0].fts is None                                                    # a frame without currentTs: no event time


def test_an_instrument_that_has_not_traded_becomes_a_quote_only_row():
    q = ticks(frame(current=ms(3, 50, 0, D), ltt=None, ltp=0.0))[0]
    assert q.ltp is None and q.ts == q.fts == datetime(2026, 11, 3, 3, 50, 0) and q.bid == 99.9           # quotes kept, no price
    assert ticks(frame(current=None, ltt=None, ltp=0.0)) == []                                             # nothing to time it with
    g = ticks(frame(current=ms(4, 0, 0, D), ltt=None, ltp=0.0, kind="greeks"))[0]
    assert g.ltp is None and g.delta == 0.5 and g.iv == pytest.approx(0.2)                                  # Greeks of an untraded option kept


def test_the_processor_stamps_arrival_time_and_turns_a_stale_trade_into_a_quote_only_row():
    now = datetime(2026, 11, 3, 4, 0, 31)
    p = feed.FeedProcessor({KEY: "A.NS"})
    p.handle(frame(current=ms(4, 0, 30, D), ltt=ms(4, 0, 1, D)), now)
    t = p.drain()[0]
    assert t.rts == now and t.ts == datetime(2026, 11, 3, 4, 0, 1) and t.fts == datetime(2026, 11, 3, 4, 0, 30) and t.ltp == 100.0
    yesterday = (2026, 11, 2)
    p.handle(frame(current=ms(4, 0, 30, D), ltt=ms(9, 0, 0, yesterday)), now)                        # the last trade was yesterday
    q = p.drain()[0]
    assert q.ltp is None and q.ts == q.fts == datetime(2026, 11, 3, 4, 0, 30) and q.rts == now and p.quote_only == 1 and p.dropped_stale == 0
    p.handle(frame(current=ms(4, 0, 30, yesterday), ltt=ms(9, 0, 0, yesterday)), now)                # message time is yesterday too
    assert p.drain() == [] and p.dropped_stale == 1


def test_event_time_columns_are_stored_typed_and_absent_when_not_set(tmp_path):
    base = datetime(2026, 11, 3, 4, 0, 0)
    tick_store.write_ticks([Tick(base, "A.NS", 1.0, fts=base + timedelta(seconds=2), rts=base + timedelta(seconds=3)),
                            Tick(base, "B.NS", None, fts=base + timedelta(seconds=1), rts=base + timedelta(seconds=3))], tmp_path / "new")
    f = next((tmp_path / "new" / "ticks").rglob("*.parquet"))
    types = {r[0]: r[1] for r in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{f}')").fetchall()}
    assert types["fts"].startswith("TIMESTAMP") and types["rts"].startswith("TIMESTAMP") and types["ltp"] == "DOUBLE"
    assert duckdb.sql(f"SELECT count(*) FROM read_parquet('{f}') WHERE ltp IS NULL").fetchall()[0][0] == 1       # the quote-only row has a NULL price
    tick_store.write_ticks([Tick(base, "A.NS", 1.0)], tmp_path / "old")                                       # code that sets neither: files unchanged
    g = next((tmp_path / "old" / "ticks").rglob("*.parquet"))
    cols = [r[0] for r in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{g}')").fetchall()]
    assert "fts" not in cols and "rts" not in cols


def test_compaction_orders_an_instruments_rows_by_arrival_not_by_last_trade_time(tmp_path):
    base = datetime(2026, 11, 3, 4, 0, 0)

    def row(sec_arrival: int, delta: float) -> Tick:
        return Tick(base, "OPT_CE", 100.0, delta=delta, fts=base + timedelta(seconds=sec_arrival), rts=base + timedelta(seconds=sec_arrival, milliseconds=5))
    tick_store.write_ticks([row(30, 0.30), row(10, 0.10)], tmp_path)                                         # same last-trade time, written out of order
    tick_store.write_ticks([row(20, 0.20)], tmp_path)
    tick_store.compact_ticks_day(date(2026, 11, 3), tmp_path)
    f = tmp_path / "ticks" / "date=2026-11-03" / "ticks.parquet"
    assert [round(r[0], 2) for r in duckdb.sql(f"SELECT delta FROM read_parquet('{f}')").fetchall()] == [0.10, 0.20, 0.30]


def lake_with(tmp_path, rows: list[Tick]) -> Lake:
    tick_store.write_ticks(rows, tmp_path)
    return Lake(tmp_path)


def test_bars_and_greeks_follow_event_time_and_quote_only_rows_make_no_bar(tmp_path):
    t0 = datetime(2026, 11, 3, 4, 0, 0)
    trade = t0 + timedelta(milliseconds=500)
    rows = [Tick(trade, "A.NS", 100.0, 1000, 99.9, 5, 100.1, 6, fts=t0 + timedelta(milliseconds=700), rts=t0 + timedelta(milliseconds=710)),
            Tick(trade, "A.NS", 100.0, 1000, 99.5, 5, 100.1, 6, fts=t0 + timedelta(seconds=30, milliseconds=100), rts=t0 + timedelta(seconds=30, milliseconds=110)),
            Tick(t0 + timedelta(seconds=5), "Q.NS", None, 0, 50.0, 1, 50.5, 1, fts=t0 + timedelta(seconds=5), rts=t0 + timedelta(seconds=5, milliseconds=1))]
    lake = lake_with(tmp_path, rows)
    bars = lake.day_bars("2026-11-03", "A.NS", "1s", 1)
    assert [(b.ts.second, b.bid) for b in bars.itertuples()] == [(0, 99.9), (30, 99.5)]                     # the later quote lands in the later second
    assert list(lake.bar_symbols("2026-11-03", "1s")["symbol"]) == ["A.NS"]                                  # no price bar for the untraded instrument
    assert lake.day_bars("2026-11-03", "Q.NS", "1s", 1).empty
    stored = duckdb.sql(f"SELECT count(*) FROM read_parquet('{tmp_path}/ticks/date=2026-11-03/*.parquet') WHERE symbol = 'Q.NS'").fetchall()[0][0]
    assert stored == 1                                                                                       # but its quote is stored

    opt = "NIFTY_25000_CE_03_NOV_26"
    greek = [Tick(trade, opt, 120.0, 10, iv=0.15, delta=0.4, gamma=0.001, fts=t0 + timedelta(seconds=s), rts=t0 + timedelta(seconds=s, milliseconds=1)) for s in (1, 40)]
    greek[1].delta = 0.6
    glake = lake_with(tmp_path / "g", greek)
    df = glake.greeks_series("2026-11-03", opt, 10)
    assert [(r.ts.second, round(r.delta, 1)) for r in df.itertuples()] == [(0, 0.4), (40, 0.6)]              # two updates under one trade time stay apart


def test_files_without_event_time_still_read_as_before(tmp_path):
    t0 = datetime(2026, 11, 3, 4, 0, 0)
    lake = lake_with(tmp_path, [Tick(t0 + timedelta(seconds=1), "A.NS", 100.0, 10, 99.9, 5, 100.1, 6), Tick(t0 + timedelta(seconds=3), "A.NS", 101.0, 15, 100.9, 5, 101.1, 6)])
    bars = lake.day_bars("2026-11-03", "A.NS", "1s", 1)
    assert [(b.ts.second, b.close, b.volume) for b in bars.itertuples()] == [(1, 100.0, 0), (3, 101.0, 5)]


def test_throttle_counts_event_time_not_last_trade_time():
    base = datetime(2026, 11, 3, 4, 0, 0)
    p = TickPolicy("throttle", min_interval_s=1.0)
    a, b, c = (Tick(base, "A.NS", 1.0, fts=base + timedelta(seconds=s)) for s in (0, 0.4, 2))
    assert [p.apply(t) is not None for t in (a, b, c)] == [True, False, True]


def test_the_live_book_is_timed_by_the_message():
    r = pb.FeedResponse.FromString(frame(current=ms(4, 0, 30, D), ltt=ms(4, 0, 1, D)))
    sym, ts, ltp, bids, asks = next(decode.books_from_response(r, {KEY: "A.NS"}))
    assert ts == datetime(2026, 11, 3, 4, 0, 30) and ltp == 100.0 and bids[0][0] == 99.9


def test_equal_arrival_times_fall_back_to_the_message_time_and_then_the_trade_time(tmp_path):
    """A fixed or coarse clock gives many rows the same arrival time; volume deltas and 'last value in the second' must still follow time."""
    t0 = datetime(2026, 11, 3, 4, 0, 0)
    same_arrival = t0 + timedelta(seconds=1)
    rows = [Tick(t0 + timedelta(milliseconds=100 * i), "A.NS", 100.0 + i, 1000 + 10 * i, 99.0 + i, 5, 100.0 + i, 6, rts=same_arrival) for i in (3, 0, 2, 1, 4)]
    lake = lake_with(tmp_path, rows)                                                # written in a shuffled order, one arrival time for all
    bar = lake.day_bars("2026-11-03", "A.NS", "1s", 1).iloc[0]
    assert (bar.open, bar.high, bar.low, bar.close, bar.volume) == (100.0, 104.0, 100.0, 104.0, 40)    # open first, close last, volume the sum of the deltas
    tick_store.compact_ticks_day(date(2026, 11, 3), tmp_path)
    f = tmp_path / "ticks" / "date=2026-11-03" / "ticks.parquet"
    assert [r[0] for r in duckdb.sql(f"SELECT ltp FROM read_parquet('{f}')").fetchall()] == [100.0, 101.0, 102.0, 103.0, 104.0]
