"""Order book levels 2 to 5 behind JARVIS_DEPTH5 (SCRUM-193): decoded and stored only when on, measured by `tick_store size`."""

from datetime import date

import duckdb

from data_layer import tick_store
from data_layer.upstox import decode, feed
from data_layer.upstox.proto import MarketDataFeedV3_pb2 as pb
from tests.helpers import ms

KEY = "NSE_FO|12345"


def frame(levels: int = 5) -> bytes:
    r = pb.FeedResponse(type=pb.live_feed, currentTs=ms(4, 0, 1, (2026, 11, 13)))
    m = r.feeds[KEY].fullFeed.marketFF
    m.ltpc.ltp, m.ltpc.ltt = 100.0, ms(4, 0, 0, (2026, 11, 13))
    m.vtt, m.oi = 5000, 1000
    for i in range(levels):
        q = m.marketLevel.bidAskQuote.add()
        q.bidP, q.bidQ, q.askP, q.askQ = 99.95 - 0.05 * i, 100 + i, 100.05 + 0.05 * i, 200 + i
    return r.SerializeToString()


def test_levels_decoded_only_when_asked():
    resp = decode.parse_frame(frame())
    off = list(decode.ticks_from_response(resp, {KEY: "NIFTY_FUT_25_NOV_26"}))[0]
    assert off.bid2 is None and off.ask5_qty is None
    on = list(decode.ticks_from_response(resp, {KEY: "NIFTY_FUT_25_NOV_26"}, depth=True))[0]
    assert (on.bid, on.bid_qty) == (99.95, 100)
    assert (on.bid2, on.bid2_qty, on.ask5, on.ask5_qty) == (99.9, 101, 100.25, 204)


def test_missing_levels_are_none():
    t = list(decode.ticks_from_response(decode.parse_frame(frame(levels=2)), {KEY: "X_FUT_25_NOV_26"}, depth=True))[0]
    assert t.bid2 == 99.9 and t.bid3 is None and t.ask5_qty is None


def test_switch(monkeypatch):
    assert feed.depth5_enabled(None, env={}) is False
    assert feed.depth5_enabled(None, env={"JARVIS_DEPTH5": "1"}) is True
    assert feed.depth5_enabled(True, env={}) is True
    assert feed.depth5_enabled(None, env={"JARVIS_DEPTH5": "fo"}, segment="fo") is True
    assert feed.depth5_enabled(None, env={"JARVIS_DEPTH5": "fo"}, segment="eq") is False
    assert feed.depth5_enabled(None, env={"JARVIS_DEPTH5": "all"}, segment="eq") is True
    p = feed.FeedProcessor({KEY: "NIFTY_FUT_25_NOV_26"}, depth=True)
    assert p.depth is True and feed.FeedProcessor({KEY: "A"}).depth is False


def test_stored_only_when_present_and_sized(tmp_path):
    resp = decode.parse_frame(frame())
    plain = list(decode.ticks_from_response(resp, {KEY: "NIFTY_FUT_25_NOV_26"}))
    tick_store.write_ticks(plain, tmp_path)
    day = date(2026, 11, 13)
    files = sorted(tick_store.segment_folder(tmp_path, day, "fo").glob("*.parquet"))
    cols = {r[0] for r in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{files[0]}')").fetchall()}
    assert "bid2" not in cols                                            # unchanged output while the switch is off
    deep = list(decode.ticks_from_response(resp, {KEY: "NIFTY_FUT_25_NOV_26"}, depth=True))
    tick_store.write_ticks(deep, tmp_path)
    files = sorted(tick_store.segment_folder(tmp_path, day, "fo").glob("*.parquet"))
    cols = {r[0] for f in files for r in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{f}')").fetchall()}
    assert {"bid2", "ask5_qty"} <= cols
    size = tick_store.day_size(day, tmp_path, ["fo"])["fo"]
    assert size["rows"] == 2 and size["depth_bytes"] > 0 and 0 < size["depth_share"] < 1


def test_compaction_merges_days_with_and_without_depth(tmp_path):
    """Turning the switch on mid-day only adds columns: compaction keeps every row, older rows read the new columns as empty."""
    resp = decode.parse_frame(frame())
    tick_store.write_ticks(list(decode.ticks_from_response(resp, {KEY: "NIFTY_FUT_25_NOV_26"})), tmp_path)
    tick_store.write_ticks(list(decode.ticks_from_response(decode.parse_frame(frame(levels=3)), {KEY: "NIFTY_FUT_25_NOV_26"}, depth=True)), tmp_path)
    day = date(2026, 11, 13)
    n = tick_store.compact_ticks_day(day, tmp_path, "fo")
    folder = tick_store.segment_folder(tmp_path, day, "fo")
    files = sorted(folder.glob("*.parquet"))
    assert [f.name for f in files] == ["ticks.parquet"]
    df = duckdb.sql(f"SELECT * FROM read_parquet('{files[0]}')").df()
    assert n == len(df) >= 1
    assert {"bid2", "bid3"} <= set(df.columns) and "bid4" not in df.columns     # a level empty in every batch is never written
    assert (df["bid2"].dropna() == 99.9).all()


# ---------------------------------------------------------------------------------- F&O over several connections (SCRUM-194)
def test_shards_split_the_list_evenly_and_completely():
    km = {f"NSE_FO|{i}": f"NIFTY_{24000 + 50 * i}_CE_25_NOV_26" for i in range(1001)}
    a, b = feed.shard_keys(km, 1, 2), feed.shard_keys(km, 2, 2)
    assert set(a) | set(b) == set(km) and not set(a) & set(b)
    assert abs(len(a) - len(b)) <= 1
    assert feed.shard_keys(km, 1, 2) == a                                 # the same split in every process
    assert feed.shard_keys(km, 1, 1) == km
    assert feed.parse_shard("2/3") == (2, 3) and feed.parse_shard(None) == (1, 1)


def test_shard_ports():
    from data_layer import live_book
    assert live_book.port_for("fo", {}, shard=2) == 8782
    assert live_book.port_for("fo", {"JARVIS_BOOK_PORT_FO": "0"}, shard=2) == 0
