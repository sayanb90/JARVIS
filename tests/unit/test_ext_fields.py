"""Extended stream fields (SCRUM-100): decoded only when asked, stored typed and compact, production output unchanged when off."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb

from data_layer import tick_store
from data_layer.tick_policy import TickPolicy
from data_layer.ticks import Tick
from data_layer.upstox import decode, feed
from data_layer.upstox.proto import MarketDataFeedV3_pb2 as pb
from tests.helpers import ms

KEY = "NSE_EQ|INE000A01010"


# ---------------------------------------------------------------------------------- extended stream fields
def ext_frame(key=KEY, ltt=None, auction=True) -> bytes:
    r = pb.FeedResponse(type=pb.live_feed)
    m = r.feeds[key].fullFeed.marketFF
    m.ltpc.ltp, m.ltpc.ltt, m.ltpc.ltq, m.ltpc.cp = 101.5, ltt or ms(4, 0, 0, (2026, 10, 23)), 7, 100.0
    m.vtt, m.atp, m.tbq, m.tsq = 5000, 101.1, 12_000, 9_000
    if auction:
        m.iep, m.ieq, m.iiqTotal, m.iiqM, m.rp, m.casEligible = 101.4, 800, -250, 100, 100.0, True
    q = m.marketLevel.bidAskQuote.add()
    q.bidP, q.bidQ, q.askP, q.askQ = 101.45, 10, 101.55, 12
    return r.SerializeToString()


def test_extended_fields_are_decoded_only_when_asked():
    resp = decode.parse_frame(ext_frame())
    off = list(decode.ticks_from_response(resp, {KEY: "A.NS"}))[0]
    assert off.ltq is off.atp is off.tbq is off.cp is off.iep is None
    on = list(decode.ticks_from_response(resp, {KEY: "A.NS"}, ext=True))[0]
    assert (on.ltq, on.atp, on.tbq, on.tsq, on.cp) == (7, 101.1, 12_000, 9_000, 100.0)
    assert (on.iep, on.ieq, on.iiq_total, on.iiq_market, on.ref_price, on.cas_eligible) == (101.4, 800, -250, 100, 100.0, True)
    assert (on.bid, on.bid_qty) == (101.45, 10)                     # level one is still taken from the book
    plain = list(decode.ticks_from_response(decode.parse_frame(ext_frame(auction=False)), {KEY: "A.NS"}, ext=True))[0]
    assert plain.iep is None and plain.cas_eligible is None and plain.ltq == 7          # zero means absent outside an auction


def test_index_and_ltpc_modes_carry_trade_size_and_previous_close():
    r = pb.FeedResponse(type=pb.live_feed)
    ix = r.feeds["NSE_INDEX|India VIX"].fullFeed.indexFF
    ix.ltpc.ltp, ix.ltpc.ltt, ix.ltpc.cp = 13.2, ms(4, 0, 1, (2026, 10, 23)), 12.9
    t = list(decode.ticks_from_response(decode.parse_frame(r.SerializeToString()), {"NSE_INDEX|India VIX": "NSE_INDEX:India VIX"}, ext=True))[0]
    assert t.ltp == 13.2 and t.cp == 12.9


def test_session_events_are_decoded_and_recorded_only_on_change():
    r = pb.FeedResponse(type=pb.market_info, currentTs=ms(3, 40, 0, (2026, 10, 23)))
    r.marketInfo.segmentStatus["NSE_EQ"] = pb.NORMAL_OPEN
    r.marketInfo.casMarketStatus["NSE_EQ"].status = "CAS_LM_START"
    raw = r.SerializeToString()
    ev = decode.events_from_response(decode.parse_frame(raw))
    assert {(e["kind"], e["segment"], e["status"]) for e in ev} == {("segment", "NSE_EQ", "NORMAL_OPEN"), ("cas", "NSE_EQ", "CAS_LM_START")}
    p = feed.FeedProcessor({KEY: "A.NS"}, ext=True)
    for _ in range(2):
        p.handle(raw, datetime(2026, 10, 23, 5, 0))
    assert len(p.drain_events()) == 2                               # the repeat is not recorded again
    off = feed.FeedProcessor({KEY: "A.NS"})
    off.handle(raw, datetime(2026, 10, 23, 5, 0))
    assert off.drain_events() == []


def one(sql: str):
    return duckdb.connect().execute(sql).fetchall()[0]


def cols(path: Path) -> list[str]:
    return [r[0] for r in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{path}', union_by_name=true, hive_partitioning=false)").fetchall()]


def test_production_tick_files_are_unchanged_while_extended_mode_is_off(tmp_path):
    off = feed.FeedProcessor({KEY: "A.NS"})
    off.handle(ext_frame(), datetime(2026, 10, 23, 5, 0))
    tick_store.write_ticks(off.drain(), tmp_path)
    f = next((tmp_path / "ticks").rglob("*.parquet"))
    assert not {"ltq", "atp", "tbq", "tsq", "cp", "iep", "cas_eligible"} & set(cols(f))


def test_extended_columns_are_stored_typed_and_compact_with_old_files(tmp_path):
    old = feed.FeedProcessor({KEY: "A.NS"})
    old.handle(ext_frame(ltt=ms(4, 0, 0, (2026, 10, 23))), datetime(2026, 10, 23, 5, 0))
    tick_store.write_ticks(old.drain(), tmp_path)
    new = feed.FeedProcessor({KEY: "A.NS"}, ext=True)
    new.handle(ext_frame(ltt=ms(4, 0, 1, (2026, 10, 23))), datetime(2026, 10, 23, 5, 0))
    tick_store.write_ticks(new.drain(), tmp_path)
    assert tick_store.compact_ticks_day(date(2026, 10, 23), tmp_path) == 2
    f = tmp_path / "ticks" / "date=2026-10-23" / "ticks.parquet"
    assert {"ltq", "atp", "tbq", "tsq", "cp", "iep", "ieq", "iiq_total", "iiq_market", "ref_price", "cas_eligible"} <= set(cols(f))
    rows = duckdb.sql(f"SELECT ltq, atp, cas_eligible FROM read_parquet('{f}') ORDER BY ts").fetchall()
    assert rows == [(None, None, None), (7, 101.1, True)]            # the old file's tick has no extended values
    tick_store.write_events([{"ts": datetime(2026, 10, 23, 3, 40), "kind": "segment", "segment": "NSE_EQ", "status": "NORMAL_OPEN",
                              "updated": None}], tmp_path)
    assert next((tmp_path / "events").rglob("*.parquet")).exists()


def test_changes_policy_notices_trade_size_and_book_totals():
    p = TickPolicy("changes")
    base = Tick(datetime(2026, 10, 23, 4, 0), "A", 10.0, 5, 9.9, 1, 10.1, 1, None, ltq=3, tbq=100)
    same = Tick(datetime(2026, 10, 23, 4, 0, 1), "A", 10.0, 5, 9.9, 1, 10.1, 1, None, ltq=3, tbq=100)
    moved = Tick(datetime(2026, 10, 23, 4, 0, 2), "A", 10.0, 5, 9.9, 1, 10.1, 1, None, ltq=3, tbq=140)
    assert p.apply(base) is not None and p.apply(same) is None and p.apply(moved) is not None

