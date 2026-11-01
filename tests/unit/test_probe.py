"""The Upstox probe (SCRUM-99): extended stream fields, REST jobs against documented response shapes, guard, report."""
from __future__ import annotations

import gzip
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from data_layer import tick_store
from data_layer.sysmon import ResourceMonitor
from data_layer.tick_policy import TickPolicy
from data_layer.ticks import Tick
from data_layer.upstox import decode, feed
from data_layer.upstox.probe import jobs
from data_layer.upstox.probe.__main__ import main as probe_main
from data_layer.upstox.probe.jobs import Ctx
from data_layer.upstox.probe.report import analyse_ticks, build_report, flags, render_markdown
from data_layer.upstox.probe.runner import Guard, ProbeRun, ProbeStream, run_check
from data_layer.upstox.probe.store import ProbeStore
from data_layer.upstox.proto import MarketDataFeedV3_pb2 as pb
from data_layer.upstox.ratelimit import RateLimiter
from data_layer.upstox.rest import UpstoxRest
from tests.bdd.fake_upstox import FakeUpstox, ThreadedFake
from tests.helpers import ms
from tests.unit.test_sysmon_rest import FakeResp, FakeSession, make_proc

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


# ---------------------------------------------------------------------------------- the stream thread against the fake server
def test_probe_stream_collects_extended_ticks_events_and_reports_column_cost(tmp_path):
    key_map = {KEY: "A.NS"}
    frames = []
    ev = pb.FeedResponse(type=pb.market_info, currentTs=ms(3, 40, 0, (2026, 10, 23)))
    ev.marketInfo.segmentStatus["NSE_EQ"] = pb.NORMAL_OPEN
    frames.append(ev.SerializeToString())
    frames += [ext_frame(ltt=ms(4, 0, i, (2026, 10, 23))) for i in range(30)]
    with ThreadedFake(FakeUpstox(frames, hold_s=0.3)) as fake:
        guard = Guard(tmp_path, min_free_gb=0.0, max_gb=50.0, ttl=0)
        now = datetime.utcnow()
        s = ProbeStream("stream_eq", key_map, tmp_path, lambda: "TOK", start=now - timedelta(seconds=1), end=now + timedelta(seconds=2.5),
                        guard=guard, url=fake.url, clock=lambda: datetime(2026, 10, 23, 5, 0), day=date(2026, 10, 23))
        s.begin()
        s.join(30)
    info = s.info()
    assert info["error"] == "" and info["ticks"] == 30 and s.compacted_rows == 30
    t = analyse_ticks(tmp_path / "stream_eq", {"equity": 2.0})
    assert t["available"] and t["rows"] == 30
    assert t["columns"]["ltq"]["extended"] and t["columns"]["ltp"]["extended"] is False
    assert t["ext_share_pct"] > 0 and t["classes"]["equity"]["ext_bytes"] > 0 and t["classes"]["equity"]["scale_to_plan"] == 2.0
    assert t["classes"]["equity"]["gb_per_year"] > t["classes"]["equity"]["gb_per_year_without_ext"] > 0
    assert (tmp_path / "stream_eq" / "events").exists()


# ---------------------------------------------------------------------------------- disk guard
def test_guard_stops_on_low_free_space_or_a_full_probe_directory(tmp_path):
    assert Guard(tmp_path, min_free_gb=1e9, ttl=0).ok() is False
    g = Guard(tmp_path, min_free_gb=1e9, ttl=0)
    g.ok()
    assert "free disk" in g.reason
    (tmp_path / "big.bin").write_bytes(b"x" * 4096)
    g2 = Guard(tmp_path, min_free_gb=0.0, max_gb=1e-9, ttl=0)
    assert g2.ok() is False and "cap" in g2.reason
    assert Guard(tmp_path, min_free_gb=0.0, max_gb=10.0, ttl=0).ok() is True


# ---------------------------------------------------------------------------------- REST jobs against documented shapes
def ok(data):
    return FakeResp(200, {"status": "success", "data": data})


CANDLE = [["2026-10-30T00:00:00+05:30", 1, 2, 0.5, 1.5, 100, 0]]
FII_TS = 1775001600000                                              # 2026-04-01
PAYLOADS = [
    (r"/v3/market-quote/quotes", ok({"NSE_EQ:A": {"instrument_token": KEY, "symbol": "A", "last_price": 10.5, "volume": 100, "average_price": 10.4,
                                                  "total_buy_quantity": 5, "total_sell_quantity": 6, "upper_circuit_limit": 11,
                                                  "lower_circuit_limit": 9, "year_high": 12, "year_low": 8, "prev_close_price": 10,
                                                  "cas_eligible": True, "ohlc": {"open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 100}}})),
    (r"/v2/option/chain", ok([{"expiry": "2026-11-03", "pcr": 0.9, "strike_price": 24500, "underlying_spot_price": 24510,
                               "call_options": {"instrument_key": "NSE_FO|1", "market_data": {"ltp": 120, "volume": 10, "oi": 1000, "prev_oi": 900,
                                                "bid_price": 119, "bid_qty": 50, "ask_price": 121, "ask_qty": 40},
                                                "option_greeks": {"delta": 0.5, "gamma": 0.001, "theta": -5, "vega": 10, "iv": 14.2, "pop": 48}},
                               "put_options": {"instrument_key": "NSE_FO|2", "market_data": {"ltp": 90, "oi": 800}, "option_greeks": {"delta": -0.5}}}])),
    (r"/v2/market/pcr", ok({"expiry_date": "03-11-2026", "pcr": 0.95, "spot_closing_price": 24500,
                            "insights": [{"pcr": 0.9, "spot_price": 24400, "time": "09:15"}, {"pcr": 0.92, "spot_price": 24420, "time": "10:15"}]})),
    (r"/v2/market/max-pain", ok({"expiry_date": "03-11-2026", "max_pain": 24500, "spot_closing_price": 24510, "insights": [{"max_pain": 24400, "spot_price": 24390, "time": "09:15"}]})),
    (r"/v2/market/oi", ok({"total_puts": 10, "total_calls": 12, "spot_closing_price": 24500, "expiry": "2026-11-03",
                           "call_put_oi_data_list": [{"strike_price": 24500, "call_oi": 5, "put_oi": 6}]})),
    (r"/v2/market/change-oi", ok({"total_put_change_oi": 1, "total_call_change_oi": 2, "spot_closing_price": 24500, "expiry": "2026-11-03",
                                  "call_put_oi_data_list": [{"strike_price": 24500, "call_change_oi": -3, "put_change_oi": 4}]})),
    (r"/v2/market/(fii|dii)", ok({"NSE_EQ|CASH": [{"time_stamp": FII_TS, "buy_amount": 1.0, "sell_amount": 2.0, "buy_contracts": 3, "sell_contracts": 4,
                                                    "oi_contracts": 5, "oi_amount": 6.0, "total_long_contracts": 7, "total_short_contracts": 8}]})),
    (r"/v2/market/smartlist/", ok({"time_stamp": 1, "smartlist": [{"instrument_key": "NSE_FO|1", "price": {"current": 1, "close_price": 2, "change_pct": 1},
                                                                    "metric": {"current": 3, "previous": 2, "change_pct": 50}}]})),
    (r"/v2/news", ok({KEY: [{"heading": "H", "summary": "S", "article_link": "u", "published_time": 1}]})),
    (r"/v3/market-quote/ltp", ok({"GLOBAL:GIFT": {"last_price": 24000, "instrument_token": "GLOBAL_INDEX|GIFT", "cp": 23900, "ltq": 0, "volume": 0}})),
    (r"/v3/historical-candle/.*/days/", ok({"candles": CANDLE})),
    (r"/v3/historical-candle/.*/minutes/", ok({"candles": CANDLE * 3})),
    (r"/v2/expired-instruments/expiries", ok(["2026-10-20", "2026-10-27"])),
    (r"/v2/expired-instruments/option/contract", ok([{"instrument_key": f"NSE_FO|{i}|27-10-2026", "trading_symbol": f"NIFTY {i} CE", "instrument_type": "CE",
                                                      "strike_price": 24000 + i, "lot_size": 75, "weekly": True} for i in range(3)])),
    (r"/v2/expired-instruments/future/contract", ok([{"instrument_key": "NSE_FO|9|27-10-2026", "trading_symbol": "NIFTY FUT", "instrument_type": "FUT",
                                                      "lot_size": 75, "weekly": False}])),
    (r"/v2/expired-instruments/historical-candle/", ok({"candles": CANDLE * 5})),
    (r"/income-statement", ok({"type": "consolidated", "time_period": "yearly", "units_in": "crore", "income_statement": [
        {"category": "revenue", "history": [{"value": 100, "period": "Mar 2026", "change": "+10%"}, {"value": 90, "period": "Mar 2025"}]},
        {"category": "net_profit", "history": [{"value": 10, "period": "Mar 2026", "change": "+5%"}]}],
        "full_statement": [{"particular": "EPS - Basic", "history": [{"period": "Mar 2025", "value": 51.47}]}]})),
    (r"/balance-sheet", ok({"history": [{"total_asset": 1, "total_liability": 2, "period": "Mar 2025"}],
                            "full_statement": [{"particular": "Equity Capital", "history": [{"period": "Mar 2025", "value": 5}]}]})),
    (r"/cash-flow", ok({"cash_flow": [{"category": "operating", "history": [{"value": 5, "period": "Mar 2025", "change": "+1%"}]}]})),
    (r"/key-ratios", ok([{"name": "P/E", "company_value": "20", "sector_value": "22"}])),
    (r"/share-holdings", ok([{"category": "promoters", "history": [{"period": "Mar 2026", "value": 50.1}, {"period": "Dec 2025", "value": 50.0}]}])),
    (r"/corporate-actions", ok([{"name": "Dividend", "expiry_date": "2026-06-01", "amount": 5, "ratio": None, "event_details": []}])),
    (r"/profile", ok({"company_profile": "x", "sector": "Energy"})),
    (r"/competitors", ok([{"isin": "INE2"}])),
    (r"/v2/market/holidays", ok([{"date": "2026-11-05", "description": "holiday"}])),
    (r"/v2/market/status/NSE$", ok({"exchange": "NSE", "status": "NORMAL_OPEN"})),
    (r"/v2/market/timings/", ok([{"exchange": "NSE", "start_time": 1, "end_time": 2}])),
]


def master_gz(items) -> bytes:
    return gzip.compress(json.dumps(items).encode())


def make_handler(overrides=None):
    table = list(overrides or []) + PAYLOADS

    def handler(url, params):
        if "assets.upstox.com" in url:
            if url.endswith("NSE.json.gz"):
                return FakeResp(200, None, text="")._replace(master_gz([
                    {"segment": "NSE_INDEX", "name": "Nifty 50", "instrument_key": "NSE_INDEX|Nifty 50"},
                    {"segment": "NSE_INDEX", "name": "India VIX", "instrument_key": "NSE_INDEX|India VIX"},
                    {"segment": "NSE_INDEX", "name": "Nifty Bank", "instrument_key": "NSE_INDEX|Nifty Bank"},
                    {"segment": "NSE_EQ", "name": "A", "instrument_key": KEY}]))
            return FakeResp(200, None, text="")._replace(master_gz([{"name": "GIFT NIFTY", "trading_symbol": "GIFT NIFTY", "instrument_key": "GLOBAL_INDEX|GIFT"},
                                                                    {"name": "S&P 500", "trading_symbol": "S&P", "instrument_key": "GLOBAL_INDEX|SPX"}]))
        path = url.split("?")[0]
        for pat, resp in table:
            if re.search(pat, path):
                return resp
        return FakeResp(404, {"status": "error", "errors": [{"errorCode": "UDAPI0404", "message": f"no fake for {path}"}]})
    return handler


class RawSession(FakeSession):
    def __init__(self, handler):
        super().__init__()
        self.handler = handler

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params})
        return self.handler(url, params)


def _replace(self, content: bytes):
    self.content, self.status_code = content, 200
    return self


FakeResp._replace = _replace


def make_ctx(tmp_path, overrides=None) -> Ctx:
    rest = UpstoxRest(lambda: "SECRET", session=RawSession(make_handler(overrides)), limiter=RateLimiter(sleep=lambda s: None), sleep=lambda s: None)
    ctx = Ctx(rest=rest, store=ProbeStore(tmp_path / "probe"), now=lambda: datetime(2026, 11, 2, 5, 0),
              eq_keys={KEY: "A.NS", "NSE_EQ|INE000B01010": "B.NS"}, fo_keys={"NSE_FO|1": "NIFTY_24500_CE_03_NOV_26", "NSE_FO|9": "NIFTY_FUT_24_NOV_26"},
              isins=["INE002A01018", "INE467B01029"], fund_sample=2)
    ctx.underlying_keys = {"NIFTY": "NSE_INDEX|Nifty 50", "BANKNIFTY": "NSE_INDEX|Nifty Bank"}
    return ctx


def test_instrument_master_job_finds_index_vix_and_global_keys_and_keeps_a_dated_copy(tmp_path):
    ctx = make_ctx(tmp_path)
    notes = jobs.job_instrument_master(ctx, "run")
    assert set(ctx.index_keys.values()) == {"NSE_INDEX:Nifty 50", "NSE_INDEX:India VIX", "NSE_INDEX:Nifty Bank"}
    assert set(ctx.global_keys.values()) == {"GLOBAL:GIFT NIFTY", "GLOBAL:S&P"} and ctx.underlying_keys["NIFTY"] == "NSE_INDEX|Nifty 50"
    assert notes["NSE_records"] == 4 and (tmp_path / "probe" / "instrument_master" / "date=2026-11-02" / "NSE-2026-11-02.json.gz").exists()


def test_market_snapshots_and_option_jobs_store_rows_and_record_depth(tmp_path):
    ctx = make_ctx(tmp_path)
    jobs.job_quotes(ctx, "run")
    jobs.job_option_chain(ctx, "run")
    n = jobs.job_option_stats(ctx, "run")
    jobs.job_flows(ctx, "run")
    jobs.job_smartlists(ctx, "run")
    jobs.job_global(ctx, "run") if ctx.global_keys else None
    sz = ctx.store.sizes()
    for ds in ("quote_snapshot", "option_chain", "optstats_pcr", "optstats_max_pain", "optstats_oi", "optstats_change_oi", "flows", "smartlists"):
        assert sz[ds]["rows"] > 0, ds
    q = one(f"SELECT last_price, cas_eligible, close FROM read_parquet('{tmp_path}/probe/quote_snapshot/*/*.parquet')")
    assert q == (10.5, True, 10.5)
    chain = one(f"SELECT count(*), max(iv), max(pop) FROM read_parquet('{tmp_path}/probe/option_chain/*/*.parquet')")
    assert chain[0] >= 2 and chain[1] == 14.2 and chain[2] == 48
    assert set(n["depth"]) == {"1d", "1w", "1m", "3m", "6m", "1y"} and n["depth"]["1w"]["insight_points"] == 2
    assert one(f"SELECT min(time_stamp) FROM read_parquet('{tmp_path}/probe/flows/*/*.parquet')")[0] == FII_TS


def test_news_is_deduplicated_across_polls(tmp_path):
    ctx = make_ctx(tmp_path)
    first, second = jobs.job_news(ctx, "run"), jobs.job_news(ctx, "run")
    assert first["new_items"] == 1 and second["new_items"] == 0 and second["items_seen"] == 1


def test_candles_expired_and_fundamentals_samples_report_depth_and_counts(tmp_path):
    ctx = make_ctx(tmp_path)
    c = jobs.job_candles_sample(ctx, "run")
    assert c["daily"]["requests"] >= 3 and c["daily"]["earliest"].startswith("2026-10-30") and c["minute"]["rows_one_month_1min"] == 6
    e = jobs.job_expired_sample(ctx, "run")
    assert e["expiries_ok"] and e["expiries_count"] == 2 and e["expiry_oldest"] == "2026-10-20" and e["option_contracts"] == 6
    assert e["candle_rows"] > 0 and ctx.out["expired_expiries"] == ["2026-10-20", "2026-10-27"]
    f = jobs.job_fundamentals_sample(ctx, "run")
    assert f["periods"]["income_yearly"]["max"] == 2 and f["periods"]["shareholding"]["max"] == 2 and f["calls"] == 12 * 2
    assert "parse_errors" not in f
    rows = duckdb.sql(f"SELECT endpoint, count(*) FROM read_parquet('{tmp_path}/probe/fundamentals/*/*.parquet') GROUP BY 1").fetchall()
    assert {r[0] for r in rows} >= {"income_yearly", "balance", "cashflow", "key_ratios", "shareholding", "corporate_actions", "profile"}


def test_a_failing_endpoint_or_a_wrong_shape_is_recorded_not_raised(tmp_path):
    gated = (r"/v2/expired-instruments/expiries", FakeResp(403, {"status": "error", "errors": [{"errorCode": "UDAPI1149", "message": "Plus only"}]}))
    wrong = (r"/v3/market-quote/quotes", FakeResp(200, {"status": "success", "data": ["not", "a", "dict"]}))
    ctx = make_ctx(tmp_path, [gated, wrong])
    e = jobs.job_expired_sample(ctx, "run")
    assert e["expiries_ok"] is False and e["expiries_status"] == 403 and e["expiries_code"] == "UDAPI1149"
    q = jobs.job_quotes(ctx, "run")
    assert "quotes" in q["parse_errors"]
    assert ctx.rest.stats["expired.expiries"]["errors"] == {"403:UDAPI1149": 1}
    assert (tmp_path / "probe" / "_samples").exists()                 # the real shape is kept for fixing the parser


def test_market_info_tries_candidate_paths_and_records_which_worked(tmp_path):
    ctx = make_ctx(tmp_path)
    n = jobs.job_market_info(ctx, "run")
    assert n["holidays_path"] == "/v2/market/holidays" and n["status_path"] == "/v2/market/status/NSE" and n["timings_path"].startswith("/v2/market/timings/")


# ---------------------------------------------------------------------------------- the run, the report, the CLI
def test_schedule_covers_the_session_in_order(tmp_path):
    ctx = make_ctx(tmp_path)
    start = datetime(2026, 11, 2, 3, 20)
    pr = ProbeRun(ctx, tmp_path / "probe", tmp_path / "logs", lambda: "T", start=start, end=datetime(2026, 11, 2, 10, 25), guard=Guard(tmp_path),
                  monitor=ResourceMonitor(None, 30, proc_root=tmp_path / "x"))
    plan = pr._schedule()
    assert plan == sorted(plan) and min(t for t, _ in plan) >= datetime(2026, 11, 2, 3, 50)
    by = {}
    for t, j in plan:
        by.setdefault(j, []).append(t)
    assert len(by["news"]) >= 12 and len(by["global_cues"]) > 60 and len(by["option_chain"]) >= 24 and len(by["quotes"]) == 2


def test_check_run_end_to_end_writes_capability_report_and_state(tmp_path):
    ctx = make_ctx(tmp_path)
    jobs.job_instrument_master(ctx, "check")
    make_proc(tmp_path / "proc", "100 0 100 800 0 0 0 0", [(5, "python -m data_layer.upstox.probe check", 80_000, 90_000, 10, 10)])
    mon = ResourceMonitor(tmp_path / "logs" / "res.jsonl", 0.05, [tmp_path], patterns=("data_layer",), proc_root=tmp_path / "proc")
    path = run_check(ctx, tmp_path / "probe", tmp_path / "logs", mon, {}, "T")
    text = path.read_text()
    assert "# Upstox probe report (check)" in text and "## What answered (capability)" in text and "fund.income_yearly" in text
    assert "stream_check" in text and "## The box" in text
    assert (tmp_path / "probe" / "_state.json").exists() and (tmp_path / "logs" / "jobs-" ).parent.exists()
    assert probe_main(["--out", str(tmp_path / "probe"), "report"]) == 0
    assert (tmp_path / "probe" / "reports" / "report-rebuilt.md").exists()


def test_report_flags_plan_gating_swap_and_parse_failures(tmp_path):
    rep = {"monitor": {"mem_total_gb": 8, "peaks": {"swap_used_gb": 0.5, "mem_available_min_gb": 0.2, "cpu_pct": 91, "iowait_pct": 30}},
           "rest": {"by_tag": {"expired.expiries": {"errors": {"403:UDAPI1149": 2}}, "x": {"errors": {"429": 3}}}},
           "results": [{"job": "quotes", "notes": {"parse_errors": {"quotes": "AttributeError: boom"}}}],
           "streams": {"stream_eq": {"error": "", "guard_stopped": "free disk 3.0 GB is below the 15 GB floor"}}}
    text = " ".join(flags(rep))
    for needle in ("Swap was used", "Free memory fell", "CPU peaked", "Disk wait", "UDAPI1149", "HTTP 429", "parser 'quotes' failed", "disk guard"):
        assert needle in text, needle


def test_report_without_a_state_file_explains_itself(tmp_path, capsys):
    assert probe_main(["--out", str(tmp_path / "empty"), "report"]) == 2
    assert "probe run" in capsys.readouterr().out


def test_store_sizes_skip_reports_and_private_folders(tmp_path):
    st = ProbeStore(tmp_path)
    st.write("news", [{"a": 1, "b": "x"}])
    (tmp_path / "reports").mkdir()
    (tmp_path / "reports" / "r.md").write_text("x")
    st.save_sample("probe_sample", {"k": 1})
    assert set(st.sizes()) == {"news"} and st.sizes()["news"]["rows"] == 1
