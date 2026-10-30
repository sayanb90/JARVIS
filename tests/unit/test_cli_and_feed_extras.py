"""Command-line entry points (dry runs, in-process) and remaining feed paths."""
import asyncio
import runpy
import sys
from datetime import datetime

import pytest
from websockets.asyncio.server import serve

from data_layer.upstox import feed
from data_layer.upstox.feed import FeedProcessor, UpstoxFeed, run_forever
from data_layer.upstox.proto import MarketDataFeedV3_pb2 as pb
from tests.helpers import ms
from tests.unit.test_upstox_adapter import KEY, KMAP, stock_frame


def run_module(monkeypatch, module, *argv):
    monkeypatch.setattr(sys, "argv", [module, *argv])
    saved = sys.modules.pop(module, None)              # runpy warns if already imported; we want __main__ semantics
    try:
        runpy.run_module(module, run_name="__main__")
    finally:
        if saved is not None:
            sys.modules[module] = saved                # leave the imported module exactly as other tests expect


@pytest.mark.parametrize("argv", [
    ("--mode", "full", "--market", "ALL", "--dry-run"),
    ("--mode", "daily", "--market", "UK", "--days-back", "3", "--dry-run"),
    ("--mode", "intraday", "--market", "IN", "--interval", "5m", "--backfill", "--dry-run"),
])
def test_yf_cli_dry_runs(world, monkeypatch, caplog, argv):
    world.set_universe({"IN": ["A.NS"], "US": ["AAPL"], "UK": ["SHEL.L"], "EU": ["SAP.DE"]})
    monkeypatch.setenv("JARVIS_ROOT", str(world.root))
    with caplog.at_level("INFO"):
        run_module(monkeypatch, "data_layer.yf_client", *argv)
    assert "DRY RUN" in caplog.text


def test_fmp_cli_dry_run_and_bad_market(world, monkeypatch, caplog):
    world.set_universe({"IN": ["A.NS", "B.NS"]})
    monkeypatch.setenv("JARVIS_ROOT", str(world.root))
    with caplog.at_level("INFO"):
        run_module(monkeypatch, "data_layer.fmp_client", "--market", "IN", "--dry-run", "--slice", "0", "0")
    assert "1 tickers × 4 endpoints" in caplog.text
    with pytest.raises(SystemExit):
        run_module(monkeypatch, "data_layer.fmp_client", "--market", "US")


def test_market_info_frame_updates_status_and_is_not_a_tick():
    p = FeedProcessor(KMAP)
    r = pb.FeedResponse(type=pb.market_info)
    r.marketInfo.segmentStatus["NSE_EQ"] = pb.NORMAL_OPEN
    p.handle(r.SerializeToString(), datetime(2026, 10, 23, 4, 0))
    assert p.status == {"NSE_EQ": "NORMAL_OPEN"} and p.ticks == 0 and p.frames == 1


def test_future_timestamp_warning_is_logged_once(caplog):
    p = FeedProcessor(KMAP)
    with caplog.at_level("WARNING"):
        for _ in range(3):
            p.handle(stock_frame(ms(13, 0, 0), 1.0, 1), datetime(2026, 10, 23, 4, 0))
    assert p.dropped_future == 3 and caplog.text.count("ahead of local clock") == 1


def test_run_forever_once_returns_after_one_session(tmp_path, monkeypatch):
    monkeypatch.setattr(feed, "utcnow", lambda: datetime(2026, 10, 23, 12, 0))      # after the close: the session window is already over
    async def handler(ws):
        await ws.recv()
        await ws.send(stock_frame(ms(3, 45, 5), 100.0, 1))
        await asyncio.sleep(0.2)

    async def main():
        async with serve(handler, "127.0.0.1", 0) as srv:
            url = f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}"
            make = lambda: UpstoxFeed("T", KMAP, url=url, lake_dir=tmp_path / "lake", flush_s=0.05)  # noqa: E731
            await asyncio.wait_for(run_forever(make, once=True), timeout=10)
    asyncio.run(main())                                  # must return promptly: the window has already closed


def test_feed_main_builds_feed_from_cli_flags(monkeypatch, tmp_path):
    for v in ("UPSTOX_FEED_TOKEN",):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", "TOK")
    monkeypatch.chdir(tmp_path)
    keys = {f"NSE_EQ|INE{i:09d}": f"S{i}.NS" for i in range(5)}
    monkeypatch.setattr(feed, "segment_key_map", lambda seg, fo_file=None: keys)
    seen = {}

    async def fake_run_forever(make, once):
        f = make()
        seen.update(n=len(f.keys), mode=f.mode, once=once, interval=f.proc.agg.interval.total_seconds(),
                    raw=f.proc.raw_ticks, dump=f.dump_left)
    monkeypatch.setattr(feed, "run_forever", fake_run_forever)
    monkeypatch.setattr(sys, "argv", ["feed", "--once", "--limit", "3", "--mode", "ltpc", "--interval", "300",
                                      "--raw-ticks", "--dump-frames", "2"])
    feed.main()
    assert seen == {"n": 3, "mode": "ltpc", "once": True, "interval": 300.0, "raw": True, "dump": 2}


def test_feed_main_without_usable_universe_exits(monkeypatch, tmp_path):
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", "TOK")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(feed, "segment_key_map", lambda seg, fo_file=None: {})
    monkeypatch.setattr(sys, "argv", ["feed"])
    with pytest.raises(SystemExit, match="ISIN"):
        feed.main()
