"""The feed keeps flushing when a write fails (SCRUM-115): the F&O feed stopped flushing and logging at 08:18:51 UTC on 2026-11-03
and wrote 2.6 million rows only at the end of the session."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

from data_layer import tick_store
from data_layer.ticks import Tick
from data_layer.upstox.feed import UpstoxFeed


def tick(i: int) -> Tick:
    return Tick(datetime(2026, 11, 3, 4, 0, 0) + timedelta(seconds=i), "A.NS", 100.0 + i, 10, 99.9, 1, 100.1, 1, None)


def make_feed(tmp_path) -> UpstoxFeed:
    return UpstoxFeed("TOK", {"NSE_EQ|X": "A.NS"}, lake_dir=tmp_path, flush_s=0.01, write_s=0.02, stale_s=3600)


class Ws:
    async def close(self):
        pass


def test_a_failed_write_puts_the_ticks_back_and_the_next_flush_writes_them(tmp_path, monkeypatch):
    f = make_feed(tmp_path)
    f.proc._ticks.extend(tick(i) for i in range(5))
    real = tick_store.write_ticks

    def failing(ticks, lake_dir=None):
        raise OSError("disk full")
    monkeypatch.setattr(tick_store, "write_ticks", failing)
    with pytest.raises(OSError):
        f.persist()
    assert [t.ltp for t in f.proc._ticks] == [100.0, 101.0, 102.0, 103.0, 104.0]          # kept, in order
    monkeypatch.setattr(tick_store, "write_ticks", real)
    f.proc._ticks.append(tick(5))
    assert f.persist() == 6                                                               # the five and the new one


def test_a_failed_events_write_keeps_the_events_but_not_the_ticks_already_written(tmp_path, monkeypatch):
    f = make_feed(tmp_path)
    f.proc.ext = True
    f.proc._ticks.append(tick(0))
    f.proc.events.append({"ts": datetime(2026, 11, 3, 3, 40), "kind": "segment", "segment": "NSE_FO", "status": "NORMAL_OPEN", "updated": None})

    def failing(events, lake_dir=None):
        raise OSError("disk full")
    monkeypatch.setattr(tick_store, "write_events", failing)
    with pytest.raises(OSError):
        f.persist()
    assert f.proc._ticks == [] and len(f.proc.events) == 1                                 # the tick is on disk, the event waits


def test_housekeeping_keeps_running_after_failed_flushes(tmp_path, monkeypatch, caplog):
    f = make_feed(tmp_path)
    calls: list[int] = []

    def flaky(final=False):
        calls.append(1)
        if len(calls) <= 2:
            raise OSError("disk full")
        return 7
    monkeypatch.setattr(f, "persist", flaky)

    async def go():
        stop = asyncio.Event()
        task = asyncio.create_task(f._housekeeping(Ws(), stop))
        await asyncio.sleep(0.5)
        stop.set()
        await asyncio.wait_for(task, 2)
    with caplog.at_level("INFO"):
        asyncio.run(go())
    assert len(calls) >= 3 and caplog.text.count("flush failed") == 2 and "ticks_written=7" in caplog.text


def test_a_dead_housekeeping_task_is_reported(caplog):
    async def boom():
        raise RuntimeError("something broke")

    async def go():
        t = asyncio.create_task(boom())
        t.add_done_callback(UpstoxFeed._housekeeping_ended)
        await asyncio.sleep(0.05)
    with caplog.at_level("ERROR"):
        asyncio.run(go())
    assert "housekeeping task died" in caplog.text and "something broke" in caplog.text


# ---------------------------------------------------------------------------- SCRUM-123: one bad tick, a dead flush task
def poison_writer(real, bad_symbol="BAD.NS"):
    def writer(ticks, lake_dir=None):
        ticks = list(ticks)
        if any(t.symbol == bad_symbol for t in ticks):
            raise ValueError("cannot convert")
        return real(ticks, lake_dir)
    return writer


def test_one_unwritable_tick_is_dropped_and_logged_and_the_rest_of_the_batch_is_written(tmp_path, monkeypatch, caplog):
    f = make_feed(tmp_path)
    ticks = [tick(i) for i in range(40)]
    ticks[17] = Tick(ticks[17].ts, "BAD.NS", 1.0, 1)
    f.proc._ticks.extend(ticks)
    monkeypatch.setattr(tick_store, "write_ticks", poison_writer(tick_store.write_ticks))
    with caplog.at_level("ERROR"):
        assert f.persist() == 39
    assert f.proc._ticks == [] and f.write_dropped == 1 and "dropped 1 tick(s)" in caplog.text
    assert f.persist() == 0                                                              # nothing is retried forever


def test_when_no_tick_can_be_written_the_whole_batch_is_kept(tmp_path, monkeypatch):
    f = make_feed(tmp_path)
    f.proc._ticks.extend(tick(i) for i in range(20))

    def failing(ticks, lake_dir=None):
        raise OSError("disk full")
    monkeypatch.setattr(tick_store, "write_ticks", failing)
    with pytest.raises(OSError):
        f.persist()
    assert len(f.proc._ticks) == 20 and f.write_dropped == 0                             # a disk problem drops nothing


def test_ticks_not_tried_because_the_budget_ran_out_wait_for_the_next_flush(tmp_path, monkeypatch):
    f = make_feed(tmp_path)
    f.BISECT_BUDGET = 6
    ticks = [tick(i) for i in range(64)]
    ticks[5] = Tick(ticks[5].ts, "BAD.NS", 1.0, 1)
    f.proc._ticks.extend(ticks)
    monkeypatch.setattr(tick_store, "write_ticks", poison_writer(tick_store.write_ticks))
    written, total = f.persist(), 0
    assert written > 0 and f.proc._ticks                                                 # part written, part waiting
    f.BISECT_BUDGET = 256
    while f.proc._ticks:
        total += f.persist()
    assert written + total + f.write_dropped == 64                                       # every tick is written or counted


def test_the_receive_loop_restarts_a_housekeeping_task_that_died(tmp_path, caplog):
    from websockets.asyncio.server import serve
    from tests.unit.test_cli_and_feed_extras import stock_frame, ms, KMAP

    f = UpstoxFeed("T", KMAP, url="ws://x", lake_dir=tmp_path / "lake", flush_s=0.01, write_s=0.02, stale_s=3600)
    starts = []

    async def hk(ws, stop):
        starts.append(1)
        if len(starts) == 1:
            raise RuntimeError("boom")                                                   # dies at once, as the old code could
        await stop.wait()
    f._housekeeping = hk

    async def handler(ws):
        await ws.recv()
        for i in range(30):
            await ws.send(stock_frame(ms(3, 45, 5 + i), 100.0 + i, 1))
            await asyncio.sleep(0.02)

    async def go():
        async with serve(handler, "127.0.0.1", 0) as srv:
            f.url = f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}"
            stop = asyncio.Event()
            task = asyncio.create_task(f.run_session(stop))
            await asyncio.sleep(0.8)
            stop.set()
            await asyncio.wait_for(task, 5)
    with caplog.at_level("ERROR"):
        asyncio.run(go())
    assert len(starts) >= 2 and "restarting the housekeeping task" in caplog.text
