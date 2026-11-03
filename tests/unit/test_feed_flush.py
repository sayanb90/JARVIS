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
