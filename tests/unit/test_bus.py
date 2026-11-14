"""In-memory tick bus (SCRUM-159): round trip, never blocks the feed, counts what a subscriber missed, the feed publishes every tick."""

import time

import pandas as pd
import pytest

from data_layer import bus


@pytest.fixture()
def env(tmp_path):
    return {"JARVIS_BUS_DIR": str(tmp_path / "bus")}


def _drain(sub, want, timeout_s=3.0):
    got, end = [], time.time() + timeout_s
    while len(got) < want and time.time() < end:
        got += [m for _, m in sub.poll(timeout_ms=50)]
    return got


def test_round_trip(env):
    pub = bus.Publisher("ticks-fo-1", env=env)
    sub = bus.Subscriber(["ticks-fo-1"], env=env)
    time.sleep(0.2)                                            # ZeroMQ: a subscription takes a moment to reach the publisher
    t0 = pd.Timestamp("2026-11-13 04:00:00.123456")
    rows = bus.tick_rows([{"ts": t0, "fts": t0, "rts": t0, "symbol": "NIFTY_FUT_25_NOV_26", "ltp": 24000.5, "bid": 24000.0, "ask": 24001.0,
                           "bid_qty": 75, "ask_qty": 150, "oi": 1000}])
    assert pub.publish("tick", list(bus.TICK_FIELDS), rows)
    msgs = _drain(sub, 1)
    df = bus.rows_frame(msgs)
    assert len(df) == 1 and df["symbol"].iloc[0] == "NIFTY_FUT_25_NOV_26"
    assert df["fts"].iloc[0] == t0 and df["ltp"].iloc[0] == 24000.5 and pd.isna(df["bid2"].iloc[0])
    pub.close(); sub.close()


def test_publishing_never_blocks_without_subscribers(env):
    pub = bus.Publisher("ticks-eq-1", env=env)
    t0 = time.perf_counter()
    for _ in range(20_000):
        pub.publish("tick", ["x"], [[1]])
    assert time.perf_counter() - t0 < 5.0 and pub.seq == 20_000
    pub.close()


def test_gaps_are_counted(env):
    pub = bus.Publisher("ticks-fo-2", env=env)
    sub = bus.Subscriber(["ticks-fo-2"], env=env)
    time.sleep(0.2)
    pub.publish("tick", ["x"], [[1]])
    pub.seq += 3                                               # three messages lost on the way
    pub.publish("tick", ["x"], [[2]])
    assert len(_drain(sub, 2)) == 2 and sub.missed == 3
    pub.close(); sub.close()


def test_feed_publishes_every_tick_and_still_writes(env, tmp_path, monkeypatch):
    from data_layer.upstox import feed
    from tests.unit.test_depth5 import KEY, frame
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    pub = bus.Publisher("ticks-fo-1")
    sub = bus.Subscriber(["ticks-fo-1"])
    time.sleep(0.2)
    f = feed.UpstoxFeed("tok", {KEY: "NIFTY_FUT_25_NOV_26"}, lake_dir=tmp_path / "lake", publisher=pub)
    f.proc.handle(frame(), pd.Timestamp("2026-11-13 04:00:02").to_pydatetime())
    df = bus.rows_frame(_drain(sub, 1))
    assert list(df["symbol"]) == ["NIFTY_FUT_25_NOV_26"] and df["bid"].iloc[0] == 99.95
    assert f.persist(final=True) == 1                          # tick storage is unchanged
    pub.close(); sub.close()


def test_a_broken_bus_never_stops_capture(tmp_path):
    from data_layer.upstox import feed
    from tests.unit.test_depth5 import KEY, frame

    class Broken:
        def publish(self, *a, **k):
            raise RuntimeError("boom")
    f = feed.UpstoxFeed("tok", {KEY: "NIFTY_FUT_25_NOV_26"}, lake_dir=tmp_path / "lake", publisher=Broken())
    f.proc.handle(frame(), pd.Timestamp("2026-11-13 04:00:02").to_pydatetime())
    assert f.proc.bus_errors == 1 and f.persist(final=True) == 1


def test_switch_and_names():
    assert bus.bus_enabled({}) is False and bus.bus_enabled({"JARVIS_TICK_BUS": "1"}) is True
    assert bus.feed_names({}) == ["ticks-eq-1", "ticks-fo-1", "ticks-fo-2"]
    assert bus.endpoint("ticks-fo-1", {"JARVIS_BUS_DIR": "/x"}) == "ipc:///x/ticks-fo-1.sock"
