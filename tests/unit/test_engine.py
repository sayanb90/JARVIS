"""The live engine (SCRUM-201, SCRUM-159): mixed cadences in one engine, ticks over the bus to signal rows on the bus and in the store,
sub-minute parity with the whole-day computation, and the overrun fallback."""

import time

import numpy as np
import pandas as pd
import pytest

from data_layer import bus
from signals.engine import BusSource, Engine
from signals.live_runner import LiveRunner, replay
from signals.options.stream import read_live
from signals.synthetic import make_raw
from tests.unit.test_live_runner import _same, split_day, trim


@pytest.fixture(scope="module")
def day():
    return split_day(make_raw())


def _feed(eng, ticks, step="1min"):
    t = ticks.copy()
    t["fts"] = pd.to_datetime(t["fts"])
    out = []
    for k, g in t.sort_values("fts", kind="stable").groupby(t["fts"].dt.floor(step)):
        eng.on_ticks(g)
        out.append(eng.step(k + pd.Timedelta(step) + pd.Timedelta(seconds=2)))
    return pd.concat([o for o in out if len(o)], ignore_index=True)


def test_mixed_cadences_in_one_engine(day):
    hist, ticks, d = day
    cad = {"skew": "1min", "term_slope": "1min", "signed_flow": "10s", "book_pressure": "10s", "fut_ofi": "1s"}
    eng = Engine(["NIFTY"], {"NIFTY": trim(hist)}, cad)
    rows = _feed(eng, ticks[pd.to_datetime(ticks["fts"]) < d + pd.Timedelta(hours=4, minutes=15)])
    by = rows.groupby("signal")["bucket"].agg(set).to_dict()
    assert by["skew"] == {"1min"} and by["signed_flow"] == {"10s"} and by["fut_ofi"] == {"1s"}
    for b, td in (("1min", "1min"), ("10s", "10s"), ("1s", "1s")):
        r = rows[rows["bucket"] == b]
        ka = pd.to_datetime(r["known_at"])
        assert (ka == ka.dt.floor(td)).all(), b                # rows are known at their bucket boundaries


def test_ticks_over_the_bus_to_signals_on_the_bus_and_in_the_store(day, tmp_path):
    hist, ticks, d = day
    env = {"JARVIS_BUS_DIR": str(tmp_path / "bus")}
    feed_pub = bus.Publisher("ticks-fo-1", env=env)
    sig_pub = bus.Publisher("signals", env=env)
    sig_sub = bus.Subscriber(["signals"], topics=["signal"], env=env)
    src = BusSource(["ticks-fo-1"], env=env)
    time.sleep(0.3)
    eng = Engine(["NIFTY"], {"NIFTY": trim(hist)}, {"pcr_near": "1min", "book_pressure": "10s"}, store=tmp_path / "store", publisher=sig_pub)
    part = ticks[pd.to_datetime(ticks["fts"]) < d + pd.Timedelta(hours=4, minutes=5)].sort_values("fts", kind="stable")
    for _, g in part.groupby(pd.to_datetime(part["fts"]).dt.floor("1min")):
        feed_pub.publish("tick", list(bus.TICK_FIELDS), bus.tick_rows(g.to_dict("records")))
    got, end = pd.DataFrame(), time.time() + 5
    while time.time() < end:
        df = src.poll(timeout_ms=100)
        if len(df):
            got = pd.concat([got, df], ignore_index=True)
        if len(got) >= len(part):
            break
    assert len(got) == len(part) and src.missed == 0
    eng.on_ticks(got)
    rows = eng.step(d + pd.Timedelta(hours=4, minutes=6))
    assert {"pcr_near", "book_pressure"} <= set(rows["signal"])
    msgs = [m for _, m in sig_sub.poll(timeout_ms=2000)]
    assert {m["rows"][0][m["cols"].index("signal")] for m in msgs} == {"pcr_near", "book_pressure"}
    assert eng.flush() == len(rows)
    stored = read_live(tmp_path / "store", d.date())
    assert len(stored) == len(rows)
    for p in (feed_pub, sig_pub):
        p.close()
    sig_sub.close()
    src.sub.close()


def test_overrunning_group_moves_to_the_next_cadence(day):
    hist, ticks, d = day
    eng = Engine(["NIFTY"], {"NIFTY": trim(hist)}, {"signed_flow": "10s", "skew": "1min"})
    for r in eng.runners.values():
        r.check_budget = lambda secs, cad: True                # every cycle counts as an overrun
    _feed(eng, ticks[pd.to_datetime(ticks["fts"]) < d + pd.Timedelta(hours=4)])
    assert ("NIFTY", "10s") not in eng.runners and "signed_flow" in eng.runners[("NIFTY", "1min")].ids
    assert eng.fallbacks and eng.fallbacks[0][:3] == ("NIFTY", "10s", "1min")


SUB_MINUTE = ["skew", "term_slope", "curvature", "pcr_near", "gex", "cp_iv_gap", "pin", "signed_flow", "spread_gate", "book_pressure",
              "illiquidity", "pcr_full", "rr25"]


@pytest.fixture(scope="module")
def ten_seconds(day):
    """The first two hours of the day at 10 s: replayed bucket by bucket, and all at once with nothing trimmed."""
    hist, ticks, d = day
    part = ticks[pd.to_datetime(ticks["fts"]) < d + pd.Timedelta(hours=5, minutes=45)]
    got = replay("NIFTY", trim(hist), part, bucket="10s", step="1min", ids=SUB_MINUTE)
    r = LiveRunner("NIFTY", hist, bucket="10s", ids=SUB_MINUTE, tick_window=pd.Timedelta(days=1), option_sessions=100,
                   index_sessions=1000, windowed=False)
    r.ingest(part)
    r.watermark = r.watermark.floor("10s") + pd.Timedelta("10s")
    return got, r.cycle()


@pytest.mark.parametrize("sid", SUB_MINUTE)
def test_ten_second_replay_equals_the_whole_day(ten_seconds, sid):
    got, ref = ten_seconds
    g, b = got[got["signal"] == sid], ref[ref["signal"] == sid]
    if b.empty:
        assert g.empty
        pytest.skip(f"{sid}: no rows")
    _same(g, b, sid)
    assert (pd.to_datetime(g["known_at"]) == pd.to_datetime(g["known_at"]).dt.floor("10s")).all()
