"""1-second base resolution: aggregation, roll-up equivalence, verification and defaults."""
import random
from dataclasses import asdict
from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from data_layer import tick_store, verify
from data_layer.ticks import BarAggregator, Tick

OPEN = datetime(2026, 10, 26, 3, 45, 0)
DAY = date(2026, 10, 26)


def random_ticks(seed, minutes=30, sym="A.NS", start=OPEN):
    rnd = random.Random(seed)
    out, t, vtt, px = [], start, 1000, 100.0
    while t < start + timedelta(minutes=minutes):
        t += timedelta(milliseconds=rnd.randint(20, 2500))
        vtt += rnd.randint(0, 40)
        px = round(px * (1 + rnd.uniform(-0.001, 0.001)), 2)
        out.append(Tick(t, sym, px, vtt, bid=round(px - .05, 2), bid_qty=rnd.randint(1, 9),
                        ask=round(px + .05, 2), ask_qty=rnd.randint(1, 9)))
    return out


# -- defaults ------------------------------------------------------------------------------------
def test_bar_aggregator_defaults_to_one_second():
    # SCRUM-77: FeedProcessor/UpstoxFeed no longer aggregate bars at all (they persist raw ticks
    # unconditionally, at whatever resolution Upstox sends), so there is no "default interval" left
    # to assert on them. BarAggregator itself is still a real utility (used below, and conceptually
    # mirrored by Lake._ticks_bars_sql for on-read aggregation) and still defaults to one second.
    assert BarAggregator().interval == timedelta(seconds=1)


# -- aggregation at 1 s ----------------------------------------------------------------------------
def test_sub_second_updates_collapse_into_one_bar_and_next_second_starts_a_new_one():
    ag = BarAggregator(1)
    out = []
    for ms_, px, v in ((0, 100.0, 10), (250, 102.0, 15), (999, 99.0, 22), (1000, 101.0, 30)):
        out += ag.add(Tick(OPEN + timedelta(milliseconds=ms_), "A.NS", px, v))
    out += ag.flush_all()
    assert [(b.ts.second, b.open, b.high, b.low, b.close, b.n_ticks) for b in out] == [
        (0, 100.0, 102.0, 99.0, 99.0, 3), (1, 101.0, 101.0, 101.0, 101.0, 1)]
    assert out[0].volume == 12 and out[1].volume == 8           # baseline is the very first update only


def test_late_update_for_a_closed_second_is_dropped_and_counted():
    ag = BarAggregator(1)
    ag.add(Tick(OPEN + timedelta(seconds=5), "A.NS", 10.0, 1))
    ag.add(Tick(OPEN + timedelta(seconds=7), "A.NS", 11.0, 2))
    assert ag.add(Tick(OPEN + timedelta(seconds=6), "A.NS", 99.0, 3)) == [] and ag.late_dropped == 1
    (b,) = ag.flush_all()
    assert b.close == 11.0                                       # the late price never leaked in


def test_quiet_seconds_produce_no_rows_and_flush_emits_after_the_second_ends():
    ag = BarAggregator(1)
    ag.add(Tick(OPEN, "A.NS", 10.0, 1))
    assert ag.flush(OPEN + timedelta(milliseconds=999)) == []
    assert len(ag.flush(OPEN + timedelta(seconds=1))) == 1
    assert ag.flush(OPEN + timedelta(seconds=60)) == []


def test_session_boundaries_hold_at_one_second():
    ag = BarAggregator(1)
    ag.add(Tick(datetime(2026, 10, 26, 3, 44, 59, 999000), "A.NS", 1.0, 1))
    ag.add(Tick(datetime(2026, 10, 26, 10, 0, 0), "A.NS", 1.0, 2))
    assert ag.flush_all() == []
    ag.add(Tick(datetime(2026, 10, 26, 9, 59, 59, 999000), "A.NS", 1.0, 3))
    assert [b.ts for b in ag.flush_all()] == [datetime(2026, 10, 26, 9, 59, 59)]


def test_ticks_derived_bars_match_bar_aggregator_exactly(world):
    """SCRUM-77: the live feed now persists raw ticks and Lake._ticks_bars_sql builds bars from them
    on read, instead of BarAggregator building them in-process. The two must agree exactly."""
    ticks = random_ticks(4, minutes=20)
    ag, direct = BarAggregator(1), []
    for t in ticks:
        direct += ag.add(t)
    direct += ag.flush_all()
    tick_store.write_ticks(ticks, world.lakedir)
    tick_store.compact_ticks_day(DAY, world.lakedir)
    derived = world.query.Lake().sql("SELECT * FROM bars_1s ORDER BY ts")
    expected = pd.DataFrame([asdict(b) for b in direct]).sort_values("ts").reset_index(drop=True)
    assert len(derived) == len(expected)
    for c in ("open", "high", "low", "close", "volume", "n_ticks", "bid", "ask", "bid_qty", "ask_qty"):
        assert (derived[c].astype(float) - expected[c].astype(float)).abs().max() == 0, c
    assert (derived["vwap"] - expected["vwap"]).abs().max() < 1e-9


# -- roll-up equivalence -----------------------------------------------------------------------------
@pytest.mark.parametrize("seed", [1, 2, 3])
@pytest.mark.parametrize("width", [60, 300, 900, 3600, 1500])
def test_rollup_of_1s_bars_equals_bars_built_directly_at_that_width(world, seed, width):
    ticks = random_ticks(seed, minutes=120)
    a1, aw = BarAggregator(1), BarAggregator(width)
    b1, bw = [], []
    for t in ticks:
        b1 += a1.add(t); bw += aw.add(t)
    b1 += a1.flush_all(); bw += aw.flush_all()
    tick_store.write_bars(b1, "1s", world.lakedir)
    rolled = world.query.Lake().rollup(width)
    direct = pd.DataFrame([asdict(b) for b in bw])
    m = rolled.merge(direct, on=["symbol", "ts"], suffixes=("_r", "_d"))
    assert len(rolled) == len(direct) == len(m)
    for c in ("open", "high", "low", "close", "volume", "n_ticks", "bid", "ask", "bid_qty", "ask_qty"):
        assert (m[c + "_r"].astype(float) - m[c + "_d"].astype(float)).abs().max() == 0, c
    assert (m.vwap_r - m.vwap_d).abs().max() < 1e-9


def test_rollup_alignment_is_per_day_even_for_widths_that_do_not_divide_a_day(world):
    day2 = OPEN + timedelta(days=1)
    bars = []
    for start in (OPEN, day2):
        ag = BarAggregator(1)
        for t in random_ticks(7, 10, start=start):
            bars += ag.add(t)
        bars += ag.flush_all()
    tick_store.write_bars(bars, "1s", world.lakedir)
    r = world.query.Lake().rollup(1500)                           # 25 minutes
    assert set(r.ts.dt.time) <= {(OPEN + timedelta(seconds=1500 * k)).time() for k in range(15)}
    assert {t.date() for t in r.ts} == {DAY, DAY + timedelta(days=1)}


def test_rollup_filters_symbol_and_window_and_validates(world):
    bars = []
    for sym in ("A.NS", "B.NS"):
        ag = BarAggregator(1)
        for t in random_ticks(5, 5, sym):
            bars += ag.add(t)
        bars += ag.flush_all()
    tick_store.write_bars(bars, "1s", world.lakedir)
    q = world.query.Lake()
    assert set(q.rollup(60, symbol="A.NS").symbol) == {"A.NS"}
    w = q.rollup(60, start="2026-10-26 03:47:00", end="2026-10-26 03:49:00")
    assert w.ts.min() == datetime(2026, 10, 26, 3, 47) and w.ts.max() == datetime(2026, 10, 26, 3, 48)
    with pytest.raises(ValueError):
        q.rollup(0)


# -- verification at 1 s ---------------------------------------------------------------------------------
def bars_for(sym, seconds, step=1, px=100.0, start=OPEN):
    ag, out = BarAggregator(1), []
    for k, off in enumerate(range(0, seconds, step)):
        out += ag.add(Tick(start + timedelta(seconds=off), sym, px + k * 0.0001, 1000 + 10 * k,
                           bid=px - 0.05, bid_qty=1, ask=px + 0.05, ask_qty=1))
    return out + ag.flush_all()


def test_verify_bars_one_second_passes_without_expecting_a_bar_every_second(world, capsys):
    world.set_universe({"IN": ["A.NS"]})
    tick_store.write_bars(bars_for("A.NS", 22_500, step=5), "1s", world.lakedir)
    rep = verify.verify_bars(world.query.Lake(), DAY, "1s")
    out = capsys.readouterr().out
    assert rep.fails == 0 and "[PASS] first bar" in out and "[PASS] last bar" in out
    assert "active seconds per symbol: min 4500" in out and "[PASS] symbols silent for more than 5 minutes" in out


def test_verify_bars_one_second_flags_long_silence_and_late_start(world, capsys):
    world.set_universe({"IN": ["A.NS", "GAP.NS"]})
    gap = bars_for("GAP.NS", 600, step=1) + bars_for("GAP.NS", 600, step=1, start=OPEN + timedelta(minutes=30))
    tick_store.write_bars(bars_for("A.NS", 22_500, step=5, start=OPEN + timedelta(minutes=2)) + gap, "1s", world.lakedir)
    verify.verify_bars(world.query.Lake(), DAY, "1s")
    out = capsys.readouterr().out
    assert "[WARN] symbols silent for more than 5 minutes" in out and "GAP.NS" in out
    assert "[PASS] first bar" in out                              # GAP.NS and the others, earliest bar is at the open


# -- live vs REST -----------------------------------------------------------------------------------------
def rest_rows(world, rows):
    from data_layer.upstox.candles import candles_to_bars
    bars = []
    for sym, minute, o, h, lo, c, v in rows:
        t = OPEN + timedelta(minutes=minute)
        bars += candles_to_bars([[f"{t + timedelta(hours=5, minutes=30):%Y-%m-%dT%H:%M:%S}+05:30", o, h, lo, c, v, 0]], sym)
    tick_store.write_bars(bars, "1m", world.lakedir)


def live_minutes(world, sym, n=10, px=100.0, vol_per_min=100):
    ag, out = BarAggregator(1), []
    for m in range(n):
        for s in (0, 30, 59):
            k = m * 3 + (s // 30)
            out += ag.add(Tick(OPEN + timedelta(minutes=m, seconds=s), sym, px + m, 1000 + vol_per_min * (m + s / 60)))
    tick_store.write_bars(out + ag.flush_all(), "1s", world.lakedir)


def test_compare_rest_needs_both_sides(world):
    assert verify.verify_against_rest(world.query.Lake(), DAY).fails == 1
    live_minutes(world, "A.NS")
    assert verify.verify_against_rest(world.query.Lake(), DAY).fails == 1


def test_compare_rest_agreement_passes_and_disagreement_warns(world, capsys):
    live_minutes(world, "A.NS")
    live = world.query.Lake().rollup(60)
    good = [("A.NS", i, r.open, r.high, r.low, r.close, r.volume) for i, r in enumerate(live.itertuples())]
    rest_rows(world, good)
    rep = verify.verify_against_rest(world.query.Lake(), DAY)
    out = capsys.readouterr().out
    assert rep.fails == 0 and "[WARN]" not in out and "100.0%" in out
    bad = [(s, i, o, h, lo, c * 1.02, v * 3) for s, i, o, h, lo, c, v in good]     # 2% off, 3x volume
    world2 = world.lakedir / "bars/interval=1m"
    import shutil
    shutil.rmtree(world2)
    rest_rows(world, bad)
    verify.verify_against_rest(world.query.Lake(), DAY)
    out = capsys.readouterr().out
    assert "[WARN] minutes whose close agrees within 0.2%" in out and "[WARN] total volume live / REST" in out


def test_compare_rest_low_coverage_fails(world):
    live_minutes(world, "A.NS", n=2)
    rest_rows(world, [("A.NS", i, 100, 101, 99, 100, 10) for i in range(10)])    # REST has 10 minutes, live only 2
    assert verify.verify_against_rest(world.query.Lake(), DAY).fails == 1


def test_verify_cli_compare_rest_exit_code(world, monkeypatch):
    import sys
    monkeypatch.setattr(sys, "argv", ["verify", "compare-rest", "--date", "2026-10-26"])
    with pytest.raises(SystemExit) as e:
        verify.main()
    assert e.value.code == 1


def test_many_small_segments_compact_to_one_deduplicated_file(world):
    """A real session writes ~1,500 segments (one per 15 s flush); compaction must keep every row once."""
    from data_layer.ticks import Bar
    rows_expected = 0
    for seg in range(300):
        recs = []
        for sec in range(15):
            ts = OPEN + timedelta(seconds=seg * 15 + sec)
            recs += [Bar(ts, f"S{i}.NS", "IN", 1.0, 2.0, 0.5, 1.5, 10, 1.5, 1, 1.4, 1.6, 1, 1) for i in range(5)]
        rows_expected += len(recs)
        tick_store.write_bars(recs, "1s", world.lakedir)
    tick_store.write_bars(recs, "1s", world.lakedir)                  # the last flush is written twice (e.g. a retry)
    n = tick_store.compact_day("1s", DAY, world.lakedir)
    assert n == rows_expected == 300 * 15 * 5
    folder = world.lakedir / "bars/interval=1s/date=2026-10-26"
    assert [p.name for p in folder.iterdir()] == ["bars.parquet"]
    df = world.query.Lake().sql("SELECT symbol, ts FROM bars_1s")
    assert not df.duplicated().any()
