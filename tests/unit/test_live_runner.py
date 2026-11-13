"""The live runner (SCRUM-197): replaying a day bucket by bucket gives the batch values for every live signal, stays bounded in memory,
and falls back from 10 s to 1 m when cycles overrun."""

import numpy as np
import pandas as pd
import pytest

from signals import catalogue  # noqa: F401
from signals.framework import Context
from signals.live_runner import LiveRunner, bar_volume, cadence, live_signal_ids, replay, ticks_to_bars
from signals.options.core import INDEX_SYMBOL, VIX_SYMBOL
from signals.synthetic import make_raw


def split_day(raw: dict):
    """(history before the last tick day, that day's ticks with the index and VIX as one tick a minute, the day)."""
    t = raw["ticks_fo"].copy()
    day = pd.to_datetime(t["fts"]).dt.normalize().max()
    hist = {}
    for k, df in raw.items():
        if df is None or not len(df) or k == "ticks_fo":
            continue
        if "ts" in df.columns:
            hist[k] = df[pd.to_datetime(df["ts"]) < day].reset_index(drop=True)
        elif "known_at" in df.columns:
            hist[k] = df[pd.to_datetime(df["known_at"]) < day].reset_index(drop=True)
        else:
            hist[k] = df
    today = t[pd.to_datetime(t["fts"]).dt.normalize() == day]
    idx = []
    for name, sym in (("index_1m", INDEX_SYMBOL["NIFTY"]), ("vix_1m", VIX_SYMBOL)):
        b = raw[name]
        b = b[pd.to_datetime(b["ts"]).dt.normalize() == day]
        idx.append(pd.DataFrame({"fts": pd.to_datetime(b["ts"]) + pd.Timedelta(seconds=59), "symbol": sym, "ltp": b["close"].to_numpy()}))
    ticks = pd.concat([today] + idx, ignore_index=True)
    ticks["ts"] = ticks["fts"]
    return hist, ticks, day


def batch_reference(hist, ticks, sid, bucket="1min"):
    """What the batch computes for the day with all of the day's ticks at once, the bars built the same way."""
    r = LiveRunner("NIFTY", hist, bucket=bucket, ids=[sid])
    r.ingest(ticks)
    r.watermark = r.watermark.floor(bucket) + pd.Timedelta(bucket)
    return r.cycle()


@pytest.fixture(scope="module")
def day():
    return split_day(make_raw())


@pytest.fixture(scope="module")
def replayed(day):
    hist, ticks, _ = day
    return replay("NIFTY", hist, ticks, step="5min")


def _same(a: pd.DataFrame, b: pd.DataFrame, sid: str):
    keys = [c for c in ("ts", "known_at", "snap", "symbol", "expiry", "strike", "cp") if c in a.columns and c in b.columns]
    a = a.drop(columns=["signal", "bucket", "underlying"], errors="ignore").sort_values(keys, kind="stable").reset_index(drop=True)
    b = b.drop(columns=["signal", "bucket", "underlying"], errors="ignore").sort_values(keys, kind="stable").reset_index(drop=True)
    assert len(a) == len(b), f"{sid}: {len(a)} replayed rows, {len(b)} batch rows"
    for c in a.columns:
        if pd.api.types.is_numeric_dtype(a[c]) and not pd.api.types.is_bool_dtype(a[c]):
            np.testing.assert_allclose(a[c].to_numpy(float), b[c].to_numpy(float), rtol=1e-9, atol=1e-12, equal_nan=True, err_msg=f"{sid}.{c}")
        else:
            assert a[c].astype(str).tolist() == b[c].astype(str).tolist(), f"{sid}.{c}"


@pytest.mark.parametrize("sid", live_signal_ids())
def test_replay_equals_batch(day, replayed, sid):
    hist, ticks, _ = day
    ref = batch_reference(hist, ticks, sid)
    got = replayed[replayed["signal"] == sid] if len(replayed) else replayed
    if ref.empty:
        assert got.empty, f"{sid}: replay wrote rows the batch does not have"
        pytest.skip(f"{sid}: no rows on the synthetic day")
    _same(got, ref[ref["signal"] == sid], sid)


def test_only_closed_buckets_are_written(day):
    hist, ticks, d = day
    r = LiveRunner("NIFTY", hist, ids=["vrp", "fut_ofi"])
    cut = d + pd.Timedelta(hours=5, minutes=0, seconds=30)
    r.ingest(ticks[pd.to_datetime(ticks["fts"]) <= cut])
    rows = r.cycle()
    assert len(rows) and (pd.to_datetime(rows["known_at"]) <= cut.floor("min")).all()


def test_ticks_are_kept_for_the_window_only(day):
    hist, ticks, d = day
    r = LiveRunner("NIFTY", hist, ids=["fut_ofi"])
    for _, g in ticks.groupby(pd.to_datetime(ticks["fts"]).dt.floor("10min")):
        r.ingest(g)
    span = pd.to_datetime(r.ticks["fts"]).max() - pd.to_datetime(r.ticks["fts"]).min()
    assert span <= pd.Timedelta(minutes=41)
    assert r.bars["ts"].dt.normalize().nunique() == 1 and len(r.bars)


def test_bars_do_not_depend_on_how_ticks_arrive(day):
    _, ticks, _ = day
    one = ticks_to_bars(ticks, "1min")
    r = LiveRunner("NIFTY", {"contracts": make_raw()["contracts"]}, ids=["fut_ofi"])
    for _, g in ticks.groupby(pd.to_datetime(ticks["fts"]).dt.floor("3min")):
        r.ingest(g)
    many = r.bars.sort_values(["ts", "symbol"]).reset_index(drop=True)
    one = one[one["symbol"].isin(many["symbol"])].sort_values(["ts", "symbol"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(one[["ts", "symbol", "close", "vol_last"]], many[["ts", "symbol", "close", "vol_last"]])
    assert (bar_volume(many) >= 0).all()


def test_fallback_after_three_overruns():
    r = LiveRunner("NIFTY", {"contracts": pd.DataFrame()}, ids=["fut_ofi"])
    assert not r.check_budget(12.0, 10.0) and not r.check_budget(11.0, 10.0)
    assert not r.check_budget(3.0, 10.0)                       # a fast cycle resets the count
    assert [r.check_budget(12.0, 10.0) for _ in range(3)] == [False, False, True]


def test_cadence_setting():
    assert cadence({}) == "1min" and cadence({"JARVIS_SIGNAL_CADENCE": "10s"}) == "10s"
    with pytest.raises(ValueError):
        cadence({"JARVIS_SIGNAL_CADENCE": "5s"})
