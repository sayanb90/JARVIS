"""Engine v2 (SCRUM-201): the per-tick chain state reproduces the batch chain on every bucket, the numpy smile and ATM reproduce the
pandas ones, and replaying a day through the operator engine gives the pandas runner's rows for every live signal."""

import numpy as np
import pandas as pd
import pytest

from signals.engine import surface
from signals.engine.core import OperatorEngine, replay as engine_replay
from signals.engine.state import Session, td_ns
from signals.framework import Context
from signals.live_runner import LiveRunner, live_signal_ids, replay as runner_replay
from signals.synthetic import make_raw
from tests.unit.test_live_runner import split_day, trim

TICK = ("ltp", "vol_today", "oi", "bid", "ask", "bid_qty", "ask_qty", "iv", "delta", "gamma")


@pytest.fixture(scope="module")
def day():
    return split_day(make_raw())


def _cut(ticks, d, minutes):
    t = ticks.copy()
    t["fts"] = pd.to_datetime(t["fts"])
    return t[t["fts"] < d + pd.Timedelta(hours=3, minutes=45 + minutes)].sort_values("fts", kind="stable")


def same_frames(a: pd.DataFrame, b: pd.DataFrame, keys, rtol=1e-9) -> list:
    """Columns that differ between two frames (rows matched on `keys`); NaN equals NaN."""
    a = a.sort_values(keys, kind="stable").reset_index(drop=True)
    b = b.sort_values(keys, kind="stable").reset_index(drop=True)
    assert len(a) == len(b), (len(a), len(b))
    assert set(a.columns) == set(b.columns), set(a.columns) ^ set(b.columns)
    bad = []
    for c in a.columns:
        x, y = a[c], b[c]
        if pd.api.types.is_numeric_dtype(x) and pd.api.types.is_numeric_dtype(y):
            ok = np.isclose(x.astype(float), y.astype(float), rtol=rtol, atol=1e-12) | (x.isna() & y.isna())
        else:
            ok = (x.astype(str) == y.astype(str)) | (x.isna() & y.isna())
        if not ok.all():
            bad.append(c)
    return bad


def session_chains(hist, t, bucket):
    """Feed ticks into a Session in event-time order, closing each bucket before a later tick; the chains of every bucket."""
    s = Session("NIFTY", hist["contracts"], t["fts"].min().normalize(), cadences=(bucket,), rates=hist.get("rates"))
    td = td_ns(bucket)
    tsn = t["fts"].to_numpy(dtype="datetime64[ns]").astype(np.int64)
    out = {"bar": [], "live": [], "smile": []}
    cur = None

    def close(upto):
        nonlocal cur
        while cur + td <= upto:
            bk = s.close(bucket, cur)
            for k in ("bar", "live"):
                ch = bk.chain(k)
                if len(ch):
                    out[k].append(ch.frame(s.options))
                    if k == "bar":
                        rows = surface.smile_rows(s.options, ch)
                        out["smile"].append((ch.frame(s.options), surface.frames(rows, surface.atm_row(rows))))
            cur += td

    for i, row in enumerate(t.itertuples(index=False)):
        x = int(tsn[i])
        cur = x - x % td if cur is None else cur
        close(x)
        s.apply(row.symbol, x, **{c: getattr(row, c) for c in TICK})
    close(cur + td)
    return out


@pytest.mark.parametrize("bucket,minutes", [("1min", 40), ("10s", 12), ("1s", 3)])
def test_chain_state_equals_the_batch_chain(day, bucket, minutes):
    hist, ticks, d = day
    t = _cut(ticks, d, minutes)
    r = LiveRunner("NIFTY", trim(hist), bucket=bucket, ids=["pcr_near", "pcr_full"])
    for _, g in t.groupby(t["fts"].dt.floor("5min")):
        r.ingest(g)
        r.cycle()
    r.watermark = r.watermark.floor(bucket) + pd.Timedelta(bucket)
    r.cycle()
    got = session_chains(hist, t, bucket)
    for kind, ref in (("bar", r.chain), ("live", r.chain_live)):
        g = pd.concat(got[kind], ignore_index=True)
        assert len(g) > 100
        assert list(g.columns) == list(ref.columns)
        assert same_frames(ref, g, ["ts", "expiry", "strike", "cp"], rtol=1e-12) == [], kind


def test_numpy_smile_and_atm_equal_pandas(day):
    hist, ticks, d = day
    got = session_chains(hist, _cut(ticks, d, 30), "1min")
    assert len(got["smile"]) > 20
    for fr, (sm, at) in got["smile"]:
        c = Context("NIFTY", {}, cache={"chain": fr})
        assert same_frames(c.get("smile")[list(sm.columns)], sm, ["ts", "expiry"], rtol=1e-12) == []
        assert same_frames(c.get("atm")[list(at.columns)], at, ["ts"], rtol=1e-12) == []


def _compare(ref, got, ids):
    bad = {}
    for sid in ids:
        a = ref[ref["signal"] == sid].dropna(axis=1, how="all")
        b = got[got["signal"] == sid].dropna(axis=1, how="all")
        if sid == "opening_flow":                      # its class columns appear when a class occurs; absent means 0
            a, b = a.fillna(0), b.fillna(0)
            cols = sorted(set(a.columns) | set(b.columns))
            a, b = a.reindex(columns=cols, fill_value=0), b.reindex(columns=cols, fill_value=0)
        if len(a) != len(b):
            bad[sid] = ("rows", len(a), len(b))
            continue
        if not len(a):
            continue
        keys = [k for k in ("ts", "known_at", "expiry", "symbol", "strike", "cp", "snap", "date") if k in a.columns]
        diff = same_frames(a, b, keys)
        if diff:
            bad[sid] = diff
    return bad


@pytest.fixture(scope="module")
def replayed_1min(day):
    hist, ticks, d = day
    t = _cut(ticks, d, 35)
    vsd = Context("NIFTY", hist).get("volume_slot_daily")
    ids = live_signal_ids()
    ref = runner_replay("NIFTY", trim(hist), t, bucket="1min", step="5min", ids=ids, volume_slot_daily=vsd)
    got = engine_replay("NIFTY", trim(hist), t, {s: "1min" for s in ids}, step="5min", volume_slot_daily=vsd)
    return ref, got, ids


def test_engine_v2_equals_the_runner_for_every_live_signal(replayed_1min):
    ref, got, ids = replayed_1min
    assert len(got) == len(ref) > 1000
    assert len(set(got["signal"])) >= 25
    assert _compare(ref, got, ids) == {}


def test_engine_v2_sub_minute_parity(day):
    hist, ticks, d = day
    t = _cut(ticks, d, 8)
    ids = ["book_pressure", "fut_ofi", "signed_flow", "pcr_full", "rr25", "pin_full", "term_slope", "cp_iv_gap", "gex", "skew"]
    ref = runner_replay("NIFTY", trim(hist), t, bucket="10s", step="1min", ids=ids)
    got = engine_replay("NIFTY", trim(hist), t, {s: "10s" for s in ids}, step="1min")
    assert len(got) == len(ref) > 100
    assert _compare(ref, got, ids) == {}


def test_late_ticks_are_counted_and_dropped(day):
    hist, ticks, d = day
    t = _cut(ticks, d, 6)
    eng = OperatorEngine("NIFTY", trim(hist), {"pcr_full": "1min"}, d)
    eng.on_ticks(t[t["fts"] < d + pd.Timedelta(hours=3, minutes=50)])
    eng.on_ticks(t[t["fts"] >= d + pd.Timedelta(hours=3, minutes=50)])
    n = eng.late
    eng.on_ticks(t[t["fts"] < d + pd.Timedelta(hours=3, minutes=47)].head(5))      # buckets already closed
    assert eng.late == n + 5


def test_reorder_window_puts_a_slightly_late_tick_in_its_bucket(day):
    hist, ticks, d = day
    t = _cut(ticks, d, 4)
    a = engine_replay("NIFTY", trim(hist), t, {"book_pressure": "10s"}, step="1min")
    eng = OperatorEngine("NIFTY", trim(hist), {"book_pressure": "10s"}, d, reorder_ms=5000)
    shuffled = t.sample(frac=1.0, random_state=3)                        # arrival order scrambled within each batch
    parts = [eng.on_ticks(g) for _, g in shuffled.groupby(pd.to_datetime(shuffled["fts"]).dt.floor("1min"), sort=True)]
    b = pd.concat(parts + [eng.finish()], ignore_index=True)
    assert eng.late == 0
    assert _compare(a, b, ["book_pressure"]) == {}


def dense_ticks(ticks: pd.DataFrame, copies: int = 5, seed: int = 11) -> pd.DataFrame:
    """The synthetic day has one tick per contract a minute; add `copies` later ticks per tick inside the same minute (quotes, sizes,
    OI and prints moved a little, some block-sized prints), so sub-minute buckets see several updates and quiet ones."""
    rng = np.random.default_rng(seed)
    t = ticks.copy()
    t["fts"] = pd.to_datetime(t["fts"])
    o = t[t["symbol"].str.startswith("NIFTY_")]
    parts = [t]
    for j in range(1, copies + 1):
        c = o.copy()
        c["fts"] = c["fts"] + pd.to_timedelta(rng.integers(1, 29, len(c)), unit="s") + pd.Timedelta(milliseconds=j)
        step = rng.choice([-1, 0, 1], len(c)) * 0.05
        for col in ("bid", "ask"):
            c[col] = c[col] + step
        c["ltp"] = np.where(rng.random(len(c)) < 0.5, c["bid"], c["ask"])
        c.loc[rng.random(len(c)) < 0.2, "ltp"] = np.nan                      # quote-only updates
        c["bid_qty"] = rng.integers(65, 6500, len(c))
        c["ask_qty"] = rng.integers(65, 6500, len(c))
        c["vol_today"] = c["vol_today"] + 65 * j
        c["oi"] = c["oi"] + rng.integers(-300, 300, len(c))
        c["ltq"] = np.where(rng.random(len(c)) < 0.02, 65 * 120, c["ltq"])     # block-sized prints now and then
        parts.append(c)
    out = pd.concat(parts, ignore_index=True)
    out["ts"] = out["fts"]
    return out.sort_values("fts", kind="stable").reset_index(drop=True)


@pytest.mark.parametrize("bucket,minutes", [("1min", 12), ("10s", 6), ("1s", 2)])
def test_native_operators_equal_the_whole_day_batch(day, bucket, minutes):
    """On dense ticks the native operators equal the batch functions run on the whole day at once.  (The windowed adapter does not
    always: its Lee-Ready tick rule forgets the last price change at the window edge; the native operators remember the session.)"""
    from signals.engine.operators import NATIVE
    hist, ticks, d = day
    t = _cut(dense_ticks(ticks), d, minutes)
    ids = list(NATIVE)
    r = LiveRunner("NIFTY", trim(hist), bucket=bucket, ids=ids, tick_window=pd.Timedelta(days=1), windowed=False)
    r.ingest(t)
    r.watermark = r.watermark.floor(bucket) + pd.Timedelta(bucket)
    a = r.cycle()
    b = engine_replay("NIFTY", trim(hist), t, {s: bucket for s in ids}, step="1min")
    assert len(a) == len(b) > 50
    assert set(a["signal"]) == set(b["signal"])
    assert {"opening_flow", "depth_features", "signed_flow"} <= set(b["signal"])
    if bucket != "1s":            # a two-minute 1 s window may hold no block print or adjacent quoted bucket
        assert {"block_prints", "fair_value_gap"} <= set(b["signal"])
    assert _compare(a, b, ids) == {}


def test_session_loop_closes_buckets_stores_rows_publishes_provisional_and_beats(day, tmp_path):
    import json
    from signals.engine.core import run, write_heartbeat
    from signals.options.stream import read_live
    hist, ticks, d = day
    t = _cut(ticks, d, 6)
    parts = iter([g for _, g in t.groupby(t["fts"].dt.floor("10s"))])
    now = {"t": d + pd.Timedelta(hours=3, minutes=45)}

    class Src:                                     # each poll: the next 10 s of ticks; the wall clock follows them by 200 ms
        def poll(self, timeout_ms=0):
            g = next(parts, None)
            if g is None:
                now["t"] += pd.Timedelta(seconds=30)
                return pd.DataFrame()
            now["t"] = g["fts"].max() + pd.Timedelta(milliseconds=200)
            return g

    def clock():
        return now["t"]

    class Pub:
        sent = []

        def publish(self, topic, cols, rows):
            self.sent.append((topic, cols, rows))

    eng = OperatorEngine("NIFTY", trim(hist), {"book_pressure": "10s", "pcr_full": "1min"}, d)
    hb = tmp_path / "live" / "engine-NIFTY.heartbeat.json"
    pub = Pub()
    stats = run([eng], Src(), d + pd.Timedelta(hours=3, minutes=53), publisher=pub, store=tmp_path, tick_ms=1, clock=clock,
                heartbeat=write_heartbeat(hb))
    assert stats["rows"] > 0 and stats["provisional"] > 0
    topics = {tp for tp, _, _ in pub.sent}
    assert topics == {"signal", "provisional"}
    sig = [(c, r) for tp, c, r in pub.sent if tp == "signal"]
    assert all({"t_vendor", "t_recv", "t_emit"} <= set(c) for c, _ in sig)          # latency stamps on the bus
    stored = read_live(tmp_path, d.date())
    assert len(stored) and "t_emit" not in stored.columns                          # ...but not in the store
    doc = json.loads(hb.read_text())
    assert doc["engines"]["NIFTY"]["buckets"]["10s"] > 10


def test_health_reads_the_engine_heartbeat(tmp_path):
    import json
    from signals.health import engine_checks
    d = pd.Timestamp("2026-11-16")
    (tmp_path / "live").mkdir()
    f = tmp_path / "live" / "engine-NIFTY.heartbeat.json"
    f.write_text(json.dumps({"at": "2026-11-16 09:59:58", "engines": {"NIFTY": {"buckets": {"1s": 22000, "10s": 2200}, "late": 3,
                                                                                  "ticks": 900000}}}))
    assert [c.status for c in engine_checks(tmp_path, d.date())] == ["OK"]
    f.write_text(json.dumps({"at": "2026-11-16 05:10:00", "engines": {"NIFTY": {"buckets": {"1s": 100}, "late": 0, "ticks": 10}}}))
    assert [c.status for c in engine_checks(tmp_path, d.date())] == ["FAIL"]                # stopped mid-session
    f.unlink()
    assert [c.status for c in engine_checks(tmp_path, d.date())] == ["WARN"]


@pytest.mark.parametrize("bucket,minutes", [("1min", 20), ("10s", 6), ("1s", 2)])
def test_native_chain_operators_equal_their_batch_functions(day, bucket, minutes):
    """Every native chain operator against its batch function run on frames cut from the same state (the adapter path)."""
    from signals.engine.chain_ops import CHAIN_NATIVE
    hist, ticks, d = day
    t = _cut(dense_ticks(ticks), d, minutes)
    cad = {s: bucket for s in CHAIN_NATIVE}
    a = engine_replay("NIFTY", trim(hist), t, cad, step="1min", native=())
    b = engine_replay("NIFTY", trim(hist), t, cad, step="1min")
    assert len(a) == len(b) > 50
    assert set(a["signal"]) == set(b["signal"]) and len(set(b["signal"])) >= 14
    assert _compare(a, b, list(CHAIN_NATIVE)) == {}


def test_native_term_slope_off_expiry_day():
    """The synthetic tick day is an expiry (no term slope rows there): the ATM-row operator against the batch on a day-out front."""
    from signals.engine.chain_ops import ChainNatives
    from signals.engine.surface import ATM_COLS
    ts = pd.Timestamp("2026-03-30 05:00")
    row = dict.fromkeys(ATM_COLS, np.nan)
    row.update(ts=ts, expiry=pd.Timestamp("2026-03-31").date(), t=1.4 / 365, atm_iv=0.14, next_expiry=pd.Timestamp("2026-04-07").date(),
               next_t=8.4 / 365, next_atm_iv=0.13, iv_m1=0.15, iv_p1=0.135, iv_m2=0.16, iv_p2=0.13, iv7=0.131, iv7_how="interp")
    td = pd.Timedelta("1min").value
    nat = ChainNatives(["term_slope", "curvature"], td, book=None)
    got = nat.run(ts.value, None, None, row)
    c = Context("NIFTY", {}, bucket="1min", snaps="all", cache={"atm": pd.DataFrame([row])})
    for sid in ("term_slope", "curvature"):
        ref = c.compute(sid).drop(columns=["underlying"])
        g = got[sid][list(ref.columns)]
        assert len(g) == 1 and same_frames(ref, g, ["known_at"]) == []
