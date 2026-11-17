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
                heartbeat=write_heartbeat(hb), status_s=0)
    assert stats["rows"] > 0 and stats["provisional"] > 0
    topics = {tp for tp, _, _ in pub.sent}
    assert topics == {"signal", "provisional"}
    sig = [(c, r) for tp, c, r in pub.sent if tp == "signal"]
    assert all({"t_vendor", "t_recv", "t_emit"} <= set(c) for c, _ in sig)          # latency stamps on the bus
    stored = read_live(tmp_path, d.date())
    assert len(stored) and "t_emit" not in stored.columns                          # ...but not in the store
    doc = json.loads(hb.read_text())
    assert doc["engines"]["NIFTY"]["buckets"]["10s"] > 10
    last = doc["engines"]["NIFTY"]["last_status"]
    assert last["underlying"] == "NIFTY" and last["groups"]["10s"]["signals"] == 1 and last["fallbacks"] == []


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
    f.write_text(json.dumps({"at": "2026-11-16 09:59:58", "engines": {"NIFTY": {"buckets": {"1s": 22000, "10s": 2200}, "late": 0,
                                                                                  "ticks": 900000, "fallbacks": [["1s", "10s", 11, "x"]]}}}))
    (c,) = engine_checks(tmp_path, d.date())
    assert c.status == "WARN" and "coarser cadence" in c.detail
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


def test_overrunning_group_moves_to_the_next_coarser_cadence_with_exact_values(day):
    """A 1 s group whose bucket closes take longer than allowed three times in a row moves to 10 s for the rest of the session;
    from its next bucket on, the 10 s values equal an engine configured at 10 s from the start (the state keeps every coarser
    cadence's buckets)."""
    hist, ticks, d = day
    t = _cut(dense_ticks(ticks), d, 4)
    ids = ["book_pressure", "pcr_full"]
    eng = OperatorEngine("NIFTY", trim(hist), {s: "1s" for s in ids}, d)
    assert list(eng.groups) == ["1s", "10s", "1min"] and eng.groups["10s"].ids == []
    eng.groups["1s"].budget_s = 0.0                                      # every close "overruns"
    parts = [eng.on_ticks(g) for _, g in t.groupby(t["fts"].dt.floor("1min"), sort=True)] + [eng.finish()]
    got = pd.concat([p for p in parts if len(p)], ignore_index=True)
    assert len(eng.fallbacks) == 1 and eng.fallbacks[0][:2] == ("1s", "10s") and set(eng.fallbacks[0][2]) == set(ids)
    assert eng.groups["1s"].ids == [] and set(eng.groups["10s"].ids) == set(ids)
    switched = pd.Timestamp(eng.fallbacks[0][3])
    late = got[(got["bucket"] == "10s") & (pd.to_datetime(got["known_at"]) > switched + pd.Timedelta("10s"))]
    assert len(late) > 20 and not len(got[(got["bucket"] == "1s") & (pd.to_datetime(got["known_at"]) > switched + pd.Timedelta("1s"))])
    ref = engine_replay("NIFTY", trim(hist), t, {s: "10s" for s in ids}, step="1min")
    ref = ref[pd.to_datetime(ref["known_at"]) > switched + pd.Timedelta("10s")]
    assert _compare(ref, late, ids) == {}
    st = eng.status(pd.Timestamp(d) + pd.Timedelta(hours=4))
    assert st["fallbacks"] and st["groups"]["10s"]["signals"] == 2


def test_status_line_reports_every_cadence_and_fallbacks(day):
    from signals.engine.core import status_line
    hist, ticks, d = day
    eng = OperatorEngine("NIFTY", trim(hist), {"book_pressure": "1s"}, d)
    eng.on_ticks(_cut(ticks, d, 2))
    line = status_line(eng.status(d + pd.Timedelta(hours=3, minutes=47), missed=3))
    assert line.startswith("status NIFTY: ticks=") and "missed_on_bus=3" in line and "1s[sig=1" in line and "lag=" in line
    assert "FALLBACKS" not in line
    assert "state=" in line and "native=" in line          # where a bucket close goes (mean ms per phase), so the log says what is slow


def test_watch_flags_stale_behind_fallback_and_errors(tmp_path):
    import json
    from signals.watch import render
    (tmp_path / "live").mkdir()
    now = pd.Timestamp("2026-11-16 05:00:10")
    ok = {"at": "2026-11-16 05:00:09.9", "engines": {"NIFTY": {"buckets": {"1s": 4500}, "late": 1, "ticks": 90000, "lag_s": 0.3,
                                                              "errors": 0, "fallbacks": [],
                                                              "last_status": {"rss_mb": 410, "groups": {"1s": {"signals": 27, "mean_ms": 95.0,
                                                                                                              "max_ms": 180.0}}}}}}
    bad = {"at": "2026-11-16 04:59:00", "engines": {"BANKNIFTY": {"buckets": {"1s": 4000}, "late": 900, "ticks": 90000, "lag_s": 12.0,
                                                                 "errors": 2, "fallbacks": [["1s", "10s", 27, "x"]]}}}
    (tmp_path / "live" / "engine-NIFTY.heartbeat.json").write_text(json.dumps(ok))
    (tmp_path / "live" / "engine-BANKNIFTY.heartbeat.json").write_text(json.dumps(bad))
    (tmp_path / "paper-nifty.log").write_text("x status paper: signal_rows=5 fills=0 kill=off\n")
    out = render(tmp_path, tmp_path, now)
    nifty = [ln for ln in out.splitlines() if ln.strip().startswith("NIFTY")][0]
    bank = [ln for ln in out.splitlines() if ln.strip().startswith("BANKNIFTY")][0]
    assert nifty.split()[-1] == "OK" and "1s mean 95.0 / max 180.0 ms" in out
    assert "STALE" in bank and "BEHIND" in bank and "FALLBACK" in bank and "ERRORS 2" in bank and "LATE" in bank
    assert "signal_rows=5" in out


def test_supervise_restarts_after_an_error_and_stops_cleanly():
    from signals.engine.core import supervise
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("boom")
        return {"rows": 1}

    res = supervise(flaky, pd.Timestamp("2099-01-01"), sleep=lambda s: None)
    assert res == {"rows": 1, "restarts": 2} and calls["n"] == 3
    res = supervise(lambda: (_ for _ in ()).throw(MemoryError()), pd.Timestamp("2099-01-01"), max_restarts=2, sleep=lambda s: None)
    assert res["stopped"] == "too many restarts"


# ---------------------------------------------------------------------------------------------- the tick window (16 Nov 2026)
class _RefTickWindow:
    """The first version of TickWindow (a copy of the whole window on every call): the reference for the fast one."""

    def __init__(self, keep):
        self.keep, self.df, self._parts = keep, pd.DataFrame(), []

    def add(self, df):
        if len(df):
            self._parts.append(df)

    def _merge(self):
        if self._parts:
            self.df = pd.concat([self.df] + self._parts, ignore_index=True) if len(self.df) else pd.concat(self._parts, ignore_index=True)
            self._parts = []

    def trim(self, now):
        self._merge()
        if not len(self.df):
            return
        old = self.df["fts"] < now - self.keep
        if old.any():
            self.df = pd.concat([self.df[old].groupby("symbol").tail(1), self.df[~old]], ignore_index=True)

    def window(self, lo, upto):
        self._merge()
        t = self.df
        if not len(t):
            return t
        keep = (t["fts"] >= lo) & (t["fts"] < upto)
        return pd.concat([t[t["fts"] < lo].groupby("symbol").tail(1), t[keep]], ignore_index=True)


def test_tick_window_returns_what_the_whole_window_copy_returned_and_stays_on_the_fast_path():
    import numpy as np
    from signals.engine.core import TickWindow
    rng = np.random.default_rng(7)
    syms = np.array([f"S{i}" for i in range(60)])
    t0 = pd.Timestamp("2026-11-16 03:45")
    keep = pd.Timedelta(minutes=3)
    fast, ref = TickWindow(keep), _RefTickWindow(keep)
    for sec in range(600):                                         # ten minutes of one-second batches
        n = int(rng.integers(0, 40))                               # some empty seconds, some busy ones
        fts = t0 + pd.Timedelta(seconds=sec) + pd.to_timedelta(rng.integers(0, 1_000_000_000, n), unit="ns")
        if sec % 7 == 0 and n:                                     # a batch that arrives out of time order
            fts = fts[::-1]
        df = pd.DataFrame({"symbol": rng.choice(syms, n), "ltp": rng.random(n), "fts": fts})
        fast.add(df)
        ref.add(df.copy())
        now = t0 + pd.Timedelta(seconds=sec + 1)
        for lo, upto in ((now - pd.Timedelta(seconds=1), now),                                   # the 1 s group
                         (now - pd.Timedelta(seconds=2), now - pd.Timedelta(seconds=1)),
                         (now - pd.Timedelta(minutes=1), now),                                  # a coarser group, reaching back
                         (now - pd.Timedelta(minutes=5), now - pd.Timedelta(minutes=4))):       # before the trim point
            a, b = fast.window(lo, upto), ref.window(lo, upto)
            pd.testing.assert_frame_equal(a.reset_index(drop=True), b.reset_index(drop=True), check_dtype=False)
        fast.trim(now)
        ref.trim(now)
    assert fast.fast > 1000                                        # the one-second windows are served without copying the window
    assert len(fast.big) > 0 and len(fast.chunks) < 40             # the old ticks moved to the big frame; only the last seconds are chunks


def test_provisional_values_follow_their_own_interval_and_the_loop_reports_where_its_time_goes(day, tmp_path, caplog):
    """17 Nov 2026: provisional values (a copy of every operator's state) ran at every 250 ms timer step and cost about half a core.  They now
    have their own interval (JARVIS_SIGNAL_PROVISIONAL_MS, 1000 ms by default), and each status interval logs the share of the loop's time
    in waiting, applying ticks, closing buckets, provisional values, publishing and writing."""
    import logging
    from signals.engine.core import provisional_ms, run
    hist, ticks, d = day
    t = _cut(ticks, d, 4)

    def go(prov_ms):
        parts = iter([g for _, g in t.groupby(t["fts"].dt.floor("10s"))])
        now = {"t": d + pd.Timedelta(hours=3, minutes=45)}

        class Src:
            def poll(self, timeout_ms=0):
                g = next(parts, None)
                if g is None:
                    now["t"] += pd.Timedelta(seconds=30)
                    return pd.DataFrame()
                now["t"] = g["fts"].max() + pd.Timedelta(milliseconds=200)
                return g

        eng = OperatorEngine("NIFTY", trim(hist), {"book_pressure": "10s", "pcr_full": "1min"}, d)
        return run([eng], Src(), d + pd.Timedelta(hours=3, minutes=50), tick_ms=1, clock=lambda: now["t"], status_s=0, prov_ms=prov_ms)

    assert provisional_ms({}) == 1000 and provisional_ms({"JARVIS_SIGNAL_PROVISIONAL_MS": "250"}) == 250
    with caplog.at_level(logging.INFO, logger="signals.engine"):
        every_step = go(None)["provisional"]
    assert every_step > 0
    assert go(10 ** 9)["provisional"] < every_step                      # a long interval: far fewer provisional computations
    loop = [r.message for r in caplog.records if r.message.startswith("status loop:")]
    assert loop and all(k in loop[0] for k in ("poll=", "apply=", "advance=", "provisional=", "publish=", "write="))


# ---------------------------------------------------------------------------------------------- vectorised reductions (17 Nov 2026)
def test_grouped_reductions_equal_the_per_symbol_helpers():
    """Grouped (last, mean, median, sum per symbol in one numpy pass) gives what the per-symbol loops gave: the loops cost about 200 ms of
    every 1 s bucket close on the live feed, with 300 contracts ticking a second."""
    from signals.engine import operators as op
    rng = np.random.default_rng(3)
    n = 400
    syms = rng.choice([f"S{i:02d}" for i in range(37)], n)
    codes, code = np.unique(syms, return_inverse=True)

    class T:                                                   # the parts of TapeTable that Grouped reads
        pass

    tp = T()
    tp.code, tp.codes, tp.n = code, codes, n
    tp.in_b = rng.random(n) < 0.9
    a = rng.normal(size=n)
    a[rng.random(n) < 0.3] = np.nan                            # NaN-heavy: some symbols have no value at all
    a[code == 5] = np.nan
    mask = rng.random(n) < 0.8
    g = op.Grouped(tp, mask)
    ref = op.TapeTable.groups(tp, mask) if False else None     # (TapeTable.groups is a method of the real table: rebuilt below)
    idx = np.flatnonzero(mask & tp.in_b)
    order = np.lexsort((idx, code[idx]))
    idx = idx[order]
    cut = np.flatnonzero(np.diff(code[idx])) + 1
    groups = [(codes[code[s[0]]], s) for s in np.split(idx, cut)]
    assert list(g.syms) == [s for s, _ in groups] and g.n == len(groups)
    for fn, helper in ((g.last, op._last), (g.mean, op._mean), (g.median, op._median)):
        want = np.array([helper(a, rows) for _, rows in groups])
        np.testing.assert_allclose(fn(a), want, rtol=1e-12, atol=0, equal_nan=True)
    np.testing.assert_allclose(g.sum(a), np.array([op._sum(a, rows) for _, rows in groups]), rtol=1e-12, equal_nan=True)
    assert op.Grouped(tp, np.zeros(n, bool)).n == 0


def test_signal_rows_by_column_equal_the_row_by_row_version():
    import datetime as dt
    from signals.engine.runner import signal_rows
    from data_layer.bus import to_ns

    def old(df):                                               # the first version, row by row
        rows = []
        for rec in df.itertuples(index=False):
            row = []
            for v in rec:
                if isinstance(v, (pd.Timestamp, dt.datetime)):
                    row.append(to_ns(v))
                elif isinstance(v, dt.date):
                    row.append(v.isoformat())
                elif v is None or (isinstance(v, float) and v != v):
                    row.append(None)
                elif hasattr(v, "item"):
                    row.append(v.item())
                else:
                    row.append(v if isinstance(v, (int, float, str, bool)) else str(v))
            rows.append(row)
        return list(df.columns), rows

    df = pd.DataFrame({"ts": pd.to_datetime(["2026-11-17 03:45:00.123456789", "2026-11-17 03:45:01.000000000"]),
                       "known_at": pd.to_datetime(["2026-11-17 03:45:01.000000", "2026-11-17 03:45:02.000000"]).as_unit("us"),
                       "symbol": ["NIFTY_24000_CE", "NIFTY_24000_PE"], "expiry": [dt.date(2026, 11, 17), dt.date(2026, 11, 24)],
                       "strike": [24000.0, np.nan], "n": [3, 4], "flag": [True, False], "mix": [1, "a"], "maybe": [None, 2.5]})
    assert signal_rows(df) == old(df)
    assert signal_rows(df.iloc[:0]) == (list(df.columns), [])


def test_write_rows_accepts_a_frame_and_a_list_of_dicts(tmp_path):
    from signals.options.stream import read_live, write_rows
    rows = [{"ts": pd.Timestamp("2026-11-17 03:45:00"), "signal": "x", "v": 1.5}, {"ts": pd.Timestamp("2026-11-17 03:45:01"), "signal": "x", "v": 2.5}]
    assert write_rows(pd.DataFrame(rows), tmp_path, prefix="a") == 2 and write_rows(rows, tmp_path, prefix="b") == 2
    assert write_rows(pd.DataFrame(), tmp_path) == 0 and write_rows([], tmp_path) == 0
    assert len(read_live(tmp_path, pd.Timestamp("2026-11-17").date())) == 4


def test_stack_sampler_reports_where_the_engine_thread_spends_its_time():
    import time
    from signals.engine.sampler import StackSampler

    def inner_work():
        t = time.perf_counter()
        x = 0
        while time.perf_counter() - t < 0.4:
            x += sum(i * i for i in range(200))
        return x

    def outer():
        return inner_work()

    sm = StackSampler(interval=0.005).start()
    outer()
    lines = sm.report()
    sm.stop()
    text = "\n".join(lines)
    assert lines[0].startswith("profile:") and "samples" in lines[0]
    assert "inner_work" in text and "outer" in text                        # the cumulative line names the functions we were inside
    assert sm.report(reset=False)[0] == "profile: no samples"              # the report reset the counters


def test_engine_gives_the_same_rows_with_object_strings_as_with_arrow_strings(day):
    """pandas 3 keeps text in Arrow arrays; the engine switches to object strings (use_object_strings) because every selection, sort and concat
    of its per-bucket frames is then about a third faster.  The rows must not change."""
    from signals.engine.core import replay, use_object_strings
    hist, ticks, d = day
    t = _cut(ticks, d, 4)
    ids = {"book_pressure": "10s", "illiquidity": "10s", "spread_gate": "10s", "depth_features": "10s", "pcr_full": "1min", "skew": "1min"}
    with pd.option_context("future.infer_string", True):
        a = replay("NIFTY", trim(hist), t, ids, step="1min")
    with pd.option_context("future.infer_string", False):
        assert use_object_strings({}) is True
        b = replay("NIFTY", trim(hist), t, ids, step="1min")
    assert len(a) == len(b) > 0
    pd.testing.assert_frame_equal(a.sort_values(["signal", "bucket", "known_at"] + [c for c in ("symbol", "expiry", "strike", "cp") if c in a.columns]).reset_index(drop=True).astype(object),
                                  b.sort_values(["signal", "bucket", "known_at"] + [c for c in ("symbol", "expiry", "strike", "cp") if c in b.columns]).reset_index(drop=True).astype(object),
                                  check_dtype=False, check_like=True, check_column_type=False, check_index_type=False)
    assert use_object_strings({"JARVIS_OBJECT_STRINGS": "0"}) is False
