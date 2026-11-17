"""Signal input health check (SCRUM-189), nightly run and coverage report (SCRUM-190)."""

import shutil
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from signals.__main__ import coverage, nightly
from signals.framework import read_output, read_trials
from signals.health import check_day, latest_day
from tests.signals_helpers import write_fake_lake

DAY = date(2026, 3, 31)


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    d = tmp_path_factory.mktemp("lake")
    write_fake_lake(d)
    return d


def _status(checks, name):
    return {c.status for c in checks if c.name == name}


def test_health_on_complete_day(lake):
    checks = check_day(lake, DAY)
    assert _status(checks, "option_greeks") == {"OK"}
    assert _status(checks, "quotes") == {"OK"}
    assert _status(checks, "events") == {"OK"}


def test_health_flags_missing_inputs(lake, tmp_path):
    broken = tmp_path / "lake"
    shutil.copytree(lake, broken)
    shutil.rmtree(broken / "ticks")
    checks = check_day(broken, DAY)
    assert "FAIL" in _status(checks, "option_ticks")
    (broken / "instruments" / "fo_expired.parquet").unlink()
    assert "FAIL" in _status(check_day(broken, DAY), "contracts")


def test_health_index_minutes(lake):
    checks = {c.detail.rsplit(":", 1)[0]: c.status for c in check_day(lake, DAY) if c.name == "index_minutes"}
    assert checks["NSE_INDEX:Nifty 50"] == "OK" and checks["NSE_INDEX:India VIX"] == "OK"
    assert checks["NSE_INDEX:Nifty Bank"] == "FAIL"                     # not in the fake lake: reported, not ignored


def test_latest_day(lake):
    assert latest_day(lake) == DAY


def test_nightly_and_coverage(lake, tmp_path, monkeypatch):
    # the intraday replay of a whole day at 1 s has its own test (test_nightly_replay_streams_...): stubbed here to keep this fast
    from signals.engine import core
    monkeypatch.setattr(core, "replay_day", lambda *a, **k: 0)
    res = nightly(lake, tmp_path, DAY, underlyings=("NIFTY",), log=lambda *a: None)
    assert res["day"] == str(DAY) and res["rows"]["NIFTY"] > 0
    v = read_output(tmp_path, "vrp", "NIFTY")
    assert set(pd.to_datetime(v["date"]).dt.date) == {DAY}
    again = nightly(lake, tmp_path, DAY, underlyings=("NIFTY",), log=lambda *a: None)        # idempotent: rewrites the day
    assert again["rows"] == res["rows"]
    assert len(read_output(tmp_path, "vrp", "NIFTY")) == len(v)
    assert len(read_trials(tmp_path)) > 0
    cov = coverage(tmp_path, underlyings=("NIFTY",))
    row = cov[cov["signal"] == "vrp"].iloc[0]
    assert row["rows"] == len(v) and row["days"] == 1
    assert not any(Path(lake).rglob("signals"))                        # nothing written under the lake


def test_release_and_backfill_scripts():
    """deploy/release.sh and deploy/data-backfill.sh parse; the backfill's dry run gates the signal history on the IV check."""
    import subprocess
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    for s in ("deploy/release.sh", "deploy/data-backfill.sh"):
        assert subprocess.run(["bash", "-n", s], cwd=root).returncode == 0
    rel = (root / "deploy/release.sh").read_text()
    assert 'crontab "$RENDERED"' in rel and "JARVIS_DEPTH5" in rel and "pytest" in rel
    out = subprocess.run(["bash", "deploy/data-backfill.sh"], cwd=root, capture_output=True, text=True).stdout
    assert out.index("validate-iv") < out.index("signals run") and "--only expired" in out


def test_nightly_replays_the_live_era_with_engine_v2(lake, tmp_path, monkeypatch):
    """With engine v2 (the default), the after-close replay runs once per underlying with every cadence group, through engine v2
    (the first engine would need about 3.6 s per 1 s bucket, a day of them in under a day is not possible)."""
    from signals.engine import core
    import signals.live_runner as lr
    seen = []
    monkeypatch.setattr(core, "replay_day", lambda lake_, root, u, day, cad, **k: seen.append((u, set(cad.values()))) or 7)
    monkeypatch.setattr(lr, "replay_day", lambda *a, **k: (_ for _ in ()).throw(AssertionError("first engine used")))
    res = nightly(lake, tmp_path, DAY, underlyings=("NIFTY",), log=lambda *a: None)
    assert res["intraday_rows"] == {"NIFTY": 7} and seen and seen[0][0] == "NIFTY" and "1s" in seen[0][1]


def test_every_bucket_history_from_candles(lake, tmp_path):
    """Snapshot signals at every 1-minute bar from candles, in their own layer (never mixed with the tick-based live-era rows)."""
    from signals.__main__ import CANDLE_LAYER, every_bucket_ids, run_every_bucket
    ids = every_bucket_ids()
    assert "vrp" in ids and "skew" in ids and not {"rr25", "pcr_full", "gex_full"} & set(ids)        # live-only need ticks
    st = run_every_bucket(lake, tmp_path, "NIFTY", DAY, DAY, ids=["skew", "term_slope", "vrp"], log=lambda *a: None)
    assert st.get("skew", 0) > 100
    files = list((tmp_path / "signals" / "skew").rglob("part.parquet"))
    assert files and all(CANDLE_LAYER in str(f) for f in files)
    s = pd.read_parquet(files[0])
    assert s["known_at"].is_unique and (pd.to_datetime(s["known_at"]) - pd.to_datetime(s["ts"]) == pd.Timedelta("1min")).all()
    assert read_output(tmp_path, "skew", "NIFTY").empty                # the batch layer is untouched


def test_check_day_on_recorded_ticks(lake):
    """python -m signals check-day: engine v2 against the batch on a recorded day of the lake."""
    from signals.engine.core import check_day
    res = check_day(lake, "NIFTY", DAY, minutes=20, log=lambda *a: None)
    assert res and all(v == "ok" or v.startswith("no rows") for v in res.values()), res
    assert sum(v == "ok" for v in res.values()) >= 3


def test_nightly_replay_streams_and_matches_the_in_memory_replay(lake, tmp_path):
    """The nightly replay reads ticks 30 minutes at a time and stages rows per window; the stored rows equal one in-memory replay."""
    from signals.engine.core import read_tick_window, replay, replay_day
    from signals.framework import REGISTRY
    from signals.live_runner import history_for
    cad = {"book_pressure": "1min", "pcr_full": "1min", "skew": "1min"}
    lines = []
    n = replay_day(lake, tmp_path, "NIFTY", DAY, cad, log=lambda *a: lines.append(" ".join(str(x) for x in a)))
    assert n > 0 and not (tmp_path / "_staging").exists() or not any((tmp_path / "_staging").iterdir())
    assert lines[0].startswith("replay NIFTY") and "start" in lines[0]            # progress: a line per five-minute slice, so a long replay is not silent
    slices = [x for x in lines if " UTC: " in x]
    assert slices and "rows so far" in slices[-1] and lines[-1].endswith("signal layers")
    d = pd.Timestamp(DAY)
    t = read_tick_window(lake, d, "NIFTY", d + pd.Timedelta(hours=3, minutes=30), d + pd.Timedelta(hours=10, minutes=30))
    ref = replay("NIFTY", history_for(lake, "NIFTY", DAY), t, cad, step="5min")
    for sid in cad:
        f = tmp_path / "signals" / sid / f"v{REGISTRY[sid][0].version}" / "intraday" / "bucket=1m" / "underlying=NIFTY" / f"date={DAY}" / "part.parquet"
        got = pd.read_parquet(f)
        assert len(got) == (ref["signal"] == sid).sum() > 0, sid


def test_nightly_replays_the_newest_closed_tick_day_and_loads_no_ticks_for_the_batch_signals(lake, tmp_path, monkeypatch):
    """16 Nov 2026: the nightly took the newest day with 1-minute bars (13 Nov, today's candles were not in yet), so it would have replayed
    the wrong day, and its batch run loaded the day's ticks into pandas (13 GB, killed).  Now the replay and the live table follow the newest
    compacted tick day, and the batch signals are run without ticks."""
    import signals.__main__ as m
    from signals.engine import core
    newer = pd.Timestamp(DAY) + pd.Timedelta(days=3)
    f = Path(lake) / "ticks" / "segment=fo" / f"date={newer.date()}" / "ticks.parquet"
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_bytes(b"")                                                  # only its presence counts: the feed writes it when the day is closed
    assert m.newest_tick_day(lake) == newer.date()
    seen, runs, kept = [], [], []
    monkeypatch.setattr(core, "replay_day", lambda lake_, root, u, day, cad, **k: seen.append((u, str(day.date() if hasattr(day, "date") else day))) or 0)
    real_run = m.run
    monkeypatch.setattr(m, "run", lambda *a, **k: runs.append((a[3], a[4], k.get("live"))) or real_run(*a, **k))
    import signals.retention as ret
    monkeypatch.setattr(ret, "nightly", lambda root, day, **k: kept.append(day) or {})
    res = m.nightly(lake, tmp_path, None, underlyings=("NIFTY",), log=lambda *a: None)
    assert seen == [("NIFTY", str(newer.date()))]                      # the replay is for the tick day
    assert runs and all(r[2] is False for r in runs)                   # no ticks for the batch signals
    assert kept == [newer.date()] and res["tick_day"] == str(newer.date())


def test_crontab_template_renders_for_any_checkout_with_the_engine_settings(tmp_path):
    """16-17 Nov 2026: the template names /home/jarvis/JARVIS, so on another user's checkout every job failed at `cd $JARVIS`; and the engines
    need the store, memory cap and bus settings from cron.  release.sh renders the template with sed for the repository's real path."""
    import subprocess
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    out = subprocess.run(["sed", "s#/home/jarvis/JARVIS#/home/tradestone/JARVIS#g", str(root / "deploy/crontab")], capture_output=True, text=True).stdout
    assert "/home/jarvis" not in out
    lines = out.splitlines()
    for want in ("JARVIS=/home/tradestone/JARVIS", "JARVIS_SIGNAL_STORE=/home/tradestone/JARVIS/store", "JARVIS_SIGNAL_LIVE_MAX_MB=12288",
                 "JARVIS_TICK_BUS=1", "JARVIS_LIVE_SIGNALS=1", "JARVIS_SIGNAL_ENGINE=operators", "PATH=/home/tradestone/JARVIS/.venv/bin:/usr/local/bin:/usr/bin:/bin"):
        assert want in lines, want
    rel = (root / "deploy/release.sh").read_text()
    assert 's#/home/jarvis/JARVIS#${ROOT}#g' in rel and "still names /home/jarvis" in rel


def test_engine_live_cap_default_is_large_enough_and_signals_live_reads_dotenv():
    import signals.live_runner as lr
    assert lr.MAX_MB_DEFAULT >= 8192                         # 2048 broke the startup load of 400 days of bars (17 Nov 2026)
    src = Path(lr.__file__).resolve().parent / "__main__.py"
    assert "load_dotenv(ENV_FILE)" in src.read_text()
    assert "load_dotenv(ENV_FILE)" in (Path(lr.__file__).resolve().parents[1] / "trading" / "paper.py").read_text()


def test_history_load_can_skip_the_constituent_minutes_and_limit_the_cores(lake):
    """17 Nov 2026: four engines loading 400 days of history at once took every core of the 4-core VPS for about 100 s (load average 6.05).  The
    load is limited to JARVIS_LOAD_THREADS cores (default 2) and skips the constituents' minutes (only the basket research signals read them)."""
    from signals import lakeio
    assert lakeio.load_threads({}) == 2 and lakeio.load_threads({"JARVIS_LOAD_THREADS": "1"}) == 1 and lakeio.load_threads({"JARVIS_LOAD_THREADS": "0"}) == 1
    assert lakeio.connect(1).execute("select current_setting('threads')").fetchone()[0] == 1
    d1 = DAY
    full = lakeio.load_raw(lake, "NIFTY", d1 - pd.Timedelta(days=10), d1, stocks=True)
    lean = lakeio.load_raw(lake, "NIFTY", d1 - pd.Timedelta(days=10), d1, stocks=False, threads=1)
    assert len(lean["stocks_1m"]) == 0 and set(lean["stocks_1m"].columns) <= set(full["stocks_1m"].columns)
    for k in ("index_1m", "vix_1m", "fut_1m", "opt_1m"):                     # everything else is the same
        pd.testing.assert_frame_equal(lean[k].reset_index(drop=True), full[k].reset_index(drop=True))


def test_crontab_starts_the_one_minute_engines_after_the_fast_ones():
    """Each engine loads 400 days of history at its start; four at once took the 4 cores (17 Nov 2026), so the one-minute engines start 3 and 5 minutes later."""
    import re
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    start = {}
    for ln in (root / "deploy/crontab").read_text().splitlines():
        m = re.match(r"^(\d+) 3 \* \* 1-5\s+cd \$JARVIS && python -m data_layer.jobrun run (signals-live-[a-z0-9-]+)", ln)
        if m:
            start[m.group(2)] = int(m.group(1))
    assert start == {"signals-live-nifty": 44, "signals-live-nifty-1min": 47, "signals-live-banknifty": 44, "signals-live-banknifty-1min": 49}


def _tick_rows(sym, t0, n, px0, vol0=0, step_s=7, oi=None, quote_only_from=None):
    rows = []
    for i in range(n):
        t = t0 + pd.Timedelta(seconds=step_s * i)
        quote_only = quote_only_from is not None and i >= quote_only_from
        rows.append({"symbol": sym, "fts": t, "ts": t, "ltp": None if quote_only else px0 + 0.5 * i, "bid": px0 + 0.5 * i - 0.1, "ask": px0 + 0.5 * i + 0.1,
                     "vol_today": float(vol0 + 10 * i), "oi": oi})
    return rows


def test_bars_from_ticks_follow_the_engine_rules_and_the_vendor_wins_where_both_exist(tmp_path):
    """17 Nov 2026: the vendor's candles of a day arrive the next day, so the next open's history ended a session early.  The bars are built from the
    day's ticks into lake/bars_ticks (not lake/bars: the candle job's gap detection counts rows there), and read_bars takes the vendor's row first."""
    from data_layer import bars_from_ticks as bt
    from signals import lakeio
    lake = tmp_path / "lake"
    day = pd.Timestamp("2026-11-16").date()
    t0 = pd.Timestamp("2026-11-16 03:45:00")
    pre = pd.Timestamp("2026-11-16 03:40:00")                                   # auction ticks before the open
    eq = pd.DataFrame(_tick_rows("NSE_INDEX:Nifty 50", t0, 60, 24000.0) + _tick_rows("NSE_INDEX:Nifty 50", pre, 3, 23000.0)
                      + _tick_rows("RELIANCE", t0, 20, 1300.0))                 # a stock: not a bar of this job
    fo = pd.DataFrame(_tick_rows("NIFTY_FUT_24_NOV_26", t0, 60, 24050.0, vol0=1000, oi=500.0, quote_only_from=40)
                      + _tick_rows("NIFTY_24000_CE_17_NOV_26", t0, 60, 120.0))   # an option: not a bar of this job
    for seg, df in (("eq", eq), ("fo", fo)):
        f = lake / "ticks" / f"segment={seg}" / f"date={day}"
        f.mkdir(parents=True, exist_ok=True)
        df.to_parquet(f / "ticks.parquet", index=False)
    assert bt.build_day(lake, day, log=lambda *a: None) > 0
    out = pd.read_parquet(bt.out_dir(lake, day) / "bars.parquet")
    assert set(out["symbol"]) == {"NSE_INDEX:Nifty 50", "NIFTY_FUT_24_NOV_26"} and set(out["src"]) == {"ticks"} and set(out["market"]) == {"IN"}
    assert out["ts"].min() == t0 and not (lake / "bars").exists()                  # the auction ticks are out; nothing in the vendor table
    idx = out[out["symbol"] == "NSE_INDEX:Nifty 50"].set_index("ts")
    first = idx.loc[t0]                                                            # ticks at 0, 7, ..., 56 s of the first minute: 9 of them
    assert (first["open"], first["close"], first["high"], first["low"]) == (24000.0, 24000.0 + 0.5 * 8, 24000.0 + 0.5 * 8, 24000.0)
    fut = out[out["symbol"] == "NIFTY_FUT_24_NOV_26"].set_index("ts")
    assert fut.loc[t0]["volume"] == 80                                              # counter 1000 at the first tick, 1080 at the last of the minute
    assert fut.loc[t0 + pd.Timedelta(minutes=1)]["volume"] == 90                    # then 1170: the change since the previous minute
    assert fut["oi"].dropna().eq(500.0).all()
    late = fut[fut.index >= t0 + pd.Timedelta(minutes=5)]                          # quote-only ticks: the mid is the price
    assert len(late) and late["close"].notna().all()
    # the vendor's row wins where both exist; ours fills the minutes it lacks
    vend = lake / "bars" / "interval=1m" / f"date={day}"
    vend.mkdir(parents=True)
    pd.DataFrame({"ts": [t0], "symbol": ["NSE_INDEX:Nifty 50"], "open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5], "volume": [0], "oi": [None],
                  "market": ["IN"]}).to_parquet(vend / "bars.parquet", index=False)
    got = lakeio.read_bars(lake, day, day, "symbol = ?", ["NSE_INDEX:Nifty 50"]).set_index("ts")
    assert got.loc[t0]["close"] == 1.5 and len(got) == len(idx)                    # minute 1 from the vendor, the others from the ticks
    assert (got.drop(index=t0)["close"] == idx.drop(index=t0)["close"]).all()


def test_anchors_job_saves_what_an_engine_loads_and_the_crontab_runs_it_before_the_engines(lake, tmp_path):
    """17 Nov 2026: `python -m signals anchors` (cron 03:30 UTC) computes the engines' start-up numbers from the lake once and saves them; an engine
    of the same day loads them (signals/anchors.load), another day's are not used."""
    from pathlib import Path
    from signals import anchors as an
    import signals.__main__ as m
    day = DAY + pd.Timedelta(days=1)
    out = an.build(lake, tmp_path, "NIFTY", day, log=lambda *a: None)
    assert (out / "manifest.json").exists() and (out / "contract_table.parquet").exists()
    from signals.engine.core import live_signal_ids
    from signals import cadence as cm
    ids = live_signal_ids()
    cad = cm.load(ids)
    got = an.load(tmp_path, day, "NIFTY", sorted(cad), list(cm.groups(cad)))
    assert got is not None and got[0].underlying == "NIFTY" and len(got[0].fixed["daily"]) > 0
    assert an.load(tmp_path, DAY, "NIFTY", sorted(cad), list(cm.groups(cad))) is None
    root = Path(__file__).resolve().parents[2]
    lines = [l for l in (root / "deploy/crontab").read_text().splitlines() if l.strip() and not l.startswith("#")]
    at = next(l for l in lines if "signals anchors" in l)
    assert (int(at.split()[1]), int(at.split()[0])) < (3, 44) and (int(at.split()[1]), int(at.split()[0])) > (3, 25)
    assert hasattr(m, "main")
