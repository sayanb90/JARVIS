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
    assert "crontab deploy/crontab" in rel and "JARVIS_DEPTH5" in rel and "pytest" in rel
    out = subprocess.run(["bash", "deploy/data-backfill.sh"], cwd=root, capture_output=True, text=True).stdout
    assert out.index("validate-iv") < out.index("signals run") and "--only expired" in out


def test_nightly_replays_the_live_era_with_engine_v2(lake, tmp_path, monkeypatch):
    """With engine v2 (the default), the after-close replay runs once per underlying with every cadence group, through engine v2
    (the first engine would need about 3.6 s per 1 s bucket, a day of them in under a day is not possible)."""
    from signals.engine import core
    import signals.live_runner as lr
    seen = []
    monkeypatch.setattr(core, "replay_day", lambda lake_, root, u, day, cad: seen.append((u, set(cad.values()))) or 7)
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
    n = replay_day(lake, tmp_path, "NIFTY", DAY, cad, log=lambda *a: None)
    assert n > 0 and not (tmp_path / "_staging").exists() or not any((tmp_path / "_staging").iterdir())
    d = pd.Timestamp(DAY)
    t = read_tick_window(lake, d, "NIFTY", d + pd.Timedelta(hours=3, minutes=30), d + pd.Timedelta(hours=10, minutes=30))
    ref = replay("NIFTY", history_for(lake, "NIFTY", DAY), t, cad, step="5min")
    for sid in cad:
        f = tmp_path / "signals" / sid / f"v{REGISTRY[sid][0].version}" / "intraday" / "bucket=1m" / "underlying=NIFTY" / f"date={DAY}" / "part.parquet"
        got = pd.read_parquet(f)
        assert len(got) == (ref["signal"] == sid).sum() > 0, sid
