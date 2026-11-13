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


def test_nightly_and_coverage(lake, tmp_path):
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
