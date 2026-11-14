"""The viewer's Signals tab API (SCRUM-199): latest bucket per signal with its time-of-day z, a series, staleness; read only."""

from datetime import datetime, timedelta

import pandas as pd
from fastapi.testclient import TestClient

from signals.options.stream import write_rows
from webapp import signals_view
from webapp.app import create_app


def fill_store(store, now=None, minutes=30):
    now = pd.Timestamp(now or datetime.utcnow()).floor("min")
    rows = []
    for i in range(minutes):
        ts = now - pd.Timedelta(minutes=minutes - i)
        rows.append({"signal": "vrp", "bucket": "1min", "underlying": "NIFTY", "ts": ts, "known_at": ts + pd.Timedelta(minutes=1),
                     "date": ts.date(), "snap": "", "iv7": 0.13, "rv_fc": 0.12, "vrp_diff": 0.01 + 0.0005 * i, "vrp_diff_tod_z": 0.1 * i})
        for k in range(6):
            t10 = ts + pd.Timedelta(seconds=10 * k)
            rows.append({"signal": "fut_ofi", "bucket": "10s", "underlying": "NIFTY", "symbol": "NIFTY_FUT_25_NOV_26", "ts": t10,
                         "known_at": t10 + pd.Timedelta(seconds=10), "ofi": float(k), "depth": 100.0, "ofi_norm": k / 100.0})
    write_rows(rows, store)
    return store


def test_summary_latest_bucket_z_and_series(tmp_path):
    now = datetime(2026, 11, 13, 6, 0, 5)
    fill_store(tmp_path, now)
    s = signals_view.summary(tmp_path, "NIFTY", now.date(), now)
    by = {(x["signal"], x["bucket"]): x for x in s["signals"]}
    assert set(by) == {("vrp", "1min"), ("fut_ofi", "10s")} and s["cadence"] == ["10s", "1min"]
    v = by[("vrp", "1min")]
    assert v["headline"] == "vrp_diff" and abs(v["values"]["vrp_diff"] - (0.01 + 0.0005 * 29)) < 1e-12
    assert abs(v["z"]["vrp_diff"] - 2.9) < 1e-9 and len(v["series"]) == 30
    assert by[("fut_ofi", "10s")]["headline"] == "ofi_norm" and s["stale"] is False


def test_stale_when_the_newest_bucket_is_old(tmp_path):
    now = datetime(2026, 11, 13, 6, 0, 5)
    fill_store(tmp_path, now)
    s = signals_view.summary(tmp_path, "NIFTY", now.date(), now + timedelta(minutes=5))
    assert s["stale"] is True


def test_api_reads_the_store_and_validates(tmp_path, monkeypatch):
    fill_store(tmp_path / "store")
    monkeypatch.setenv("JARVIS_SIGNAL_STORE", str(tmp_path / "store"))
    before = sorted(p.name for p in (tmp_path / "store").rglob("*"))
    c = TestClient(create_app(tmp_path / "lake"))
    r = c.get("/api/signals/live", params={"underlying": "NIFTY"})
    assert r.status_code == 200 and {x["signal"] for x in r.json()["signals"]} == {"vrp", "fut_ofi"}
    assert c.get("/api/signals/live", params={"underlying": "nifty;drop"}).status_code == 400
    assert c.get("/api/signals/live", params={"underlying": "NIFTY", "date": "not-a-date"}).status_code == 400
    assert sorted(p.name for p in (tmp_path / "store").rglob("*")) == before             # read only
    empty = c.get("/api/signals/live", params={"underlying": "BANKNIFTY"}).json()
    assert empty["signals"] == [] and empty["newest"] is None
