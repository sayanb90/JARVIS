"""The storage report: sizes per dataset, hard links counted once, size per session and runway."""

import os
from datetime import date

from data_layer.storage import report


def _f(p, n):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"x" * n)


def test_storage_report_counts_hard_links_once_and_projects_growth(tmp_path):
    lake, store, snaps = tmp_path / "lake", tmp_path / "store", tmp_path / "snaps"
    for i, d in enumerate(["2026-09-01", "2026-11-12", "2026-11-13"]):
        _f(lake / "ticks" / "segment=fo" / f"date={d}" / "ticks.parquet", 400_000 + i)
        _f(lake / "ticks" / "segment=eq" / f"date={d}" / "ticks.parquet", 100_000)
        _f(store / "signals" / "vrp" / "v1" / "intraday" / "bucket=1s" / "underlying=NIFTY" / f"date={d}" / "part.parquet", 50_000)
    _f(lake / "eod_fo" / "x.parquet", 20_000)
    snap = snaps / "2026-11-13" / "ticks" / "segment=fo" / "date=2026-11-13"
    snap.mkdir(parents=True)
    os.link(lake / "ticks" / "segment=fo" / "date=2026-11-13" / "ticks.parquet", snap / "ticks.parquet")     # a hard link
    r = report(lake, store, snaps, None, days=10, today=date(2026, 11, 14))
    by = {d["dataset"]: d for d in r["datasets"]}
    assert "snapshots" not in by                                       # its only file is the lake's, counted once
    fo = by["lake/ticks/segment=fo"]
    assert fo["where"] == "offload" and fo["sessions_seen"] == 3 and fo["offloadable_gb"] > 0          # 2026-09-01 is old
    assert by["lake/eod_fo"]["where"] == "keep"
    assert any(k.startswith("store/signals (intraday bucket=1s)") for k in by)
    assert r["growth_mb_per_session"] > 0.5 and r["runway_sessions"] > 0
