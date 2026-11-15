"""The live table after the close: compare with the history, compact the day, remove old days only when their history exists."""

from datetime import date, timedelta

import pandas as pd

from signals import retention
from signals.framework import REGISTRY, write_output
from signals.options.stream import read_live, write_rows

DAY = date(2026, 11, 16)


def _rows(day, n, sid="book_pressure", bucket="1s"):
    t0 = pd.Timestamp(day) + pd.Timedelta(hours=4)
    return [{"signal": sid, "underlying": "NIFTY", "bucket": bucket, "ts": t0 + pd.Timedelta(seconds=i),
             "known_at": t0 + pd.Timedelta(seconds=i + 1), "qi": 0.1 * i} for i in range(n)]


def _history(store, day, n, sid="book_pressure", bucket="1s"):
    df = pd.DataFrame(_rows(day, n, sid, bucket)).drop(columns=["signal"])
    write_output(store, REGISTRY[sid][0], df, layer=f"intraday/bucket={retention.LAYER[bucket]}")


def test_compact_keeps_every_row_and_compare_reports_counts(tmp_path):
    for k in range(4):                                             # four 15-second writes
        write_rows(_rows(DAY, 10)[k * 2:(k + 1) * 2 + 2], tmp_path, prefix=f"engine-{k}")
    before = read_live(tmp_path, DAY)
    _history(tmp_path, DAY, 10)
    res = retention.nightly(tmp_path, DAY, log=lambda *a: None)
    files = list(retention.live_folder(tmp_path, DAY).glob("*.parquet"))
    assert [f.name for f in files] == [retention.COMPACT_NAME] and res["compact"]["compacted"]
    after = read_live(tmp_path, DAY)
    assert len(after) == len(before) == 16
    assert res["compared"] == 1 and res["differ"][0]["rows"] == 16 and res["differ"][0]["history_rows"] == 10


def test_old_days_removed_only_with_their_history(tmp_path):
    old_ok, old_missing, recent = DAY - timedelta(days=7), DAY - timedelta(days=8), DAY - timedelta(days=2)
    for d in (old_ok, old_missing, recent):
        write_rows(_rows(d, 5), tmp_path, prefix="engine")
    _history(tmp_path, old_ok, 5)
    res = retention.nightly(tmp_path, DAY, log=lambda *a: None)
    by = {r["day"]: r for r in res["removed"]}
    assert by[str(old_ok)]["removed"] and not retention.live_folder(tmp_path, old_ok).exists()
    assert not by[str(old_missing)]["removed"] and retention.live_folder(tmp_path, old_missing).exists()
    assert str(recent) not in by and retention.live_folder(tmp_path, recent).exists()          # inside the 5 days
    assert (tmp_path / "signals" / "book_pressure").exists()                                     # history untouched
