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


def test_compact_days_merges_every_day_on_its_own_and_leaves_a_day_still_being_written(tmp_path):
    """17 and 20 Nov 2026: the nightly job compacts only after its hours-long replay, which did not finish, so those days stayed in 6,400 small files.
    `python -m signals compact` does it alone: all uncompacted days, none whose newest file is too young, and a late file beside a merge is merged in."""
    import os
    import time
    days = [DAY - timedelta(days=2), DAY - timedelta(days=1), DAY]
    for d in days:
        for k in range(3):
            write_rows(_rows(d, 10)[k * 3:k * 3 + 4], tmp_path, prefix=f"engine-{k}")
    now = time.time()
    for f in retention.live_folder(tmp_path, DAY).glob("*.parquet"):          # the last day is still being written
        os.utime(f, (now - 5, now - 5))
    for d in days[:2]:
        for f in retention.live_folder(tmp_path, d).glob("*.parquet"):
            os.utime(f, (now - 3600, now - 3600))
    res = retention.compact_days(tmp_path, None, quiet_s=180, now=now, log=lambda *a: None)
    by = {r["day"]: r for r in res}
    assert by[str(days[0])]["compacted"] and by[str(days[1])]["compacted"]
    assert not by[str(DAY)]["compacted"] and "under" in by[str(DAY)]["skipped"]
    assert [f.name for f in retention.live_folder(tmp_path, days[0]).glob("*.parquet")] == [retention.COMPACT_NAME]
    assert len(list(retention.live_folder(tmp_path, DAY).glob("*.parquet"))) == 3              # the young day was left as it was
    assert len(read_live(tmp_path, days[0])) == 12                                               # no row lost in the merge
    # a file an engine wrote after the merge sits beside it, and the next run merges it in once
    write_rows(_rows(days[0], 3), tmp_path, prefix="engine-late")
    folder = retention.live_folder(tmp_path, days[0])
    for f in folder.glob("*.parquet"):
        os.utime(f, (now - 3600, now - 3600))
    again = retention.compact_days(tmp_path, [days[0]], quiet_s=180, now=now, log=lambda *a: None)
    assert again[0]["compacted"] and [f.name for f in folder.glob("*.parquet")] == [retention.COMPACT_NAME]
    assert len(read_live(tmp_path, days[0])) == 15
    assert retention.compact_days(tmp_path, [days[0]], now=now, log=lambda *a: None) == []        # nothing left to do: no work, no report
