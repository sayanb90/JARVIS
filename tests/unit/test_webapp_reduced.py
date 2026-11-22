"""The reduced day (SCRUM-216): a day's signals reduced once on disk by a short-lived process, read by the viewer one slice at a time.  Same rows as the
in-process loader, incremental, safe against compaction under it, bounded in memory; and the capped child runner for heavy work."""

import os
import shutil
import sys
import threading
import time
from datetime import datetime, timedelta

import pandas as pd
import pytest

from data_layer.query import QueryBusy
from signals import retention
from signals.options.stream import write_rows
from webapp import heavy, reduced, signals_store as ss, signals_view
from tests.unit.test_webapp_signals_tab import DAY, NOW, T0, contract_rows, fill, single_rows


@pytest.fixture()
def on(monkeypatch):
    monkeypatch.setenv("JARVIS_VIEWER_REDUCED", "1")
    ss.forget()
    yield
    ss.forget()


def _norm(df):
    df = df.sort_values(list(df.columns), kind="stable").reset_index(drop=True)
    return df.reindex(sorted(df.columns), axis=1)


def _legacy_frames(store):
    ld = ss.LiveDay(store, DAY.date())
    ld.refresh()
    return ld, {k: ld.frame(*k) for k in list(ld.chunks)}


def test_the_reduced_copy_has_the_same_keys_and_rows_as_the_in_process_loader(tmp_path, on):
    fill(tmp_path)
    ld, want = _legacy_frames(tmp_path)
    res = reduced.reduce_day(tmp_path, DAY.date(), log_fn=lambda *a: None)
    assert res["error"] is None and res["keys"] == len(want)
    rd = reduced.ReducedDay(tmp_path, DAY.date())
    assert sorted(tuple(k) for k in rd.manifest()["keys"]) == sorted(want)
    for key, df in want.items():
        got = rd.frame(*key)
        assert got is not None and len(got) == len(df), key
        pd.testing.assert_frame_equal(_norm(got.dropna(axis=1, how="all")), _norm(df.dropna(axis=1, how="all")), check_dtype=False, check_like=True)
    for und in ("NIFTY",):
        assert rd.signals(und) == ld.signals(und)
    assert rd.underlyings() == ld.underlyings()


def test_the_tab_gives_the_same_answers_from_the_reduced_copy(tmp_path, monkeypatch):
    """summary, series, by strike and chain through signals_view, once with the in-process loader and once with the reduced copy."""
    fill(tmp_path)
    monkeypatch.setenv("JARVIS_VIEWER_REDUCED", "0")
    ss.forget()
    a = [signals_view.summary(tmp_path, "NIFTY", DAY.date(), NOW, wait_s=60),
         signals_view.series(tmp_path, "NIFTY", "book_pressure", "1s", DAY.date()),
         signals_view.contracts(tmp_path, "NIFTY", "book_pressure", "1s", DAY.date()),
         signals_view.chain(tmp_path, "NIFTY", DAY.date())]
    monkeypatch.setenv("JARVIS_VIEWER_REDUCED", "1")
    ss.forget()
    b = [signals_view.summary(tmp_path, "NIFTY", DAY.date(), NOW, wait_s=60),
         signals_view.series(tmp_path, "NIFTY", "book_pressure", "1s", DAY.date()),
         signals_view.contracts(tmp_path, "NIFTY", "book_pressure", "1s", DAY.date()),
         signals_view.chain(tmp_path, "NIFTY", DAY.date())]
    ss.forget()
    for x, y in zip(a, b):
        x, y = dict(x), dict(y)
        x.pop("load"), y.pop("load")
        assert x == y


def test_a_second_run_reads_only_the_files_it_has_not_read(tmp_path, monkeypatch):
    fill(tmp_path, minutes=4)
    r1 = reduced.reduce_day(tmp_path, DAY.date(), log_fn=lambda *a: None)
    n1 = r1["read"]
    seen = []
    orig = ss.LiveDay._batches
    monkeypatch.setattr(ss.LiveDay, "_batches", staticmethod(lambda paths, thin=False: (seen.extend(p.name for p in paths) or orig(paths, thin))))
    r2 = reduced.reduce_day(tmp_path, DAY.date(), log_fn=lambda *a: None)
    assert r2["read"] == n1 and seen == []                                   # nothing new: nothing read
    write_rows(single_rows("skew", 3, start=20), tmp_path, prefix="engine-NIFTY-1m-new")
    r3 = reduced.reduce_day(tmp_path, DAY.date(), log_fn=lambda *a: None)
    assert r3["read"] == n1 + 1 and len(seen) == 1 and seen[0].startswith("engine-NIFTY-1m-new")
    rd = reduced.ReducedDay(tmp_path, DAY.date())
    assert len(rd.frame("single", "skew", "1min", "NIFTY")) == 4 + 3


def test_a_day_compacted_under_the_reduced_copy_is_reduced_afresh_with_the_same_rows(tmp_path, monkeypatch):
    fill(tmp_path)
    reduced.reduce_day(tmp_path, DAY.date(), log_fn=lambda *a: None)
    before = {tuple(k): len(reduced.ReducedDay(tmp_path, DAY.date()).frame(*k)) for k in reduced.read_manifest(reduced.reduced_dir(tmp_path, DAY.date()))["keys"]}
    folder = retention.live_folder(tmp_path, DAY.date())
    for f in folder.glob("*.parquet"):
        os.utime(f, (time.time() - 3600, time.time() - 3600))
    assert retention.compact(tmp_path, DAY.date())["compacted"]
    rd = reduced.ReducedDay(tmp_path, DAY.date())
    assert rd.needs_refresh()                                                # the files it was made from are gone
    res = reduced.reduce_day(tmp_path, DAY.date(), log_fn=lambda *a: None)
    assert res["complete"] and res["error"] is None
    rd = reduced.ReducedDay(tmp_path, DAY.date())
    after = {tuple(k): len(rd.frame(*k)) for k in rd.manifest()["keys"]}
    assert after == before and not rd.needs_refresh()


def test_many_small_flushes_are_merged_into_one_file_per_key_without_losing_rows(tmp_path, monkeypatch):
    monkeypatch.setattr(reduced, "CONSOLIDATE_AT", 3)
    monkeypatch.setattr(ss, "BATCH", 1)                                       # a flush per file
    for k in range(10):
        write_rows(single_rows("skew", 2, start=2 * k), tmp_path, prefix=f"engine-NIFTY-1m-{k}")
    reduced.reduce_day(tmp_path, DAY.date(), log_fn=lambda *a: None)
    d = reduced.key_dir(reduced.reduced_dir(tmp_path, DAY.date()), "single", "skew", "1min", "NIFTY")
    assert len(list(d.glob("*.parquet"))) <= 4
    assert len(reduced.ReducedDay(tmp_path, DAY.date()).frame("single", "skew", "1min", "NIFTY")) == 20


def test_the_answer_cache_is_bounded_in_bytes(tmp_path, monkeypatch):
    fill(tmp_path)
    reduced.reduce_day(tmp_path, DAY.date(), log_fn=lambda *a: None)
    rd = reduced.ReducedDay(tmp_path, DAY.date())
    keys = rd.manifest()["keys"]
    sizes = sorted(int(rd.frame(*k).memory_usage(deep=True).sum()) for k in keys)
    cap_mb = (sizes[-1] + sizes[-2]) / 2**20                                   # room for the two biggest slices only
    monkeypatch.setenv("JARVIS_VIEWER_ANSWER_CACHE_MB", str(cap_mb))
    rd = reduced.ReducedDay(tmp_path, DAY.date())
    for k in keys:
        rd.frame(*k)
        assert rd._cache_bytes <= cap_mb * 2**20 + 1
    assert 0 < len(rd._cache) < len(keys)


def test_prune_keeps_the_newest_days_and_a_day_with_no_live_files_is_still_listed(tmp_path):
    for d in range(3):
        day = DAY.date() - timedelta(days=d)
        write_rows([{"signal": "skew", "bucket": "1min", "underlying": "NIFTY", "ts": T0 - pd.Timedelta(days=d), "known_at": T0 - pd.Timedelta(days=d) + pd.Timedelta(minutes=1), "skew1_s": 0.1}],
                   tmp_path, prefix="engine-NIFTY-1m")
        reduced.reduce_day(tmp_path, day, log_fn=lambda *a: None)
    assert reduced.reduced_days(tmp_path) == [str(DAY.date() - timedelta(days=d)) for d in range(3)]
    shutil.rmtree(tmp_path / "live")                                          # the live folders are removed after five days: the reduced copies stay
    assert ss.live_days(tmp_path) == [str(DAY.date() - timedelta(days=d)) for d in range(3)]
    assert reduced.prune(tmp_path, keep=1) == [f"date={DAY.date() - timedelta(days=2)}", f"date={DAY.date() - timedelta(days=1)}"]


def test_the_viewer_falls_back_to_its_own_loader_when_the_reducer_failed(tmp_path, monkeypatch, on):
    fill(tmp_path)
    monkeypatch.setattr(reduced, "reduce_day", lambda *a, **k: {"day": "x", "error": "boom"})
    s = signals_view.summary(tmp_path, "NIFTY", DAY.date(), NOW, wait_s=60)
    assert len(s["signals"]) == 5 and s["load"]["error"] is None             # the in-process loader answered


def test_the_reducer_child_runs_as_a_real_process_and_a_running_day_is_refreshed_when_stale(tmp_path, monkeypatch, on):
    monkeypatch.setenv("JARVIS_VIEWER_CHILD", "1")
    fill(tmp_path)
    s = signals_view.summary(tmp_path, "NIFTY", DAY.date(), NOW, wait_s=120)
    assert len(s["signals"]) == 5 and s["load"]["loaded"] and s["load"]["error"] is None
    rd = reduced.get_reduced(tmp_path, DAY.date())
    assert rd.manifest()["files_done"] >= 1
    write_rows(single_rows("skew", 3, start=30), tmp_path, prefix="engine-NIFTY-1m-late")
    m = rd.manifest()
    m["updated"] = time.time() - 120                                          # an hour-long session: the copy is more than STALE_S old
    (rd.base / reduced.MANIFEST).write_text(__import__("json").dumps({**m, "complete": False}))
    assert rd.needs_refresh()
    rd.ensure(0)
    rd.thread.join(60)
    assert len(rd.frame("single", "skew", "1min", "NIFTY")) == 10 + 3


# ------------------------------------------------------------------------------------------------ the child runner
def test_a_child_is_killed_over_its_time_and_its_memory_and_a_second_caller_is_told_to_wait():
    r = heavy.run_child(["-c", "import time; time.sleep(30)"], timeout_s=0.8, mem_mb=2000)
    assert r.killed == "timeout" and not r.ok
    r = heavy.run_child(["-c", "import time; x = bytearray(300 * 2**20); [x.__setitem__(i, 1) for i in range(0, len(x), 4096)]; time.sleep(30)"], timeout_s=20, mem_mb=150)
    assert r.killed == "memory" and r.peak_mb > 150
    ok = heavy.run_child(["-c", "print('hi')"])
    assert ok.ok and ok.stdout.strip() == "hi"
    t = threading.Thread(target=lambda: heavy.run_child(["-c", "import time; time.sleep(2)"], timeout_s=10))
    t.start()
    time.sleep(0.4)
    with pytest.raises(QueryBusy):
        heavy.run_child(["-c", "print(1)"], wait_slot_s=0.3)
    t.join()
    assert not heavy.slot_busy()


# ------------------------------------------------------------------------------------------------ heavy calls in a child
def test_overview_and_chain_come_from_a_capped_child_and_a_finished_day_from_its_file(tmp_path, monkeypatch):
    """With children on (the viewer's setting) the Market overview and the F&O chain are computed in a child process; the answer equals the in-process one;
    a finished day's answer is read from the day cache without starting a child; a busy slot gives 503 with Retry-After."""
    from fastapi.testclient import TestClient
    from tests.unit.test_open_interest import seed_fo_chain
    from webapp.app import create_app
    seed_fo_chain(tmp_path)
    params = {"date": "2026-10-23", "underlying": "NIFTY"}
    c0 = TestClient(create_app(tmp_path))
    want_chain, want_over = c0.get("/api/fo/chain", params=params).json(), c0.get("/api/overview", params={"date": "2026-10-23"}).json()
    monkeypatch.setenv("JARVIS_VIEWER_CHILD", "1")
    started = []
    real = heavy.run_child
    monkeypatch.setattr(heavy, "run_child", lambda args, **k: (started.append(args[2]), real(args, **k))[1])
    c = TestClient(create_app(tmp_path))
    assert c.get("/api/fo/chain", params=params).json() == want_chain and started == ["chain"]
    assert c.get("/api/overview", params={"date": "2026-10-23"}).json() == want_over and started == ["chain", "overview"]
    assert c.get("/api/fo/chain", params=params).json() == want_chain and started == ["chain", "overview"]          # a minute's answer is kept: no new child
    old = time.time() - 3 * 3600
    for f in tmp_path.rglob("*.parquet"):
        os.utime(f, (old, old))
    c2 = TestClient(create_app(tmp_path))
    assert c2.get("/api/fo/chain", params=params).json() == want_chain and started[-1] == "chain"                    # settled: computed once more, by a child, and saved
    n = len(started)
    c3 = TestClient(create_app(tmp_path))
    assert c3.get("/api/fo/chain", params=params).json() == want_chain and len(started) == n                         # now from the file
    assert c3.get("/api/overview", params={"date": "2026-10-23"}).status_code == 200
    monkeypatch.setattr(heavy, "run_child", lambda *a, **k: (_ for _ in ()).throw(QueryBusy("busy")))
    c4 = TestClient(create_app(tmp_path))
    r = c4.get("/api/fo/chain", params={**params, "expiry": "2026-11-24"})
    assert r.status_code == 503 and r.headers["retry-after"]


def test_only_one_reducer_runs_a_day_at_a_time(tmp_path):
    import fcntl
    fill(tmp_path)
    lock = reduced.reduced_root(tmp_path) / f".lock-{DAY.date()}"
    lock.parent.mkdir(parents=True, exist_ok=True)
    with open(lock, "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)                                        # another reducer holds the day
        assert "another reducer" in reduced.reduce_day(tmp_path, DAY.date(), log_fn=lambda *a: None)["note"]
    assert reduced.reduce_day(tmp_path, DAY.date(), log_fn=lambda *a: None)["error"] is None


def test_one_big_compacted_file_is_handed_over_in_pieces_not_held_whole(tmp_path, monkeypatch):
    """A compacted day is ONE file, so 'after each batch of files' meant after the whole day: the reducer held the day's thinned rows (1.06 GB peak on a
    6.5-million-row synthetic day).  It now flushes every FLUSH_ROWS kept rows inside the file (640 MB), with the same rows at the end."""
    import duckdb
    small, wide = tmp_path / "small", tmp_path / "wide"
    fill(small)
    folder = wide / "live" / f"date={DAY.date()}"
    folder.mkdir(parents=True)
    duckdb.connect().execute(f"COPY (SELECT * FROM read_parquet('{small}/live/date={DAY.date()}/*.parquet', union_by_name=true)) TO '{folder}/live-compacted.parquet' (FORMAT PARQUET)")
    monkeypatch.setattr(ss, "TAIL_MAX_BYTES", 1)
    monkeypatch.setattr(ss, "FLUSH_ROWS", 300)
    monkeypatch.setattr(ss, "BATCH_ROWS", 2048)
    flushes = []
    real = reduced.Writer.__call__
    monkeypatch.setattr(reduced.Writer, "__call__", lambda self, taken: (flushes.append(sum(len(f) for fr in taken.values() for f in fr)), real(self, taken))[1])
    res = reduced.reduce_day(wide, DAY.date(), log_fn=lambda *a: None)
    assert res["complete"] and res["error"] is None and len(flushes) > 2
    ld, want = _legacy_frames(small)
    rd = reduced.ReducedDay(wide, DAY.date())
    assert sorted(tuple(k) for k in rd.manifest()["keys"]) == sorted(want)
    for key, df in want.items():
        assert len(rd.frame(*key)) == len(df), key
