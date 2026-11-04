"""Compacting one day's ticks from several processes at once (SCRUM-114): the equity service and the F&O job both compact
lake/ticks/date=<day> when they stop at 10:05 UTC.  No exception, no lost rows, no duplicated rows."""
from __future__ import annotations

import threading
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
import pytest

from data_layer import tick_store

DAY = date(2026, 11, 3)


def segment(folder, n: int, rows: int = 100, symbol: str = "A") -> None:
    base = datetime(2026, 11, 3, 4, 0) + timedelta(seconds=n * rows)
    pd.DataFrame({"ts": [base + timedelta(seconds=i) for i in range(rows)], "symbol": symbol, "ltp": 1.0 + n}).to_parquet(folder / f"seg-{n}.parquet")


def rows_and_distinct(folder) -> tuple[int, int]:
    f = folder / "ticks.parquet"
    return duckdb.connect().execute(f"SELECT count(*), count(DISTINCT (symbol, ts)) FROM read_parquet('{f}')").fetchone()


@pytest.fixture()
def folder(tmp_path):
    f = tmp_path / "ticks" / f"date={DAY}"
    f.mkdir(parents=True)
    return f


def test_a_missing_day_is_zero_rows_and_creates_nothing(tmp_path):
    assert tick_store.compact_ticks_day(DAY, tmp_path) == 0
    assert not (tmp_path / "ticks").exists()


def test_concurrent_compactions_neither_fail_nor_lose_nor_duplicate_rows(tmp_path, folder):
    for rep in range(8):
        for n in range(30):
            segment(folder, rep * 100 + n)
        errors: list[Exception] = []

        def compact():
            try:
                tick_store.compact_ticks_day(DAY, tmp_path)
            except Exception as exc:                                    # noqa: BLE001
                errors.append(exc)

        def writer():
            for n in range(10):
                segment(folder, rep * 100 + 50 + n)

        threads = [threading.Thread(target=compact) for _ in range(6)] + [threading.Thread(target=writer)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
    tick_store.compact_ticks_day(DAY, tmp_path)                         # one last pass folds in anything written late
    total = 8 * 40 * 100
    assert rows_and_distinct(folder) == (total, total)
    assert sorted(p.name for p in folder.iterdir() if p.suffix in (".parquet", ".tmp")) == ["ticks.parquet"]


def test_a_segment_written_during_a_compaction_is_kept_and_merged_once(tmp_path, folder, monkeypatch):
    for n in range(5):
        segment(folder, n)
    real_replace = tick_store.os.replace

    def replace_after_a_late_segment(src, dst):
        segment(folder, 99)                                              # appears after the listing and the read
        real_replace(src, dst)
    monkeypatch.setattr(tick_store.os, "replace", replace_after_a_late_segment)
    assert tick_store.compact_ticks_day(DAY, tmp_path) == 500
    assert (folder / "seg-99.parquet").exists()                          # not deleted, because it was not part of this merge
    monkeypatch.undo()
    assert tick_store.compact_ticks_day(DAY, tmp_path) == 600
    assert rows_and_distinct(folder) == (600, 600) and not (folder / "seg-99.parquet").exists()


def test_a_failed_compaction_leaves_the_segments_and_no_temporary_file(tmp_path, folder):
    for n in range(3):
        segment(folder, n)
    (folder / "seg-bad.parquet").write_bytes(b"not parquet")
    with pytest.raises(Exception):
        tick_store.compact_ticks_day(DAY, tmp_path)
    assert not list(folder.glob("*.tmp")) and len(list(folder.glob("seg-*.parquet"))) == 4 and not (folder / "ticks.parquet").exists()


def test_compacting_twice_is_idempotent(tmp_path, folder):
    for n in range(4):
        segment(folder, n)
    assert tick_store.compact_ticks_day(DAY, tmp_path) == 400
    assert tick_store.compact_ticks_day(DAY, tmp_path) == 400
    assert rows_and_distinct(folder) == (400, 400)


# ---------------------------------------------------------------------------------- repairing a day that holds rows twice
def doubled_day(folder, rows: int = 300):
    base = datetime(2026, 11, 3, 4, 0)
    df = pd.DataFrame({"ts": [base + timedelta(seconds=i) for i in range(rows)], "symbol": "A", "ltp": 1.0, "iv": float("nan")})
    pd.concat([df, df.iloc[:200]]).to_parquet(folder / "ticks.parquet")          # 200 rows appear twice, as after the overlapping compactions
    return rows


def test_dedupe_removes_rows_identical_in_every_column_and_keeps_a_backup(tmp_path, folder):
    unique = doubled_day(folder)
    assert tick_store.dedupe_ticks_day(DAY, tmp_path) == (unique + 200, unique)
    assert rows_and_distinct(folder) == (unique, unique)
    assert (folder / "ticks.pre-dedupe.bak").exists() and not list(folder.glob("*.tmp"))
    assert [p.name for p in folder.glob("*.parquet")] == ["ticks.parquet"]       # the backup is not a *.parquet file
    assert tick_store.dedupe_ticks_day(DAY, tmp_path) == (unique, unique)         # idempotent


def test_dedupe_keeps_rows_that_differ_in_any_column_and_can_skip_the_backup(tmp_path, folder):
    base = datetime(2026, 11, 3, 4, 0)
    pd.DataFrame({"ts": [base, base, base], "symbol": "A", "ltp": [1.0, 2.0, 2.0]}).to_parquet(folder / "ticks.parquet")
    assert tick_store.dedupe_ticks_day(DAY, tmp_path, keep_backup=False) == (3, 2)
    assert not (folder / "ticks.pre-dedupe.bak").exists()


def test_dedupe_refuses_while_segments_exist_and_ignores_a_missing_day(tmp_path, folder):
    doubled_day(folder)
    segment(folder, 1)
    with pytest.raises(RuntimeError, match="run compact first"):
        tick_store.dedupe_ticks_day(DAY, tmp_path)
    assert rows_and_distinct(folder)[0] == 500                                    # nothing was changed
    assert tick_store.dedupe_ticks_day(date(2026, 1, 1), tmp_path) == (0, 0)


def test_command_line_compact_and_dedupe(tmp_path, folder, capsys):
    for n in range(3):
        segment(folder, n)
    assert tick_store.main(["compact", "2026-11-03", "--lake", str(tmp_path)]) == 0
    assert "300 rows" in capsys.readouterr().out
    pd.concat([pd.read_parquet(folder / "ticks.parquet")] * 2).to_parquet(folder / "ticks.parquet")
    assert tick_store.main(["dedupe", "2026-11-03", "--lake", str(tmp_path), "--no-backup"]) == 0
    assert "600 rows before, 300 after, 300 duplicates removed" in capsys.readouterr().out


# ---------------------------------------------------------------------------- SCRUM-127: a memory cap, and the sort spills to disk
def wide_segments(folder, segments: int = 6, rows: int = 60_000) -> None:
    import numpy as np
    rng = np.random.default_rng(7)
    for n in range(segments):
        ts = datetime(2026, 11, 3, 4, 0) + pd.to_timedelta(rng.integers(0, 20_000_000, rows), unit="ms")
        pd.DataFrame({"ts": ts, "symbol": rng.choice([f"S{i}.NS" for i in range(40)], rows), "ltp": rng.random(rows) * 1000,
                      "vol_today": rng.integers(0, 10**7, rows), "bid": rng.random(rows), "ask": rng.random(rows),
                      "oi": rng.integers(0, 10**6, rows), "iv": rng.random(rows).astype("float32"),
                      "fts": ts + pd.Timedelta(milliseconds=3), "rts": ts + pd.Timedelta(milliseconds=8)}
                     ).to_parquet(folder / f"seg-{n}.parquet")


def test_the_memory_limit_comes_from_the_environment_and_falls_back_on_nonsense(monkeypatch):
    monkeypatch.delenv("JARVIS_COMPACT_MEMORY_GB", raising=False)
    assert tick_store._compact_memory_limit() == f"{tick_store.COMPACT_MEMORY_GB:g}GB"
    monkeypatch.setenv("JARVIS_COMPACT_MEMORY_GB", "3")
    assert tick_store._compact_memory_limit() == "3GB"
    monkeypatch.setenv("JARVIS_COMPACT_MEMORY_GB", "banana")
    assert tick_store._compact_memory_limit() == f"{tick_store.COMPACT_MEMORY_GB:g}GB"
    monkeypatch.setenv("JARVIS_COMPACT_MEMORY_GB", "0")
    assert tick_store._compact_memory_limit() == "0.1GB"                                     # never below a floor that still works


def test_compaction_applies_the_limit_and_a_spill_folder_and_gives_the_same_rows(tmp_path, monkeypatch):
    """The limit and the temp directory are set on the connection, the spill folder is gone afterwards, and the rows and their order
    equal those of a compaction with a very large limit.  (That the sort really spills was measured on a 9 million row day:
    840 MB spilled and the process stayed at 1.3 GB against 2.7 GB without a cap; DuckDB's minimum working memory is too close
    to the data of a unit test to show it here.)"""
    seen: list[str] = []
    real_connect = duckdb.connect

    class Spy:
        def __init__(self, con):
            self.con = con

        def execute(self, sql, *a):
            seen.append(sql)
            return self.con.execute(sql, *a)

        def close(self):
            self.con.close()

    out = {}
    for name, limit in (("capped", "300MB"), ("free", "64GB")):
        folder = tmp_path / name / "ticks" / f"date={DAY}"
        folder.mkdir(parents=True)
        wide_segments(folder)
        monkeypatch.setattr(tick_store, "_compact_memory_limit", lambda limit=limit: limit)
        monkeypatch.setattr(tick_store.duckdb, "connect", lambda *a, **k: Spy(real_connect(*a, **k)))
        assert tick_store.compact_ticks_day(DAY, tmp_path / name) == 360_000
        monkeypatch.undo()
        out[name] = folder
        assert sorted(p.name for p in folder.iterdir()) == ["ticks.parquet"]               # no spill folder, no temp file left behind
    assert "SET memory_limit='300MB'" in seen and any(q.startswith("SET temp_directory='") and ".compact-spill." in q for q in seen)
    con = duckdb.connect()
    a, b = (f"read_parquet('{out[n] / 'ticks.parquet'}')" for n in ("capped", "free"))
    assert con.execute(f"SELECT count(*) FROM (SELECT * FROM {a} EXCEPT ALL SELECT * FROM {b})").fetchone()[0] == 0
    assert con.execute(f"SELECT count(*) FROM (SELECT * FROM {b} EXCEPT ALL SELECT * FROM {a})").fetchone()[0] == 0
    bad = con.execute(f"""SELECT count(*) FROM (SELECT symbol, rts, lag(symbol) OVER () AS ps, lag(rts) OVER () AS pr FROM {a})
                          WHERE ps = symbol AND pr > rts""").fetchone()[0]
    assert bad == 0                                                                        # still ordered by symbol, then arrival
