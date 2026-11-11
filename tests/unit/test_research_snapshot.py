"""Research snapshots (workstream 1): closed-file rule, hard-link freezing, manifest, verify, prune, limits, and the guarantee that the lake is never touched."""
import errno
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from research import access, snapshot as sn

NOW = datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)          # after the 10:05 compaction and after CLOSED_AFTER_UTC


def parquet(path: Path, rows=3, **extra):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"date": pd.to_datetime(["2026-11-03"] * rows), "symbol": ["A.NS"] * rows, "market": "IN", "open": 1.0, "high": 2.0,
                  "low": 0.5, "close": 1.5, "volume": 10, **extra}).to_parquet(path, index=False)
    old = min(time.time(), NOW.timestamp()) - 3600
    os.utime(path, (old, old))                                    # an hour old by the real clock and by the test clock: not "in flight"
    return path


@pytest.fixture
def lake(tmp_path):
    root = tmp_path / "lake"
    parquet(root / "ticks/segment=eq/date=2026-11-03/ticks.parquet")
    parquet(root / "ticks/segment=fo/date=2026-11-10/ticks.parquet")
    parquet(root / "ticks/segment=eq/date=2026-11-10/ticks.parquet")
    parquet(root / "ticks/segment=eq/date=2026-11-10/seg-000000000000001.parquet")         # an open day: segments not yet compacted
    parquet(root / "ticks/segment=fo/date=2026-11-11/ticks.parquet")                        # today, compacted, after 11:00 UTC: closed
    parquet(root / "ticks/segment=eq/date=2026-11-12/ticks.parquet")                        # tomorrow: never
    parquet(root / "bars/interval=1m/date=2026-11-04/bars.parquet")
    parquet(root / "ohlc/year=2026/ohlc.parquet")
    parquet(root / "fundamentals/income_statement.parquet")
    parquet(root / "fundamentals/pillar_metrics.parquet.tmp")                               # temporary: never
    parquet(root / "upstox_fundamentals/raw/date=2026-11-03/X.json.gz")                     # raw payload: excluded by default
    parquet(root / "backfill/state.json")                                                   # backfill progress state: excluded
    parquet(root / "_pre_split/ticks/date=2026-11-02/ticks.parquet")
    parquet(root / "fundamentals/.hidden.parquet")
    parquet(root / "other_unknown_folder/x.parquet")                                        # not in the allow-list
    return root


@pytest.fixture
def root(tmp_path):
    return tmp_path / "research"


@pytest.fixture
def ohlc_lake(tmp_path):
    """A lake a `Lake` can open: daily candles only (the other test tables hold stand-in columns)."""
    root = tmp_path / "olake"
    parquet(root / "ohlc/year=2026/ohlc.parquet")
    return root


def paths(m):
    return {f["path"] for f in m["files"]}


def test_only_closed_files_of_known_tables_are_included(lake, root):
    m = sn.create(lake, root, now=NOW)
    assert paths(m) == {"ticks/segment=eq/date=2026-11-03/ticks.parquet", "ticks/segment=fo/date=2026-11-10/ticks.parquet",
                        "ticks/segment=fo/date=2026-11-11/ticks.parquet", "bars/interval=1m/date=2026-11-04/bars.parquet",
                        "ohlc/year=2026/ohlc.parquet", "fundamentals/income_statement.parquet"}
    reasons = {k["path"]: k["reason"] for k in m["skipped"]}
    assert "open" in reasons["ticks/segment=eq/date=2026-11-10"] and "not over" in reasons["ticks/segment=eq/date=2026-11-12"]
    assert (root / "snapshots" / m["snapshot_id"] / "ohlc/year=2026/ohlc.parquet").is_file()
    assert (root / "snapshots" / "latest").resolve().name == m["snapshot_id"]


def test_today_is_open_until_the_cutoff_and_raw_can_be_added(lake, root):
    early = datetime(2026, 11, 11, 9, 0, tzinfo=timezone.utc)
    m = sn.create(lake, root, now=early)
    assert "ticks/segment=fo/date=2026-11-11/ticks.parquet" not in paths(m)
    m2 = sn.create(lake, root / "r2", include_raw=True, now=NOW)
    assert "upstox_fundamentals/raw/date=2026-11-03/X.json.gz" in paths(m2) and "backfill/state.json" not in paths(m2)


def test_files_are_hard_links_so_no_space_is_used_and_the_lake_is_untouched(lake, root):
    before = {p: (p.stat().st_ino, p.stat().st_mtime_ns, p.stat().st_mode, p.stat().st_size) for p in lake.rglob("*") if p.is_file()}
    tree = sorted(str(p.relative_to(lake)) for p in lake.rglob("*"))
    m = sn.create(lake, root, now=NOW)
    for f in m["files"]:
        a, b = (lake / f["path"]).stat(), (root / "snapshots" / m["snapshot_id"] / f["path"]).stat()
        assert a.st_ino == b.st_ino and a.st_nlink == 2
    assert tree == sorted(str(p.relative_to(lake)) for p in lake.rglob("*"))                  # no file created or removed in the lake
    assert before == {p: (p.stat().st_ino, p.stat().st_mtime_ns, p.stat().st_mode, p.stat().st_size) for p in lake.rglob("*") if p.is_file()}


def test_a_snapshot_keeps_the_old_file_when_the_lake_replaces_it_by_rename(lake, root):
    m1 = sn.create(lake, root, now=NOW)
    target = lake / "ohlc/year=2026/ohlc.parquet"
    tmp = target.with_suffix(".tmp2")
    pd.DataFrame({"date": pd.to_datetime(["2026-11-10"]), "symbol": ["B.NS"], "market": "IN", "open": 9.0, "high": 9.0, "low": 9.0,
                  "close": 9.0, "volume": 1}).to_parquet(tmp, index=False)
    os.replace(tmp, target)
    os.utime(target, (min(time.time(), NOW.timestamp()) - 3600,) * 2)
    m2 = sn.create(lake, root, now=NOW)
    assert m1["snapshot_id"] != m2["snapshot_id"]
    old = pd.read_parquet(root / "snapshots" / m1["snapshot_id"] / "ohlc/year=2026/ohlc.parquet")
    new = pd.read_parquet(root / "snapshots" / m2["snapshot_id"] / "ohlc/year=2026/ohlc.parquet")
    assert set(old.symbol) == {"A.NS"} and set(new.symbol) == {"B.NS"}
    assert sn.verify(root / "snapshots" / m1["snapshot_id"], deep=True) == []


def test_an_unchanged_lake_gives_the_same_id_and_hashes_are_reused(lake, root, monkeypatch):
    m1 = sn.create(lake, root, now=NOW)
    calls = []
    monkeypatch.setattr(sn, "sha256_of", lambda p: calls.append(p) or "x")
    m2 = sn.create(lake, root, now=NOW)
    assert m1["snapshot_id"] == m2["snapshot_id"] and not calls
    assert len(sn.list_snapshots(root)) == 1


def test_recently_modified_single_files_and_files_that_change_while_hashed_are_skipped(lake, root, monkeypatch):
    os.utime(lake / "fundamentals/income_statement.parquet", (NOW.timestamp() - 10,) * 2)
    m = sn.create(lake, root, now=NOW)
    assert "fundamentals/income_statement.parquet" not in paths(m)
    assert any("last 120" in k["reason"] for k in m["skipped"])
    real = sn.sha256_of
    def racing(p):
        d = real(p)
        if p.name == "ohlc.parquet":
            os.utime(p, (NOW.timestamp() - 5000,) * 2)
        return d
    monkeypatch.setattr(sn, "sha256_of", racing)
    m2 = sn.create(lake, root / "r2", now=NOW)
    assert "ohlc/year=2026/ohlc.parquet" not in paths(m2) and any("changed while" in k["reason"] for k in m2["skipped"])


def test_a_file_that_disappears_is_skipped(lake, root, monkeypatch):
    real = sn.plan_lake
    def plan(*a, **k):
        p = real(*a, **k)
        p.files.append(Path("ohlc/year=2025/ohlc.parquet"))
        return p
    monkeypatch.setattr(sn, "plan_lake", plan)
    m = sn.create(lake, root, now=NOW)
    assert any("disappeared" in k["reason"] for k in m["skipped"])


def test_files_other_users_cannot_read_are_reported_not_chmodded(lake, root):
    f = lake / "ohlc/year=2026/ohlc.parquet"
    f.chmod(0o600)
    m = sn.create(lake, root, now=NOW)
    assert any("ohlc.parquet is not readable by other users" in w for w in m["warnings"]) and f.stat().st_mode & 0o777 == 0o600


def test_manifest_records_coverage_and_hashes(lake, root):
    m = sn.create(lake, root, now=NOW)
    disk = json.loads((root / "snapshots" / m["snapshot_id"] / sn.MANIFEST).read_text())
    assert disk["snapshot_id"] == m["snapshot_id"] and m["snapshot_id"].startswith("20261111-")
    cov = m["coverage"]
    assert cov["ticks/segment=eq"] == {"first": "2026-11-03", "last": "2026-11-03", "days": 1, "files": 1}
    assert cov["ticks/segment=fo"]["first"] == "2026-11-10" and cov["ticks/segment=fo"]["last"] == "2026-11-11" and cov["ticks/segment=fo"]["days"] == 2
    assert cov["ohlc"]["first"] == "2026-01-01" and cov["fundamentals"]["first"] is None
    assert all(len(f["sha256"]) == 64 for f in m["files"])


def test_verify_finds_missing_and_changed_files(lake, root):
    m = sn.create(lake, root, now=NOW)
    snap = root / "snapshots" / m["snapshot_id"]
    assert sn.verify(snap) == []
    (snap / "fundamentals/income_statement.parquet").unlink()
    assert sn.verify(snap) == ["missing: fundamentals/income_statement.parquet"]
    m2 = sn.create(lake, root / "r2", now=NOW)
    s2 = root / "r2" / "snapshots" / m2["snapshot_id"]
    f = s2 / "ohlc/year=2026/ohlc.parquet"
    data = f.read_bytes()
    f.unlink()
    f.write_bytes(data[:-1] + bytes([data[-1] ^ 1]))                # same size, one flipped bit (a copy, so the lake's file is not changed)
    assert sn.verify(s2) == [] and sn.verify(s2, deep=True) == ["checksum differs: ohlc/year=2026/ohlc.parquet"]
    f.write_bytes(b"x")
    assert sn.verify(s2) == ["size differs: ohlc/year=2026/ohlc.parquet"]
    assert sn._load_manifest(s2)
    with pytest.raises(sn.SnapshotError, match="not a snapshot"):
        sn.verify(root)


def test_different_filesystem_is_refused_and_copy_mode_works(lake, root, monkeypatch):
    def nolink(a, b):
        raise OSError(errno.EXDEV, "cross-device")
    monkeypatch.setattr(os, "link", nolink)
    with pytest.raises(sn.SnapshotError, match="different filesystem"):
        sn.create(lake, root, now=NOW)
    assert not [p for p in (root / "snapshots").iterdir() if p.name.startswith(".building")]       # the half-built folder is removed
    m = sn.create(lake, root, copy=True, now=NOW)
    a, b = (lake / "ohlc/year=2026/ohlc.parquet").stat(), (root / "snapshots" / m["snapshot_id"] / "ohlc/year=2026/ohlc.parquet").stat()
    assert m["mode"] == "copy" and a.st_ino != b.st_ino


def test_device_mismatch_is_caught_before_any_work(lake, root, monkeypatch):
    real = Path.stat
    def fake(self, *a, **k):
        st = real(self, *a, **k)
        if self.name == "snapshots":
            class S:
                st_dev = st.st_dev + 1
            return S()
        return st
    monkeypatch.setattr(Path, "stat", fake)
    with pytest.raises(sn.SnapshotError, match="different filesystem"):
        sn.create(lake, root, now=NOW)


def test_copy_refuses_when_it_would_leave_too_little_disk(lake, root, monkeypatch):
    monkeypatch.setattr(sn, "MIN_FREE_GB", 10**9)
    with pytest.raises(sn.SnapshotError, match="not copied"):
        sn.create(lake, root, copy=True, now=NOW)


def test_other_link_errors_are_not_hidden(lake, root, monkeypatch):
    def nolink(a, b):
        raise OSError(errno.EPERM, "not permitted")
    monkeypatch.setattr(os, "link", nolink)
    with pytest.raises(OSError):
        sn.create(lake, root, now=NOW)


def test_the_root_may_not_be_inside_or_around_the_lake(lake, tmp_path):
    with pytest.raises(sn.SnapshotError, match="inside the lake"):
        sn.create(lake, lake / "research", now=NOW)
    with pytest.raises(sn.SnapshotError, match="contains the lake"):
        sn.create(lake, tmp_path, now=NOW)
    with pytest.raises(sn.SnapshotError, match="no lake"):
        sn.create(tmp_path / "nothing", tmp_path / "r", now=NOW)


def test_prune_keeps_the_newest_and_only_touches_snapshot_folders(lake, root):
    ids = []
    for day in (8, 9, 10):
        parquet(lake / f"bars/interval=1m/date=2026-11-0{day - 5}/bars.parquet")
        ids.append(sn.create(lake, root, now=datetime(2026, 11, day, 12, 0, tzinfo=timezone.utc))["snapshot_id"])
    (root / "snapshots" / "not-a-snapshot").mkdir()
    (root / "snapshots" / "not-a-snapshot" / "keep.txt").write_text("x")
    assert sn.prune(root, 2) == [ids[0]]
    assert sn.list_snapshots(root) == ids[1:] and (root / "snapshots" / "not-a-snapshot" / "keep.txt").exists()
    assert (lake / "ohlc/year=2026/ohlc.parquet").exists()                                       # the lake still has every file
    assert sn.prune(root, 0) == []
    m = sn.create(lake, root, now=datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc), keep=1)
    assert sn.list_snapshots(root) == [m["snapshot_id"]]


def test_status_reports_missing_fresh_and_stale(lake, root):
    ok, line = sn.status(root, now=NOW)
    assert not ok and "no usable snapshot" in line
    m = sn.create(lake, root, now=NOW)
    ok, line = sn.status(root, now=NOW)
    assert ok and m["snapshot_id"] in line and "newest tick day 2026-11-11" in line
    ok, line = sn.status(root, 2, now=datetime(2026, 11, 20, tzinfo=timezone.utc))
    assert not ok and "days old" in line


def test_resolve_names_the_missing_snapshot(root):
    with pytest.raises(sn.SnapshotError, match="research.snapshot create"):
        sn.resolve(root)
    with pytest.raises(sn.SnapshotError, match="no snapshot 'x'"):
        sn.resolve(root, "x")


# -- access ------------------------------------------------------------------------------------
def test_a_snapshot_opens_as_a_lake_with_limits_and_provenance(ohlc_lake, root, tmp_path):
    m = sn.create(ohlc_lake, root, now=NOW)
    rs = access.open_snapshot(root=root, memory_gb=1, threads=1, temp_dir=tmp_path / "spill", temp_gb=2)
    assert list(rs.lake.ohlc("A.NS").symbol.unique()) == ["A.NS"] and rs.provenance["snapshot_id"] == m["snapshot_id"]
    con = rs.lake.con
    assert con.execute("SELECT current_setting('memory_limit')").fetchone()[0].startswith("953.")          # 1 GB (10^9 bytes) as DuckDB prints it
    assert con.execute("SELECT current_setting('threads')").fetchone()[0] == 1
    assert (tmp_path / "spill").is_dir() and len(rs.provenance["manifest_sha256"]) == 64
    with pytest.raises(Exception, match="(?i)lock|configuration"):
        con.execute("SET memory_limit='64GB'")
    assert rs.lake.sql("SELECT count(*) n FROM ohlc").n[0] == 3


def test_access_refuses_a_damaged_snapshot_and_reads_env_limits(ohlc_lake, root, monkeypatch):
    m = sn.create(ohlc_lake, root, now=NOW)
    (root / "snapshots" / m["snapshot_id"] / "ohlc/year=2026/ohlc.parquet").unlink()
    with pytest.raises(sn.SnapshotError, match="does not match its manifest"):
        access.open_snapshot(root=root)
    monkeypatch.setenv("JARVIS_RESEARCH_MEMORY_GB", "oops")
    monkeypatch.setenv("JARVIS_RESEARCH_THREADS", "3")
    assert access._env_float("JARVIS_RESEARCH_MEMORY_GB", 4.0) == 4.0 and access._env_float("JARVIS_RESEARCH_THREADS", 2) == 3.0
    rs = access.open_snapshot(root=root, check=False, temp_dir=root / "spill")
    assert rs.lake.con.execute("SELECT current_setting('threads')").fetchone()[0] == 3


# -- command line ----------------------------------------------------------------------------------
def test_cli_create_status_verify_list(lake, root, capsys):
    base = ["--root", str(root)]
    assert sn.main([*base, "status"]) == 1 and "STALE" in capsys.readouterr().out
    assert sn.main([*base, "create", "--lake", str(lake)]) == 0
    out = capsys.readouterr().out
    assert out.startswith("snapshot ") and "skipped" in out
    assert sn.main([*base, "list"]) == 0 and capsys.readouterr().out.strip() == sn.list_snapshots(root)[0]
    assert sn.main([*base, "verify", "--deep"]) == 0 and "matches its manifest" in capsys.readouterr().out
    (root / "snapshots" / "latest" / "ohlc/year=2026/ohlc.parquet").unlink()
    assert sn.main([*base, "verify"]) == 1 and "missing" in capsys.readouterr().out
    assert sn.main([*base, "verify", "nope"]) == 1 and "STOPPED" in capsys.readouterr().err


def test_cli_create_reports_warnings(lake, root, capsys):
    (lake / "ohlc/year=2026/ohlc.parquet").chmod(0o600)
    assert sn.main(["--root", str(root), "create", "--lake", str(lake), "--keep", "5"]) == 0
    assert "WARNING" in capsys.readouterr().out


# -- the setup script (dry run only: it must change nothing) --------------------------------------
def test_setup_script_dry_run_prints_every_step_and_changes_nothing(tmp_path):
    import subprocess
    script = Path(__file__).resolve().parents[2] / "deploy" / "research" / "setup.sh"
    assert subprocess.run(["bash", "-n", str(script)]).returncode == 0
    lake = tmp_path / "home" / "svc" / "lake"
    parquet(lake / "ohlc/year=2026/ohlc.parquet")
    root = tmp_path / "srv" / "research"
    env = {**os.environ, "LAKE": str(lake), "ROOT": str(root), "SVC_USER": "root", "RES_USER": "jarvis-research-test"}
    out = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
    assert out.returncode == 0 and "DRY RUN" in out.stdout
    for step in ("1. the research user", "2. the research root", "3. limits", "4. the research user's Python", "5. the first snapshot"):
        assert step in out.stdout
    assert "useradd --create-home" in out.stdout and "CPUQuota=200%" in out.stdout and "rsync" in out.stdout and "--exclude='.env*'" in out.stdout
    assert not root.exists()                                                            # nothing was created
    assert subprocess.run(["id", "jarvis-research-test"], capture_output=True).returncode != 0          # no user was created
    missing = subprocess.run(["bash", str(script)], env={**env, "LAKE": str(tmp_path / "nolake")}, capture_output=True, text=True)
    assert missing.returncode == 1 and "no lake at" in missing.stdout
