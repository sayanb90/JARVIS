"""Operations tooling: backups, health report, universe importer, and the deployment files in deploy/."""
import configparser
import importlib.util
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from data_layer import backup, health, tick_store, universe
from data_layer.ticks import BarAggregator, Tick
from tests.helpers import OHLC_H, UNI_HEADER, write_csv

REPO = Path(__file__).resolve().parent.parent.parent


# ============================== backup ==============================
@pytest.fixture()
def src(tmp_path):
    r = tmp_path / "repo"
    (r / "lake/bars/interval=1s/date=2026-10-23").mkdir(parents=True)
    duckdb.sql(f"COPY (SELECT 1 AS a) TO '{r}/lake/bars/interval=1s/date=2026-10-23/bars.parquet' (FORMAT PARQUET)")
    write_csv(r / "data/universe.csv", ["symbol"], [["A.NS"]])
    write_csv(r / "data/fund_meta.csv", ["symbol"], [["A.NS"]])
    (r / ".env").write_text("SECRET=1\n")
    (r / "logs").mkdir()
    (r / "logs/x.log").write_text("log")
    (r / "data/bars/5m/in").mkdir(parents=True)
    (r / "data/bars/5m/in/half.csv.tmp").write_text("partial")
    return r


def test_snapshot_contains_data_but_never_secrets_logs_or_partial_files(src, tmp_path):
    s = backup.run_backup(tmp_path / "bk", root=src, today=date(2026, 10, 26))
    snap = tmp_path / "bk/2026-10-26"
    names = {p.relative_to(snap).as_posix() for p in snap.rglob("*") if p.is_file()}
    assert "lake/bars/interval=1s/date=2026-10-23/bars.parquet" in names and "data/universe.csv" in names
    assert ".env" not in names and "logs/x.log" not in names and not any(n.endswith(".tmp") for n in names)
    assert s["files"] == s["copied"] == 3 and s["linked"] == 0 and (snap / "MANIFEST.json").exists()
    assert backup.list_snapshots(tmp_path / "bk") == ["2026-10-26"]


def test_second_snapshot_hardlinks_unchanged_and_copies_only_what_changed(src, tmp_path):
    dest = tmp_path / "bk"
    backup.run_backup(dest, root=src, today=date(2026, 10, 26))
    new = src / "lake/bars/interval=1s/date=2026-10-27"
    new.mkdir(parents=True)
    duckdb.sql(f"COPY (SELECT 2 AS a) TO '{new}/bars.parquet' (FORMAT PARQUET)")
    (src / "data/fund_meta.csv").write_text("symbol\nA.NS\nB.NS\n")
    (src / "data/universe.csv").unlink()                                          # removed at the source
    s = backup.run_backup(dest, root=src, today=date(2026, 10, 27))
    assert s["copied"] == 2 and s["linked"] == 1 and s["files"] == 3
    a, b = dest / "2026-10-26", dest / "2026-10-27"
    p = "lake/bars/interval=1s/date=2026-10-23/bars.parquet"
    assert os.stat(a / p).st_ino == os.stat(b / p).st_ino                         # shared on disk
    assert not (b / "data/universe.csv").exists() and (a / "data/universe.csv").exists()
    assert (b / "data/fund_meta.csv").read_text().count("B.NS") == 1 and "B.NS" not in (a / "data/fund_meta.csv").read_text()


def test_running_twice_in_a_day_replaces_that_days_snapshot(src, tmp_path):
    dest = tmp_path / "bk"
    backup.run_backup(dest, root=src, today=date(2026, 10, 26))
    (src / "data/fund_meta.csv").write_text("symbol\nZ.NS\n")
    backup.run_backup(dest, root=src, today=date(2026, 10, 26))
    assert backup.list_snapshots(dest) == ["2026-10-26"] and "Z.NS" in (dest / "2026-10-26/data/fund_meta.csv").read_text()
    assert not [p for p in dest.iterdir() if p.name.startswith((".tmp", ".old"))]


def test_retention_keeps_7_daily_and_4_sundays(src, tmp_path):
    dest = tmp_path / "bk"
    d = date(2026, 8, 3)
    while d <= date(2026, 10, 26):
        backup.run_backup(dest, root=src, today=d)
        d += timedelta(days=1)
    kept = backup.list_snapshots(dest)
    newest7 = [(date(2026, 10, 26) - timedelta(days=i)).isoformat() for i in range(7)]
    sundays = [s for s in kept if date.fromisoformat(s).weekday() == 6]
    assert set(newest7) <= set(kept) and len(sundays) >= 4
    assert len(kept) <= 7 + 4 and all(s in newest7 or date.fromisoformat(s).weekday() == 6 for s in kept)


def test_verify_detects_missing_resized_and_corrupt_parquet(src, tmp_path):
    dest = tmp_path / "bk"
    backup.run_backup(dest, root=src, today=date(2026, 10, 26))
    snap = dest / "2026-10-26"
    assert backup.verify_snapshot(snap) == []
    pq = snap / "lake/bars/interval=1s/date=2026-10-23/bars.parquet"
    good = pq.read_bytes()
    pq.unlink(); pq.write_bytes(b"\0" * len(good))                                # same size, garbage content
    csvf = snap / "data/fund_meta.csv"
    csvf.unlink(); csvf.write_text("tampered, different size")
    (snap / "data/universe.csv").unlink()
    probs = " | ".join(backup.verify_snapshot(snap))
    assert "unreadable parquet" in probs and "size changed: data/fund_meta.csv" in probs and "missing: data/universe.csv" in probs
    (snap / "MANIFEST.json").write_text("not json")
    assert backup.verify_snapshot(snap)[0].startswith("manifest unreadable")


def test_restore_into_empty_dir_roundtrips_and_refuses_to_overwrite(src, tmp_path):
    dest = tmp_path / "bk"
    backup.run_backup(dest, root=src, today=date(2026, 10, 26))
    out = tmp_path / "restored"
    assert backup.restore(dest / "2026-10-26", out) == 3
    assert duckdb.sql(f"SELECT a FROM read_parquet('{out}/lake/bars/interval=1s/date=2026-10-23/bars.parquet')").fetchall() == [(1,)]
    with pytest.raises(SystemExit, match="already exist"):
        backup.restore(dest / "2026-10-26", out)
    (out / "data/fund_meta.csv").write_text("changed")
    backup.restore(dest / "2026-10-26", out, force=True)
    assert (out / "data/fund_meta.csv").read_text().startswith("symbol")


def test_backup_cli(src, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(backup, "ROOT", src)
    dest = str(tmp_path / "bk")
    assert backup.main(["list", "--dest", dest]) == 0 and "no snapshots" in capsys.readouterr().out
    assert backup.main(["verify", "--dest", dest]) == 1
    assert backup.main(["run", "--dest", dest]) == 0 and "copied 3" in capsys.readouterr().out
    assert backup.main(["verify", "--dest", dest]) == 0 and "OK" in capsys.readouterr().out
    assert backup.main(["verify", "--dest", dest, "--snapshot", "2001-01-01"]) == 1
    assert backup.main(["restore", "--dest", dest, "--to", str(tmp_path / "r")]) == 0
    monkeypatch.delenv("JARVIS_BACKUP_DIR", raising=False)
    with pytest.raises(SystemExit, match="no backup destination"):
        backup.main(["run"])
    monkeypatch.setenv("JARVIS_BACKUP_DIR", dest)
    assert backup.main(["list"]) == 0


# ============================== universe ==============================
NSE_CSV = ("﻿Company Name,Industry,Symbol,Series,ISIN Code\n"
           "Reliance Industries Ltd.,Oil Gas & Consumable Fuels,RELIANCE,EQ,INE002A01018\n"
           "Tata Consultancy Services Ltd.,Information Technology,TCS,EQ,INE467B01029\n"
           "Broken Co,Financial Services,BROKEN,EQ,US0378331005\n"
           "Reliance Industries Ltd.,Oil Gas & Consumable Fuels,RELIANCE,EQ,INE002A01018\n"
           ",,,,\n")


def test_parse_nse_csv_handles_bom_invalid_isin_and_duplicates(tmp_path):
    f = tmp_path / "n.csv"; f.write_text(NSE_CSV, encoding="utf-8")
    rows, problems = universe.parse_nse_csv(f)
    assert [r["symbol"] for r in rows] == ["RELIANCE.NS", "TCS.NS"]
    assert rows[0]["isin"] == "INE002A01018" and rows[0]["exchange"] == "NSE" and rows[0]["currency"] == "INR"
    assert rows[1]["name"].startswith("Tata") and rows[1]["sector"] == "Information Technology"
    assert any("BROKEN" in p and "invalid ISIN" in p for p in problems) and any("duplicate" in p for p in problems)


def test_parse_requires_symbol_and_isin_columns(tmp_path):
    f = tmp_path / "n.csv"; f.write_text("Name,Price\nA,1\n")
    with pytest.raises(SystemExit, match="Symbol column"):
        universe.parse_nse_csv(f)


def test_import_merges_keeps_other_markets_and_comments(tmp_path):
    uni = tmp_path / "universe.csv"
    uni.write_text("# keep this comment\n" + ",".join(UNI_HEADER) + "\n"
                   "AAPL,Apple,US,NASDAQ,,,,,,USD\nOLD.NS,Old Co,IN,NSE,INE000000019,,,,,INR\n")
    f = tmp_path / "n.csv"; f.write_text(NSE_CSV, encoding="utf-8")
    s = universe.import_nse(f, uni)
    txt = uni.read_text()
    assert txt.startswith("# keep this comment") and "AAPL" in txt
    assert s["imported"] == 2 and s["added"] == 2 and s["kept_old"] == 1 and s["india_total"] == 3 and "OLD.NS" in txt
    s2 = universe.import_nse(f, uni, replace_india=True)
    assert "OLD.NS" not in uni.read_text() and s2["india_total"] == 2 and "AAPL" in uni.read_text()
    assert universe.import_nse(f, uni, replace_india=True)["added"] == 0                       # idempotent


def test_check_finds_the_problems_that_break_the_feed(tmp_path):
    uni = tmp_path / "u.csv"
    uni.write_text(",".join(UNI_HEADER) + "\nA.NS,A,IN,NSE,INE002A01018,,,,,INR\nA.NS,dup,IN,NSE,INE002A01018,,,,,INR\n"
                   "B,B,IN,NSE,,,,,,INR\nC.NS,C,IN,NSE,,,,,,INR\nD,D,MARS,X,,,,,,\n")
    p = " | ".join(universe.check(uni))
    assert "duplicate symbol A.NS" in p and "B: India symbols must end in .NS" in p and "C.NS: missing or invalid ISIN" in p
    assert "unknown market 'MARS'" in p
    assert universe.check(tmp_path / "none.csv") == ["universe.csv has no rows"]


def test_universe_cli(tmp_path, monkeypatch, capsys, world):
    f = tmp_path / "n.csv"; f.write_text(NSE_CSV, encoding="utf-8")
    assert universe.main(["import-nse", str(f)]) == 0 and "imported 2 NSE stocks" in capsys.readouterr().out
    assert universe.main(["check"]) == 0 and "OK" in capsys.readouterr().out


# ============================== health ==============================
NOW = datetime(2026, 10, 27, 12, 0)                  # Tuesday, after the close, outside the session


@pytest.fixture(autouse=True)
def _plenty_of_disk(monkeypatch):
    """The sandbox's real free space must not decide test results."""
    monkeypatch.setattr(health.shutil, "disk_usage", lambda p: type("D", (), {"total": 100, "free": 60})())


@pytest.fixture()
def healthy(world, tmp_path, monkeypatch):
    world.set_universe({"IN": ["A.NS", "B.NS"], "US": ["AAPL"], "UK": ["SHEL.L"], "EU": ["SAP.DE"]})
    for m, sym in (("in", "A.NS"), ("us", "AAPL"), ("uk", "SHEL.L"), ("eu", "SAP.DE")):
        write_csv(world.data / f"ohlc/ohlc_2026_{m}.csv", OHLC_H, [["2026.10.26", sym, m.upper(), 1, 2, 1, 1.5, 10]])
    world.lake.load_ohlc()
    ag, bars = BarAggregator(1), []
    for sym in ("A.NS", "B.NS"):
        bars += ag.add(Tick(datetime(2026, 10, 27, 3, 45, 1), sym, 100.0, 1))
    tick_store.write_bars(bars + ag.flush_all(), "1s", world.lakedir)
    tick_store.compact_day("1s", date(2026, 10, 27), world.lakedir)
    (world.root / "logs/scheduler.log").write_text("2026-10-26 22:31:00,100  INFO  [scheduler]  Scheduler finished successfully\n")
    bk = tmp_path / "bk"
    (bk / "2026-10-27").mkdir(parents=True); (bk / "2026-10-27/MANIFEST.json").write_text("{}")
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", "tok"); monkeypatch.delenv("UPSTOX_FEED_TOKEN", raising=False)
    monkeypatch.setattr(health.shutil, "which", lambda x: None)
    monkeypatch.setattr(health, "utcnow", lambda: NOW)
    return world, bk


def run_health(world, bk, **kw):
    return health.check(NOW, lake_dir=world.lakedir, root=world.root, backup_path=bk, **kw)


def test_all_green(healthy, capsys):
    world, bk = healthy
    r = run_health(world, bk, after_close=True)
    out = capsys.readouterr().out
    assert r.fails == 0 and "[WARN]" not in out, out
    assert "latest bar day 2026-10-27" in out and "daily OHLC IN: latest 2026-10-26" in out and "newest backup 2026-10-27" in out


def test_each_problem_is_reported(healthy, capsys, monkeypatch):
    world, bk = healthy
    (world.root / "logs/scheduler.log").write_text(
        "2026-10-26 22:31:00,1  INFO  Scheduler finished successfully\n2026-10-27 11:00:00,1  ERROR  Scheduler finished with errors\n")
    seg = world.lakedir / "bars/interval=1s/date=2026-10-26"; seg.mkdir(parents=True)
    duckdb.sql(f"COPY (SELECT TIMESTAMP '2026-10-26 03:45:00' AS ts, 'A.NS' AS symbol) TO '{seg}/bars.parquet' (FORMAT PARQUET)")
    duckdb.sql(f"COPY (SELECT TIMESTAMP '2026-10-26 03:45:01' AS ts, 'A.NS' AS symbol) TO '{seg}/seg-0001.parquet' (FORMAT PARQUET)")  # an old day never compacted
    monkeypatch.delenv("UPSTOX_ACCESS_TOKEN")
    monkeypatch.setattr(health.shutil, "disk_usage", lambda p: type("D", (), {"total": 100, "free": 3})())
    (bk / "2026-10-27").rename(bk / "2026-10-20")
    r = run_health(world, bk, after_close=True)
    out = capsys.readouterr().out
    assert r.fails == 3                                                         # disk, scheduler, backup age
    assert "[FAIL] disk free 3%" in out and "[FAIL] scheduler: last success" in out and "[FAIL] newest backup 2026-10-20" in out
    assert "[WARN] no Upstox token" in out and "uncompacted segment files: ['2026-10-26']" in out


def test_a_damaged_parquet_file_is_reported_not_a_crash(healthy, capsys):
    world, bk = healthy
    bad = world.lakedir / "bars/interval=1s/date=2026-10-26"; bad.mkdir(parents=True)
    (bad / "seg-0001.parquet").write_bytes(b"not parquet")
    r = run_health(world, bk, after_close=True)
    out = capsys.readouterr().out
    assert r.fails == 1 and "[FAIL] the lake cannot be opened" in out
    assert "[PASS] newest backup" in out and "[PASS] scheduler" in out            # the other checks still ran


def test_empty_lake_and_no_backup_configured(world, capsys, monkeypatch):
    world.set_universe({"IN": ["A.NS"]})
    monkeypatch.delenv("JARVIS_BACKUP_DIR", raising=False)
    monkeypatch.setattr(health, "utcnow", lambda: NOW)
    r = health.check(NOW, lake_dir=world.lakedir, root=world.root)
    out = capsys.readouterr().out
    assert r.fails == 1 and "[FAIL] no daily OHLC" in out and "[WARN] no 1-second bars" in out and "[WARN] no backup destination" in out
    assert "[WARN] no logs/scheduler.log" in out


def test_live_session_check_fails_when_the_feed_is_silent(healthy, capsys, monkeypatch):
    world, bk = healthy
    inside = datetime(2026, 10, 27, 5, 0)
    monkeypatch.setattr(health, "utcnow", lambda: inside)
    today = world.lakedir / "bars/interval=1s/date=2026-10-27"
    f = next(today.glob("*.parquet"))
    os.utime(f, (inside.timestamp() - 10, inside.timestamp() - 10))
    assert health.check(inside, lake_dir=world.lakedir, root=world.root, backup_path=bk).fails == 0
    os.utime(f, (inside.timestamp() - 600, inside.timestamp() - 600))
    r = health.check(inside, lake_dir=world.lakedir, root=world.root, backup_path=bk)
    assert r.fails == 1 and "live feed is NOT writing" in capsys.readouterr().out


def test_service_state_and_probe(healthy, capsys, monkeypatch):
    world, bk = healthy
    monkeypatch.setattr(health.shutil, "which", lambda x: "/bin/systemctl")
    monkeypatch.setattr(health.subprocess, "run", lambda *a, **k: type("R", (), {"stdout": "inactive\n"})())
    async def accepted(tok): return "accepted, no frame in 6s"
    monkeypatch.setattr("data_layer.upstox.auth.probe_feed", accepted)
    r = run_health(world, bk, probe=True)
    out = capsys.readouterr().out
    assert "[WARN] systemd service jarvis-feed: inactive" in out and "[PASS] Upstox feed handshake: accepted" in out and r.fails == 0
    async def rejected(tok): return "rejected HTTP 401"
    monkeypatch.setattr("data_layer.upstox.auth.probe_feed", rejected)
    assert run_health(world, bk, probe=True).fails == 1


def test_weekday_helpers():
    assert health.prev_weekday(date(2026, 10, 26)) == date(2026, 10, 23)           # Monday -> previous Friday
    assert health.last_weekday(date(2026, 10, 25)) == date(2026, 10, 23)           # Sunday -> Friday
    assert health.in_session(datetime(2026, 10, 26, 4, 0)) and not health.in_session(datetime(2026, 10, 25, 4, 0))


def test_health_cli_exit_code(world, monkeypatch):
    monkeypatch.delenv("JARVIS_BACKUP_DIR", raising=False)
    assert health.main([]) == 1                                                       # empty lake FAILs


# ============================== deploy files ==============================
def cron_lines():
    return [ln for ln in (REPO / "deploy/crontab").read_text().splitlines() if ln.strip() and not ln.startswith("#") and "=" not in ln.split()[0]]


def test_crontab_lines_are_valid_and_point_at_real_commands():
    lines = cron_lines()
    assert len(lines) >= 4
    for ln in lines:
        f = ln.split(None, 5)
        assert len(f) == 6, ln
        for field, hi in zip(f[:5], (59, 23, 31, 12, 7)):
            assert re.fullmatch(r"[\d*/,\-]+", field), ln
            for n in re.findall(r"\d+", field):
                assert int(n) <= hi, ln
        cmd = f[5]
        for job in re.findall(r"scheduler\.py --job ([\w-]+)", cmd):
            assert job in ("ohlc-daily", "fundamentals", "bars", "all"), cmd
        for mod in re.findall(r"python -m ([\w.]+)", cmd):
            assert importlib.util.find_spec(mod) is not None, mod
        assert "logs/" in cmd and "2>&1" in cmd


def test_crontab_runs_nothing_during_the_session_that_could_collide_with_the_feed():
    # "segment fo" is the one deliberate exception: the fo feed is its own connection, meant to run
    # alongside the equity feed during the session (runbook 17), confirmed live on 2026-10-30 that both
    # run together without conflict.
    for ln in cron_lines():
        h, m = ln.split()[1], ln.split()[0]
        if h.isdigit() and m.isdigit():
            minutes = int(h) * 60 + int(m)
            assert not (3 * 60 + 40 <= minutes < 10 * 60 + 5) or "health" in ln or "segment fo" in ln, \
                f"job inside the feed window: {ln}"


@pytest.mark.parametrize("unit,module", [("jarvis-feed.service", "data_layer.upstox.feed"), ("jarvis-viewer.service", "webapp")])
def test_systemd_units_parse_and_run_real_modules(unit, module):
    cp = configparser.ConfigParser(strict=False, interpolation=None)
    cp.optionxform = str
    cp.read(REPO / "deploy" / unit)
    exe = cp["Service"]["ExecStart"]
    assert exe.endswith(f"-m {module}") or f"-m {module} " in exe
    assert importlib.util.find_spec(module) is not None
    assert cp["Service"]["User"] == "jarvis" and cp["Service"]["Restart"] == "always"
    assert cp["Service"]["EnvironmentFile"].endswith("/.env") and "WorkingDirectory" in cp["Service"]
    assert cp["Install"]["WantedBy"] == "multi-user.target"


def test_start_limits_are_in_the_unit_section_where_systemd_reads_them():
    """systemd ignores StartLimit* under [Service] (systemd-analyze verify warns about it)."""
    cp = configparser.ConfigParser(strict=False, interpolation=None)
    cp.optionxform = str
    cp.read(REPO / "deploy/jarvis-feed.service")
    assert "StartLimitBurst" in cp["Unit"] and "StartLimitIntervalSec" in cp["Unit"]
    assert "StartLimitBurst" not in cp["Service"] and "StartLimitIntervalSec" not in cp["Service"]


def test_ssh_hardening_dropin_sorts_before_cloud_init():
    text = (REPO / "docs/RUNBOOK.md").read_text()
    assert "sshd_config.d/00-jarvis.conf" in text and "99-jarvis.conf" not in text


def test_feed_unit_stops_gracefully_and_viewer_binds_locally():
    feed = (REPO / "deploy/jarvis-feed.service").read_text()
    assert "KillSignal=SIGTERM" in feed and re.search(r"TimeoutStopSec=\d+", feed)
    viewer = (REPO / "deploy/jarvis-viewer.service").read_text()
    assert "--host 127.0.0.1" in viewer and "MemoryMax" in viewer


def test_logrotate_covers_the_log_directory():
    t = (REPO / "deploy/logrotate-jarvis").read_text()
    assert "/home/jarvis/JARVIS/logs/*.log" in t and "copytruncate" in t and "rotate 8" in t


def test_backup_cli_reads_the_destination_from_the_env_file(src, tmp_path, monkeypatch, capsys):
    """Cron must not need to source .env in the shell (a password with $ or spaces would break that)."""
    from data_layer import lake as lake_mod
    envf = tmp_path / ".env"
    envf.write_text(f"WEBAPP_PASSWORD=has $dollar and spaces\nJARVIS_BACKUP_DIR={tmp_path / 'from-env'}\n")
    monkeypatch.setattr(lake_mod, "ENV_FILE", envf)
    monkeypatch.setattr(backup, "ROOT", src)
    monkeypatch.delenv("JARVIS_BACKUP_DIR", raising=False)
    assert backup.main(["run"]) == 0
    assert backup.list_snapshots(tmp_path / "from-env") == [date.today().isoformat()]
    assert "$JARVIS" not in (REPO / "deploy/crontab").read_text().split("backup")[1].split("\n")[0]


def test_runbook_commands_name_real_modules_and_flags():
    """Every `python -m <module>` in section 0 of the runbook must exist (guards against the docs drifting from the code)."""
    text = (REPO / "docs/RUNBOOK.md").read_text()
    section0 = text[text.index("## 0. Start here"):text.index("## 1. Scope")]
    mods = set(re.findall(r"python -m ([\w.]+)", section0))
    assert {"data_layer.universe", "data_layer.backup", "data_layer.health", "data_layer.upstox.auth", "data_layer.verify"} <= mods
    for m in mods:
        if m != "pytest":
            assert importlib.util.find_spec(m) is not None, m
    for f in re.findall(r"deploy/([\w.\-]+)", section0):
        assert (REPO / "deploy" / f).exists(), f
