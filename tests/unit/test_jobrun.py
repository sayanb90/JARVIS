"""Job heartbeat: the wrapper records start and end and passes the exit code through; cron expressions, states and log redaction."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta

import pytest

from data_layer import jobrun as jr

NOW = datetime(2026, 11, 16, 12, 0)          # a Monday, 12:00 UTC


def runs(*recs):
    return list(recs)


def start(job, at, id="a", pid=1):
    return {"ev": "start", "job": job, "id": id, "at": at, "pid": pid}


def end(job, at, code=0, id="a", secs=5.0):
    return {"ev": "end", "job": job, "id": id, "at": at, "code": code, "secs": secs}


def job(expr="0 11 * * 1-5", name="j", wrapped=True, log="logs/j.log"):
    return jr.Job(name, jr.Schedule.parse(expr), "cmd", log, wrapped=wrapped)


# -- the wrapper --------------------------------------------------------------------------------
def test_wrapper_records_start_and_end_and_passes_the_exit_code(tmp_path):
    code = jr.run_job("ok", argv=[sys.executable, "-c", "import sys; sys.exit(0)"], root=tmp_path)
    bad = jr.run_job("bad", argv=[sys.executable, "-c", "import sys; sys.exit(3)"], root=tmp_path)
    assert (code, bad) == (0, 3)
    recs = jr.load_runs(tmp_path)
    assert [(r["ev"], r["job"]) for r in recs] == [("start", "ok"), ("end", "ok"), ("start", "bad"), ("end", "bad")]
    assert recs[3]["code"] == 3 and recs[1]["secs"] >= 0


def test_wrapper_shell_line_and_missing_command(tmp_path):
    assert jr.run_job("chain", shell="true && exit 4", root=tmp_path) == 4
    assert jr.run_job("nope", argv=["/no/such/command"], root=tmp_path) == 127
    ends = [r for r in jr.load_runs(tmp_path) if r["ev"] == "end"]
    assert [(r["job"], r["code"]) for r in ends] == [("chain", 4), ("nope", 127)]


def test_torn_lines_are_skipped(tmp_path):
    p = jr.runs_path(tmp_path)
    p.parent.mkdir(parents=True)
    p.write_text(json.dumps(start("a", "2026-11-16T11:00:00")) + "\n{\"ev\": \"st\n[1,2]\n")
    assert len(jr.load_runs(tmp_path)) == 1


def test_compact_drops_old_records(tmp_path):
    p = jr.runs_path(tmp_path)
    p.parent.mkdir(parents=True)
    old, new = "2026-09-01T00:00:00", "2026-11-15T00:00:00"
    p.write_text("".join(json.dumps(r) + "\n" for r in (start("a", old), end("a", old), start("a", new, "b"))))
    assert jr.compact(tmp_path, now=NOW) == 1


# -- cron expressions ---------------------------------------------------------------------------
def test_schedule_matches_weekdays_ranges_steps_and_sunday():
    s = jr.Schedule.parse("*/15 3-4 * * 1-5")
    assert s.matches(datetime(2026, 11, 16, 3, 45)) and not s.matches(datetime(2026, 11, 16, 3, 40))
    assert not s.matches(datetime(2026, 11, 15, 3, 45))                     # a Sunday
    sun = jr.Schedule.parse("0 13 * * 0")
    assert sun.matches(datetime(2026, 11, 15, 13, 0)) and jr.Schedule.parse("0 13 * * 7").matches(datetime(2026, 11, 15, 13, 0))


def test_next_and_last_due():
    s = jr.Schedule.parse("30 10 * * 1-5")
    assert s.last_due(NOW) == datetime(2026, 11, 16, 10, 30)
    assert s.next_due(NOW) == datetime(2026, 11, 17, 10, 30)
    assert s.next_due(datetime(2026, 11, 20, 12, 0)) == datetime(2026, 11, 23, 10, 30)      # over the weekend


def test_bad_expression_is_rejected():
    for bad in ("* * *", "61 * * * *", "a b c d e"):
        with pytest.raises(ValueError):
            jr.Schedule.parse(bad)


# -- the crontab --------------------------------------------------------------------------------
CRON = """SHELL=/bin/bash
JARVIS=/x
# comment
0 23 * * 1-5   cd $JARVIS && python -m data_layer.jobrun run ohlc-daily -- python scheduler.py --job ohlc-daily >> logs/scheduler.log 2>&1
30 0 * * *     cd $JARVIS && python -m data_layer.fx refresh >> logs/fx.log 2>&1
40 3 * * 1-5   cd $JARVIS && python -m data_layer.upstox.feed --once --shard 1/2 >> logs/fo-feed.log 2>&1
41 3 * * 1-5   cd $JARVIS && python -m data_layer.upstox.feed --once --shard 2/2 >> logs/fo-feed.log 2>&1
not a schedule line
"""


def test_parse_crontab_names_wrapped_and_unwrapped_jobs():
    jobs = jr.parse_crontab(CRON)
    assert [(j.name, j.wrapped, j.log) for j in jobs] == [
        ("ohlc-daily", True, "logs/scheduler.log"), ("fx", False, "logs/fx.log"),
        ("fo-feed", False, "logs/fo-feed.log"), ("fo-feed-2", False, "logs/fo-feed.log")]


def test_the_real_crontab_parses_every_line():
    text = (jr.lake_mod.ROOT / "deploy" / "crontab").read_text()
    lines = [x for x in text.splitlines() if x.strip() and not x.lstrip().startswith("#") and "=" not in x.split()[0]]
    assert len(jr.parse_crontab(text)) == len(lines)


def test_every_cron_line_is_wrapped_so_the_jobs_tab_tracks_it():
    jobs = jr.parse_crontab((jr.lake_mod.ROOT / "deploy" / "crontab").read_text())
    assert jobs and [j.name for j in jobs if not j.wrapped] == []
    assert len({j.name for j in jobs}) == len(jobs)                                    # unique names: one row each
    assert all(j.log for j in jobs)                                                    # every job has a log to tail


# -- states -------------------------------------------------------------------------------------
def st(j, recs, now=NOW, alive=lambda p: True, root=None):
    return jr.states([j], recs, now, root=root, pid_alive=alive)[0]


def test_ok_failed_and_never():
    assert st(job(), runs(start("j", "2026-11-16T11:00:01"), end("j", "2026-11-16T11:02:00"))).state == "ok"
    f = st(job(), runs(start("j", "2026-11-16T11:00:01"), end("j", "2026-11-16T11:02:00", code=2)))
    assert f.state == "failed" and f.exit_code == 2 and f.secs == 5.0
    assert st(job(), []).state == "never"


def test_running_and_lost():
    r = runs(start("j", "2026-11-16T11:55:00", pid=77))
    assert st(job("0 * * * *"), r, alive=lambda p: p == 77).state == "running"
    assert st(job("0 * * * *"), r, alive=lambda p: False).state == "lost"                     # the process is gone, no end line
    old = runs(start("j", "2026-11-15T01:00:00"))
    assert st(job("0 1 * * *"), old).state == "lost"                                          # older than MAX_RUNNING_HOURS


def test_late_when_due_and_nothing_started():
    j = job("0 11 * * 1-5")                                                  # was due 11:00 today; last run was yesterday
    s = st(j, runs(start("j", "2026-11-13T11:00:02"), end("j", "2026-11-13T11:01:00")))
    assert s.state == "late" and s.due_at == "2026-11-16T11:00:00"
    on_time = st(j, runs(start("j", "2026-11-16T11:00:02"), end("j", "2026-11-16T11:01:00")))
    assert on_time.state == "ok"
    just_due = st(j, runs(start("j", "2026-11-13T11:00:02"), end("j", "2026-11-13T11:01:00")), now=datetime(2026, 11, 16, 11, 5))
    assert just_due.state == "ok"                                            # inside the grace period


def test_untracked_job_reports_the_log_write_time(tmp_path):
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "j.log").write_text("x")
    s = st(job(wrapped=False), [], root=tmp_path, now=datetime.utcnow())
    assert s.state == "untracked" and s.log_written is not None and s.next_run


def test_history_is_newest_first():
    recs = []
    for i, day in enumerate(("12", "13", "16")):
        recs += [start("j", f"2026-11-{day}T11:00:00", id=str(i)), end("j", f"2026-11-{day}T11:01:00", id=str(i))]
    h = st(job(), recs).history
    assert [x["start"][:10] for x in h] == ["2026-11-16", "2026-11-13", "2026-11-12"]


# -- the log tail -------------------------------------------------------------------------------
def test_redact_hides_secrets():
    t = jr.redact("GET https://x/y?apikey=ABCDEF&a=1 Authorization: Bearer abc.def token=XYZ123 password: hunter2 "
                  "key 0123456789abcdef0123456789abcdef0123 fine text")
    assert "ABCDEF" not in t and "XYZ123" not in t and "hunter2" not in t and "0123456789abcdef0123456789abcdef0123" not in t
    assert "fine text" in t and "a=1" in t


def test_tail_returns_last_lines_redacted_and_refuses_other_paths(tmp_path):
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "j.log").write_text("\n".join(f"line {i}" for i in range(1000)) + "\ntoken=SECRETVALUE\n")
    out = jr.tail("logs/j.log", 3, tmp_path)
    assert out.splitlines() == ["line 998", "line 999", "token=[hidden]"]
    assert len(jr.tail("logs/j.log", 10_000, tmp_path).splitlines()) == jr.MAX_TAIL_LINES
    (tmp_path / ".env").write_text("TOKEN=abc")
    for bad in ("../.env", ".env", "logs/../.env", "/etc/passwd", "logs/j.log/../../.env"):
        with pytest.raises((ValueError, OSError)):
            jr.tail(bad, 5, tmp_path)
