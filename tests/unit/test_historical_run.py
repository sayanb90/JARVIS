"""deploy/historical-run.sh: stage order, dry run, resumable state, partial and failed stages (a fake python stands in for every command)."""
import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "deploy" / "historical-run.sh"
FAKE = """#!/usr/bin/env bash
echo "$*" >> "$FAKE_LOG"
case "$*" in *"$FAKE_PARTIAL"*) [ -n "$FAKE_PARTIAL" ] && exit 75;; esac
case "$*" in *"$FAKE_FAIL"*) [ -n "$FAKE_FAIL" ] && exit 1;; esac
exit 0
"""


@pytest.fixture
def box(tmp_path):
    (tmp_path / "deploy").mkdir()
    shutil.copy(SCRIPT, tmp_path / "deploy" / "historical-run.sh")
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "universe_global.csv").write_text("symbol\nAAPL\n")                # the universe stage keeps an existing file
    fake = tmp_path / "fakepy"
    fake.write_text(FAKE)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    return tmp_path


def run(box, *args, **env):
    e = {**os.environ, "PY": str(box / "fakepy"), "FAKE_LOG": str(box / "calls.log"), "FAKE_FAIL": "", "FAKE_PARTIAL": "", "FORCE": "1",
         "MIN_FREE_GB": "1", **env}
    return subprocess.run(["bash", str(box / "deploy" / "historical-run.sh"), *args], cwd=box, env=e, capture_output=True, text=True)


def calls(box):
    p = box / "calls.log"
    return [c for c in p.read_text().splitlines() if not c.endswith("backfill status")] if p.exists() else []         # `status` is not a stage


def test_syntax_and_dry_run_print_every_stage_in_order_and_run_nothing(box):
    assert subprocess.run(["bash", "-n", str(SCRIPT)]).returncode == 0
    out = run(box)
    assert out.returncode == 0 and "DRY RUN" in out.stdout
    stages = [l.split()[2] for l in out.stdout.splitlines() if l.startswith("=== stage")]
    assert stages == ["universe", "daily", "1m", "fx", "fundamentals", "trial-spot", "trial-expired", "spot", "expired"]
    for frag in ("--mode full --market ALL --years 20", "--load ohlc", "scheduler.py --job bars --market ALL --interval 1m --backfill",
                 "data_layer.fx refresh", "yf_fundamentals run", "yf_pillars build", "backfill spot --limit 3", "backfill expired --underlying NIFTY --limit 1",
                 "backfill spot --from 2022-01-01 --to 2026-11-02 --budget-30min 1500 --max-minutes", "backfill expired --from 2022-01-01"):
        assert frag in out.stdout, frag
    assert "--to 2026-11-02 --budget-30min 1500 --dump-raw 2" in out.stdout            # the trial stops before the live feed's first day too
    assert calls(box) == [] and (box / "logs" / "historical-run.state").read_text() == ""


def test_apply_runs_every_stage_records_state_and_a_second_run_does_nothing(box):
    out = run(box, "--apply")
    assert out.returncode == 0 and "ALL SELECTED STAGES DONE" in out.stdout
    first = calls(box)
    assert any("--years 20" in c for c in first) and any("backfill spot --from 2022-01-01" in c for c in first) and any("backfill expired --from" in c for c in first)
    assert any(c.startswith("-s INT") or "yf_fundamentals run" in c for c in first)
    state = (box / "logs" / "historical-run.state").read_text()
    assert state.count("\tok\t") == 9
    again = run(box, "--apply")
    assert again.returncode == 0 and calls(box) == first and "done on an earlier run, skipped" in again.stdout
    redo = run(box, "--apply", "--redo", "daily", "--only", "daily")
    assert redo.returncode == 0 and len(calls(box)) == len(first) + 2 and "(redo daily)" in redo.stdout
    assert "ok" in run(box, "status").stdout and (box / "logs").glob("historical-run-*.log")


def test_a_cut_off_long_stage_is_partial_exit_75_and_the_next_run_continues_it(box):
    out = run(box, "--apply", FAKE_PARTIAL="backfill spot --from")
    assert out.returncode == 75 and "NOT FINISHED" in out.stdout
    st = run(box, "status").stdout
    assert "spot" in [l.split()[0] for l in st.splitlines() if "partial" in l]
    n = len(calls(box))
    out2 = run(box, "--apply")                                                                   # only the partial stage (and what depended on it) runs
    assert out2.returncode == 0 and len(calls(box)) - n <= 2 and any("backfill spot --from" in c for c in calls(box)[n:])


def test_a_failed_trial_blocks_its_long_stage_and_fails_the_run(box):
    out = run(box, "--apply", FAKE_FAIL="backfill expired --underlying")
    assert out.returncode == 1 and "SOME STAGES FAILED" in out.stdout
    assert "trial-expired is not done" in out.stdout and not any("backfill expired --from" in c for c in calls(box))
    assert any("backfill spot --from 2022-01-01" in c for c in calls(box))                          # the stages that do not depend on it still ran
    assert "failed" in [l.split()[1] for l in run(box, "status").stdout.splitlines() if l.startswith("trial-expired")]


def test_fundamentals_cut_off_by_the_time_limit_is_partial_and_skips_the_pillars(box):
    out = run(box, "--apply", "--only", "fundamentals", FAKE_PARTIAL="yf_fundamentals run")
    assert out.returncode == 75
    assert not any("yf_pillars" in c for c in calls(box))
    assert "stopped at the time limit" in out.stdout


def test_missing_python_and_unknown_arguments_stop_with_a_message(box):
    out = run(box, "--apply", PY=str(box / "nope"))
    assert out.returncode == 1 and "not found" in out.stdout
    bad = run(box, "--nonsense")
    assert bad.returncode == 1 and "unknown argument" in bad.stdout


def test_a_stage_that_starts_late_only_gets_the_time_left_until_stop_by():
    """The limit used to be worked out once when the script started, so `expired` after a long `spot` could run for hours past STOP_BY."""
    import re
    src = SCRIPT.read_text()
    assert "LIMIT_MIN" not in src and src.count("$(limit_min)") == 3                         # fundamentals, spot and expired each ask when they start
    fn = re.search(r"left_min\(\) \{.*?\n\}", src, re.S).group(0)
    out = subprocess.run(["bash", "-c", f'{fn}\nSTOP_BY=00:00; left_min'], capture_output=True, text=True).stdout.strip()
    assert out == "0"                                                                          # STOP_BY has passed today: no time left, never tomorrow's
    out = subprocess.run(["bash", "-c", f'{fn}\nSTOP_BY=23:59:59; left_min'], capture_output=True, text=True).stdout.strip()
    assert out.isdigit() and 0 <= int(out) <= 1439
