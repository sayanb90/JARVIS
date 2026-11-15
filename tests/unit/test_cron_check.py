"""Scheduler check (SCRUM-112): expected jobs present or missing, when they fire, and the exit code.  No cron, no systemd."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from data_layer import cron_check as cc

TEMPLATE_TEXT = Path(cc.TEMPLATE).read_text()
MON = datetime(2026, 11, 2, 4, 0)                 # Monday 2026-11-02, 04:00 UTC


def test_the_tracked_template_holds_the_production_and_fundamentals_jobs():
    keys = " ".join(e.key for e in cc.parse_crontab(TEMPLATE_TEXT))
    for part in ("fo_instruments import --download", "fo_instruments select --watchlist data/fo_watchlist.csv",
                 "data_layer.upstox.feed --once --segment fo --shard 1/2", "data_layer.upstox.feed --once --segment fo --shard 2/2", "fundamentals run --set daily", "fundamentals run --set full --max-age-days 20"):
        assert part in keys, part


def test_env_lines_comments_and_blank_lines_are_ignored_and_paths_do_not_matter():
    mine = cc.parse_crontab("SHELL=/bin/bash\n# note\n\n40 3 * * 1-5  cd $JARVIS && python -m data_layer.upstox.feed --once --segment fo >> logs/fo-feed.log 2>&1\n")
    theirs = cc.parse_crontab("MAILTO=x\n40 3 * * 1-5 cd /home/tradestone/JARVIS && /home/tradestone/JARVIS/.venv/bin/python3 -m data_layer.upstox.feed --once --segment fo >> /tmp/x.log 2>&1\n")
    assert len(mine) == len(theirs) == 1 and mine[0].key == theirs[0].key
    assert cc.compare(mine, theirs) == {"present": mine, "missing": [], "extra": []}
    other = cc.parse_crontab("45 3 * * 1-5 cd $JARVIS && python -m data_layer.upstox.feed --once --segment fo\n")
    assert cc.compare(mine, other)["missing"] == mine and cc.compare(mine, other)["extra"] == other      # a different time is a different job


def test_schedule_firing_times_and_next_run():
    wk = cc.Schedule("25 3 * * 1-5")
    assert wk.fires_on(date(2026, 11, 3)) == ["03:25"] and wk.fires_on(date(2026, 11, 7)) == []        # Tuesday yes, Saturday no
    assert wk.next_run(MON) == datetime(2026, 11, 3, 3, 25)
    assert cc.Schedule("0 13 * * 0").next_run(MON) == datetime(2026, 11, 8, 13, 0)                    # Sunday
    assert cc.Schedule("*/15 9-10 * * *").fires_on(date(2026, 11, 3)) == ["09:00", "09:15", "09:30", "09:45", "10:00", "10:15", "10:30", "10:45"]
    assert cc.Schedule("50 12 * * 1-5").fires_on(date(2026, 11, 3)) == ["12:50"]
    assert cc.Schedule("0 0 1 * 5").matches(datetime(2026, 11, 6, 0, 0)) and cc.Schedule("0 0 1 * 5").matches(datetime(2026, 11, 1, 0, 0))   # dom OR dow
    with pytest.raises(ValueError):
        cc.Schedule("* * *")


def test_report_lists_fundamentals_for_tomorrow_and_flags_what_is_missing():
    expected = cc.parse_crontab(TEMPLATE_TEXT)
    ok_text, ok = cc.report(expected, expected, date(2026, 11, 3), MON, True, "active", {"jarvis-feed": "active", "jarvis-viewer": "active"})
    assert ok and "OK: every expected job is installed." in ok_text
    assert "12:50" in ok_text and "upstox.fundamentals run --set daily" in ok_text and "03:40" in ok_text        # fires tomorrow
    assert "Sun 11-08 13:00 / 18:30" in ok_text                                                                # weekly pass, next run in UTC and IST
    partial = [e for e in expected if "upstox.fundamentals" not in e.command]
    text, ok = cc.report(expected, partial + cc.parse_crontab("0 3 * * * python -m data_layer.upstox.probe run\n"), date(2026, 11, 3), MON, True, "active", {})
    assert not ok and text.count("MISSING") == 2 and "2 expected job(s) missing" in text and "upstox.probe" in text        # the extra job is shown
    _, ok = cc.report(expected, expected, date(2026, 11, 3), MON, False, "active", {})
    assert not ok
    _, ok = cc.report(expected, expected, date(2026, 11, 3), MON, True, "not running", {})
    assert not ok


def test_command_line_exit_codes(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cc, "clock_is_utc", lambda now=None: True)
    full = tmp_path / "full"
    full.write_text(TEMPLATE_TEXT)
    assert cc.main(["--crontab-file", str(full), "--now", "2026-11-02T04:00", "--no-services"]) == 0
    out = capsys.readouterr().out
    assert "Scheduler check for 2026-11-03" in out and "fundamentals" in out
    lacking = tmp_path / "lacking"
    lacking.write_text("\n".join(ln for ln in TEMPLATE_TEXT.splitlines() if "fundamentals" not in ln))
    assert cc.main(["--crontab-file", str(lacking), "--now", "2026-11-02T04:00", "--no-services"]) == 1
    assert "MISSING" in capsys.readouterr().out
    monkeypatch.setattr(cc, "installed_crontab", lambda: None)
    assert cc.main(["--now", "2026-11-02T04:00", "--no-services"]) == 1 and "No crontab installed" in capsys.readouterr().out


def test_services_installed_for_a_user_are_found(monkeypatch):
    calls = []

    def fake(cmd):
        calls.append(cmd)
        return "active" if "--user" in cmd else "inactive"
    monkeypatch.setattr(cc, "_run", fake)
    assert cc.service_state("jarvis-feed") == "active (user)"
    assert ["systemctl", "--user", "is-active", "jarvis-feed"] in calls
    assert cc.service_state("jarvis-feed", user_units=False) == "inactive"                  # the cron daemon is a system service
    monkeypatch.setattr(cc, "_run", lambda cmd: "active" if "--user" not in cmd else "inactive")
    assert cc.service_state("jarvis-feed") == "active"                                      # a system unit needs no second look
    monkeypatch.setattr(cc, "_run", lambda cmd: "inactive")
    assert cc.service_state("jarvis-feed") == "inactive"
    text, ok = cc.report(cc.parse_crontab(TEMPLATE_TEXT), cc.parse_crontab(TEMPLATE_TEXT), date(2026, 11, 3), MON, True, "active",
                         {"jarvis-feed": "active (user)", "jarvis-viewer": "active (user)"})
    assert ok and "jarvis-feed: active (user)" in text


def test_system_state_helpers_degrade_without_systemd(monkeypatch):
    monkeypatch.setattr(cc, "_run", lambda cmd: None)
    assert cc.service_state("jarvis-feed").startswith("unknown") and cc.cron_daemon_state().startswith("unknown")
    monkeypatch.setattr(cc, "_run", lambda cmd: "active" if cmd[-1] == "cron" else "inactive")
    assert cc.cron_daemon_state() == "active"
    monkeypatch.setattr(cc, "_run", lambda cmd: "inactive")
    assert cc.cron_daemon_state() == "not running"
    assert cc.clock_is_utc(datetime(2026, 1, 1, tzinfo=timezone.utc)) and not cc.clock_is_utc(datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=5, minutes=30))))
