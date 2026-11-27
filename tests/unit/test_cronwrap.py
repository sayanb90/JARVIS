"""Keeping the installed crontab tracked and in step with deploy/crontab (SCRUM-219): `jobrun wrap`, `jobrun check`, the health line."""

from pathlib import Path

import pytest

from data_layer import cronwrap, jobrun
from data_layer.jobrun import parse_crontab

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "crontab_vps_20261121.txt"       # the VPS's installed crontab, from the 21 Nov audit


def template():
    return cronwrap.template_text()


def test_every_job_of_deploy_crontab_is_wrapped_with_a_unique_name():
    """A job added to the template must be wrapped, or the Jobs tab cannot see it (the rule in AGENTS.md)."""
    jobs = parse_crontab(template())
    assert jobs and all(j.wrapped for j in jobs), [j.name for j in jobs if not j.wrapped]
    names = [j.name for j in jobs]
    assert len(names) == len(set(names))


def test_the_vps_crontab_is_wrapped_with_the_templates_names_and_nothing_else_changes():
    text = FIXTURE.read_text()
    out, rep = cronwrap.wrap(text, template())
    before, after = parse_crontab(text), parse_crontab(out)
    assert len(before) == len(after) == 33 and all(j.wrapped for j in after) and not any(j.wrapped for j in before)
    assert {j.name for j in after} <= {j.name for j in parse_crontab(template())}                  # the template's names
    assert rep.renamed["fo-feed"] == "fo-feed-1" and rep.renamed["scheduler-2"] == "bars-1m" and rep.unmatched == []
    for a, b in zip(text.splitlines(), out.splitlines()):                                          # comments and variables byte for byte
        if not a.strip() or a.lstrip().startswith("#") or "=" in a.split()[0]:
            assert a == b
    for j0, j1 in zip(before, after):                                                              # same schedule, same command inside the wrapper
        assert j0.schedule_text == j1.schedule_text and j0.log == j1.log
        assert cronwrap._inner(j0.command) == cronwrap._inner(j1.command)
    assert cronwrap.wrap(out, template())[0] == out                                                # a second run changes nothing
    assert set(rep.missing) == {j.name for j in parse_crontab(template())} - {j.name for j in after}


def test_add_missing_appends_the_templates_jobs_and_template_schedules_takes_its_times():
    text = FIXTURE.read_text().replace("30 13 * * 1-5  cd $JARVIS && JARVIS_SIGNAL_STORE", "45 13 * * 1-5  cd $JARVIS && JARVIS_SIGNAL_STORE")
    out, rep = cronwrap.wrap(text, template(), add_missing=True)
    names = [j.name for j in parse_crontab(out)]
    assert rep.added and set(rep.added) <= set(names) and len(names) == 33 + len(rep.added)
    assert ("signals-viewer-warm", "45 13 * * 1-5", "30 13 * * 1-5") in rep.schedules
    out2, rep2 = cronwrap.wrap(text, template(), template_schedules=True)
    warm = next(j for j in parse_crontab(out2) if j.name == "signals-viewer-warm")
    assert warm.schedule_text == "30 13 * * 1-5" and rep2.schedules_taken == ["signals-viewer-warm"]


def test_a_command_with_single_quotes_survives_the_wrapper():
    t = "0 1 * * * cd $JARVIS && python -m x --label 'a b' >> logs/x.log 2>&1\n"
    out, _ = cronwrap.wrap(t, t)
    j = parse_crontab(out)[0]
    assert j.wrapped and cronwrap._inner(j.command) == "python -m x --label 'a b' >> logs/x.log 2>&1"
    assert "--shell 'python -m x --label '\\''a b'\\'' >> logs/x.log 2>&1'" in out


def test_a_line_without_cd_is_left_and_reported():
    out, rep = cronwrap.wrap("0 1 * * * /usr/bin/true >> /tmp/x.log\n", "")
    assert rep.unwrappable == [1] and "jobrun" not in out


def test_check_reports_drift_and_the_command_line(tmp_path, capsys):
    rep = cronwrap.check(FIXTURE.read_text(), template())
    assert rep.drift and len(rep.unwrapped_left) == 33 and rep.missing
    good, _ = cronwrap.wrap(FIXTURE.read_text(), template(), add_missing=True, template_schedules=True)
    assert not cronwrap.check(good, template()).drift
    f = tmp_path / "c.txt"
    f.write_text(FIXTURE.read_text())
    assert jobrun.main(["check", "--file", str(f)]) == 1
    assert "NOT WRAPPED 33" in capsys.readouterr().out
    assert jobrun.main(["wrap", "--file", str(f), "--add-missing"]) == 0
    printed = capsys.readouterr()
    assert all(j.wrapped for j in parse_crontab(printed.out)) and "wrapped 33" in printed.err
    f.write_text(good)
    assert jobrun.main(["check", "--file", str(f)]) == 0


def test_health_prints_one_crontab_line(tmp_path, monkeypatch, capsys):
    """The daily health report warns on drift, and passes once the installed crontab is wrapped and complete."""
    import shutil
    from datetime import datetime
    from data_layer import health
    (tmp_path / "deploy").mkdir()
    shutil.copy(Path(cronwrap.__file__).resolve().parents[1] / "deploy" / "crontab", tmp_path / "deploy" / "crontab")
    (tmp_path / "lake").mkdir()
    monkeypatch.setattr(jobrun, "installed_crontab", lambda run=None: FIXTURE.read_text())
    health.check(datetime(2026, 11, 28, 4, 0), lake_dir=tmp_path / "lake", root=tmp_path, backup_path=tmp_path / "bk")
    out = capsys.readouterr().out
    line = next(x for x in out.splitlines() if "crontab:" in x)
    assert "WARN" in line and "NOT WRAPPED 33" in line and "jobrun wrap --add-missing" in line
    good, _ = cronwrap.wrap(FIXTURE.read_text(), template(), add_missing=True, template_schedules=True)
    monkeypatch.setattr(jobrun, "installed_crontab", lambda run=None: good)
    health.check(datetime(2026, 11, 28, 4, 0), lake_dir=tmp_path / "lake", root=tmp_path, backup_path=tmp_path / "bk")
    line = next(x for x in capsys.readouterr().out.splitlines() if "crontab:" in x)
    assert "PASS" in line
