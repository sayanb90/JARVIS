"""Setting .env keys without printing values (SCRUM-109)."""
from __future__ import annotations

import stat
from datetime import datetime

import pytest

from data_layer import envfile


def test_replaces_existing_keys_appends_new_ones_and_keeps_everything_else(tmp_path):
    f = tmp_path / ".env"
    f.write_text("# settings\nUPSTOX_TOKEN=secret\nJARVIS_GREEKS=all   \n\nOTHER=1\n")
    f.chmod(0o600)
    keys, saved = envfile.set_keys(f, {"JARVIS_GREEKS": "core", "JARVIS_GREEKS_RATE": "0.065"}, now=datetime(2026, 11, 3, 19, 0, 0))
    assert keys == ["JARVIS_GREEKS", "JARVIS_GREEKS_RATE"]
    assert f.read_text() == "# settings\nUPSTOX_TOKEN=secret\nJARVIS_GREEKS=core\n\nOTHER=1\nJARVIS_GREEKS_RATE=0.065\n"
    assert saved.name == ".env.bak-20261103-190000" and "JARVIS_GREEKS=all" in saved.read_text()      # the original, untouched
    assert stat.S_IMODE(f.stat().st_mode) == 0o600 and stat.S_IMODE(saved.stat().st_mode) == 0o600


def test_a_commented_key_is_not_replaced_and_a_missing_file_is_created_private(tmp_path):
    f = tmp_path / ".env"
    f.write_text("# JARVIS_GREEKS=all\n")
    envfile.set_keys(f, {"JARVIS_GREEKS": "core"}, backup=False)
    assert f.read_text() == "# JARVIS_GREEKS=all\nJARVIS_GREEKS=core\n"
    g = tmp_path / "new.env"
    assert envfile.set_keys(g, {"A": "1"}) == (["A"], None) and stat.S_IMODE(g.stat().st_mode) == 0o600


def test_a_key_given_twice_is_set_once_and_bad_names_are_refused(tmp_path):
    f = tmp_path / ".env"
    f.write_text("A=1\nA=2\n")
    envfile.set_keys(f, {"A": "9"}, backup=False)
    assert f.read_text() == "A=9\nA=2\n"                                    # the first line is the one a dotenv loader would use last... only it changes
    with pytest.raises(ValueError):
        envfile.set_keys(f, {"bad key": "1"})


def test_command_line_prints_key_names_only(tmp_path, capsys):
    f = tmp_path / ".env"
    f.write_text("TOKEN=hunter2\n")
    assert envfile.main(["set", "JARVIS_GREEKS=core", "SECRET_THING=do-not-print", "--file", str(f), "--no-backup"]) == 0
    out = capsys.readouterr().out
    assert "JARVIS_GREEKS" in out and "SECRET_THING" in out and "do-not-print" not in out and "hunter2" not in out and "core" not in out
    with pytest.raises(SystemExit):
        envfile.main(["set", "NOEQUALS", "--file", str(f)])
