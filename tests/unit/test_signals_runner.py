"""The signal runner end to end on a lake-shaped directory (SCRUM-146, 173): reader, two passes, store, trial log, IV gate."""

from datetime import date
from pathlib import Path

import pytest

from signals.__main__ import main, run, validate_iv
from signals.framework import read_output, read_trials, specs
from signals.lakeio import load_raw
from tests.signals_helpers import write_fake_lake


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    d = tmp_path_factory.mktemp("lake")
    write_fake_lake(d)
    return d


def test_reader(lake):
    raw = load_raw(lake, "NIFTY", date(2026, 3, 1), date(2026, 3, 31), option_from=date(2026, 3, 20))
    assert len(raw["index_1m"]) and len(raw["vix_1m"]) and len(raw["fut_1m"]) and len(raw["opt_1m"])
    assert raw["opt_1m"]["ts"].min().date() >= date(2026, 3, 20)
    assert len(raw["ticks_fo"]) and len(raw["contracts"]) and len(raw["participant"])
    assert raw["opt_1m"]["symbol"].str.match(r"^NIFTY_\d").all()


def test_run_writes_every_signal(lake, tmp_path):
    stats = run(lake, tmp_path, "NIFTY", date(2026, 3, 1), date(2026, 3, 31), log=lambda *a: None)
    assert set(stats) == {s.id for s in specs()}
    assert len(read_output(tmp_path, "vrp", "NIFTY")) > 0
    assert len(read_output(tmp_path, "gex", "NIFTY")) > 0
    assert len(read_output(tmp_path, "buy_permission", "NIFTY")) > 0
    assert (tmp_path / "derived" / "atm").is_dir() and (tmp_path / "derived" / "option_chain").is_dir()
    assert len(read_trials(tmp_path)) == len(specs())


def test_store_inside_lake_is_refused(lake):
    with pytest.raises(SystemExit):
        run(lake, lake / "store", "NIFTY", date(2026, 3, 1), date(2026, 3, 31), log=lambda *a: None)


def test_validate_iv(lake, tmp_path):
    res = validate_iv(lake, tmp_path, "NIFTY", date(2026, 3, 27), date(2026, 3, 31))
    assert res["pass"] and res["n"] > 1000
    assert any(Path(tmp_path, "reports").glob("iv_gate_NIFTY_*.json"))


def test_cli_list(capsys):
    assert main(["list"]) == 0
    out = capsys.readouterr().out
    assert "vrp" in out and "rr25" in out
