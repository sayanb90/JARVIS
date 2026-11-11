"""Scheduler: lock, market scoping, each job's success/failure paths, CLI exit codes."""
import sys

import pytest


def test_lock_blocks_second_instance_and_cleans_up(sched):
    with sched.SchedulerLock():
        assert sched.LOCK_FILE.exists()
        with pytest.raises(SystemExit) as e:
            with sched.SchedulerLock():
                pass
        assert e.value.code == 2
        assert sched.LOCK_FILE.exists()               # the failed second attempt must not delete the live lock
    assert not sched.LOCK_FILE.exists()


def test_lock_released_even_if_job_raises(sched):
    with pytest.raises(RuntimeError):
        with sched.SchedulerLock():
            raise RuntimeError("job blew up")
    assert not sched.LOCK_FILE.exists()


@pytest.mark.parametrize("arg,expected", [("ALL", ("IN", "US", "UK", "EU", "JP", "KR", "TW", "SG", "CN", "HK")), ("UK", ("UK",))])
def test_markets_of(sched, arg, expected):
    assert sched.markets_of(arg) == expected


def test_ohlc_daily_fetches_then_loads_daily_into_lake(sched, monkeypatch):
    calls = []
    monkeypatch.setattr(sched, "run_ohlc_daily", lambda **kw: calls.append(("fetch", kw)))
    monkeypatch.setattr(sched.lake, "load_ohlc", lambda m, daily=False: calls.append(("load", m, daily)))
    assert sched.job_ohlc_daily("ALL", days_back=5) is True
    assert calls[0] == ("fetch", {"market": "ALL", "days_back": 5, "dry_run": False})
    assert calls[1] == ("load", ("IN", "US", "UK", "EU", "JP", "KR", "TW", "SG", "CN", "HK"), True)


def test_ohlc_daily_fetch_failure_returns_false_and_skips_load(sched, monkeypatch):
    monkeypatch.setattr(sched, "run_ohlc_daily", lambda **kw: (_ for _ in ()).throw(RuntimeError("x")))
    monkeypatch.setattr(sched.lake, "load_ohlc", lambda *a, **k: pytest.fail("must not load after a failed fetch"))
    assert sched.job_ohlc_daily() is False


def test_ohlc_daily_dry_run_skips_load(sched, monkeypatch):
    monkeypatch.setattr(sched, "run_ohlc_daily", lambda **kw: None)
    monkeypatch.setattr(sched.lake, "load_ohlc", lambda *a, **k: pytest.fail("dry run loads nothing"))
    assert sched.job_ohlc_daily(dry_run=True) is True


def test_load_failure_is_reported_false(sched, monkeypatch):
    monkeypatch.setattr(sched, "run_ohlc_daily", lambda **kw: None)
    monkeypatch.setattr(sched.lake, "load_ohlc", lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")))
    assert sched.job_ohlc_daily() is False


def test_bars_job_passes_backfill_and_loads_the_whole_window(sched, monkeypatch):
    seen = {}
    monkeypatch.setattr(sched, "run_bars", lambda **kw: seen.update(kw))
    monkeypatch.setattr(sched.lake, "load_bars", lambda iv, since, markets: seen.update(load=(iv, since, markets)))
    assert sched.job_bars("IN", "5m", backfill=True) is True
    assert seen["backfill"] is True and seen["load"][0] == "5m" and seen["load"][2] == ("IN",)
    assert (sched.date.today() - seen["load"][1]).days == 61                  # 60 days of 5m files, not just the last 7


def test_bars_job_for_all_means_every_market_but_india_and_loads_the_last_week(sched, monkeypatch):
    seen = {}
    monkeypatch.setattr(sched, "run_bars", lambda **kw: seen.update(kw))
    monkeypatch.setattr(sched.lake, "load_bars", lambda iv, since, markets: seen.update(load=(iv, since, markets)))
    assert sched.job_bars("ALL", "1m") is True
    assert seen["market"] == "ALL" and seen["load"] == ("1m", None, ("US", "UK", "EU", "JP", "KR", "TW", "SG", "CN", "HK"))


def test_a_failed_market_is_reported_but_the_others_are_still_loaded(sched, monkeypatch):
    loaded = []
    monkeypatch.setattr(sched, "run_bars", lambda **kw: (_ for _ in ()).throw(RuntimeError("bars 1m failed for market(s): UK")))
    monkeypatch.setattr(sched.lake, "load_bars", lambda iv, since, markets: loaded.append(markets))
    assert sched.job_bars("ALL", "1m") is False and loaded == [("US", "UK", "EU", "JP", "KR", "TW", "SG", "CN", "HK")]


def test_main_exit_codes_and_dry_run(sched, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["scheduler.py", "--job", "ohlc-daily", "--dry-run"])
    monkeypatch.setattr(sched, "run_ohlc_daily", lambda **kw: None)
    assert sched.main() == 0
    monkeypatch.setattr(sys, "argv", ["scheduler.py", "--job", "ohlc-daily"])
    monkeypatch.setattr(sched, "job_ohlc_daily", lambda **kw: False)
    assert sched.main() == 1
    assert not sched.LOCK_FILE.exists()


def test_main_rejects_unknown_job(sched, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["scheduler.py", "--job", "nope"])
    with pytest.raises(SystemExit) as e:
        sched.main()
    assert e.value.code == 2
