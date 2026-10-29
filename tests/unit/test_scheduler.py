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


@pytest.mark.parametrize("arg,expected", [("ALL", ("IN", "US", "UK", "EU")), ("UK", ("UK",))])
def test_markets_of(sched, arg, expected):
    assert sched.markets_of(arg) == expected


def test_ohlc_daily_fetches_then_loads_daily_into_lake(sched, monkeypatch):
    calls = []
    monkeypatch.setattr(sched, "run_ohlc_daily", lambda **kw: calls.append(("fetch", kw)))
    monkeypatch.setattr(sched.lake, "load_ohlc", lambda m, daily=False: calls.append(("load", m, daily)))
    assert sched.job_ohlc_daily("ALL", days_back=5) is True
    assert calls[0] == ("fetch", {"market": "ALL", "days_back": 5, "dry_run": False})
    assert calls[1] == ("load", ("IN", "US", "UK", "EU"), True)


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


def test_fundamentals_always_india_only_and_uses_stale_selection(sched, world, monkeypatch):
    world.set_universe({"IN": ["A.NS", "B.NS"], "US": ["AAPL"]})
    fetched = []
    monkeypatch.setattr(sched, "fmp_run", lambda **kw: fetched.append(kw))
    monkeypatch.setattr(sched.lake, "load_fundamentals", lambda m: fetched.append(("load", m)))
    assert sched.job_fundamentals("US", budget=248) is True            # asking for US still means India
    assert fetched[0]["market"] == "IN" and sorted(fetched[0]["tickers"]) == ["A.NS", "B.NS"]
    assert fetched[1] == ("load", ("IN",))


def test_fundamentals_fmp_failure_is_false_but_still_loads_what_exists(sched, world, monkeypatch):
    world.set_universe({"IN": ["A.NS"]})
    monkeypatch.setattr(sched, "fmp_run", lambda **kw: (_ for _ in ()).throw(RuntimeError("429")))
    loaded = []
    monkeypatch.setattr(sched.lake, "load_fundamentals", lambda m: loaded.append(m))
    assert sched.job_fundamentals() is False and loaded


def test_fundamentals_empty_universe_is_a_noop_success(sched, world, monkeypatch):
    (world.data / "universe.csv").write_text("symbol,name,market\n")
    monkeypatch.setattr(sched, "fmp_run", lambda **kw: pytest.fail("nothing to refresh"))
    assert sched.job_fundamentals() is True


def test_bars_job_is_india_only_and_passes_backfill(sched, monkeypatch):
    seen = {}
    monkeypatch.setattr(sched, "run_bars", lambda **kw: seen.update(kw))
    monkeypatch.setattr(sched.lake, "load_bars", lambda iv, markets: seen.update(load=(iv, markets)))
    assert sched.job_bars("IN", "5m", backfill=True) is True
    assert seen["backfill"] is True and seen["load"] == ("5m", ("IN",))


def test_bars_job_for_us_fails_cleanly(sched, world):
    world.set_universe({"IN": ["A.NS"]})
    assert sched.job_bars("US") is False


def test_job_all_runs_both_and_ands_the_results(sched, monkeypatch):
    monkeypatch.setattr(sched, "job_ohlc_daily", lambda **kw: True)
    monkeypatch.setattr(sched, "job_fundamentals", lambda **kw: False)
    assert sched.job_all() is False


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
