import sys

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

scenarios("../features/scheduler_ops.feature")
pytestmark = pytest.mark.bdd


def run_cron(ctx, sched, monkeypatch, *argv):
    monkeypatch.setattr(sys, "argv", ["scheduler.py", *argv])
    ctx.exit = None
    try:
        ctx.code = sched.main()
    except SystemExit as e:
        ctx.exit, ctx.code = e.code, None


@given("Yahoo would fail the test if it were called")
def _no_yahoo(ctx, monkeypatch):
    def boom(*a, **k):
        raise AssertionError("Yahoo must not be called in a dry run")
    monkeypatch.setattr(ctx.w.yf, "_batch_download", boom)


@given("Yahoo is down")
def _down(ctx, monkeypatch):
    monkeypatch.setattr(ctx.w.yf, "_batch_download", lambda c, **kw: {})


@given("another scheduler run is in progress")
def _locked(sched):
    sched.LOCK_FILE.write_text("99999")


@when("cron runs the daily OHLC job")
def _daily(ctx, sched, monkeypatch):
    run_cron(ctx, sched, monkeypatch, "--job", "ohlc-daily")


@when("cron runs all jobs as a dry run")
def _dry(ctx, sched, monkeypatch):
    run_cron(ctx, sched, monkeypatch, "--job", "all", "--dry-run")


@when(parsers.parse("cron runs the bars job for market {market}"))
def _bars(ctx, sched, monkeypatch, market):
    run_cron(ctx, sched, monkeypatch, "--job", "bars", "--market", market)


@then("the job succeeds")
def _ok(ctx):
    assert ctx.code == 0


@then("the job fails")
def _fail(ctx):
    assert ctx.code == 1


@then("no data files were written")
def _nothing(ctx):
    assert not list((ctx.w.data / "ohlc").glob("*.csv")) and not list((ctx.w.data / "fundamentals").glob("*.csv"))
    assert not ctx.w.lakedir.exists() or not list(ctx.w.lakedir.rglob("*.parquet"))


@then("the second run refuses to start")
def _refused(ctx):
    assert ctx.exit == 2 and ctx.code is None


@then("the first run's lock is left in place")
def _lock_kept(sched):
    assert sched.LOCK_FILE.read_text() == "99999"


@then("the scheduler lock is released")
def _released(sched):
    assert not sched.LOCK_FILE.exists()


@then("no bars are stored")
def _no_bars(ctx):
    assert not list(ctx.w.lakedir.rglob("bars*.parquet")) if ctx.w.lakedir.exists() else True
