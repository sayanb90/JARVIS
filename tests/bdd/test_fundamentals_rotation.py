from datetime import date, timedelta

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

scenarios("../features/fundamentals_rotation.feature")
pytestmark = pytest.mark.bdd


class Clock:
    today = date(2026, 10, 1)


class StubFmp:
    """Replaces FMPExtractor: records every (day, ticker) fetched and can fail chosen tickers."""
    log: list[tuple[date, str]] = []
    failing: set[str] = set()
    world = None

    def __init__(self, key):
        pass

    def fetch_ticker(self, t, m):
        if t in StubFmp.failing:
            raise RuntimeError("fmp error")
        StubFmp.log.append((Clock.today, t))
        f = StubFmp.world.fmp
        return {"income": [f.IncomeRow("2025.12.31", t, m, 10, 2, 1)], "balance": [f.BalanceRow("2025.12.31", t, m, 1, 1, 1, 1)],
                "cash_flow": [f.CashFlowRow("2025.12.31", t, m, 3)], "ratios": [f.RatiosRow("2025.12.31", t, m, 0.1, 5)]}


@pytest.fixture(autouse=True)
def fmp_stub(ctx, monkeypatch):
    Clock.today = date(2026, 10, 1)
    StubFmp.log, StubFmp.failing, StubFmp.world = [], set(), ctx.w

    class FakeDate(date):
        @classmethod
        def today(cls):
            return Clock.today
    monkeypatch.setattr(ctx.w.fmp, "date", FakeDate)
    monkeypatch.setattr(ctx.w.fmp, "FMPExtractor", StubFmp)
    monkeypatch.setenv("FMP_API_KEY", "K")


def run_days(ctx, sched, n, market="IN"):
    for _ in range(n):
        Clock.today += timedelta(days=1)
        assert sched.job_fundamentals(market, budget=ctx.budget) in (True, False)


@given(parsers.parse("FMP allows {calls:d} calls a day, which is {n:d} stocks"))
def _budget(ctx, calls, n):
    ctx.budget = calls
    assert calls // 4 == n


@given(parsers.parse("{n:d} stocks were refreshed 30 days ago and the rest 2 days ago"))
def _history(ctx, n):
    syms = ctx.universe["IN"]
    old, recent = Clock.today + timedelta(days=1) - timedelta(days=30), Clock.today + timedelta(days=1) - timedelta(days=2)
    ctx.old = syms[:n]
    lines = ["symbol,market,last_updated"] + [
        f"{s},IN,{(old if s in ctx.old else recent):%Y.%m.%d}" for s in syms]
    (ctx.w.data / "fund_meta.csv").write_text("\n".join(lines) + "\n")


@given(parsers.parse("FMP fails for {sym}"))
def _fails(sym):
    StubFmp.failing.add(sym)


@when("FMP recovers")
def _recovers():
    StubFmp.failing.clear()


@when(parsers.parse("the fundamentals job runs on {n:d} consecutive days"))
@when(parsers.parse("the fundamentals job runs on {n:d} day"))
@when(parsers.parse("the fundamentals job runs on {n:d} days"))
def _run(ctx, sched, n):
    run_days(ctx, sched, n)


@when(parsers.parse("the fundamentals job is asked for market {market}"))
def _market(ctx, sched, market):
    Clock.today += timedelta(days=1)
    sched.job_fundamentals(market, budget=ctx.budget)


@then(parsers.parse("all {n:d} stocks were fetched at least once"))
def _all_fetched(ctx, n):
    assert {t for _, t in StubFmp.log} == set(ctx.universe["IN"]) and len(ctx.universe["IN"]) == n


@then(parsers.parse("only {n:d} fetches happened in total"))
def _total(n):
    assert len(StubFmp.log) == n


@then(parsers.parse("no day used more than {calls:d} FMP calls"))
def _per_day(calls):
    per_day = {}
    for d, _ in StubFmp.log:
        per_day[d] = per_day.get(d, 0) + 4
    assert max(per_day.values()) <= calls


@then(parsers.parse("the lake holds fundamentals for all {n:d} stocks"))
def _lake(ctx, n):
    q = ctx.w.query.Lake()
    assert q.sql("SELECT count(DISTINCT symbol) n FROM income_statement").n[0] == n


@then("the stocks fetched are exactly the 3 that were refreshed 30 days ago")
def _stalest(ctx):
    assert {t for _, t in StubFmp.log} == set(ctx.old)


@then(parsers.parse("{sym} has no refresh date"))
def _no_date(ctx, sym):
    assert sym not in ctx.w.fmp.read_fund_meta()


@then(parsers.parse("{sym} has a refresh date"))
def _has_date(ctx, sym):
    assert sym in ctx.w.fmp.read_fund_meta()


@then(parsers.parse("{sym} was fetched on the second day"))
def _second_day(sym):
    assert (date(2026, 10, 3), sym) in StubFmp.log


@then("every stock fetched is an Indian stock")
def _india(ctx):
    assert StubFmp.log and all(t.endswith(".NS") for _, t in StubFmp.log)
