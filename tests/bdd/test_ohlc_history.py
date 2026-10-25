import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from data_layer import verify

scenarios("../features/ohlc_history.feature")
pytestmark = pytest.mark.bdd


def count_rows(ctx):
    return ctx.w.query.Lake().sql("SELECT count(*) n FROM ohlc").n[0]


@given(parsers.parse("Yahoo fails for the batch containing {sym}"))
def _fails(ctx, yahoo, monkeypatch, sym):
    monkeypatch.setattr(ctx.w.yf, "CHUNK_SIZE", 2)
    yahoo.fail_for.add(sym)


@given(parsers.parse("Yahoo has no data for {sym}"))
def _unknown(yahoo, sym):
    yahoo.unknown.add(sym)


@given(parsers.parse("Yahoo now reports a close of {px:f} for {sym} on its latest day"))
def _correction(yahoo, px, sym):
    yahoo.corrections[sym] = px


@given("I have run the full OHLC backfill for all markets and loaded the lake")
@when("I run the full OHLC backfill for all markets and load the lake")
def _backfill(ctx, yahoo):
    ctx.w.yf.run_ohlc("ALL")
    ctx.w.lake.load_ohlc()


@when("I run the daily OHLC update and load the lake")
def _daily(ctx, yahoo):
    if ctx.rowcount is None and (ctx.w.lakedir / "ohlc").exists():
        ctx.rowcount = count_rows(ctx)
    ctx.w.yf.run_ohlc_daily("ALL")
    ctx.w.lake.load_ohlc(daily=True)


@then(parsers.parse("every symbol has at least {n:d} daily rows"))
def _min_rows(ctx, n):
    per = ctx.w.query.Lake().sql("SELECT symbol, count(*) n FROM ohlc GROUP BY 1")
    assert len(per) == sum(len(v) for v in ctx.universe.values()) and per.n.min() >= n


@then("the OHLC verification reports no failures")
def _verify_ok(ctx, capsys):
    rep = verify.verify_ohlc(ctx.w.query.Lake(), years=5)
    assert rep.fails == 0, capsys.readouterr().out


@then(parsers.parse("the OHLC verification fails because {sym} has no data"))
def _verify_fail(ctx, capsys, sym):
    rep = verify.verify_ohlc(ctx.w.query.Lake(), years=5)
    out = capsys.readouterr().out
    assert rep.fails >= 1
    assert any("[FAIL]" in ln and "symbols with no data" in ln and sym in ln for ln in out.splitlines()), out


@then("the other symbols still have their full history")
def _others(ctx):
    per = ctx.w.query.Lake().sql("SELECT symbol, count(*) n FROM ohlc GROUP BY 1")
    assert (per.n >= 1200).all() and "US1" not in set(per.symbol)


@then("the number of OHLC rows is unchanged")
def _unchanged(ctx):
    if ctx.rowcount is None:
        pytest.fail("no earlier row count recorded")
    assert count_rows(ctx) == ctx.rowcount


@then(parsers.parse("the latest stored close for {sym} is {px:f}"))
def _latest_close(ctx, sym, px):
    assert ctx.w.query.Lake().latest_price(sym) == px
