import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from tests.helpers import seed_scoring, write_csv

scenarios("../features/pillar_scoring.feature")
pytestmark = pytest.mark.bdd

WEAKNESS = {
    "none": {},
    "five-year average P/E is too high": {"avg_ni": 1},
    "return on capital is below 9 percent": {"roic": 0.05},
    "revenue shrank over the period": {"revenue": (90, 100)},
    "net income shrank over the period": {"net_income": (20, 30)},
    "the share count increased": {"shares": (110, 100), "pm_shares": 90},
    "long-term debt is over 5 times FCF": {"ltd": 1000},
    "free cash flow shrank over the period": {"fcf": (10, 20)},
    "price is just above 20 times FCF": {},
}


@given(parsers.parse("a stock that passes all eight pillars at a price of {price:f}"))
def _stock(ctx, price):
    ctx.price = price
    ctx.q = seed_scoring(ctx.w, price=price)


@given(parsers.parse('its weakness is "{weakness}"'))
def _weakness(ctx, weakness):
    ctx.q = seed_scoring(ctx.w, price=ctx.price, **WEAKNESS[weakness])


@given("a second stock with fundamentals but no price")
def _unpriced(ctx):
    write_csv(ctx.w.data / "fundamentals" / "pillar_metrics_in.csv",
              ["symbol", "market", "avg_net_income_5y", "avg_fcf_5y", "latest_revenue", "latest_ebit",
               "latest_total_assets", "latest_total_liabilities", "latest_long_term_debt",
               "latest_total_equity", "latest_roic", "latest_shares_outstanding"],
              [["UNPRICED.NS", "IN", 45, 45, 200, 1, 1000, 400, 50, 600, 0.15, 90]])
    ctx.w.lake.load_fundamentals(("IN",))


@when("I score the stock")
def _score(ctx):
    ctx.score = ctx.w.query.Lake().score_ticker("GOOD.NS", ctx.price)


@when("I score a stock that is not in the lake")
def _score_missing(ctx):
    ctx.score = ctx.w.query.Lake().score_ticker("NOT-THERE.NS", ctx.price)


@when("I screen the India universe")
def _screen(ctx):
    ctx.ranking = ctx.w.query.Lake().score_all("IN")


@then(parsers.parse("its score is {n:d}"))
def _is(ctx, n):
    assert ctx.score == n


@then("the ranking is GOOD.NS with 8 then UNPRICED.NS with 0")
def _ranking(ctx):
    assert list(zip(ctx.ranking.symbol, ctx.ranking.score)) == [("GOOD.NS", 8), ("UNPRICED.NS", 0)]
