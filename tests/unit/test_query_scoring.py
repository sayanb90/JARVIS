"""Lake query helpers and the 8-pillar scorer (each pillar broken in isolation)."""
import pytest

from tests.helpers import OHLC_H, seed_scoring, write_csv

# (case id, overrides, price, expected score).  Base case is 8/8 at price 9.
CASES = [
    ("all pillars pass", {}, 9.0, 8),
    ("P1 5y avg P/E too high", {"avg_ni": 1}, 9.0, 7),
    ("P2 ROIC under 9%", {"roic": 0.05}, 9.0, 7),
    ("P3 revenue shrank", {"revenue": (90, 100)}, 9.0, 7),
    ("P4 net income shrank", {"net_income": (20, 30)}, 9.0, 7),
    ("P5 shares increased", {"shares": (110, 100), "pm_shares": 90}, 9.0, 7),
    ("P6 LT debt over 5x FCF", {"ltd": 1000}, 9.0, 7),
    ("P7 FCF shrank", {"fcf": (10, 20)}, 9.0, 7),
    ("P8 P/FCF just over 20", {}, 10.5, 7),
    ("P1+P8 expensive stock", {}, 20.0, 6),
]


@pytest.mark.parametrize("name,over,price,expected", CASES, ids=[c[0] for c in CASES])
def test_each_pillar_in_isolation(world, name, over, price, expected):
    q = seed_scoring(world, price=price, **over)
    assert q.score_ticker("GOOD.NS", price) == expected


def test_pe_and_pfcf_boundaries_are_strict(world):
    q = seed_scoring(world)
    # mktcap = price * 90; P/FCF = mktcap / 45 -> price 10 gives exactly 20, which fails "< 20"
    assert q.score_ticker("GOOD.NS", 9.99) == 8 and q.score_ticker("GOOD.NS", 10.0) == 7


def test_roic_boundary_is_strict(world):
    q = seed_scoring(world, roic=0.09)
    assert q.score_ticker("GOOD.NS", 9.0) == 7


def test_unknown_ticker_missing_price_and_zero_fcf_do_not_crash(world):
    q = seed_scoring(world, avg_fcf=0, avg_ni=-5)
    assert q.score_ticker("NOPE.NS", 9.0) == 0
    assert q.score_ticker("GOOD.NS", None) == 0
    assert q.score_ticker("GOOD.NS", 9.0) <= 6             # non-positive averages can never pass P1/P6/P8


def test_growth_pillars_need_two_data_points(world):
    q = seed_scoring(world, single_year=True)
    assert q.score_ticker("GOOD.NS", 9.0) == 6              # P3 and P4 cannot be shown with one year


def test_score_all_sorts_desc_and_unpriced_score_zero(world):
    q = seed_scoring(world, "GOOD.NS")
    write_csv(world.data / "fundamentals" / "pillar_metrics_in.csv",
              ["symbol", "market", "avg_net_income_5y", "avg_fcf_5y", "latest_revenue", "latest_ebit",
               "latest_total_assets", "latest_total_liabilities", "latest_long_term_debt",
               "latest_total_equity", "latest_roic", "latest_shares_outstanding"],
              [["GOOD.NS", "IN", 45, 45, 200, 1, 1000, 400, 50, 600, 0.15, 90],
               ["UNPRICED.NS", "IN", 45, 45, 200, 1, 1000, 400, 50, 600, 0.15, 90]])
    world.lake.load_fundamentals(("IN",))
    from data_layer.query import Lake
    df = Lake().score_all("IN")
    assert df.symbol.tolist() == ["GOOD.NS", "UNPRICED.NS"] and df.score.tolist() == [8, 0]
    assert Lake().top_by_score("IN", 1).symbol.tolist() == ["GOOD.NS"]


# -- price / bar / fundamentals accessors ---------------------------------------------------
@pytest.fixture()
def lake_with_prices(world):
    rows = [["2026.10.20", "A.NS", "IN", 1, 1, 1, 10, 5], ["2026.10.21", "A.NS", "IN", 1, 1, 1, 11, 5],
            ["2026.10.21", "B.NS", "IN", 1, 1, 1, 20, 5], ["2026.10.21", "AAPL", "US", 1, 1, 1, 30, 5]]
    write_csv(world.data / "ohlc" / "ohlc_2026_in.csv", OHLC_H, [r for r in rows if r[2] == "IN"])
    write_csv(world.data / "ohlc" / "ohlc_2026_us.csv", OHLC_H, [r for r in rows if r[2] == "US"])
    world.lake.load_ohlc()
    return world.query.Lake()


def test_latest_prices_per_symbol_and_market_filter(lake_with_prices):
    q = lake_with_prices
    assert dict(zip(q.latest_prices("IN").symbol, q.latest_prices("IN").close)) == {"A.NS": 11.0, "B.NS": 20.0}
    assert set(q.latest_prices().symbol) == {"A.NS", "B.NS", "AAPL"}
    assert q.latest_price("A.NS") == 11.0 and q.latest_price("ZZZ") is None


def test_ohlc_range_inclusive_and_market_query(lake_with_prices):
    q = lake_with_prices
    assert q.ohlc("A.NS").close.tolist() == [10.0, 11.0]
    assert q.ohlc("A.NS", "2026-10-21").close.tolist() == [11.0]
    assert q.ohlc("A.NS", end="2026-10-20").close.tolist() == [10.0]
    assert len(q.ohlc_market("IN", "2026-10-20", "2026-10-21")) == 3


def test_tables_symbols_markets_and_empty_lake(world, lake_with_prices):
    assert "ohlc" in lake_with_prices.tables()
    empty = world.query.Lake(world.root / "nowhere")
    assert empty.tables() == []


def test_symbols_markets_come_from_pillar_metrics(world):
    seed_scoring(world)
    from data_layer.query import Lake
    q = Lake()
    assert q.symbols() == ["GOOD.NS"] and q.symbols("US") == [] and q.markets() == ["IN"]
    assert set(q.fundamentals("GOOD.NS")) == {"income_statement", "balance_sheet", "cash_flow", "ratios"}
    assert q.fundamentals("GOOD.NS")["income_statement"].revenue.tolist() == [200.0, 100.0]       # newest first
    assert len(q.pillar_metrics("IN")) == 1 and q.pillar_metrics("US").empty


def test_bars_accessor_window_is_half_open(world):
    from datetime import datetime
    from data_layer.ticks import BarAggregator, Tick
    ag = BarAggregator(60)
    bars = []
    for k in range(3):
        bars += ag.add(Tick(datetime(2026, 10, 23, 3, 45 + k), "A.NS", 10 + k, 1 + k))
    bars += ag.flush_all()
    world.tick_store.write_bars(bars, "1m")
    q = world.query.Lake()
    assert q.bars("A.NS", "2026-10-23 03:45", "2026-10-23 03:47", "1m").close.tolist() == [10.0, 11.0]
