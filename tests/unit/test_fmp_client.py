"""FMP client: HTTP session, field mapping, pillar aggregates, CSV writers, rotation, run()."""
from datetime import date, timedelta

import pytest
import requests

from tests.helpers import write_csv


class FakeResp:
    def __init__(self, status=200, payload=None):
        self.status_code, self._payload = status, payload if payload is not None else []

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeHttp:
    def __init__(self, script):
        self.script, self.calls = list(script), []

    def get(self, url, params, timeout):
        self.calls.append((url, dict(params)))
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def session(world, script):
    s = world.fmp.FMPSession("KEY", delay=0)
    s._sess = FakeHttp(script)
    return s


# -- FMPSession --------------------------------------------------------------
def test_session_sends_apikey_and_returns_json(world):
    s = session(world, [FakeResp(200, [{"a": 1}])])
    assert s.get("income-statement/X.NS", period="annual") == [{"a": 1}]
    url, params = s._sess.calls[0]
    assert url.endswith("/income-statement/X.NS") and params == {"period": "annual", "apikey": "KEY"}


def test_session_retries_429_then_succeeds_with_exponential_backoff(world, no_sleep):
    s = session(world, [FakeResp(429), FakeResp(429), FakeResp(200, [1])])
    assert s.get("e") == [1]
    assert no_sleep == [2.0 ** 1, 2.0 ** 2]


def test_session_retries_5xx_and_request_errors_then_gives_up_empty(world, no_sleep):
    s = session(world, [FakeResp(503), requests.ConnectionError("boom"), FakeResp(500)])
    assert s.get("e") == []                       # MAX_RETRIES exhausted -> empty list, not an exception
    assert len(s._sess.calls) == 3


def test_session_4xx_is_not_retried_as_success(world, no_sleep):
    s = session(world, [FakeResp(403), FakeResp(403), FakeResp(403)])
    assert s.get("e") == []                       # raise_for_status -> RequestException -> retried -> []


def test_session_enforces_minimum_gap(world, monkeypatch):
    slept = []
    monkeypatch.setattr(world.fmp.time, "sleep", slept.append)
    ticks = iter([100.0, 100.0, 100.1, 100.1])    # monotonic readings
    monkeypatch.setattr(world.fmp.time, "monotonic", lambda: next(ticks, 100.2))
    s = world.fmp.FMPSession("K", delay=0.5)
    s._sess = FakeHttp([FakeResp(200, [1]), FakeResp(200, [2])])
    s.get("a"); s.get("b")
    assert slept and 0 < slept[-1] <= 0.5


# -- extractor mapping ---------------------------------------------------------
def extractor(world, payloads):
    ex = world.fmp.FMPExtractor.__new__(world.fmp.FMPExtractor)
    ex._sess = type("S", (), {"get": lambda self, ep, **kw: payloads.get(ep.split("/")[0], [])})()
    return ex


def test_extractor_maps_all_four_statements(world):
    ex = extractor(world, {
        "income-statement": [{"date": "2025-03-31", "revenue": 100, "netIncome": "20", "ebitda": 30}],
        "balance-sheet-statement": [{"date": "2025-03-31", "totalAssets": 500, "totalLiabilities": 200,
                                     "longTermDebt": 50, "totalStockholdersEquity": 300}],
        "cash-flow-statement": [{"date": "2025-03-31", "freeCashFlow": 12.5}],
        "ratios": [{"date": "2025-03-31", "roic": 0.18, "weightedAverageSharesDiluted": 1e9}],
    })
    d = ex.fetch_ticker("INFY.NS", "IN")
    assert d["income"][0].date == "2025.03.31" and d["income"][0].net_income == 20.0
    assert d["balance"][0].total_equity == 300.0 and d["balance"][0].long_term_debt == 50.0
    assert d["cash_flow"][0].free_cash_flow == 12.5
    assert (d["ratios"][0].roic, d["ratios"][0].shares_outstanding) == (0.18, 1e9)
    assert {r.market for k in d for r in d[k]} == {"IN"}


def test_extractor_tolerates_missing_fields_and_non_list_payloads(world):
    ex = extractor(world, {"income-statement": [{"date": "2025-03-31"}], "ratios": {"error": "limit"}})
    d = ex.fetch_ticker("X.NS", "IN")
    assert d["income"][0].revenue is None and d["income"][0].ebit is None
    assert d["ratios"] == [] and d["balance"] == []


def test_ebit_is_currently_sourced_from_fmp_ebitda(world):
    """KNOWN QUIRK, characterised so a change is deliberate: `ebit` is filled from FMP's `ebitda`.
    No pillar uses ebit today; fix before any analytics relies on it."""
    ex = extractor(world, {"income-statement": [{"date": "2025-03-31", "ebitda": 30, "operatingIncome": 25}]})
    assert ex.fetch_ticker("X.NS", "IN")["income"][0].ebit == 30.0


@pytest.mark.parametrize("raw,expected", [(None, None), ("", None), ("abc", None), ("3.5", 3.5), (7, 7.0)])
def test_float_coercion(world, raw, expected):
    assert world.fmp._f(raw) == expected


def test_csv_date_format(world):
    assert world.fmp._csv_date("2025-03-31") == "2025.03.31" and world.fmp._csv_date("") == ""


# -- pillar aggregates -----------------------------------------------------------
def rows(world, ni=(30, 20, None), fcf=(10, None, 20)):
    f = world.fmp
    inc = [f.IncomeRow(f"202{5 - i}.12.31", "X", "IN", 100 - i, n, 1) for i, n in enumerate(ni)]
    cf = [f.CashFlowRow(f"202{5 - i}.12.31", "X", "IN", c) for i, c in enumerate(fcf)]
    bal = [f.BalanceRow("2025.12.31", "X", "IN", 1000, 400, 50, 600)]
    rat = [f.RatiosRow("2025.12.31", "X", "IN", 0.15, 90)]
    return inc, bal, cf, rat


def test_pillar_metrics_average_ignores_missing_and_uses_newest_row(world):
    inc, bal, cf, rat = rows(world)
    m = world.fmp.compute_pillar_metrics(inc, bal, cf, rat, "X", "IN")
    assert m.avg_net_income_5y == 25.0 and m.avg_fcf_5y == 15.0
    assert (m.latest_revenue, m.latest_long_term_debt, m.latest_roic, m.latest_shares_outstanding) == (100, 50, 0.15, 90)


def test_pillar_metrics_empty_inputs_give_none(world):
    m = world.fmp.compute_pillar_metrics([], [], [], [], "X", "IN")
    assert m.avg_net_income_5y is None and m.latest_revenue is None and m.latest_roic is None


# -- CSV writers -------------------------------------------------------------------
def test_write_fundamentals_and_pillars_csv_shape(world):
    inc, bal, cf, rat = rows(world)
    world.fmp.write_fundamentals(inc, bal, cf, rat, "IN")
    world.fmp.write_pillar_metrics([world.fmp.compute_pillar_metrics(inc, bal, cf, rat, "X", "IN")], "IN")
    f = world.data / "fundamentals"
    assert (f / "income_statement_in.csv").read_text().splitlines()[0] == "date,symbol,market,revenue,net_income,ebit"
    assert "2025.12.31,X,IN,100,30,1" in (f / "income_statement_in.csv").read_text()
    assert ",,1" in (f / "income_statement_in.csv").read_text()          # None -> blank, loadable as NULL
    assert (f / "pillar_metrics_in.csv").read_text().splitlines()[0].startswith("symbol,market,avg_net_income_5y")


def test_written_fundamentals_load_into_lake_with_nulls(world):
    inc, bal, cf, rat = rows(world)
    world.fmp.write_fundamentals(inc, bal, cf, rat, "IN")
    world.lake.load_fundamentals(("IN",))
    q = world.query.Lake()
    assert len(q.sql("SELECT * FROM income_statement")) == 3
    assert q.sql("SELECT count(*) n FROM income_statement WHERE net_income IS NULL").n[0] == 1


# -- universe + fund_meta --------------------------------------------------------------
def test_read_universe_filters_market_and_skips_comments(world):
    (world.data / "universe.csv").write_text(
        "# a comment\nsymbol,name,market\nA.NS,A,IN\n# mid\nAAPL,Apple,US\n")
    assert [r["symbol"] for r in world.fmp.read_universe("IN")] == ["A.NS"]
    assert [r["symbol"] for r in world.fmp.read_universe()] == ["A.NS", "AAPL"]


def test_read_universe_falls_back_to_nifty_when_file_missing(world):
    rows_ = world.fmp.read_universe("IN")
    assert len(rows_) >= 40 and all(r["market"] == "IN" for r in rows_)
    assert world.fmp.read_universe("US") == []


def test_fund_meta_roundtrip_and_preserves_others(world):
    world.set_universe({"IN": ["A.NS", "B.NS"]})
    world.fmp.write_fund_meta(["A.NS"], "IN")
    first = world.fmp.read_fund_meta()
    assert first == {"A.NS": date.today()}
    world.fmp.write_fund_meta(["B.NS"], "IN")
    assert set(world.fmp.read_fund_meta()) == {"A.NS", "B.NS"}
    assert "A.NS,IN," in world.fmp.FUND_META_CSV.read_text()


def test_read_fund_meta_accepts_iso_and_dotted_dates_and_ignores_garbage(world):
    (world.data / "fund_meta.csv").write_text(
        "symbol,market,last_updated\nA.NS,IN,2026.10.01\nB.NS,IN,2026-10-02\nC.NS,IN,not-a-date\n")
    assert world.fmp.read_fund_meta() == {"A.NS": date(2026, 10, 1), "B.NS": date(2026, 10, 2)}


def test_read_fund_meta_missing_file_is_empty(world):
    assert world.fmp.read_fund_meta() == {}


# -- rotation ----------------------------------------------------------------------------
def test_stale_selection_puts_never_fetched_first_then_oldest(world):
    world.set_universe({"IN": ["NEW.NS", "OLD.NS", "MID.NS", "FRESH.NS"]})
    today = date.today()
    (world.data / "fund_meta.csv").write_text(
        "symbol,market,last_updated\n"
        f"OLD.NS,IN,{(today - timedelta(days=30)):%Y.%m.%d}\n"
        f"MID.NS,IN,{(today - timedelta(days=10)):%Y.%m.%d}\n"
        f"FRESH.NS,IN,{today:%Y.%m.%d}\n")
    got = [r["symbol"] for r in world.fmp.get_stale_tickers("IN", budget=12)]    # 12 // 4 = 3 tickers
    assert got == ["NEW.NS", "OLD.NS", "MID.NS"]


@pytest.mark.parametrize("budget,n", [(0, 0), (3, 0), (4, 1), (248, 10), (1000, 10)])
def test_stale_selection_respects_call_budget(world, budget, n):
    world.set_universe({"IN": [f"S{i}.NS" for i in range(10)]})
    assert len(world.fmp.get_stale_tickers("IN", budget=budget)) == n


def test_full_cycle_visits_every_ticker_before_repeating(world):
    syms = [f"S{i}.NS" for i in range(10)]
    world.set_universe({"IN": syms})
    seen = []
    for _ in range(4):                                       # budget 12 -> 3 tickers per day
        batch = [r["symbol"] for r in world.fmp.get_stale_tickers("IN", budget=12)]
        seen += batch
        world.fmp.write_fund_meta(batch, "IN")
    assert set(seen) == set(syms)


# -- run() ---------------------------------------------------------------------------------
class StubExtractor:
    fail = {"BAD.NS"}

    def __init__(self, key):
        pass

    def fetch_ticker(self, t, m):
        if t in self.fail:
            raise RuntimeError("fmp down")
        f = StubExtractor.world.fmp
        return {"income": [f.IncomeRow("2025.12.31", t, m, 10, 2, 1)],
                "balance": [f.BalanceRow("2025.12.31", t, m, 1, 1, 1, 1)],
                "cash_flow": [f.CashFlowRow("2025.12.31", t, m, 3)],
                "ratios": [f.RatiosRow("2025.12.31", t, m, 0.1, 5)]}


def test_run_skips_failed_ticker_and_stamps_only_successes(world, monkeypatch):
    StubExtractor.world = world
    monkeypatch.setattr(world.fmp, "FMPExtractor", StubExtractor)
    monkeypatch.setenv("FMP_API_KEY", "K")
    world.set_universe({"IN": ["OK.NS", "BAD.NS"]})
    world.fmp.run(tickers=["OK.NS", "BAD.NS"], market="IN")
    assert set(world.fmp.read_fund_meta()) == {"OK.NS"}
    assert "BAD.NS" not in (world.data / "fundamentals" / "income_statement_in.csv").read_text()


def test_run_dry_run_makes_no_calls_and_writes_nothing(world, monkeypatch, caplog):
    monkeypatch.setattr(world.fmp, "FMPExtractor", lambda k: pytest.fail("must not call FMP"))
    world.set_universe({"IN": ["A.NS", "B.NS"]})
    with caplog.at_level("INFO"):
        world.fmp.run(market="IN", dry_run=True)
    assert "2 tickers × 4 endpoints = 8 FMP calls" in caplog.text
    assert not list((world.data / "fundamentals").glob("*.csv"))


def test_run_rejects_non_india_market_and_missing_key(world, monkeypatch):
    with pytest.raises(SystemExit):
        world.fmp.run(market="US", dry_run=True)
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    with pytest.raises(SystemExit):
        world.fmp.run(market="IN")


def test_run_slice_limits_universe(world, monkeypatch, caplog):
    world.set_universe({"IN": [f"S{i}.NS" for i in range(10)]})
    with caplog.at_level("INFO"):
        world.fmp.run(market="IN", dry_run=True, slice_start=2, slice_end=4)
    assert "3 tickers × 4 endpoints" in caplog.text
