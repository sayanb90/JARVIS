"""Upstox fundamentals loader (SCRUM-103): parsers for the documented response shapes, versioned storage, raw payloads, the log,
refresh skipping and failure handling.  No network: a fake HTTP session answers by path."""
from __future__ import annotations

import gzip
import json
import threading
from datetime import datetime, timedelta

import pandas as pd
import pytest

from data_layer.upstox import fundamentals as fu
from data_layer.upstox.ratelimit import RateLimiter
from data_layer.upstox.rest import UpstoxRest
from tests.unit.test_sysmon_rest import FakeResp

ISIN = "INE002A01018"
BODIES = {
    "income-statement": {"status": "success", "data": {
        "income_statement": [{"category": "Revenue", "history": [{"period": "2025-03", "value": 1000, "change": 5.5}, {"period": "2024-03", "value": 950, "change": 3}]},
                             {"category": "Net Profit", "history": [{"period": "2025-03", "value": 120, "change": 8.0}]}],
        "full_statement": [{"particular": "EPS Basic", "history": [{"period": "2025-03", "value": 12.5, "change": 1.0}]}]}},
    "balance-sheet": {"status": "success", "data": {
        "history": [{"period": "2025-03", "total_asset": 5000, "total_liability": 3000}],
        "full_statement": [{"particular": "Equity Capital", "history": [{"period": "2025-03", "value": 100, "change": None}]}]}},
    "cash-flow": {"status": "success", "data": {
        "cash_flow": [{"category": "Operating", "history": [{"period": "2025-03", "value": 200, "change": 2}]}], "full_statement": []}},
    "key-ratios": {"status": "success", "data": [{"name": "P/E", "company_value": 22.1, "sector_value": 25.0}, {"name": "Note", "company_value": "n/a", "sector_value": None}]},
    "share-holdings": {"status": "success", "data": [{"category": "Promoters", "history": [{"period": "Sep 2025", "value": 50.1, "change": -0.1}]}]},
    "corporate-actions": {"status": "success", "data": [{"name": "Dividend", "expiry_date": "2025-06-14", "amount": 10, "ratio": None}]},
    "profile": {"status": "success", "data": {"name": "ABC Ltd", "sector": "IT", "employees": 1200, "tags": ["a", "b"]}},
}


class FakeHttp:
    """Answers /v2/fundamentals/<who>/<path> from BODIES; `bodies` can be changed between runs; `fail` paths return an error."""
    def __init__(self, fail=("competitors",)):
        self.bodies, self.fail, self.calls = {k: json.loads(json.dumps(v)) for k, v in BODIES.items()}, set(fail), []

    def get(self, url, params=None, headers=None, timeout=None):
        who, path = url.split("/v2/fundamentals/")[1].split("/", 1)
        self.calls.append((who, path, dict(params or {})))
        if path in self.fail:
            return FakeResp(400, {"status": "error", "errors": [{"errorCode": "UDAPI100011", "message": "Invalid Instrument key"}]})
        return FakeResp(200, self.bodies[path])


def client(http):
    return UpstoxRest(lambda: "TOKEN", session=http, limiter=RateLimiter(windows=((1, 10**6), (60, 10**6), (1800, 10**6))), sleep=lambda s: None)


NOW = datetime(2026, 11, 3, 12, 0)


# ---------------------------------------------------------------------------------- parsers
def test_statement_shapes_become_item_period_value_change_rows():
    rows = fu.parse("income_yearly", BODIES["income-statement"]["data"])
    rev = [r for r in rows if r["item"] == "Revenue"]
    assert {(r["period"], r["value"], r["change"]) for r in rev} == {("2025-03", 1000.0, 5.5), ("2024-03", 950.0, 3.0)}
    assert any(r["item"] == "full:EPS Basic" and r["value"] == 12.5 for r in rows)
    bal = fu.parse("balance_full", BODIES["balance-sheet"]["data"])
    assert {r["item"] for r in bal} == {"total_asset", "total_liability", "full:Equity Capital"}
    assert fu.parse("cashflow", BODIES["cash-flow"]["data"])[0]["item"] == "Operating"


def test_ratios_holdings_actions_and_profile_keep_everything():
    kr = {r["item"]: r for r in fu.parse("key_ratios", BODIES["key-ratios"]["data"])}
    assert kr["P/E"]["value"] == 22.1 and kr["P/E"]["change"] == 25.0 and kr["P/E"]["period"] == "snapshot"
    assert kr["Note"]["value"] is None and kr["Note"]["text"] == "n/a"                            # text is kept, not dropped
    assert fu.parse("shareholding", BODIES["share-holdings"]["data"])[0]["value"] == 50.1
    act = fu.parse("corporate_actions", BODIES["corporate-actions"]["data"])[0]
    assert (act["item"], act["period"], act["value"]) == ("Dividend", "2025-06-14", 10.0) and json.loads(act["text"])["amount"] == 10
    prof = {r["item"]: r for r in fu.parse("profile", BODIES["profile"]["data"])}
    assert prof["name"]["text"] == "ABC Ltd" and prof["employees"]["value"] == 1200.0 and json.loads(prof["tags"]["text"]) == ["a", "b"]
    assert fu.parse("competitors", ["INE1", {"isin": "INE2"}])[1]["text"] == '{"isin": "INE2"}'
    assert fu.parse("income_yearly", None) == [] and fu.parse("key_ratios", []) == []


# ---------------------------------------------------------------------------------- storage
def test_run_stores_versions_raw_payloads_and_a_log_and_survives_a_failing_endpoint(world):
    http = FakeHttp()
    s = fu.run(client(http), {ISIN: "RELIANCE"}, now=NOW)
    assert s["companies"] == 1 and s["calls"] == 12 and s["ok"] == 11 and s["failed"] == {"competitors:400:UDAPI100011": 1}
    v = pd.read_parquet(fu.store_dir() / "versions.parquet")
    assert set(v["endpoint"]) >= {"income_yearly", "balance_full", "key_ratios", "shareholding", "corporate_actions", "profile"}
    assert (v["first_seen"] == "2026-11-03").all() and (v["last_seen"] == "2026-11-03").all() and s["new_versions"] == len(v)
    raw = json.loads(gzip.open(fu.store_dir() / "raw" / "date=2026-11-03" / f"{ISIN}.json.gz", "rt").read())
    assert raw["income_yearly"]["data"]["data"]["income_statement"][0]["category"] == "Revenue" and raw["competitors"]["ok"] is False
    lg = pd.read_parquet(fu.store_dir() / "fetch_log.parquet")
    assert len(lg) == 12 and lg[lg["endpoint"] == "competitors"].iloc[0]["error_code"] == "UDAPI100011"
    assert ("NSE_EQ|" + ISIN, "competitors", {}) in http.calls                                 # peers are asked for by instrument key
    assert (ISIN, "income-statement", {"time_period": "yearly", "fs": "true"}) in http.calls
    assert fu.status()["failing_now"] == {"competitors": 1} and fu.status()["companies"] == 1


def test_an_unchanged_value_only_moves_last_seen_and_a_restated_value_is_a_new_version(world):
    http = FakeHttp()
    fu.run(client(http), {ISIN: "RELIANCE"}, ["income_yearly"], now=NOW)
    n = len(pd.read_parquet(fu.store_dir() / "versions.parquet"))
    s = fu.run(client(http), {ISIN: "RELIANCE"}, ["income_yearly"], now=NOW + timedelta(days=30))
    v = pd.read_parquet(fu.store_dir() / "versions.parquet")
    assert s["new_versions"] == 0 and len(v) == n and (v["first_seen"] == "2026-11-03").all() and (v["last_seen"] == "2026-12-03").all()
    http.bodies["income-statement"]["data"]["income_statement"][0]["history"][0]["value"] = 1010          # restated revenue
    s = fu.run(client(http), {ISIN: "RELIANCE"}, ["income_yearly"], now=NOW + timedelta(days=60))
    v = pd.read_parquet(fu.store_dir() / "versions.parquet")
    rev = v[(v["item"] == "Revenue") & (v["period"] == "2025-03")].sort_values("first_seen")
    assert s["new_versions"] == 1 and list(rev["value"]) == [1000.0, 1010.0] and list(rev["first_seen"]) == ["2026-11-03", "2027-01-02"]
    assert rev.iloc[0]["last_seen"] == "2026-12-03"                                                        # the old value stays, with the last day seen


def test_max_age_skips_recent_companies_and_stop_keeps_the_work_done(world):
    http = FakeHttp(fail=())
    fu.run(client(http), {ISIN: "RELIANCE"}, ["key_ratios"], now=NOW)
    calls = len(http.calls)
    s = fu.run(client(http), {ISIN: "RELIANCE", "INE009A01021": "INFY"}, ["key_ratios"], now=NOW + timedelta(days=2), max_age_days=5)
    assert s["skipped"] == 1 and s["companies"] == 1 and len(http.calls) == calls + 1                      # only the new company was fetched
    s = fu.run(client(http), {ISIN: "RELIANCE"}, ["key_ratios"], now=NOW + timedelta(days=9), max_age_days=5)
    assert s["skipped"] == 0 and s["companies"] == 1                                                        # old enough: fetched again
    stop = threading.Event()
    stop.set()
    s = fu.run(client(http), {"INE1": "A", "INE2": "B"}, ["key_ratios"], now=NOW + timedelta(days=20), stop=stop)
    assert s["stopped"] and s["companies"] == 0


def test_a_wrong_shape_is_logged_not_raised_and_unknown_endpoints_are_refused(world, monkeypatch):
    http = FakeHttp(fail=())
    http.bodies["key-ratios"] = {"status": "success", "data": 5}                                          # not a list
    s = fu.run(client(http), {ISIN: "RELIANCE"}, ["key_ratios", "profile"], now=NOW)
    assert s["companies"] == 1 and s["failed"] == {"key_ratios:200:PARSE": 1} and s["ok"] == 1
    assert fu.status()["failing_now"] == {"key_ratios": 1}
    with pytest.raises(ValueError, match="unknown endpoints"):
        fu.run(client(http), {ISIN: "X"}, ["nope"])


def test_command_line_run_and_status(world, monkeypatch, capsys):
    http = FakeHttp(fail=())
    monkeypatch.setattr(fu, "_token", lambda: "TOKEN")
    monkeypatch.setattr("data_layer.upstox.rest.UpstoxRest", lambda tok: client(http))
    assert fu.main(["run", "--isin", ISIN, "--endpoints", "key_ratios,profile"]) == 0
    assert fu.main(["status"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["companies"] == 1 and out["versions"] >= 5 and out["calls_logged"] == 2
    monkeypatch.setattr(fu, "_token", lambda: "")
    assert fu.main(["run", "--isin", ISIN]) == 2


def test_the_admin_catalogue_reads_the_upstox_files_through_vendor_neutral_tables(world):
    from webapp.admin import BY_KEY
    items, log = BY_KEY[("fundamentals", "items")], BY_KEY[("operations", "fetch_log")]
    assert "upstox_fundamentals/versions.parquet" in [loc for _, loc, _ in items.parts]
    assert "upstox_fundamentals/fetch_log.parquet" in [loc for _, loc, _ in log.parts]


def test_the_log_records_the_time_of_each_call_and_what_went_wrong(world):
    class Ticking:                                   # every call to the clock is a second later than the last
        t = datetime(2026, 11, 3, 12, 0)
    import data_layer.upstox.fundamentals as mod
    real = mod.datetime

    class Clock(real):
        @classmethod
        def now(cls, tz=None):
            Ticking.t += timedelta(seconds=1)
            return Ticking.t.replace(tzinfo=tz)
    mod.datetime = Clock
    try:
        fu.run(client(FakeHttp()), {ISIN: "RELIANCE"})
    finally:
        mod.datetime = real
    lg = pd.read_parquet(fu.store_dir() / "fetch_log.parquet")
    assert lg["fetched_at"].nunique() > 1                                        # not one stamp for the whole run
    assert {"error_message", "attempts", "latency_s"} <= set(lg.columns)
    bad = lg[lg["endpoint"] == "competitors"].iloc[0]
    assert bad["error_message"] == "Invalid Instrument key" and bad["attempts"] == 1
