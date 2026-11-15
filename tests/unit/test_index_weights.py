"""Index constituents and weights (SCRUM-183): real member lists and factsheets, approximate weights, snapshots that change only on change."""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from data_layer import index_weights as w
from data_layer.nse_archive import ParseError

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "index_weights"


def body(name: str) -> bytes:
    return (FIX / name).read_bytes()


@pytest.mark.parametrize("key,name,n", [("NIFTY50", "ind_nifty50list.csv", 50), ("NIFTYBANK", "ind_niftybanklist.csv", 14),
                                        ("NIFTYFIN", "ind_niftyfinancelist.csv", 20)])
def test_member_lists(key, name, n):
    m = w.parse_members(body(name), w.INDICES[key])
    assert len(m) == n and m["isin"].str.startswith("IN").all()
    assert "HDFCBANK" in set(m["nse_symbol"])


def test_member_list_with_the_wrong_count_is_rejected():
    with pytest.raises(ParseError, match="members, expected"):
        w.parse_members(body("ind_niftybanklist.csv"), w.INDICES["NIFTY50"])


@pytest.mark.parametrize("pdf,first,weight", [("ind_nifty50.pdf", "HDFC Bank Ltd.", 9.95), ("ind_nifty_bank.pdf", "HDFC Bank Ltd.", 18.18)])
def test_factsheet_top_ten(pdf, first, weight):
    as_of, rows = w.parse_factsheet(w.factsheet_text(body(pdf)))
    assert as_of == date(2026, 10, 30) and len(rows) == 10 and rows[0] == (first, weight)
    assert sum(x for _, x in rows) < 100


def test_factsheet_names_map_to_members():
    members = w.parse_members(body("ind_niftybanklist.csv"), w.INDICES["NIFTYBANK"])
    df = w.factsheet_rows(*w.parse_factsheet(w.factsheet_text(body("ind_nifty_bank.pdf"))), members)
    assert list(df["nse_symbol"][:3]) == ["HDFCBANK", "ICICIBANK", "KOTAKBANK"]
    with pytest.raises(ParseError, match="not in the member list"):
        w.factsheet_rows(date(2026, 10, 30), [("Some Other Bank Ltd.", 5.0)], members)


def test_an_html_page_is_not_a_factsheet():
    with pytest.raises(ParseError, match="not a PDF"):
        w.factsheet_text(b"<!DOCTYPE html><html>...</html>")


def write_prices(lake: Path, members: pd.DataFrame, bump: dict | None = None):
    """A Yahoo profile table and two daily closes per member (equal caps, but HDFCBANK twice the others)."""
    syms = [w.universe_symbol(s) for s in members["nse_symbol"]]
    prof = pd.DataFrame({"symbol": syms, "market": "IN", "shares_outstanding": [2e9 if s == "HDFCBANK.NS" else 1e9 for s in syms],
                         "market_cap": 1e12})
    (lake / "yf_fundamentals").mkdir(parents=True, exist_ok=True)
    prof.to_parquet(lake / "yf_fundamentals" / "profile.parquet")
    rows = []
    for s in syms:
        for d in (date(2026, 11, 11), date(2026, 11, 12)):
            rows.append(dict(date=d, symbol=s, market="IN", close=100.0 * (bump or {}).get(s, 1.0) if d == date(2026, 11, 12) else 90.0))
    (lake / "ohlc" / "year=2026").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(lake / "ohlc" / "year=2026" / "ohlc.parquet")


class FakeSites:
    def __init__(self):
        self.calls = []

    def __call__(self, url):
        self.calls.append(url)
        if "ind_niftybanklist.csv" in url:
            return 200, body("ind_niftybanklist.csv")
        if url.endswith("ind_nifty_bank.pdf"):
            return 200, body("ind_nifty_bank.pdf")
        return 404, b""


def test_refresh_stores_members_factsheet_and_approx_then_only_changes(tmp_path):
    members = w.parse_members(body("ind_niftybanklist.csv"), w.INDICES["NIFTYBANK"])
    write_prices(tmp_path, members)
    res = w.refresh(tmp_path, fetch=FakeSites(), sleep_s=0, today=date(2026, 11, 13), indices=["NIFTYBANK"])["NIFTYBANK"]
    assert res["members_stored"] and res["factsheet_stored"] and res["approx_stored"] and res["approx_covered"] == 14
    approx = w.latest(tmp_path, "NIFTYBANK", "approx_mcap").set_index("nse_symbol")
    assert approx["weight_pct"].sum() == pytest.approx(100.0)
    assert approx.loc["HDFCBANK", "weight_pct"] == pytest.approx(200 / 15)          # 2 shares' worth of 15
    assert approx.loc["HDFCBANK", "symbol"] == "HDFCBANK.NS" and approx.loc["HDFCBANK", "as_of"] == date(2026, 11, 12)
    assert (approx["fo_symbol"] == "BANKNIFTY").all()
    # the next day, nothing changed: nothing is appended
    res2 = w.refresh(tmp_path, fetch=FakeSites(), sleep_s=0, today=date(2026, 11, 14), indices=["NIFTYBANK"])["NIFTYBANK"]
    assert not res2["members_stored"] and not res2["factsheet_stored"] and not res2["approx_stored"]
    # a big price move in one member: a new approximate snapshot, first seen that day; the old one stays in force before it
    write_prices(tmp_path, members, bump={"YESBANK.NS": 3.0})
    res3 = w.refresh(tmp_path, fetch=FakeSites(), sleep_s=0, today=date(2026, 11, 16), indices=["NIFTYBANK"])["NIFTYBANK"]
    assert res3["approx_stored"]
    assert w.latest(tmp_path, "NIFTYBANK", "approx_mcap")["date"].iloc[0] == date(2026, 11, 16)
    assert w.latest(tmp_path, "NIFTYBANK", "approx_mcap", on=date(2026, 11, 15))["date"].iloc[0] == date(2026, 11, 13)
    s = w.status(tmp_path)
    assert s["NIFTYBANK/approx_mcap"]["snapshots"] == 2 and s["NIFTYBANK/members"]["snapshots"] == 1 and s["bytes"] > 0


def test_membership_change_is_a_new_snapshot(tmp_path):
    ix = w.INDICES["NIFTYBANK"]
    m = w.parse_members(body("ind_niftybanklist.csv"), ix)
    assert w.store_snapshot(ix, "members", m, date(2026, 11, 13), tmp_path)
    assert not w.store_snapshot(ix, "members", m, date(2026, 11, 14), tmp_path)
    assert w.store_snapshot(ix, "members", m.iloc[:-1], date(2026, 11, 15), tmp_path)
    assert len(w.latest(tmp_path, "NIFTYBANK", "members")) == 13


def test_no_prices_means_no_approx_and_a_clear_note(tmp_path):
    res = w.refresh(tmp_path, fetch=FakeSites(), sleep_s=0, today=date(2026, 11, 13), indices=["NIFTYBANK"])["NIFTYBANK"]
    assert "no prices" in res["approx"] and res["members_stored"]


def test_import_factsheet(tmp_path):
    p = tmp_path / "fs.pdf"
    p.write_bytes(body("ind_nifty50.pdf"))
    fake = lambda url: (200, body("ind_nifty50list.csv"))                # noqa: E731
    assert w.import_factsheet(p, "NIFTY50", tmp_path, fetch=fake, today=date(2026, 11, 13)) == 10
    top = w.latest(tmp_path, "NIFTY50", "factsheet_top10").set_index("symbol")
    assert len(top) == 10 and top.loc["HDFCBANK.NS", "weight_pct"] == 9.95 and top.loc["HDFCBANK.NS", "as_of"] == date(2026, 10, 30)
