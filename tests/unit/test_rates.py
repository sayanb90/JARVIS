"""Money-market rates (SCRUM-186): the RBI home page's Current Rates panel (real, trimmed), files saved by hand, and rate_on."""

from datetime import date
from pathlib import Path

import pytest

from data_layer import rates as r
from data_layer.nse_archive import ParseError

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "rates"
PAGE = (FIX / "rbi_home_2026-11-13.html").read_text()


def test_rbi_page_values():
    df = r.parse_rbi(PAGE, date(2026, 11, 13))
    v = {(d, s): x for d, s, x in zip(df["date"], df["series"], df["value_pct"])}
    t = date(2026, 11, 13)
    assert v[(t, "repo")] == 5.50 and v[(t, "sdf")] == 5.25 and v[(t, "msf")] == 5.75 and v[(t, "bank_rate")] == 5.75
    assert v[(t, "reverse_repo")] == 3.35
    assert v[(t, "tbill_91d")] == 5.8681 and v[(t, "tbill_182d")] == 6.5183 and v[(t, "tbill_364d")] == 6.6394
    assert v[(date(2026, 11, 12), "call_low")] == 4.30 and v[(date(2026, 11, 12), "call_high")] == 5.60
    assert (df["source"] == "RBI").all() and len(df) == 10


def test_commented_out_old_dates_are_ignored():
    assert "<!--January 27, 2026-->" in PAGE                     # RBI leaves the old date in a comment next to the real one
    assert set(r.parse_rbi(PAGE, date(2026, 11, 13))["date"]) == {date(2026, 11, 13), date(2026, 11, 12)}


def test_checks():
    with pytest.raises(ParseError, match="not on the page"):
        r.parse_rbi(PAGE.replace("Policy Repo Rate", "Policy Rate"), date(2026, 11, 13))
    with pytest.raises(ParseError, match="corridor"):
        r.parse_rbi(PAGE.replace("5.25%", "6.25%"), date(2026, 11, 13))
    with pytest.raises(ParseError, match="as on"):
        r.parse_rbi(PAGE, date(2026, 11, 11))                  # call rates dated after "today"
    with pytest.raises(ParseError, match="as on"):
        r.parse_rbi(PAGE, date(2026, 12, 31))                  # a stale page


def test_daily_stores_and_rate_on(tmp_path):
    res = r.daily(tmp_path, fetch=lambda url: (200, PAGE.encode()), today=date(2026, 11, 13))
    assert res["stored"] == 10 and r.table_path(tmp_path).exists()
    assert r.rate_on(tmp_path, date(2026, 11, 13), "repo") == pytest.approx(0.055)
    assert r.rate_on(tmp_path, date(2026, 11, 20), "tbill_91d") == pytest.approx(0.058681)
    assert r.rate_on(tmp_path, date(2026, 11, 12), "repo") == 0.065            # nothing on or before that day: the default
    assert r.rate_on(tmp_path, date(2026, 11, 13)) == 0.065                    # no MIBOR stored yet
    assert r.rate_on(tmp_path / "nowhere", date(2026, 11, 13), default=0.07) == 0.07
    again = r.daily(tmp_path, fetch=lambda url: (200, PAGE.encode()), today=date(2026, 11, 13))
    assert again["rows"] == 10                                                   # upserted, not doubled
    s = r.status(tmp_path)
    assert s["repo"]["value_pct"] == 5.5 and s["bytes"] > 0


def test_daily_reports_a_refusal(tmp_path):
    assert "HTTP 403" in r.daily(tmp_path, fetch=lambda url: (403, b""), today=date(2026, 11, 13))["error"]


def test_import_a_rate_file_saved_by_hand(tmp_path):
    p = tmp_path / "mibor.csv"
    p.write_text("Date,Overnight MIBOR (%)\n10-11-2026,5.52\n11-11-2026,5.49\n12-11-2026,5.45\n")
    df = r.parse_rate_file(p, "mibor_overnight", "FBIL")
    assert list(df["value_pct"]) == [5.52, 5.49, 5.45] and df["date"].iloc[0] == date(2026, 11, 10)
    r.save(df, tmp_path)
    assert r.rate_on(tmp_path, date(2026, 11, 13)) == pytest.approx(0.0545)
    assert r.rate_on(tmp_path, date(2026, 11, 11)) == pytest.approx(0.0549)
    bad = tmp_path / "bad.csv"
    bad.write_text("Date,Rate\n10-11-2026,545\n")
    with pytest.raises(ParseError, match="outside"):
        r.parse_rate_file(bad, "mibor_overnight", "FBIL")
