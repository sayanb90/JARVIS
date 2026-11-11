"""Daily FX rates (SCRUM-160): the pairs and their inverse fallback, pence, weekend lookups, a missing currency, idempotent storage."""
from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from data_layer import fx


def frame(values, start="2026-11-02"):
    idx = pd.bdate_range(start, periods=len(values))
    return pd.DataFrame({"Close": values}, index=idx)


def fake_download(table):
    """{ticker: values}: answers like yf_client._batch_download, and only for the tickers it has."""
    calls = []

    def dl(tickers, **kw):
        calls.append(list(tickers))
        return {t: frame(table[t]) for t in tickers if t in table}
    dl.calls = calls
    return dl


TABLE = {"EURUSD=X": [1.10, 1.11, 1.12, 1.13, 1.14], "GBPUSD=X": [1.30, 1.31, 1.32, 1.33, 1.34],
         "USDJPY=X": [150.0, 150.0, 160.0, 160.0, 160.0],          # only the inverse pair exists for the yen
         "HKDUSD=X": [0.128] * 5, "CNYUSD=X": [0.138] * 5}


def stored(tmp_path, **kw):
    return fx.refresh(tmp_path, currencies=["EUR", "GBP", "JPY", "HKD", "CNY"], download=fake_download(TABLE), **kw)


def test_refresh_stores_direct_pairs_and_the_inverse_of_the_other_way_round(tmp_path):
    res = stored(tmp_path, now=datetime(2026, 11, 9, 1, 0))
    assert res["ok"] and res["rows"] == 25 and res["currencies"] == 5 and res["missing"] == []
    r = fx.load_rates(tmp_path)
    assert r.currencies() == ["CNY", "EUR", "GBP", "HKD", "JPY"]
    assert r.usd_per_unit("EUR", date(2026, 11, 4)) == pytest.approx(1.12)
    assert r.usd_per_unit("JPY", date(2026, 11, 4)) == pytest.approx(1 / 160.0)             # inverse of USDJPY=X


def test_refresh_twice_does_not_duplicate_and_new_rates_win(tmp_path):
    stored(tmp_path)
    table2 = dict(TABLE, **{"EURUSD=X": [9.0, 9.0, 9.0, 9.0, 9.0]})
    fx.refresh(tmp_path, currencies=["EUR"], download=fake_download(table2))
    r = fx.load_rates(tmp_path)
    import duckdb
    n = duckdb.connect().execute(f"SELECT count(*), count(DISTINCT (date, currency)) FROM read_parquet('{fx.rates_path(tmp_path)}')").fetchall()[0]
    assert n[0] == n[1] == 25 and r.usd_per_unit("EUR", date(2026, 11, 4)) == 9.0


def test_first_run_takes_five_years_and_later_runs_ten_days(tmp_path):
    seen = []

    def dl(tickers, **kw):
        seen.append(tickers)
        return {t: frame([1.0, 1.0]) for t in tickers}
    asked = []
    orig = fx.fetch_rates
    try:
        fx.fetch_rates = lambda cur, period, download: (asked.append(period) or orig(cur, period, dl))
        fx.refresh(tmp_path, currencies=["EUR"])
        fx.refresh(tmp_path, currencies=["EUR"])
    finally:
        fx.fetch_rates = orig
    assert asked == ["5y", "10d"]


def test_weekends_use_the_latest_earlier_rate_but_an_old_rate_is_not_used(tmp_path):
    stored(tmp_path)
    r = fx.load_rates(tmp_path)
    assert r.usd_per_unit("EUR", date(2026, 11, 7)) == pytest.approx(1.14)                   # a Saturday: Friday's close
    assert r.usd_per_unit("EUR", date(2026, 11, 20)) is None                                 # two weeks after the last rate
    assert r.usd_per_unit("EUR", date(2026, 10, 1)) is None                                  # before the first rate


def test_pence_are_a_hundredth_of_a_pound_and_usd_needs_no_rate(tmp_path):
    stored(tmp_path)
    r = fx.load_rates(tmp_path)
    assert fx.split_currency("GBp") == ("GBP", 0.01) and fx.split_currency("GBX") == ("GBP", 0.01) and fx.split_currency("usd") == ("USD", 1.0)
    d = date(2026, 11, 4)
    assert r.usd_per_unit("GBp", d) == pytest.approx(0.0132)
    assert r.convert(1000.0, "USD", "USD", d) == 1000.0
    # Shell: earnings in USD, price in pence: 132 USD is 10,000 pence at 1.32 USD per pound
    assert r.convert(132.0, "USD", "GBp", d) == pytest.approx(10000.0)
    assert r.convert(5.0, "GBP", "GBp", d) == pytest.approx(500.0)


def test_a_hong_kong_listing_with_yuan_statements(tmp_path):
    stored(tmp_path)
    r = fx.load_rates(tmp_path)
    assert r.convert(1000.0, "CNY", "HKD", date(2026, 11, 4)) == pytest.approx(1000 * 0.138 / 0.128)


def test_a_missing_currency_is_none_never_one(tmp_path):
    stored(tmp_path)
    r = fx.load_rates(tmp_path)
    assert r.convert(10.0, "XXX", "USD", date(2026, 11, 4)) is None and r.convert(10.0, "EUR", "XXX", date(2026, 11, 4)) is None
    assert r.convert(None, "EUR", "USD", date(2026, 11, 4)) is None and r.convert(float("nan"), "EUR", "USD", date(2026, 11, 4)) is None


def test_a_failed_or_empty_download_keeps_the_file_and_a_missing_required_currency_fails(tmp_path):
    stored(tmp_path)
    before = fx.rates_path(tmp_path).read_bytes()
    boom = fx.refresh(tmp_path, currencies=["EUR"], download=lambda t, **kw: (_ for _ in ()).throw(OSError("blocked")))
    empty = fx.refresh(tmp_path, currencies=["EUR"], download=lambda t, **kw: {})
    assert not boom["ok"] and "download failed" in boom["message"] and not empty["ok"] and "kept the existing file" in empty["message"]
    assert fx.rates_path(tmp_path).read_bytes() == before
    partial = fx.refresh(tmp_path, currencies=["EUR", "KRW"], download=fake_download(TABLE))                # KRW is required and has no data
    assert partial["missing"] == ["KRW"] and not partial["ok"] and partial["rows"] == 5
    odd = fx.refresh(tmp_path, currencies=["EUR", "TRY"], download=fake_download(TABLE))                    # TRY is not required
    assert odd["ok"] and odd["missing"] == ["TRY"]


def test_command_line(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(fx, "rates_path", lambda lake_dir=None: tmp_path / "fx" / "rates.parquet")
    assert fx.main(["show"]) == 1 and "no FX rates yet" in capsys.readouterr().out
    fx.refresh(tmp_path, currencies=["EUR", "HKD"], download=fake_download(TABLE))
    assert fx.main(["show"]) == 0 and "EUR  2026-11-06" in capsys.readouterr().out
    assert fx.main(["convert", "100", "EUR", "USD", "--date", "2026-11-04"]) == 0 and "112.0000 USD" in capsys.readouterr().out
    assert fx.main(["convert", "100", "EUR", "XXX"]) == 1
