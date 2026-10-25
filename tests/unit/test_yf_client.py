"""Yahoo client: frame parsing, chunking, CSV staging writers, intraday bar guards, market rules."""
import numpy as np
import pandas as pd
import pytest

from tests.helpers import bday_frame


def frame(index, closes, vol=100):
    c = np.array(closes, dtype=float)
    return pd.DataFrame({"Open": c, "High": c + 1, "Low": c - 1, "Close": c, "Volume": vol}, index=pd.DatetimeIndex(index))


# -- scalar coercion --------------------------------------------------------------
@pytest.mark.parametrize("v,f,i", [(1.5, 1.5, 1), ("2", 2.0, 2), (None, None, None), (float("nan"), None, None),
                                   ("x", None, None), (7.9, 7.9, 7)])
def test_fv_iv(world, v, f, i):
    assert world.yf._fv(v) == f and world.yf._iv(v) == i


def test_df_to_rows_formats_dates_and_skips_nan_close(world):
    df = frame(["2026-10-21", "2026-10-22", "2026-10-23"], [10, np.nan, 12])
    rows = world.yf._df_to_rows(df, "INFY.NS", "IN")
    assert [r.date for r in rows] == ["2026.10.21", "2026.10.23"]
    assert rows[0].market == "IN" and rows[1].close == 12.0 and rows[1].volume == 100


# -- batch download shapes -----------------------------------------------------------
def test_batch_download_single_ticker_plain_frame(world, monkeypatch):
    df = frame(["2026-10-21"], [10])
    monkeypatch.setattr(world.yf.yf, "download", lambda **kw: df)
    assert list(world.yf._batch_download(["A.NS"], period="5y")) == ["A.NS"]


def test_batch_download_multi_ticker_drops_missing_and_empty(world, monkeypatch):
    good = frame(["2026-10-21"], [10])
    empty = good.iloc[0:0]
    cols = pd.MultiIndex.from_product([["A.NS", "B.NS"], good.columns])
    raw = pd.concat([good, empty.reindex(good.index)], axis=1)
    raw.columns = cols
    monkeypatch.setattr(world.yf.yf, "download", lambda **kw: raw)
    out = world.yf._batch_download(["A.NS", "B.NS", "ZZZ.NS"], period="5y")
    assert list(out) == ["A.NS"]                      # B all-NaN, ZZZ absent


def test_batch_download_empty_input_and_empty_response(world, monkeypatch):
    assert world.yf._batch_download([]) == {}
    monkeypatch.setattr(world.yf.yf, "download", lambda **kw: pd.DataFrame())
    assert world.yf._batch_download(["A.NS"]) == {}


# -- chunking + failure isolation --------------------------------------------------------
def test_full_fetch_chunks_and_survives_a_failed_chunk(world, monkeypatch):
    monkeypatch.setattr(world.yf, "CHUNK_SIZE", 2)
    seen = []

    def fake(chunk, **kw):
        seen.append((list(chunk), kw))
        if "C.NS" in chunk:
            raise RuntimeError("yahoo 429")
        return {t: frame(["2026-10-21"], [10]) for t in chunk}

    monkeypatch.setattr(world.yf, "_batch_download", fake)
    rows = world.yf.fetch_ohlc_full(["A.NS", "B.NS", "C.NS", "D.NS", "E.NS"], "IN")
    assert [c for c, _ in seen] == [["A.NS", "B.NS"], ["C.NS", "D.NS"], ["E.NS"]]
    assert {r.symbol for r in rows} == {"A.NS", "B.NS", "E.NS"}       # C/D chunk lost, others kept
    assert seen[0][1] == {"period": "5y"}


def test_daily_fetch_uses_start_date(world, monkeypatch):
    kws = []
    monkeypatch.setattr(world.yf, "_batch_download", lambda c, **kw: kws.append(kw) or {})
    world.yf.fetch_ohlc_daily(["A.NS"], "IN", days_back=3)
    assert set(kws[0]) == {"start"} and len(kws[0]["start"]) == 10


# -- staging writers ------------------------------------------------------------------------
def test_write_ohlc_splits_by_year_and_daily_is_flat(world):
    OR = world.yf.OHLCRow
    rows = [OR("2025.12.31", "A.NS", "IN", 1, 2, 0, 1.5, 10), OR("2026.01.02", "A.NS", "IN", 1, 2, 0, 1.6, None)]
    world.yf.write_ohlc(rows, "IN")
    assert sorted(p.name for p in (world.data / "ohlc").glob("ohlc_2*")) == ["ohlc_2025_in.csv", "ohlc_2026_in.csv"]
    world.yf.write_ohlc_daily(rows, "IN")
    txt = (world.data / "ohlc" / "ohlc_daily_in.csv").read_text().splitlines()
    assert txt[0] == "date,symbol,market,open,high,low,close,volume" and txt[2].endswith(",1.6,")   # None volume -> blank


# -- intraday bars ---------------------------------------------------------------------------------
NOW = pd.Timestamp("2026-10-23 05:00", tz="UTC")


def test_bars_to_rows_handles_naive_and_tz_aware_and_drops_forming_bar(world):
    idx_ist = pd.DatetimeIndex(["2026-10-23 09:15", "2026-10-23 09:20"]).tz_localize("Asia/Kolkata")
    rows = world.yf._bars_to_rows(frame(idx_ist, [10, 11]), "A.NS", "IN", "5m", NOW)
    assert rows[0].ts == "2026.10.23D03:45:00.000000000"            # 09:15 IST -> 03:45 UTC
    naive = frame(["2026-10-23 04:55", "2026-10-23 05:00"], [1, 2])   # naive == UTC; 05:00 bar still forming
    assert [r.close for r in world.yf._bars_to_rows(naive, "A.NS", "IN", "5m", NOW)] == [1.0]


def test_fetch_bars_validates_interval_and_clips_days(world, monkeypatch):
    with pytest.raises(ValueError):
        world.yf.fetch_bars(["A.NS"], "IN", interval="3m")
    seen = {}
    monkeypatch.setattr(world.yf, "_batch_download", lambda c, **kw: seen.update(kw) or {})
    world.yf.fetch_bars(["A.NS"], "IN", interval="1m", days_back=30)
    assert seen["period"] == "7d" and seen["interval"] == "1m" and seen["prepost"] is False


def test_write_bars_is_idempotent_and_corrections_win(world):
    BR = world.yf.BarRow
    mk = lambda c: BR("2026.10.23D03:45:00.000000000", "A.NS", "IN", 1, 2, 0, c, 5)  # noqa: E731
    world.yf.write_bars([mk(1.5), BR("2026.10.23D03:50:00.000000000", "A.NS", "IN", 1, 2, 0, 1.7, 6)], "IN", "5m")
    world.yf.write_bars([mk(9.0)], "IN", "5m")
    lines = (world.data / "bars/5m/in/2026-10-23.csv").read_text().splitlines()
    assert len(lines) == 3 and ",9.0," in lines[1] and ",1.7," in lines[2]


@pytest.mark.parametrize("market", ["US", "UK", "EU"])
def test_run_bars_refuses_non_india(world, market):
    with pytest.raises(ValueError, match="intraday"):
        world.yf.run_bars(market=market, tickers=["X"])


def test_run_bars_all_means_india_and_backfill_uses_deepest_window(world, monkeypatch):
    got = {}
    monkeypatch.setattr(world.yf, "fetch_bars", lambda u, m, iv, d: got.update(m=m, d=d, iv=iv) or [object()])
    monkeypatch.setattr(world.yf, "write_bars", lambda *a: None)
    world.set_universe({"IN": ["A.NS"]})
    world.yf.run_bars(market="ALL", interval="5m", backfill=True)
    assert got == {"m": "IN", "d": 59, "iv": "5m"}
    world.yf.run_bars(market="IN", interval="60m", backfill=True)
    assert got["d"] == 729


def test_run_ohlc_all_covers_four_markets_and_dry_run_downloads_nothing(world, monkeypatch):
    world.set_universe({"IN": ["A.NS"], "US": ["AAPL"], "UK": ["SHEL.L"], "EU": ["SAP.DE"]})
    monkeypatch.setattr(world.yf, "_batch_download", lambda *a, **k: pytest.fail("dry run must not download"))
    world.yf.run_ohlc("ALL", dry_run=True)
    world.yf.run_ohlc_daily("ALL", dry_run=True)
    called = []
    monkeypatch.setattr(world.yf, "_batch_download", lambda c, **kw: called.append(tuple(c)) or bday_frame(list(c), 30))
    world.yf.run_ohlc("ALL")
    assert called == [("A.NS",), ("AAPL",), ("SHEL.L",), ("SAP.DE",)]
    names = {p.name.split("_")[-1] for p in (world.data / "ohlc").glob("ohlc_*.csv")}
    assert names == {"in.csv", "us.csv", "uk.csv", "eu.csv"}


def test_resolve_universe_prefers_explicit_tickers(world):
    world.set_universe({"IN": ["A.NS", "B.NS"]})
    assert world.yf._resolve_universe("IN", ["X.NS"]) == ["X.NS"]
    assert world.yf._resolve_universe("IN", None) == ["A.NS", "B.NS"]
    assert world.yf._resolve_universe("UK", None) == []


# -- total outage must be loud ----------------------------------------------------------------------
def test_zero_rows_for_a_nonempty_universe_raises(world, monkeypatch):
    world.set_universe({"IN": ["A.NS"]})
    monkeypatch.setattr(world.yf, "_batch_download", lambda c, **kw: {})
    for call in (lambda: world.yf.run_ohlc("IN"), lambda: world.yf.run_ohlc_daily("IN"),
                 lambda: world.yf.run_bars("IN", interval="5m")):
        with pytest.raises(RuntimeError, match="no data"):
            call()


def test_empty_universe_is_not_an_error(world, monkeypatch):
    monkeypatch.setattr(world.yf, "_batch_download", lambda c, **kw: {})
    world.yf.run_ohlc("UK")                      # nothing configured for UK: nothing to fetch, nothing to fail


def test_all_markets_keep_going_after_one_market_fails_then_raise(world, monkeypatch):
    world.set_universe({"IN": ["A.NS"], "US": ["AAPL"], "UK": ["SHEL.L"], "EU": ["SAP.DE"]})
    monkeypatch.setattr(world.yf, "_batch_download",
                        lambda c, **kw: {} if c == ["AAPL"] else bday_frame(list(c), 30))
    with pytest.raises(RuntimeError, match="US"):
        world.yf.run_ohlc("ALL")
    names = {p.name.split("_")[-1] for p in (world.data / "ohlc").glob("ohlc_*.csv")}
    assert names == {"in.csv", "uk.csv", "eu.csv"}          # the healthy markets were still written
