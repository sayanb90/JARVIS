"""Spread, book imbalance, realised volatility and day summaries: exact values on hand-made frames."""
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from analytics import microstructure as ms

OPEN = datetime(2026, 10, 26, 3, 45, 0)


def frame(closes, step_s=1, bid=None, ask=None, bq=10, aq=10, vol=100, start=OPEN):
    n = len(closes)
    c = np.array(closes, dtype=float)
    return pd.DataFrame({
        "ts": [start + timedelta(seconds=step_s * i) for i in range(n)],
        "open": c, "high": c, "low": c, "close": c, "volume": vol, "vwap": c, "n_ticks": 1,
        "bid": c - 0.05 if bid is None else bid, "ask": c + 0.05 if ask is None else ask,
        "bid_qty": bq, "ask_qty": aq})


# -- book metrics -----------------------------------------------------------------------------------
def test_spread_mid_bps_and_imbalance_exact():
    d = ms.add_book_metrics(frame([100.0], bid=99.9, ask=100.1, bq=300, aq=100))
    r = d.iloc[0]
    assert r.mid == pytest.approx(100.0) and r.spread == pytest.approx(0.2) and r.spread_bps == pytest.approx(20.0)
    assert r.imbalance == pytest.approx(0.5)                                  # (300-100)/(300+100)


@pytest.mark.parametrize("bid,ask", [(100.1, 100.0), (0.0, 100.0), (None, 100.0), (100.0, None)])
def test_crossed_zero_or_missing_books_give_nan_not_wrong_numbers(bid, ask):
    d = frame([100.0])
    d["bid"], d["ask"] = bid, ask
    r = ms.add_book_metrics(d).iloc[0]
    assert np.isnan(r.spread) and np.isnan(r.spread_bps) and np.isnan(r.mid) and np.isnan(r.imbalance)


def test_locked_book_has_zero_spread_and_zero_size_gives_nan_imbalance():
    r = ms.add_book_metrics(frame([100.0], bid=100.0, ask=100.0, bq=0, aq=0)).iloc[0]
    assert r.spread == 0 and r.spread_bps == 0 and np.isnan(r.imbalance)


def test_imbalance_extremes_and_input_not_mutated():
    d = frame([100.0, 100.0], bq=[50, 0], aq=[0, 50])
    before = d.copy()
    out = ms.add_book_metrics(d)
    assert out.imbalance.tolist() == [1.0, -1.0]
    pd.testing.assert_frame_equal(d, before)


# -- returns and volatility ---------------------------------------------------------------------------
def test_log_returns_first_is_nan_and_nonpositive_prices_are_nan():
    r = ms.log_returns(pd.Series([100.0, 110.0, 0.0, 50.0]))
    assert np.isnan(r[0]) and r[1] == pytest.approx(np.log(1.1)) and np.isnan(r[2]) and np.isnan(r[3])


def test_day_realised_vol_matches_formula():
    closes = [100.0, 101.0, 100.0, 102.0, 101.0]
    r = np.diff(np.log(closes))
    assert ms.day_realised_vol(frame(closes)) == pytest.approx(np.sqrt((r ** 2).sum() * 250))
    assert np.isnan(ms.day_realised_vol(frame([100.0])))
    assert ms.day_realised_vol(frame([100.0] * 10)) == 0.0


def test_rolling_vol_of_constant_price_is_zero_and_warmup_is_nan():
    d = frame([100.0] * 400)
    v = ms.rolling_realised_vol(d, 300)
    assert v.iloc[:60].isna().all() and (v.iloc[60:] == 0).all()


def test_rolling_vol_equals_closed_form_for_alternating_returns():
    """Returns of +/-r every second for a full window -> variance per second r^2, annualised by the session scale."""
    r = 0.001
    closes = 100 * np.exp(np.cumsum([0.0] + [r * (-1) ** i for i in range(1, 700)]))
    d = frame(closes)
    v = ms.rolling_realised_vol(d, 300).iloc[-1]
    expected = np.sqrt(r ** 2 * ms.SESSION_SECONDS * ms.TRADING_DAYS)       # r^2 per second over the window
    assert v == pytest.approx(expected, rel=0.01)


def test_quiet_seconds_add_no_variance_but_stretch_the_window():
    busy = ms.rolling_realised_vol(frame([100 + (i % 2) for i in range(400)], step_s=1), 300).iloc[-1]
    quiet = ms.rolling_realised_vol(frame([100 + (i % 2) for i in range(80)], step_s=5), 300).iloc[-1]
    assert quiet == pytest.approx(busy / np.sqrt(5), rel=0.1)                # same moves spread over 5x the seconds


def test_rolling_vol_and_mean_validate_window():
    with pytest.raises(ValueError):
        ms.rolling_realised_vol(frame([1.0, 2.0]), 0)
    with pytest.raises(ValueError):
        ms.rolling_mean(frame([1.0, 2.0]), "close", -1)


def test_rolling_mean_is_trailing_and_ignores_nan():
    d = frame([1.0, 2.0, 3.0, 4.0])
    d.loc[1, "close"] = np.nan
    m = ms.rolling_mean(d, "close", 2)                                        # window = last 2 seconds
    assert m.tolist() == [1.0, 1.0, 3.0, 3.5]


# -- day summary -------------------------------------------------------------------------------------------
def test_day_summary_values():
    d = frame([100.0, 102.0, 99.0, 101.0], vol=[10, 20, 30, 40])
    d["vwap"] = [100.0, 102.0, 99.0, 101.0]
    s = ms.day_summary(d)
    assert (s["open"], s["close"], s["high"], s["low"], s["bars"], s["volume"]) == (100.0, 101.0, 102.0, 99.0, 4, 100)
    assert s["vwap"] == pytest.approx((100 * 10 + 102 * 20 + 99 * 30 + 101 * 40) / 100)
    assert s["return_pct"] == pytest.approx(1.0) and s["avg_spread_bps"] == pytest.approx(0.1 / 100 * 1e4, rel=0.02)
    assert s["book_coverage_pct"] == 100 and s["crossed_books"] == 0 and s["realised_vol_pct"] > 0


def test_day_summary_without_book_or_vwap_or_volume():
    d = frame([100.0, 101.0]); d["bid"] = d["ask"] = np.nan; d["vwap"] = np.nan; d["volume"] = 0
    s = ms.day_summary(d)
    assert s["avg_spread_bps"] is None and s["vwap"] is None and s["book_coverage_pct"] == 0 and s["volume"] == 0
    d["volume"] = [5, 5]
    assert ms.day_summary(d)["vwap"] == pytest.approx(100.5)                  # falls back to volume-weighted close


def test_day_summary_counts_crossed_books_and_handles_empty():
    d = frame([100.0, 100.0], bid=[99.9, 100.2], ask=[100.1, 100.0])
    assert ms.day_summary(d)["crossed_books"] == 1
    assert ms.day_summary(frame([]).iloc[0:0]) == {}


# -- payload ----------------------------------------------------------------------------------------------------
def test_series_payload_is_json_safe_aligned_and_in_ist():
    d = frame([100.0, 101.0, 102.0, 101.5], bid=[99.9, np.nan, 101.9, 101.4], ask=[100.1, np.nan, 102.1, 101.6])
    p = ms.series_payload(d, 300)
    assert p["ts"][0] == "2026-10-26 09:15:00" and p["ts"][3] == "2026-10-26 09:15:03"
    n = len(p["ts"])
    assert all(len(v) == n for v in p.values())
    assert p["spread_bps"][1] is None and p["bid"][1] is None and p["cum_volume"] == [100.0, 200.0, 300.0, 400.0]
    import json
    assert "NaN" not in json.dumps(p)
    for c in ("bid_qty_avg", "imbalance_avg", "spread_bps_avg", "vol_ann"):
        assert c in p


def test_to_ist_strings_shifts_five_thirty():
    assert ms.to_ist_strings(pd.Series([datetime(2026, 10, 26, 10, 0, 0)])) == ["2026-10-26 15:30:00"]
