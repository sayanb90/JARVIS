"""Basket fair value, the expiry-day manipulation detector (SCRUM-191) and the flow proxies (SCRUM-192)."""

import numpy as np
import pandas as pd
import pytest

from signals import catalogue  # noqa: F401
from signals.framework import Context
from signals.options.basket import weights_asof
from signals.options.flow import classify
from signals.synthetic import make_raw


@pytest.fixture(scope="module")
def planted():
    return Context("NIFTY", make_raw(plant_pattern=True))


@pytest.fixture(scope="module")
def clean():
    return Context("NIFTY", make_raw(plant_pattern=False))


def test_basket_tracks_index(clean):
    b = clean.compute("basket_fair_value")
    assert len(b) and b["coverage"].eq(1).all()
    assert b["gap_index_bp"].abs().median() < 3
    assert {"gap_future_bp", "gap_options_bp"} <= set(b.columns)


def test_weights_are_point_in_time():
    w = pd.DataFrame({"date": [pd.Timestamp("2026-01-05").date(), pd.Timestamp("2026-02-02").date()], "symbol": ["A.NS", "A.NS"],
                      "weight": [0.4, 0.6]})
    assert weights_asof(w, pd.Timestamp("2026-01-05").date()).empty                   # a snapshot is known at its close, not on its day
    assert weights_asof(w, pd.Timestamp("2026-02-02").date())["A.NS"] == 0.4
    assert weights_asof(w, pd.Timestamp("2026-02-03").date())["A.NS"] == 0.6


def test_manipulation_flag_on_planted_expiry_only(planted, clean):
    f = planted.compute("manipulation_flag")
    hit = f[f["flag"]]
    assert len(hit) == 1 and bool(hit["is_expiry"].iloc[0])
    assert not clean.compute("manipulation_flag")["flag"].any()


def test_constituent_flow_follows_the_push(planted):
    f = planted.compute("manipulation_flag")
    day = f.loc[f["flag"], "date"].iloc[0]
    row = f[f["date"] == day].iloc[0]
    assert row["flow_am"] > 0 > row["flow_pm"]
    assert row["am_put_oi"] > row["am_call_oi"]


def test_classify_sign_rules():
    s = classify(pd.Series([5, -5, 5, -5, 5]), pd.Series([10, 10, -10, -10, 0]), pd.Series([5] * 5))
    assert list(s) == ["open_buy", "open_sell", "close_short", "close_long", "none"]


def test_opening_flow_and_trade_size(clean):
    o = clean.compute("opening_flow")
    assert len(o) and {"open_buy_calls", "open_sell_puts", "net_open_buy_calls"} <= set(o.columns)
    t = clean.compute("trade_size_flow")
    assert len(t) and any(c.startswith("opt_") for c in t.columns) and any(c.startswith("fut_") for c in t.columns)
    d = clean.compute("opening_flow_daily")
    assert len(d) and "net_open_buy_puts" in d.columns


def test_block_prints_found():
    raw = make_raw()
    t = raw["ticks_fo"].copy()
    i = t.index[t["symbol"].str.contains("_FUT_")][10]
    t.loc[i, "ltq"] = 65 * 80                          # an 80-lot futures print
    j = t.index[t["symbol"].str.contains("_CE_")][50]
    t.loc[j, "ltq"] = 65 * 150                         # a 150-lot option print
    ctx = Context("NIFTY", {**raw, "ticks_fo": t})
    b = ctx.compute("block_prints")
    assert set(b["instrument_type"]) == {"FUT", "CE"}
    assert (b["lots"] >= 50).all()


def test_implied_corr_proxy(clean):
    c = clean.compute("implied_corr_proxy")
    assert len(c) and c["implied_corr"].notna().all() and (c["basket_vol"] > 0).all()


def test_weights_blend_official_top_ten_with_approximate_rest():
    from signals.lakeio import blend_weights
    d = pd.Timestamp("2026-11-13").date()
    w = pd.DataFrame({"date": [d] * 5, "source": ["factsheet_top10", "approx_mcap", "approx_mcap", "approx_mcap", "members"],
                      "symbol": ["A.NS", "A.NS", "B.NS", "C.NS", "A.NS"], "weight": [0.5, 0.6, 0.3, 0.1, None]})
    b = blend_weights(w).set_index("symbol")["weight"]
    assert b["A.NS"] == 0.5                                   # the official weight wins
    assert abs(b.sum() - 1.0) < 1e-12 and abs(b["B.NS"] / b["C.NS"] - 3.0) < 1e-12   # the rest keeps its proportions
