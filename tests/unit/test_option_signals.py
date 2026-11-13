"""Option chain and the option signal catalogue on the synthetic market (SCRUM-148, 173, 174, 177-182)."""

import numpy as np
import pandas as pd
import pytest

from signals import catalogue  # noqa: F401
from signals.framework import Context, cut_raw, specs
from signals.options import core
from signals.options.live import lee_ready
from signals.options.positioning import max_pain
from signals.options.validate import compare_iv, gate
from signals.synthetic import make_raw

SIGMA, SKEW = 0.13, 0.6


@pytest.fixture(scope="module")
def raw():
    return make_raw(sigma=SIGMA, skew=SKEW)


@pytest.fixture(scope="module")
def ctx(raw):
    c = Context("NIFTY", raw)
    for s in specs():
        c.compute(s.id)
    return c


def true_vol(k):
    return SIGMA + SKEW * np.maximum(-k, 0) + 0.3 * SKEW * np.maximum(k, 0)


def test_chain_recovers_iv_and_forward(ctx):
    ch = ctx.get("chain")
    f = ch[(ch["age"] == 0) & ch["iv"].notna() & (ch["t"] * 365 > 1)]
    assert (f["fwd_src"] == "parity").mean() > 0.95
    assert (f["iv"] - true_vol(f["k"])).abs().median() < 2e-4
    sp = ctx.get("spot").set_index("ts")["close"]
    implied_spot = f["fwd"] * np.exp(-ctx.r * f["t"])
    assert (implied_spot / f["ts"].map(sp) - 1).abs().median() < 2e-4


def test_chain_staleness_and_min_price(ctx):
    ch = ctx.get("chain")
    assert ch["age"].max() <= core.STALE_MAX
    assert ch.loc[ch["age"] > core.IV_AGE, "iv"].isna().all()
    assert ch.loc[ch["close"] < core.MIN_IV_PRICE, "iv"].isna().all()


def test_forward_falls_back_to_future_without_pairs(raw):
    opt = raw["opt_1m"]
    calls_only = opt[opt["symbol"].str.contains("_CE_")]
    c = Context("NIFTY", {**raw, "opt_1m": calls_only})
    ch = c.get("chain")
    assert set(ch["fwd_src"]) == {"future"}
    f = ch[(ch["age"] == 0) & ch["iv"].notna() & (ch["t"] * 365 > 1)]
    assert (f["iv"] - true_vol(f["k"])).abs().median() < 1e-3


def test_atm_and_constant_maturity(ctx):
    a = ctx.get("atm")
    good = a[(a["t"] * 365 > 1)]
    assert (good["atm_iv"] - SIGMA).abs().median() < 2e-3
    assert (a["iv7"].dropna() - SIGMA).abs().median() < 3e-3
    assert set(a["iv7_how"].dropna()) <= {"interp", "flat", "exact"}


def test_every_signal_computes(ctx):
    empty_ok = {"cost_log", "volume_burst", "block_prints"}          # no fills, bursts or block-size prints in the synthetic market
    for s in specs():
        out = ctx.compute(s.id)
        if s.id not in empty_ok:
            assert len(out), s.id
        assert {"underlying", "known_at"} <= set(out.columns), s.id


@pytest.mark.parametrize("cut_at", ["last_midday", "five_days_back_close", "open_last_day"])
def test_prefix_invariance_all_signals(raw, ctx, cut_at):
    days = sorted(raw["opt_1m"]["ts"].dt.normalize().unique())
    cut = {"last_midday": days[-2] + pd.Timedelta(hours=7, minutes=3), "five_days_back_close": days[-5] + pd.Timedelta(hours=10, minutes=30),
           "open_last_day": days[-1] + pd.Timedelta(hours=4, minutes=31, seconds=10)}[cut_at]
    part = Context("NIFTY", cut_raw(raw, cut), cut=cut)
    bad = []
    for s in specs():
        a = ctx.compute(s.id)
        b = part.compute(s.id)
        a = a[pd.to_datetime(a["known_at"]) <= cut].reset_index(drop=True)
        b = b[pd.to_datetime(b["known_at"]) <= cut].reset_index(drop=True)
        if len(a) != len(b):
            bad.append(f"{s.id}: {len(a)} vs {len(b)} rows")
            continue
        for c in a.columns if len(a) else []:
            x, y = a[c], b[c]
            if pd.api.types.is_numeric_dtype(x) and not pd.api.types.is_bool_dtype(x) and pd.api.types.is_numeric_dtype(y):
                if not np.allclose(x.to_numpy(float), y.to_numpy(float), rtol=1e-9, atol=1e-9, equal_nan=True):
                    bad.append(f"{s.id}.{c}")
                    break
            elif not x.astype(str).equals(y.astype(str)):
                bad.append(f"{s.id}.{c}")
                break
    assert not bad, bad


def test_vrp_near_zero_when_iv_equals_realised(ctx):
    v = ctx.compute("vrp")
    assert v["vrp_diff"].abs().median() < 0.02


def test_skew_positive_for_put_skew(ctx):
    s = ctx.compute("skew")
    assert s["skew1"].median() > 0


def test_rr25_matches_the_smile(ctx):
    r = ctx.compute("rr25").dropna(subset=["rr25"])
    assert len(r) and r["rr25"].median() > 0


def test_term_slope_flat_for_flat_term_structure(ctx):
    t = ctx.compute("term_slope")
    assert t["slope"].abs().median() < 3e-3


def test_gex_conventions_and_flip(ctx):
    g = ctx.compute("gex").dropna(subset=["gex_participant"])
    # synthetic clients are net long calls and net short puts: dealers short calls, long puts -> the Indian sign flips the US one
    assert np.allclose(g["gex_participant"], -g["gex_us"])
    assert (g["call_wall"] > 0).all() and (g["put_wall"] > 0).all()


def test_participant_positioning(ctx):
    p = ctx.compute("participant_positioning")
    assert set(p["participant"]) == {"Client", "DII", "FII", "Pro"}
    assert (p["net_calls"] == p["net_calls"]).all()


def test_iv_gate_passes_on_synthetic(ctx):
    res = gate(compare_iv(ctx))
    assert res["n"] > 1000 and res["pass"], res


def test_day_night_split_signs(ctx):
    from signals.options.holding import summarise_day_night
    s = summarise_day_night(ctx.compute("day_night_returns"))
    assert set(s["leg"]) == {"overnight", "intraday"}


def test_composite_is_equal_weighted(ctx):
    from signals.framework import REGISTRY
    assert REGISTRY["buy_permission"][0].params["weights"] == "equal"
    b = ctx.compute("buy_permission")
    assert "score" in b.columns


def test_max_pain_small_example():
    oi = pd.DataFrame({"strike": [100, 110, 120, 100, 110, 120], "cp": ["C", "C", "C", "P", "P", "P"], "oi": [10, 50, 100, 100, 50, 10]})
    assert max_pain(oi) == 110


def test_lee_ready_signs():
    t = pd.DataFrame({"t_evt": pd.to_datetime(["2026-01-01 04:00:00", "2026-01-01 04:00:01", "2026-01-01 04:00:02", "2026-01-01 04:00:03"]),
                      "symbol": "X", "ltp": [100.0, 101.0, 99.0, 100.0], "vol_today": [10, 20, 35, 40],
                      "bid": [99.5, 99.5, 99.5, 99.5], "ask": [100.5, 100.5, 100.5, 100.5]})
    s = lee_ready(t)
    assert list(s["sign"].iloc[1:]) == [1, -1, 1]            # above mid, below mid, at mid -> tick rule (up from 99)
    assert list(s["signed"].iloc[1:]) == [10, -15, 5]


def test_era_labels():
    assert core.era_of("2024-11-19") == "pre_2024-11-20"
    assert core.era_of("2024-11-20") == "from_2024-11-20"
    assert core.era_of("2025-09-01") == "from_2025-09-01"


def test_chain_uses_the_dated_rate_from_the_next_session():
    """SCRUM-186: MIBOR where imported (else repo), applied from the day after it is fixed; the flat default before any value."""
    import numpy as np
    from signals.framework import Context
    from signals.options.core import rate_by_row, rate_table
    from signals.synthetic import make_raw
    rates = pd.DataFrame({"date": [pd.Timestamp("2026-01-01").date()] * 2 + [pd.Timestamp("2026-01-10").date()],
                          "series": ["repo", "mibor_overnight", "mibor_overnight"], "value_pct": [5.5, 5.6, 5.9]})
    tab = rate_table(rates)
    assert np.allclose(tab["r"], [0.056, 0.059])                                      # MIBOR wins over repo
    ts = pd.Series(pd.to_datetime(["2025-12-31 05:00", "2026-01-02 05:00", "2026-01-10 05:00", "2026-01-11 05:00"]))
    assert np.allclose(rate_by_row(ts, tab, 0.065), [0.065, 0.056, 0.056, 0.059])  # a day never sees its own fixing
    raw = make_raw()
    d0 = pd.to_datetime(raw["opt_1m"]["ts"]).dt.date.min()
    ch = Context("NIFTY", {**raw, "rates": pd.DataFrame({"date": [d0 - pd.Timedelta(days=1)], "series": ["repo"], "value_pct": [3.0]})}).get("chain")
    assert np.allclose(ch["r"], 0.03)
    assert np.allclose(Context("NIFTY", raw).get("chain")["r"], 0.065)
