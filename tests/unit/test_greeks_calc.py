"""Black-Scholes Greeks from IV and the calibration against Upstox's own values (SCRUM-108).  No network."""
from __future__ import annotations

from datetime import date, datetime

import numpy as np
import pandas as pd
import pytest

from data_layer import greeks_calc as gc


def test_atm_call_matches_the_textbook_values_and_put_call_relations_hold():
    r = gc.bs_greeks(True, 100.0, 100.0, 1.0, 20.0, rate=0.05)
    p = gc.bs_greeks(False, 100.0, 100.0, 1.0, 20.0, rate=0.05)
    assert r["delta"] == pytest.approx(0.6368, abs=1e-3) and r["gamma"] == pytest.approx(0.018762, abs=1e-5)
    assert r["vega"] == pytest.approx(37.524, abs=1e-2)                          # per 1.0 of volatility
    assert r["delta"] - p["delta"] == pytest.approx(1.0) and r["gamma"] == pytest.approx(p["gamma"]) and r["vega"] == pytest.approx(p["vega"])
    assert r["theta"] - p["theta"] == pytest.approx(-0.05 * 100.0 * np.exp(-0.05), rel=1e-9)   # per year
    assert r["rho"] - p["rho"] == pytest.approx(100.0 * np.exp(-0.05), rel=1e-9)
    assert r["theta"] < 0 and r["rho"] > 0 and p["rho"] < 0


def test_display_units_are_per_day_per_vol_point_and_per_percent():
    raw = gc.bs_greeks(True, 100.0, 100.0, 1.0, 20.0, rate=0.05)
    disp = gc.to_display(raw)
    assert disp["theta"] == pytest.approx(raw["theta"] / 365.0) and disp["vega"] == pytest.approx(raw["vega"] / 100.0)
    assert disp["rho"] == pytest.approx(raw["rho"] / 100.0) and disp["delta"] == raw["delta"]


def test_vectors_broadcast_and_a_zero_iv_gives_nan_not_an_error():
    g = gc.bs_greeks(np.array([True, False, True]), 100.0, np.array([90.0, 100.0, 110.0]), 0.1, np.array([20.0, 20.0, 0.0]))
    assert g["delta"].shape == (3,) and np.isnan(g["delta"][2]) and 0 < g["delta"][0] < 1 and -1 < g["delta"][1] < 0


def test_symbol_parsing_and_time_to_expiry():
    assert gc.parse_option("NIFTY_25000_CE_03_NOV_26") == ("NIFTY", 25000.0, "CE", date(2026, 11, 3))
    assert gc.parse_option("BANK_NIFTY_52500.5_PE_29_DEC_26")[:3] == ("BANK_NIFTY", 52500.5, "PE")
    assert gc.parse_option("NIFTY_FUT_24_NOV_26") is None and gc.parse_option("RELIANCE") is None
    assert gc.parse_future("NIFTY_FUT_24_NOV_26") == ("NIFTY", date(2026, 11, 24)) and gc.parse_future("NIFTY_25000_CE_03_NOV_26") is None
    t = gc.years_to_expiry([datetime(2026, 11, 3, 9, 0), datetime(2026, 11, 4, 0, 0)], date(2026, 11, 3))
    assert t[0] == pytest.approx(3600 / gc.YEAR_S) and t[1] == gc.MIN_T          # 15:30 IST is 10:00 UTC; never below the floor


def test_add_computed_fills_only_the_missing_greeks():
    ts = pd.date_range("2026-11-02 05:00", periods=3, freq="min")
    df = pd.DataFrame({"ts": ts, "iv": [0.15, 0.155, 0.16], "delta": [0.5, 0.51, 0.52], "gamma": [0.001] * 3,
                       "theta": np.nan, "vega": np.nan, "rho": np.nan})
    out = gc.add_computed(df, "NIFTY_25000_CE_03_NOV_26", pd.Series([25000.0, 25010.0, 25020.0]))
    assert out.attrs["computed"] == ["theta", "vega", "rho"]
    assert list(out["delta"]) == [0.5, 0.51, 0.52] and (out["theta"] < 0).all() and (out["vega"] > 0).all()
    full = out.copy()
    again = gc.add_computed(full, "NIFTY_25000_CE_03_NOV_26", pd.Series([25000.0] * 3))
    assert again.attrs["computed"] == [] and list(again["theta"]) == list(full["theta"])        # nothing is overwritten
    assert gc.add_computed(df, "NIFTY_FUT_24_NOV_26", pd.Series([1.0] * 3)).attrs["computed"] == []


# ---------------------------------------------------------------------------------- calibration
def write_day(tmp_path, scale_theta=1.0, rate=0.065, carry=0.0, iv_percent=False, theta_on_future=False):
    """A fake all-Greeks day: Upstox-style values (IV as a fraction, theta per day, vega per vol point, rho per 1%) from a known rate."""
    rows = []
    start = pd.Timestamp("2026-11-02 04:00")
    rows.append({"ts": start - pd.Timedelta(minutes=1), "symbol": "NIFTY_FUT_24_NOV_26", "ltp": 25000.0})
    for i in range(20):
        ts = start + pd.Timedelta(minutes=10 * i)
        fut = 25000.0 + 5 * i
        under = fut * float(np.exp(-carry * gc.years_to_expiry([ts], date(2026, 11, 24))[0]))      # what Upstox prices off
        rows.append({"ts": ts - pd.Timedelta(seconds=1), "symbol": "NIFTY_FUT_24_NOV_26", "ltp": fut})
        for k in range(24500, 25501, 100):
            for kind in ("CE", "PE"):
                iv = 14.0 + (k - 25000) / 400.0
                t = gc.years_to_expiry([ts], date(2026, 11, 3))[0]
                raw = gc.bs_greeks(kind == "CE", under, k, t, iv, rate)
                if theta_on_future:                                   # Upstox-style: the future's price held fixed
                    raw["theta"] = gc.theta_for_basis(raw["theta"], under, raw["delta"], "future", carry)
                g = gc.to_display(raw)
                rows.append({"ts": ts, "symbol": f"NIFTY_{k}_{kind}_03_NOV_26", "ltp": 120.0, "iv": iv if iv_percent else iv / 100.0, "delta": g["delta"].item(),
                             "gamma": round(g["gamma"].item(), 4), "theta": g["theta"].item() * scale_theta, "vega": g["vega"].item(), "rho": g["rho"].item()})
    folder = tmp_path / "ticks" / "date=2026-11-02"
    folder.mkdir(parents=True)
    pd.DataFrame(rows).to_parquet(folder / "ticks.parquet")
    return tmp_path


def test_calibration_recovers_the_rate_and_units_and_passes(tmp_path, capsys):
    res = gc.calibrate(write_day(tmp_path))
    assert res["ok"] and res["rate"] == 0.065 and res["carry"] == 0.0 and res["iv_scale"] == 1.0
    assert "JARVIS_GREEKS_CARRY=0" in gc.render(res) and "JARVIS_GREEKS_IV_SCALE=1" in gc.render(res) and "IV stored as a fraction" in gc.render(res)
    assert res["stored"]["iv"]["max"] < 1.0 and res["stored"]["delta"]["distinct"] > 10
    assert {g: res["greeks"][g]["unit"] for g in ("theta", "vega", "rho")} == {"theta": "per day", "vega": "per vol point", "rho": "per 1%"}
    assert all(res["greeks"][g]["median_rel_error"] < 0.001 for g in ("delta", "theta", "vega"))
    assert res["greeks"]["gamma"]["median_abs_error"] <= gc.GAMMA_ABS_TOLERANCE                   # gamma is stored with four decimals
    assert "judged by absolute error" in gc.render(res)
    assert "PASS" in gc.render(res)
    assert gc.main(["calibrate", "--stream", str(tmp_path)]) == 0 and "PASS" in capsys.readouterr().out


def test_calibration_finds_out_whether_iv_is_stored_as_a_fraction_or_a_percent(tmp_path):
    frac = gc.calibrate(write_day(tmp_path / "frac"))
    pct = gc.calibrate(write_day(tmp_path / "pct", iv_percent=True))
    assert frac["ok"] and frac["iv_scale"] == 1.0 and pct["ok"] and pct["iv_scale"] == 0.01
    assert "IV stored as a percent" in gc.render(pct) and "JARVIS_GREEKS_IV_SCALE=0.01" in gc.render(pct)


def test_iv_helpers_and_the_setting(monkeypatch):
    assert gc.iv_to_percent(0.15) == pytest.approx(15.0) and gc.iv_to_percent(15.0, scale=0.01) == pytest.approx(15.0)
    monkeypatch.setenv("JARVIS_GREEKS_IV_SCALE", "0.01")
    assert gc.iv_scale() == 0.01 and gc.iv_to_percent(15.0) == pytest.approx(15.0)


def test_calibration_recovers_the_future_carry(tmp_path):
    res = gc.calibrate(write_day(tmp_path, carry=0.05))
    assert res["ok"] and res["carry"] == 0.05 and res["rate"] == 0.065
    assert "JARVIS_GREEKS_CARRY=0.05" in gc.render(res)


def test_calibration_fails_when_a_greek_does_not_match(tmp_path, capsys):
    root = write_day(tmp_path, scale_theta=3.0)
    res = gc.calibrate(root)
    assert not res["ok"] and res["greeks"]["theta"]["median_rel_error"] > 0.5
    assert gc.main(["calibrate", "--stream", str(root)]) == 1
    assert "FAIL" in capsys.readouterr().out and "JARVIS_GREEKS=all" in gc.render(res)


def test_calibration_explains_a_day_that_did_not_store_all_greeks(tmp_path):
    folder = tmp_path / "ticks" / "date=2026-11-02"
    folder.mkdir(parents=True)
    pd.DataFrame({"ts": [pd.Timestamp("2026-11-02 04:00")], "symbol": ["NIFTY_25000_CE_03_NOV_26"], "ltp": [1.0], "iv": [14.0]}).to_parquet(folder / "ticks.parquet")
    with pytest.raises(SystemExit, match="all six"):
        gc.calibrate(tmp_path)


# ---------------------------------------------------------------------------------- the viewer fills in what was not stored
def core_day(world, with_future=True):
    from datetime import timedelta
    from data_layer.query import Lake
    from data_layer.tick_policy import TickPolicy
    from data_layer.ticks import Tick
    opt, fut = "NIFTY_25000_CE_03_NOV_26", "NIFTY_FUT_24_NOV_26"
    pol = TickPolicy(greeks="core")
    start = datetime(2026, 11, 2, 4, 0)
    ticks = []
    for i in range(30):
        ts = start + timedelta(seconds=i * 20)
        under = 25000.0 + i
        g = gc.to_display(gc.bs_greeks(True, under, 25000.0, gc.years_to_expiry([ts], date(2026, 11, 3))[0], 15.0, 0.065))
        ticks.append(pol.apply(Tick(ts, opt, 120.0, 10, 119.9, 5, 120.1, 5, 1000, iv=0.15, delta=g["delta"].item(), theta=g["theta"].item(),
                                    gamma=g["gamma"].item(), vega=g["vega"].item(), rho=g["rho"].item())))
        if with_future:
            ticks.append(Tick(ts, fut, under, 10, under - 0.5, 5, under + 0.5, 5, 5000))
    world.tick_store.write_ticks(ticks, world.lakedir)
    inst = world.lakedir / "instruments"
    inst.mkdir(parents=True)
    pd.DataFrame({"symbol": [opt, fut], "underlying": ["NIFTY", "NIFTY"]}).to_parquet(inst / "fo.parquet")
    return Lake(world.lakedir), opt, ticks


def test_viewer_computes_theta_vega_rho_when_only_core_greeks_were_stored(world, monkeypatch):
    monkeypatch.setenv("JARVIS_GREEKS_CARRY", "0")                                       # the synthetic day prices off the future itself
    lake, opt, ticks = core_day(world)
    assert all(t.theta is None and t.vega is None and t.rho is None for t in ticks if t.symbol == opt)       # not stored
    df = lake.greeks_series("2026-11-02", opt, 60)
    assert df.attrs["computed"] == ["theta", "vega", "rho"] and len(df) >= 5
    assert df["iv"].notna().all() and (df["theta"] < 0).all() and (df["vega"] > 0).all() and (df["rho"] > 0).all()
    assert df["delta"].between(0.4, 0.8).all()                                         # delivered, not computed
    ref = gc.to_display(gc.bs_greeks(True, 25000.0 + 29, 25000.0, gc.years_to_expiry([datetime(2026, 11, 2, 4, 9, 40)], date(2026, 11, 3))[0], 15.0))
    assert df["theta"].iloc[-1] == pytest.approx(ref["theta"].item(), rel=0.02)         # the last bar is the 04:09:40 tick
    assert df["iv"].iloc[0] == pytest.approx(15.0, abs=1e-4)                           # stored 0.15, shown as a percent
    assert lake.greeks_last("2026-11-02", [opt])[opt]["iv"] == pytest.approx(15.0, abs=1e-4)


def test_viewer_leaves_greeks_empty_when_the_future_has_no_bars(world):
    lake, opt, _ = core_day(world, with_future=False)
    df = lake.greeks_series("2026-11-02", opt, 60)
    assert df.attrs["computed"] == [] and df["theta"].isna().all() and df["iv"].notna().all()


def test_the_future_carry_lowers_the_spot_and_the_setting_is_read_from_the_environment(monkeypatch):
    ts = [datetime(2026, 11, 2, 5, 0)]
    monkeypatch.setenv("JARVIS_GREEKS_CARRY", "0.05")
    spot = gc.spot_from_future([25000.0], ts, date(2026, 11, 24))
    assert spot[0] == pytest.approx(25000.0 * np.exp(-0.05 * gc.years_to_expiry(ts, date(2026, 11, 24))[0]))
    assert gc.carry() == 0.05 and gc.spot_from_future([25000.0], ts, date(2026, 11, 24), 0.0)[0] == 25000.0
    monkeypatch.setenv("JARVIS_GREEKS_CARRY", "nonsense")
    assert gc.carry() == gc.DEFAULT_CARRY


def test_api_marks_which_greeks_were_computed(world):
    from fastapi.testclient import TestClient
    from webapp.app import create_app
    _, opt, _ = core_day(world)
    c = TestClient(create_app(world.lakedir))
    d = c.get("/api/fo/greeks", params={"date": "2026-11-02", "symbol": opt, "width": 60}).json()
    assert d["available"] and d["computed"] == ["theta", "vega", "rho"] and all(v is not None for v in d["series"]["theta"])


def test_calibration_finds_theta_held_on_the_future_and_the_viewer_follows_the_setting(tmp_path, monkeypatch):
    stream = write_day(tmp_path, rate=0.065, carry=0.065, theta_on_future=True)
    res = gc.calibrate(stream)
    assert res["ok"] and res["theta_basis"] == "future" and res["greeks"]["theta"]["median_rel_error"] < 0.02
    plain = gc.calibrate(write_day(tmp_path / "b", rate=0.065, carry=0.065))
    assert plain["ok"] and plain["theta_basis"] == "spot"
    assert "JARVIS_GREEKS_THETA_BASIS=future" in gc.render(res)
    # the viewer's computed theta moves by carry * spot * delta per year (per day: / 365) when the basis is the future
    ts = pd.Series([pd.Timestamp("2026-11-02 04:00")])
    df = pd.DataFrame({"ts": ts, "iv": [0.15], "delta": [0.5], "gamma": [0.0004]})
    under = pd.Series([25000.0])
    monkeypatch.setenv("JARVIS_GREEKS_RATE", "0.065")
    monkeypatch.setenv("JARVIS_GREEKS_CARRY", "0.065")
    monkeypatch.setenv("JARVIS_GREEKS_THETA_BASIS", "spot")
    a = gc.add_computed(df, "NIFTY_25000_CE_03_NOV_26", under)["theta"].iloc[0]
    monkeypatch.setenv("JARVIS_GREEKS_THETA_BASIS", "future")
    b = gc.add_computed(df, "NIFTY_25000_CE_03_NOV_26", under)["theta"].iloc[0]
    assert b - a == pytest.approx(0.065 * 25000.0 * gc.bs_greeks(True, 25000.0, 25000, gc.years_to_expiry([ts.iloc[0]], date(2026, 11, 3))[0], 15.0, 0.065)["delta"].item() / 365.0, rel=1e-6)
