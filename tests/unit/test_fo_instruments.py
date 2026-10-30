"""Futures and options: instrument file parsing, the contract table, selection, key maps and the CLI."""
from __future__ import annotations

import gzip
import json
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from data_layer.upstox import fo_instruments as fo

IST = timezone(timedelta(hours=5, minutes=30))
DAY = date(2026, 10, 23)


def ist_ms(d: date) -> int:
    """Midnight IST of `d` as epoch ms, the way the Upstox instrument file stores expiries."""
    return int(datetime(d.year, d.month, d.day, tzinfo=IST).timestamp() * 1000)


def rec(under, itype, expiry, strike=None, token=None, **kw):
    token = token or f"{abs(hash((under, itype, expiry, strike))) % 10**6}"
    label = "FUT" if itype == "FUT" else f"{strike:g} {itype}"
    r = {"segment": "NSE_FO", "instrument_type": itype, "instrument_key": f"NSE_FO|{token}",
         "trading_symbol": f"{under} {label} {expiry:%d %b %y}".upper().replace("  ", " "),
         "underlying_symbol": under, "underlying_key": f"NSE_INDEX|{under}", "expiry": ist_ms(expiry),
         "lot_size": 75, "tick_size": 5.0, "weekly": False, "exchange_token": token, "freeze_quantity": 1800}
    if strike is not None:
        r["strike_price"] = float(strike)
    r.update(kw)
    return r


def master_records() -> list[dict]:
    """A small NSE instrument file: NIFTY (weekly + monthly options) and RELIANCE, plus junk that must be ignored."""
    out = []
    for e in (date(2026, 10, 20), date(2026, 10, 27), date(2026, 11, 24), date(2026, 12, 29)):   # the first is already expired
        out.append(rec("NIFTY", "FUT", e))
    for e in (date(2026, 10, 27), date(2026, 11, 3)):
        for k in range(24000, 25001, 100):
            out += [rec("NIFTY", "CE", e, k), rec("NIFTY", "PE", e, k)]
    for e in (date(2026, 10, 27), date(2026, 11, 24)):
        out.append(rec("RELIANCE", "FUT", e, lot_size=250, underlying_key="NSE_EQ|INE002A01018"))
    for k in range(2400, 2601, 20):
        out += [rec("RELIANCE", "CE", date(2026, 11, 24), k), rec("RELIANCE", "PE", date(2026, 11, 24), k)]
    out.append({"segment": "NSE_EQ", "instrument_type": "EQ", "instrument_key": "NSE_EQ|INE002A01018", "trading_symbol": "RELIANCE"})
    out.append({"segment": "NSE_FO", "instrument_type": "FUT", "trading_symbol": "BROKEN FUT", "underlying_symbol": "X",
                "expiry": ist_ms(date(2026, 11, 24))})                                 # no instrument key
    return out


@pytest.fixture()
def table():
    return fo.parse_master(master_records())


# -- parsing -----------------------------------------------------------------
def test_expiry_is_the_exchange_date_not_the_utc_date():
    assert fo.expiry_date(ist_ms(date(2026, 11, 24))) == date(2026, 11, 24)       # 18:30 UTC the day before
    assert fo.expiry_date("2026-11-24") == date(2026, 11, 24)
    assert fo.expiry_date(None) is None and fo.expiry_date(0) is None and fo.expiry_date("not a date") is None


def test_parse_keeps_only_derivatives_with_a_key_and_expiry(table, caplog):
    assert set(table.instrument_type) == {"FUT", "CE", "PE"}
    assert not table.symbol.str.contains("BROKEN").any()
    assert table.instrument_key.str.startswith("NSE_FO|").all()
    assert list(table.columns) == fo.COLUMNS
    caplog.clear()
    fo.parse_master(master_records())
    assert "1 derivative records dropped" in caplog.text


def test_symbols_are_unique_readable_and_futures_have_no_strike(table):
    assert table.symbol.is_unique
    row = table[(table.underlying == "NIFTY") & (table.instrument_type == "CE") & (table.strike == 24500)
                & (table.expiry == date(2026, 10, 27))].iloc[0]
    assert row.symbol == "NIFTY_24500_CE_27_OCT_26" and row.lot_size == 75
    assert table[table.instrument_type == "FUT"].strike.isna().all()
    assert table[(table.underlying == "RELIANCE") & (table.instrument_type == "FUT")].lot_size.eq(250).all()


def test_symbol_falls_back_to_a_built_name_without_trading_symbol():
    r = rec("NIFTY", "CE", date(2026, 11, 24), 24500)
    del r["trading_symbol"]
    assert fo.parse_master([r]).symbol.iloc[0] == "NIFTY_24500_CE_24_NOV_26"
    f = rec("NIFTY", "FUT", date(2026, 11, 24))
    del f["trading_symbol"]
    assert fo.parse_master([f]).symbol.iloc[0] == "NIFTY_FUT_24_NOV_26"


def test_option_without_a_strike_is_dropped_and_duplicate_symbols_are_refused():
    bad = rec("NIFTY", "CE", date(2026, 11, 24), 24500)
    del bad["strike_price"]
    assert fo.parse_master([bad]).empty
    a, b = rec("NIFTY", "FUT", date(2026, 11, 24), token="1"), rec("NIFTY", "FUT", date(2026, 11, 24), token="2")
    with pytest.raises(ValueError, match="not unique"):
        fo.parse_master([a, b])


def test_underlying_falls_back_to_asset_symbol_then_name():
    r = rec("NIFTY", "FUT", date(2026, 11, 24))
    del r["underlying_symbol"]
    assert fo.parse_master([{**r, "asset_symbol": "NIFTY"}]).underlying.iloc[0] == "NIFTY"
    assert fo.parse_master([{**r, "name": "NIFTY 50"}]).underlying.iloc[0] == "NIFTY 50"
    assert fo.parse_master([r]).empty


def test_read_records_handles_gzip_plain_json_and_rejects_other_shapes(tmp_path):
    recs = master_records()[:3]
    gz, plain, bad = tmp_path / "a.json.gz", tmp_path / "a.json", tmp_path / "b.json"
    gz.write_bytes(gzip.compress(json.dumps(recs).encode()))
    plain.write_text(json.dumps(recs))
    bad.write_text(json.dumps({"not": "a list"}))
    assert fo.read_records(gz) == recs == fo.read_records(plain)
    with pytest.raises(ValueError, match="JSON list"):
        fo.read_records(bad)


def test_download_writes_the_file_and_explains_a_refusal(tmp_path):
    class R:
        def __init__(self, code, content=b""): self.status_code, self.content = code, content
    got = {}
    ok = fo.download(tmp_path / "x" / "NSE.json.gz", get=lambda url, **k: got.update(url=url) or R(200, b"abc"))
    assert ok.read_bytes() == b"abc" and got["url"] == fo.MASTER_URL
    with pytest.raises(RuntimeError, match="in a browser"):
        fo.download(tmp_path / "y.gz", get=lambda url, **k: R(403))


# -- the lake table ------------------------------------------------------------
def test_table_keeps_expired_contracts_and_first_seen(world, table):
    assert fo.load_table().empty
    n1 = fo.save_table(table, date(2026, 10, 19))
    assert n1 == len(table)
    # next import: the first NIFTY future (expiry 20 Oct) is no longer in the file; one new contract appears
    later = pd.concat([table[table.expiry != date(2026, 10, 20)],
                       fo.parse_master([rec("NIFTY", "FUT", date(2027, 1, 26))])], ignore_index=True)
    n2 = fo.save_table(later, date(2026, 10, 21))
    t = fo.load_table().set_index("symbol")
    assert n2 == len(table) + 1
    assert t.loc["NIFTY_FUT_20_OCT_26", "last_seen"] == date(2026, 10, 19)           # expired, still resolvable
    assert t.loc["NIFTY_FUT_27_OCT_26", "first_seen"] == date(2026, 10, 19)          # unchanged
    assert t.loc["NIFTY_FUT_27_OCT_26", "last_seen"] == date(2026, 10, 21)
    assert t.loc["NIFTY_FUT_26_JAN_27", "first_seen"] == date(2026, 10, 21)
    assert not (world.lakedir / "instruments" / "fo.parquet.tmp").exists()


def test_resolve_finds_live_and_expired_contracts(world, table):
    fo.save_table(table, DAY)
    assert fo.resolve("NIFTY_FUT_20_OCT_26")["instrument_type"] == "FUT"
    assert fo.resolve("NIFTY_99999_CE_01_JAN_20") is None


# -- selection -----------------------------------------------------------------
def test_selection_picks_nearest_futures_and_a_strike_window_around_spot(table):
    sel, notes = fo.select_contracts(table, ["NIFTY"], DAY, futures=2, option_expiries=1, strikes=2, spot={"NIFTY": 24510})
    futs, opts = sel[sel.instrument_type == "FUT"], sel[sel.instrument_type != "FUT"]
    assert list(futs.expiry) == [date(2026, 10, 27), date(2026, 11, 24)]          # the expired 20 Oct future is skipped
    assert set(opts.expiry) == {date(2026, 10, 27)}                                 # nearest option expiry only
    assert sorted(opts.strike.unique()) == [24300, 24400, 24500, 24600, 24700]      # 2 either side of 24500
    assert len(opts) == 10 and notes == {"missing": [], "no_spot": []}
    assert list(sel.columns) == fo.UNIVERSE_COLUMNS


def test_selection_without_spot_centres_on_the_ladder_and_says_so(table):
    sel, notes = fo.select_contracts(table, ["NIFTY"], DAY, futures=0, option_expiries=1, strikes=1)
    assert sorted(sel.strike.unique()) == [24400, 24500, 24600] and notes["no_spot"] == ["NIFTY"]


def test_selection_edges_window_is_clipped_and_unknown_underlyings_reported(table):
    sel, notes = fo.select_contracts(table, ["NIFTY", "NOSUCH"], DAY, futures=1, option_expiries=2, strikes=50,
                                     spot={"NIFTY": 99999})
    assert notes["missing"] == ["NOSUCH"]
    assert sel[sel.instrument_type == "CE"].groupby("expiry").size().eq(11).all()   # whole ladder, both expiries
    assert set(sel[sel.instrument_type == "CE"].expiry) == {date(2026, 10, 27), date(2026, 11, 3)}


def test_selection_can_be_futures_only_and_empty_selection_is_a_frame(table):
    sel, _ = fo.select_contracts(table, ["NIFTY", "RELIANCE"], DAY, futures=1, types=("FUT",))
    assert set(sel.instrument_type) == {"FUT"} and len(sel) == 2
    none, notes = fo.select_contracts(table, ["NOSUCH"], DAY)
    assert none.empty and list(none.columns) == fo.UNIVERSE_COLUMNS and notes["missing"] == ["NOSUCH"]


def test_selection_ignores_contracts_that_expire_before_the_day(table):
    sel, _ = fo.select_contracts(table, ["NIFTY"], date(2026, 10, 28), futures=1, option_expiries=1, strikes=1, spot={"NIFTY": 24500})
    assert set(sel.expiry) == {date(2026, 11, 3), date(2026, 11, 24)}               # 27 Oct has gone


def test_spot_from_lake_uses_the_last_close_of_the_nearest_future(world, table):
    from data_layer.ticks import Bar
    fo.save_table(table, DAY)
    assert fo.spot_from_lake(table, ["NIFTY"], DAY) == {}                           # no bars collected yet
    t0 = datetime(2026, 10, 22, 4, 0, 0)
    world.tick_store.write_bars([Bar(t0 + timedelta(seconds=i), "NIFTY_FUT_27_OCT_26", "IN", 24400 + i, 24401 + i,
                                     24399, 24400 + i, 5, None, 1, None, None, None, None) for i in range(3)], "1s")
    assert fo.spot_from_lake(table, ["NIFTY", "RELIANCE"], DAY) == {"NIFTY": 24402.0}


# -- the contract list the collectors read ---------------------------------------
def test_universe_roundtrip_and_expired_rows_are_dropped(world, table, caplog):
    sel, _ = fo.select_contracts(table, ["NIFTY"], DAY, futures=2, option_expiries=0)
    path = fo.write_universe(sel)
    assert path == world.data / "fo_universe.csv"
    km = fo.fo_key_map(day=DAY)
    assert km == dict(zip(sel.instrument_key, sel.symbol)) and len(km) == 2
    km2 = fo.fo_key_map(day=date(2026, 10, 28))                                     # the 27 Oct future has expired
    assert list(km2.values()) == ["NIFTY_FUT_24_NOV_26"] and "expired" in caplog.text
    assert fo.fo_key_map(path, DAY) == km


def test_missing_universe_file_tells_you_what_to_run(world):
    with pytest.raises(SystemExit, match="import, then select"):
        fo.fo_key_map()


# -- command line ----------------------------------------------------------------
def test_cli_import_list_select_and_peek(world, tmp_path, capsys):
    f = tmp_path / "NSE.json.gz"
    f.write_bytes(gzip.compress(json.dumps(master_records()).encode()))
    fo.main(["import", "--file", str(f), "--asof", "2026-10-23"])
    out = capsys.readouterr().out
    assert "imported" in out and "2 underlyings" in out
    fo.main(["list", "--underlying", "RELIANCE", "--type", "FUT", "--day", "2026-10-23"])
    out = capsys.readouterr().out
    assert "RELIANCE_FUT_27_OCT_26" in out and "(2 contracts)" in out
    fo.main(["select", "--underlying", "NIFTY", "RELIANCE", "NOSUCH", "--futures", "1", "--option-expiries", "1",
             "--strikes", "1", "--spot", "NIFTY=24500", "--day", "2026-10-23"])
    out = capsys.readouterr().out
    assert "NOT FOUND: ['NOSUCH']" in out and "no spot given for ['RELIANCE']" in out
    sel = pd.read_csv(world.data / "fo_universe.csv")
    assert set(sel.underlying) == {"NIFTY", "RELIANCE"} and (sel.symbol.str.startswith("NIFTY_24")).any()
    fo.main(["peek", "--file", str(f), "--underlying", "NIFTY", "-n", "1"])
    assert '"instrument_type"' in capsys.readouterr().out
    fo.main(["peek", "--file", str(f), "--underlying", "NOSUCH"])
    assert "segments present" in capsys.readouterr().out


def test_cli_select_futures_only_and_error_paths(world, tmp_path, capsys):
    with pytest.raises(SystemExit, match="import first"):
        fo.main(["select", "--underlying", "NIFTY"])
    with pytest.raises(SystemExit, match="--file"):
        fo.main(["import"])
    empty = tmp_path / "e.json"
    empty.write_text(json.dumps([{"segment": "NSE_EQ"}]))
    with pytest.raises(SystemExit, match="peek"):
        fo.main(["import", "--file", str(empty)])
    f = tmp_path / "m.json"
    f.write_text(json.dumps(master_records()))
    fo.main(["import", "--file", str(f)])
    capsys.readouterr()
    fo.main(["select", "--underlying", "NIFTY", "--no-options", "--futures", "2", "--day", "2026-10-23",
             "--out", str(tmp_path / "u.csv")])
    assert set(pd.read_csv(tmp_path / "u.csv").instrument_type) == {"FUT"}
    with pytest.raises(SystemExit, match="nothing selected"):
        fo.main(["select", "--underlying", "NOSUCH", "--day", "2026-10-23"])


def test_cli_import_download_uses_the_downloader(world, tmp_path, monkeypatch, capsys):
    f = tmp_path / "dl.json"
    f.write_text(json.dumps(master_records()))
    monkeypatch.setattr(fo, "download", lambda dest: f)
    fo.main(["import", "--download"])
    assert "imported" in capsys.readouterr().out


def test_segment_key_map_selects_the_universe(world, table):
    from data_layer.upstox.instruments import segment_key_map
    sel, _ = fo.select_contracts(table, ["NIFTY"], DAY, futures=1, option_expiries=0)
    fo.write_universe(sel)
    monkey = pd.read_csv(world.data / "fo_universe.csv")
    # the contract in the file is alive today only if its expiry is in the future; use an explicit day-proof path
    path = world.data / "fo_universe.csv"
    monkey["expiry"] = (date.today() + timedelta(days=5)).isoformat()
    monkey.to_csv(path, index=False)
    assert list(segment_key_map("fo").values()) == list(monkey.symbol)
    assert list(segment_key_map("fo", path).values()) == list(monkey.symbol)
    world.set_universe({"IN": ["RELIANCE.NS"]})
    assert all(k.startswith("NSE_EQ|") for k in segment_key_map("eq"))


# -- watchlist: the tracked per-underlying subscription list ---------------------
def test_read_watchlist_parses_all_strikes_and_applies_defaults(tmp_path):
    p = tmp_path / "wl.csv"
    p.write_text("underlying,futures,option_expiries,strikes,no_options\n"
                 "NIFTY,1,1,all,false\n"
                 "RELIANCE,,,3,true\n")
    rows = fo.read_watchlist(p)
    assert rows[0] == {"underlying": "NIFTY", "futures": 1, "option_expiries": 1, "strikes": 10**6, "no_options": False}
    assert rows[1] == {"underlying": "RELIANCE", "futures": 2, "option_expiries": 1, "strikes": 3, "no_options": True}


def test_select_from_watchlist_gives_each_underlying_its_own_window(table):
    rows = [{"underlying": "NIFTY", "futures": 1, "option_expiries": 1, "strikes": 10**6, "no_options": False},
            {"underlying": "RELIANCE", "futures": 1, "option_expiries": 0, "strikes": 0, "no_options": True},
            {"underlying": "NOSUCH", "futures": 1, "option_expiries": 1, "strikes": 1, "no_options": False}]
    sel, notes = fo.select_from_watchlist(table, rows, DAY, spot={})
    nifty_opts = sel[(sel.underlying == "NIFTY") & (sel.instrument_type != "FUT")]
    assert sorted(nifty_opts.strike.unique()) == list(range(24000, 25001, 100))         # whole ladder, "all" honoured
    reliance = sel[sel.underlying == "RELIANCE"]
    assert set(reliance.instrument_type) == {"FUT"} and len(reliance) == 1              # no_options respected
    assert notes["missing"] == ["NOSUCH"] and notes["no_spot"] == ["NIFTY"]
    assert list(sel.columns) == fo.UNIVERSE_COLUMNS


def test_select_from_watchlist_empty_rows_is_a_frame():
    sel, notes = fo.select_from_watchlist(pd.DataFrame(columns=fo.COLUMNS), [], DAY)
    assert sel.empty and list(sel.columns) == fo.UNIVERSE_COLUMNS and notes == {"missing": [], "no_spot": []}


def test_cli_select_from_watchlist_default_path_and_explicit_path(world, tmp_path, capsys):
    f = tmp_path / "m.json"
    f.write_text(json.dumps(master_records()))
    fo.main(["import", "--file", str(f)])
    capsys.readouterr()
    wl = world.data / "fo_watchlist.csv"
    wl.write_text("underlying,futures,option_expiries,strikes,no_options\n"
                 "NIFTY,1,1,all,false\n"
                 "RELIANCE,1,0,0,true\n")
    fo.main(["select", "--day", "2026-10-23"])                                         # no --underlying -> default watchlist
    out = capsys.readouterr().out
    sel = pd.read_csv(world.data / "fo_universe.csv")
    assert set(sel.underlying) == {"NIFTY", "RELIANCE"} and "no spot given for ['NIFTY']" in out
    assert len(sel[(sel.underlying == "NIFTY") & (sel.instrument_type == "CE")]) == 11  # whole ladder
    assert set(sel[sel.underlying == "RELIANCE"].instrument_type) == {"FUT"}

    other = tmp_path / "other_wl.csv"
    other.write_text("underlying,futures,option_expiries,strikes,no_options\nRELIANCE,1,0,0,true\n")
    fo.main(["select", "--watchlist", str(other), "--day", "2026-10-23"])
    assert set(pd.read_csv(world.data / "fo_universe.csv").underlying) == {"RELIANCE"}


def test_cli_select_with_no_underlying_and_no_watchlist_file_errors(world, tmp_path):
    f = tmp_path / "m.json"
    f.write_text(json.dumps(master_records()))
    fo.main(["import", "--file", str(f)])
    with pytest.raises(SystemExit, match="not found"):
        fo.main(["select", "--day", "2026-10-23"])
