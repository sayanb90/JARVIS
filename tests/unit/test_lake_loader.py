"""Lake loader edge cases and the verify command's PASS/WARN/FAIL logic."""
import sys
from datetime import date, datetime, timedelta

import pytest

from data_layer.ticks import BarAggregator, Tick
from tests.helpers import BAR_H, OHLC_H, write_csv

TODAY = date.today()


def ohlc_rows(sym, mkt, days, start_px=100.0, step=0.0, d0=None, vol=10):
    d0 = d0 or TODAY - timedelta(days=days)
    out, px = [], start_px
    for i in range(days):
        d = d0 + timedelta(days=i)
        if d.weekday() >= 5:
            continue
        px += step
        out.append([f"{d:%Y.%m.%d}", sym, mkt, px, px + 1, px - 1, px, vol])
    return out


# -- loader edge cases ----------------------------------------------------------------------------
def test_load_with_no_csvs_is_harmless(world, caplog):
    world.lake.load_ohlc()
    assert world.lake.load_bars("5m") == 0
    world.lake.load_fundamentals()
    assert not world.lakedir.exists() or not list(world.lakedir.rglob("*.parquet"))
    assert "no OHLC CSVs" in caplog.text


def test_blank_volume_and_numeric_cells_load_as_null(world):
    write_csv(world.data / "ohlc/ohlc_2026_in.csv", OHLC_H, [["2026.10.21", "A.NS", "IN", 1, 2, 0.5, 1.5, ""]])
    world.lake.load_ohlc(("IN",))
    df = world.query.Lake().sql("SELECT * FROM ohlc")
    assert df.volume.isna().all() and df.close[0] == 1.5


def test_both_date_formats_and_timestamp_formats_are_accepted(world):
    write_csv(world.data / "ohlc/ohlc_2026_in.csv", OHLC_H,
              [["2026.10.21", "A.NS", "IN", 1, 2, 0.5, 1.5, 1], ["2026-10-22", "A.NS", "IN", 1, 2, 0.5, 1.6, 1]])
    write_csv(world.data / "bars/5m/in/2026-10-21.csv", BAR_H,
              [["2026.10.21D03:45:00.000000000", "A.NS", "IN", 1, 2, 0.5, 1, 1],
               ["2026-10-21T03:50:00", "A.NS", "IN", 1, 2, 0.5, 2, 1]])
    world.lake.load_ohlc(("IN",)); world.lake.load_bars("5m", date(2026, 10, 1), ("IN",))
    q = world.query.Lake()
    assert len(q.sql("SELECT * FROM ohlc")) == 2
    assert [str(t) for t in q.sql("SELECT ts FROM bars_5m ORDER BY ts").ts] == ["2026-10-21 03:45:00", "2026-10-21 03:50:00"]


def test_bars_since_filter_and_ignored_filenames(world):
    row = lambda d: [[f"{d}T03:45:00", "A.NS", "IN", 1, 2, 0.5, 1, 1]]  # noqa: E731
    for name in ("2026-10-01", "2026-10-20", "notes", "2026-10-21.bak"):
        write_csv(world.data / f"bars/5m/in/{name}.csv", BAR_H, row("2026-10-20"))
    assert world.lake.load_bars("5m", date(2026, 10, 15), ("IN",)) == 1
    days = sorted(p.name for p in (world.lakedir / "bars/interval=5m").iterdir())
    assert days == ["date=2026-10-20"]


def test_upsert_preserves_other_rows_and_leaves_no_temp_files(world):
    write_csv(world.data / "ohlc/ohlc_2026_in.csv", OHLC_H,
              [["2026.10.20", "A.NS", "IN", 1, 2, 0, 1, 1], ["2026.10.21", "A.NS", "IN", 1, 2, 0, 2, 1]])
    world.lake.load_ohlc(("IN",))
    write_csv(world.data / "ohlc/ohlc_daily_in.csv", OHLC_H,
              [["2026.10.21", "A.NS", "IN", 1, 2, 0, 9, 1], ["2026.10.22", "A.NS", "IN", 1, 2, 0, 3, 1]])
    world.lake.load_ohlc(("IN",), daily=True)
    assert world.query.Lake().ohlc("A.NS").close.tolist() == [1.0, 9.0, 3.0]
    assert not list(world.lakedir.rglob("*.tmp"))


def test_cli_bars_for_us_loads_nothing_instead_of_crashing(world, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["lake", "--load", "bars", "--market", "US"])
    world.lake.main()
    monkeypatch.setattr(sys, "argv", ["lake", "--load", "fundamentals", "--market", "UK"])
    world.lake.main()


def test_cli_daily_and_all(world, monkeypatch):
    write_csv(world.data / "ohlc/ohlc_daily_in.csv", OHLC_H, [["2026.10.21", "A.NS", "IN", 1, 2, 0, 2, 1]])
    monkeypatch.setattr(sys, "argv", ["lake", "--load", "daily", "--market", "IN"])
    world.lake.main()
    assert len(world.query.Lake().sql("SELECT * FROM ohlc")) == 1


# -- verify ohlc ------------------------------------------------------------------------------------
def load(world, rows_by_market):
    for m, rows in rows_by_market.items():
        write_csv(world.data / f"ohlc/ohlc_2026_{m.lower()}.csv", OHLC_H, rows)
    world.lake.load_ohlc()
    return world.query.Lake()


def out_of(capsys):
    return capsys.readouterr().out


def test_verify_ohlc_without_table_fails(world):
    from data_layer import verify
    assert verify.verify_ohlc(world.query.Lake()).fails == 1


def test_verify_ohlc_clean_data_has_no_fail(world, capsys):
    from data_layer import verify
    world.set_universe({"IN": ["A.NS"]})
    q = load(world, {"IN": ohlc_rows("A.NS", "IN", 1830, step=0.01, d0=TODAY - timedelta(days=1830))})
    rep = verify.verify_ohlc(q, years=5, today=TODAY)
    assert rep.fails == 0, out_of(capsys)


def test_verify_ohlc_flags_each_problem(world, capsys):
    from data_layer import verify
    world.set_universe({"IN": ["GOOD.NS", "GHOST.NS", "ZERO.NS", "JUMP.NS", "OLD.NS", "GAP.NS"]})
    d0 = TODAY - timedelta(days=60)
    rows = ohlc_rows("GOOD.NS", "IN", 60, d0=d0)
    zero = ohlc_rows("ZERO.NS", "IN", 60, d0=d0); zero[3][6] = 0
    jump = ohlc_rows("JUMP.NS", "IN", 60, d0=d0); jump[10][6] = jump[9][6] * 3
    old = ohlc_rows("OLD.NS", "IN", 5, d0=TODAY - timedelta(days=40))
    gap = ohlc_rows("GAP.NS", "IN", 10, d0=d0) + ohlc_rows("GAP.NS", "IN", 10, d0=TODAY - timedelta(days=15))
    q = load(world, {"IN": rows + zero + jump + old + gap})
    rep = verify.verify_ohlc(q, years=5, today=TODAY)
    out = out_of(capsys)
    assert rep.fails == 2                                            # GHOST has no data; ZERO has a zero close
    assert "[FAIL] IN symbols with no data: ['GHOST.NS']" in out
    assert "[FAIL] IN null/zero close" in out
    assert "[WARN] IN one-day moves over 40%" in out and "JUMP.NS" in out
    assert "[WARN] IN last bar older than 7 days" in out and "OLD.NS" in out
    assert "[WARN] IN symbols with a >10 day gap" in out and "GAP.NS" in out
    assert "[WARN] IN history starting after" in out                  # 60 days is far short of 5 years


# -- verify bars ------------------------------------------------------------------------------------
DAY = date(2026, 10, 23)


def session_bars(sym="A.NS", minutes=375, px=100.0, skip=()):
    ag, out = BarAggregator(60), []
    for k in range(minutes):
        if k in skip:
            continue
        out += ag.add(Tick(datetime(2026, 10, 23, 3, 45) + timedelta(minutes=k), sym, px + k * 0.01, 1000 + 10 * k))
    return out + ag.flush_all()


def test_verify_bars_missing_view_or_day_fails(world):
    from data_layer import verify
    assert verify.verify_bars(world.query.Lake(), DAY).fails == 1
    world.tick_store.write_bars(session_bars(), "1m")
    assert verify.verify_bars(world.query.Lake(), date(2026, 10, 24)).fails == 1


def test_verify_bars_full_session_passes_and_compares_with_yahoo(world, capsys):
    from data_layer import verify
    world.set_universe({"IN": ["A.NS"]})
    bars = session_bars()
    world.tick_store.write_bars(bars, "1m")
    last_close = bars[-1].close
    total_vol = sum(b.volume for b in bars)
    write_csv(world.data / "ohlc/ohlc_2026_in.csv", OHLC_H, [["2026.10.23", "A.NS", "IN", 1, 2, 0, last_close, total_vol]])
    world.lake.load_ohlc(("IN",))
    rep = verify.verify_bars(world.query.Lake(), DAY)
    out = out_of(capsys)
    assert rep.fails == 0, out
    assert "[PASS] first bar 2026-10-23 03:45:00" in out and "[PASS] last-bar close vs Yahoo" in out
    assert "median 1.00" in out


def test_verify_bars_detects_duplicates_bad_ohlc_gaps_and_close_mismatch(world, capsys):
    from data_layer import verify
    world.set_universe({"IN": ["A.NS", "NOBARS.NS"]})
    bars = session_bars(minutes=100)
    world.tick_store.write_bars(bars, "1m")
    world.tick_store.write_bars(bars[:5], "1m")                      # uncompacted duplicate segment
    bad = session_bars("B.NS", minutes=3)
    bad[0].low = bad[0].high + 1                                     # low above high
    world.tick_store.write_bars(bad, "1m")
    write_csv(world.data / "ohlc/ohlc_2026_in.csv", OHLC_H, [["2026.10.23", "A.NS", "IN", 1, 2, 0, 999.0, 5]])
    world.lake.load_ohlc(("IN",))
    rep = verify.verify_bars(world.query.Lake(), DAY)
    out = out_of(capsys)
    assert rep.fails == 2
    assert "[FAIL] duplicate (symbol, ts): 5" in out
    assert "[FAIL] bars where open/close fall outside low-high: 1" in out
    assert "[WARN] symbols with under 50% of 375 bars" in out
    assert "[WARN] last-bar close vs Yahoo daily close differs >0.5%: ['A.NS']" in out
    assert "[WARN] universe symbols with no bars" in out and "NOBARS.NS" in out
    assert "[WARN] last bar" in out                                    # 100 minutes never reaches 09:55 UTC


def test_verify_cli_exit_codes(world, monkeypatch, capsys):
    from data_layer import verify
    monkeypatch.setattr(sys, "argv", ["verify", "ohlc"])
    with pytest.raises(SystemExit) as e:
        verify.main()
    assert e.value.code == 1
