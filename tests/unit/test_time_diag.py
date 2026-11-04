"""The time-column analysis (SCRUM-128): known delays, shared last-trade times, a backwards step and a short column set."""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd

from data_layer import time_diag

DAY = date(2026, 11, 4)
T0 = datetime(2026, 11, 4, 4, 0, 30, 50_000)                 # 50 ms past a second, so a 20 ms lag never crosses a bar edge


def write_day(lake, with_times: bool = True) -> None:
    rows = []
    for i in range(1000):                                              # equity: unique fts every 100 ms, delay 5 ms, a new trade every 10th row
        fts = T0 + timedelta(milliseconds=100 * i)
        trade = i % 10 == 0
        rows.append({"symbol": "A.NS", "ts": fts - timedelta(milliseconds=20) if trade else T0 + timedelta(milliseconds=100 * (i - i % 10) - 20),
                     "ltp": 100.0, "vol_today": 10 * (i // 10 + 1), "fts": fts, "rts": fts + timedelta(milliseconds=5)})
    for i in range(500):                                               # option: delay 8 ms, but three rows arrive with an earlier fts and two have a negative delay
        fts = T0 + timedelta(milliseconds=200 * i)
        if i in (100, 200, 300):
            fts -= timedelta(milliseconds=500)
        delay = -80 if i in (50, 60) else 8
        rows.append({"symbol": "NIFTY_25000_CE_09_NOV_26", "ts": T0 - timedelta(seconds=30), "ltp": 5.0, "vol_today": 100,
                     "fts": fts, "rts": T0 + timedelta(milliseconds=200 * i + delay)})
    df = pd.DataFrame(rows)
    if not with_times:
        df = df.drop(columns=["fts", "rts"])
    folder = lake / "ticks" / f"date={DAY}"
    folder.mkdir(parents=True)
    df.to_parquet(folder / "ticks.parquet")


def test_the_measurements_match_what_was_built_in(tmp_path):
    lake = tmp_path / "lake"
    write_day(lake)
    before = sorted(p.relative_to(tmp_path) for p in tmp_path.rglob("*"))
    m = time_diag.analyse(DAY, lake, tmp_path / "spill")
    assert m["rows"] == {"equity": 1000, "fo": 500}
    assert m["delay"]["equity"]["median_ms"] == 5 and m["delay"]["equity"]["negative"] == 0
    assert m["delay"]["fo"]["negative"] > 0 and m["delay"]["fo"]["negative_over_50ms"] > 0                     # the -80 ms rows
    assert m["columns"]["fts"]["equity"]["rows_sharing_a_value_pct"] == 0                                        # fts is unique per instrument
    assert m["columns"]["ts"]["equity"]["rows_sharing_a_value_pct"] > 90                                         # ts repeats between trades
    t = m["trade_vs_message"]["equity"]
    assert t["fts_minus_ts_median_ms"] == 20 and t["within_100ms_pct"] == 100.0 and t["over_5s_pct"] == 0
    assert t["new_trade_rows_in_a_different_bar_pct"] == {"1s": 0.0, "5s": 0.0, "60s": 0.0}                       # 20 ms never changes a bar here
    assert m["trade_vs_message"]["fo"]["stale_ts_median_ms"] > 30_000                                            # the option's ts is stale
    assert m["order"]["fo"]["fts_goes_backwards_pct"] > 0 and m["order"]["fo"]["largest_fts_step_back_ms"] == -300
    assert m["order"]["equity"]["fts_goes_backwards_pct"] == 0
    assert m["bounds"]["fts"]["before_open"] == 0 and m["bounds"]["fts"]["after_close"] == 0                     # the test day lies inside the session
    assert m["repeats"]["equity"]["repeated_pct"] is not None
    assert any(line.startswith("fts as the event time of every row") for line in m["verdict"])
    assert before == sorted(p.relative_to(tmp_path) for p in tmp_path.rglob("*") if "spill" not in p.parts)       # nothing written beside the data


def test_a_day_without_the_new_columns_says_so(tmp_path):
    write_day(tmp_path / "lake", with_times=False)
    m = time_diag.analyse(DAY, tmp_path / "lake", tmp_path / "spill")
    assert "no fts/rts columns" in m["error"]
    assert "no fts/rts columns" in time_diag.render(m)
    assert "no tick files" in time_diag.analyse(date(2020, 1, 1), tmp_path / "lake", tmp_path / "spill")["error"]


def test_the_command_line_writes_the_report_and_cleans_up(tmp_path, capsys):
    write_day(tmp_path / "lake")
    out = tmp_path / "logs" / "time.md"
    assert time_diag.main(["--day", str(DAY), "--lake", str(tmp_path / "lake"), "--logs", str(tmp_path / "logs"), "--out", str(out), "--skip-repeats"]) == 0
    text = out.read_text()
    assert text.startswith("# Time columns of the ticks of 2026-11-04") and "## Verdict" in text and "## 6." not in text
    assert "saved to" in capsys.readouterr().out
