"""Read-only end-of-day diagnostics (SCRUM-115): gaps in the progress log, errors, memory, identical rows, stored Greeks."""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from data_layer import day_diag as dd

DAY = date(2026, 11, 3)


def progress(t: str, n: int) -> str:
    return f"{DAY} {t},123  INFO      frames={n} ticks={n * 60} ticks_written=6000 policy_dropped=0 stale=1 future=0"


FO_LOG = "\n".join([
    f"{DAY - timedelta(days=1)} 10:05:36,000  INFO      session done",                       # yesterday's line: must not count
    f"{DAY} 03:40:01,000  INFO      subscribed 1061 instruments in mode full",
    progress("08:18:19", 1), progress("08:18:35", 2), progress("08:18:51", 3),
    f"{DAY} 08:20:17,135  INFO      resources: cpu 1.3% load 0.04",
    f"{DAY} 08:30:17,135  INFO      resources: cpu 1.1% load 0.05",
    f"{DAY} 09:00:00,000  ERROR     boom",
    "Traceback (most recent call last):", '  File "x.py", line 1', "ValueError: bad", "after the traceback",
    f"{DAY} 10:05:36,000  INFO      session {DAY} done: compacted 14827427 ticks",
])


def test_progress_gaps_find_the_silence_to_the_end_of_the_file_and_between_lines():
    gaps = dd.progress_gaps(dd.day_text(FO_LOG, DAY))
    assert gaps == [{"after": "08:18:51", "until": "10:05:36", "gap_s": 6405, "to_end_of_file": True}]
    steady = "\n".join(progress(f"08:{m:02d}:{s:02d}", i) for i, (m, s) in enumerate((m, s) for m in range(0, 5) for s in (0, 30)))
    assert dd.progress_gaps(steady) == []
    mid = "\n".join([progress("08:00:00", 1), progress("08:00:15", 2), progress("08:10:15", 3), progress("08:10:30", 4)])
    assert dd.progress_gaps(mid) == [{"after": "08:00:15", "until": "08:10:15", "gap_s": 600}]


def test_day_text_window_and_error_lines():
    text = dd.day_text(FO_LOG, DAY)
    assert "session done" not in text.splitlines()[0] and text.splitlines()[0].startswith(f"{DAY} 03:40:01")
    assert dd.day_text(FO_LOG, date(2030, 1, 1)) == ""
    assert [line[11:19] for line in dd.window(text, "08:18:30", "08:20:20")] == ["08:18:35", "08:18:51", "08:20:17"]
    errs = dd.error_lines(text)
    assert errs[0].endswith("ERROR     boom") and "ValueError: bad" in errs and "after the traceback" not in errs      # the error and the three lines after it
    assert dd.error_lines("all fine\nnothing here") == []
    assert len(dd.error_lines("\n".join(["x ERROR y"] * 100), context=0, max_lines=10)) == 10


def write_resources(path: Path, rows: list[tuple[str, str, float, float]]) -> None:
    path.write_text("\n".join(json.dumps({"ts": f"{DAY}T{t}:00Z", "procs": [{"pid": 1, "cmd": cmd, "rss_mb": rss, "peak_rss_mb": peak}]})
                              for t, cmd, rss, peak in rows) + "\n")


def test_rss_trend_picks_the_nearest_sample_and_the_right_process(tmp_path):
    f = tmp_path / "r.jsonl"
    write_resources(f, [("04:02", "data_layer.upstox.feed --once --segment fo", 285.0, 288.0), ("08:16", "data_layer.upstox.feed --once --segment fo", 292.5, 298.6),
                        ("08:29", "data_layer.upstox.feed --once --segment fo", 499.1, 499.1), ("10:01", "data_layer.upstox.feed --once --segment fo", 2203.4, 2203.4)])
    t = dict((m, (r, p)) for m, r, p in dd.rss_trend(f, DAY, "feed"))
    assert t["04:00"] == (285.0, 288.0) and t["08:15"] == (292.5, 298.6) and t["08:30"] == (499.1, 499.1) and t["10:00"] == (2203.4, 2203.4)
    assert t["06:00"] == (None, None) and t["09:00"] == (None, None)                              # no sample within 15 minutes
    assert all(r is None for _, r, _ in dd.rss_trend(f, DAY, "feed", exclude=("segment fo",)))      # the equity view excludes the F&O process
    assert all(r is None for _, r, _ in dd.rss_trend(tmp_path / "missing.jsonl", DAY, "feed"))


def test_identical_rows_are_counted_per_class(tmp_path):
    base = datetime(2026, 11, 3, 4, 0)
    rows = [{"ts": base + timedelta(seconds=i), "symbol": s, "ltp": 1.0, "iv": 0.2 if s.endswith("CE_03_NOV_26") else None}
            for s in ("RELIANCE.NS", "NIFTY_25000_CE_03_NOV_26") for i in range(10)]
    df = pd.DataFrame(rows)
    f = tmp_path / "ticks.parquet"
    pd.concat([df, df.iloc[:4], df.iloc[10:13]]).to_parquet(f)
    d = dd.duplicate_counts(f)
    assert d["equity"] == {"total": 14, "distinct": 10, "duplicates": 4, "pct": 28.6}
    assert d["fo"] == {"total": 13, "distinct": 10, "duplicates": 3, "pct": 23.1}


def test_timestamp_sharing_counts_rows_that_carry_another_rows_time(tmp_path):
    base = datetime(2026, 11, 3, 4, 0)
    rows = [{"ts": base, "symbol": "A.NS", "ltp": 1.0}] * 3 + [{"ts": base + timedelta(seconds=5), "symbol": "A.NS", "ltp": 1.0}]
    rows += [{"ts": base + timedelta(seconds=i), "symbol": "NIFTY_25000_CE_03_NOV_26", "ltp": 1.0} for i in range(4)]
    f = tmp_path / "ticks.parquet"
    pd.DataFrame(rows).to_parquet(f)
    ts = dd.timestamp_sharing(f)
    assert ts["equity"] == {"rows": 4, "distinct_ts": 2, "rows_sharing_a_ts": 3, "max_rows_one_ts": 3, "instruments": 1, "pct_sharing": 75.0}
    assert ts["fo"]["pct_sharing"] == 0.0 and ts["fo"]["rows"] == 4 and ts["fo"]["distinct_ts"] == 4
    assert "pct_sharing_fts" not in ts["equity"]                                                  # files without event time say nothing about it
    df = pd.DataFrame(rows)
    df["fts"] = [base + timedelta(seconds=i) for i in range(len(df))]                              # every row has its own event time
    g = tmp_path / "with_fts.parquet"
    df.to_parquet(g)
    both = dd.timestamp_sharing(g)
    assert both["equity"]["pct_sharing"] == 75.0 and both["equity"]["pct_sharing_fts"] == 0.0 and both["equity"]["max_rows_one_fts"] == 1


def stream(tmp_path: Path, iv: float = 0.15) -> Path:
    folder = tmp_path / "stream_fo" / "ticks" / "date=2026-11-02"
    folder.mkdir(parents=True)
    base = datetime(2026, 11, 2, 4, 0)
    n = 30
    pd.DataFrame({"ts": [base + timedelta(seconds=i) for i in range(n)], "symbol": "NIFTY_25000_CE_03_NOV_26", "ltp": 100.0,
                  "iv": [iv if i % 3 else None for i in range(n)], "delta": [0.05 + i * 0.03 for i in range(n)], "gamma": [0.0001 * (i % 5) for i in range(n)],
                  "theta": [-i / 7 for i in range(n)], "vega": [i / 3 for i in range(n)], "rho": 1.0}).to_parquet(folder / "ticks.parquet")
    return tmp_path / "stream_fo"


def test_greek_report_shows_the_units_and_where_iv_is_missing(tmp_path):
    g = dd.greek_report(stream(tmp_path))
    assert g["ce_rows"] == 30 and g["iv_rows"] == 20 and g["iv_min"] == pytest.approx(0.15) and g["iv_max"] == pytest.approx(0.15)
    assert g["distinct_gamma"] == 5 and g["distinct_delta"] == 30
    assert sum(b for _, b in g["iv_missing_by_delta"].values()) == 30 and g["iv_missing_by_delta"]["delta 0.1 to 0.9"][0] > 0
    assert dd.greek_report(tmp_path / "nothing") == {}


def test_full_report_and_command_line(tmp_path, capsys):
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "fo-feed.log").write_text(FO_LOG + "\n")
    write_resources(logs / f"resources-fo-{DAY:%Y%m%d}.jsonl", [("04:01", "feed --segment fo", 290.0, 295.0), ("10:00", "feed --segment fo", 2200.0, 2300.0)])
    lake = tmp_path / "lake"
    folder = lake / "ticks" / f"date={DAY}"
    folder.mkdir(parents=True)
    pd.DataFrame({"ts": [datetime(2026, 11, 3, 4, 0)] * 2, "symbol": ["A.NS", "A.NS"], "ltp": [1.0, 1.0]}).to_parquet(folder / "ticks.parquet")
    probe = stream(tmp_path)
    assert dd.main(["--day", "2026-11-03", "--logs", str(logs), "--lake", str(lake), "--probe", str(probe)]) == 0
    out = capsys.readouterr().out
    report = (logs / "diag-20261103.md").read_text()
    assert report in out
    assert "F&O progress log silent from 08:18:51 to 10:05:36 UTC (106 min)" in report
    assert "F&O feed memory grew 8x" in report and "Identical rows: equity 50.0%, fo 0.0%" in report and "Rows sharing a last-trade time" in report and "equity 100.0%" in report and "gamma has 5 distinct values" in report
    assert "ValueError: bad" in report and f"{DAY} 08:18:51" not in report.splitlines()[2]
    assert dd.main(["--day", "2026-11-04", "--logs", str(logs), "--lake", str(lake), "--probe", str(tmp_path / "none")]) == 0
    assert "No F&O feed log lines for the day" in (logs / "diag-20261104.md").read_text()
