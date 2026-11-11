"""The fundamentals diagnostic: synthetic logs and tables with known faults, and the findings it must report."""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd

from data_layer import fundamentals_diag as fd

TODAY = date.today()


def day(n: int) -> date:
    return TODAY - timedelta(days=n)


def build(tmp_path):
    root, lake = tmp_path / "root", tmp_path / "lake"
    (root / "data").mkdir(parents=True)
    (root / "logs").mkdir()
    up = lake / "upstox_fundamentals"
    up.mkdir(parents=True)
    (lake / "fundamentals").mkdir(parents=True)

    isins = [f"INE{i:09d}" for i in range(300)]
    (root / "data" / "universe.csv").write_text(
        "symbol,name,market,isin\n" + "".join(f"S{i}.NS,S{i},IN,{isins[i]}\n" for i in range(298)) + "BAD.NS,Bad,IN,\nDUP.NS,D,IN,INE000000001\nDUP.NS,D,IN,INE000000001\n")

    # one full run on day(2): calls 1..1200 ok, then the quota runs out and everything fails with 429
    rows, stamp = [], f"{day(2)}T12:50:01"
    n = 0
    for i, isin in enumerate(isins):
        for ep in ("key_ratios", "profile", "balance", "cashflow"):
            n += 1
            ok = n <= 700
            rows.append({"isin": isin, "symbol": f"S{i}.NS", "endpoint": ep, "fetched_at": stamp, "ok": ok, "status": 200 if ok else 429,
                         "error_code": "" if ok else "UDAPI10005", "rows": 5 if ok else 0})
    # one company that fails on three different days on one endpoint, never succeeding
    for k in (5, 4, 3):
        rows.append({"isin": "INE999999999", "symbol": "WEIRD.NS", "endpoint": "profile", "fetched_at": f"{day(k)}T12:50:01",
                     "ok": False, "status": 400, "error_code": "UDAPI100011", "rows": 0})
    pd.DataFrame(rows).to_parquet(up / "fetch_log.parquet", index=False)
    pd.DataFrame({"isin": isins[:100], "endpoint": "key_ratios", "item": "P/E", "period": "snapshot", "value": 1.0, "text": None,
                  "change": None, "first_seen": str(day(2)), "last_seen": str(day(2))}).to_parquet(up / "versions.parquet", index=False)

    d1, d2 = day(2), day(1)
    (root / "logs" / "fundamentals.log").write_text(
        f"{d1} 12:50:01,100 INFO fundamentals: 300 companies, endpoints key_ratios,profile,balance,cashflow, max age None days\n"
        + "".join(f"{d1} 13:{m:02d}:00,000 WARNING /v2/fundamentals/INE{m:09d}/profile -> HTTP 429 (attempt 1/3), waiting 2s\n" for m in range(10, 15))
        + f"{d2} 12:50:01,100 INFO fundamentals: 300 companies, endpoints key_ratios, max age None days\n")      # no 'done' line: killed
    return root, lake


def text_of(tmp_path):
    root, lake = build(tmp_path)
    return fd.analyse(lake, root, days=30).text()


def test_it_reports_runs_that_never_finished_and_days_without_a_run(tmp_path):
    t = text_of(tmp_path)
    assert f"The run of {day(1):%Y-%m-%d} 12:50 has no 'fundamentals done' line" in t
    assert "No Upstox fundamentals run on" not in t or day(40).isoformat() not in t        # nothing is said about days before the log begins
    assert "Every retry waited 2 s" in t


def test_it_reports_reference_data_gaps(tmp_path):
    t = text_of(tmp_path)
    assert "1 universe stocks have no usable ISIN" in t and "Duplicate symbols in the universe: DUP.NS" in t


def test_missing_files_are_reported_not_fatal(tmp_path):
    rep = fd.analyse(tmp_path / "lake", tmp_path / "root", days=7)
    t = rep.text()
    assert "No Upstox fetch log at all" in t and "[PROBLEM]" in t


def test_the_command_writes_its_report_and_exits_nonzero_on_a_problem(tmp_path, capsys):
    root, lake = build(tmp_path)
    assert fd.main(["--lake", str(lake), "--root", str(root)]) == 1
    assert list((root / "logs").glob("fundamentals-diag-*.md"))
    assert "Findings" in capsys.readouterr().out
