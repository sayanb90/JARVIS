"""lake_cleanup: empty tables and FMP leftovers are found and set aside, nothing else is touched, dry run changes nothing."""
from __future__ import annotations

import pandas as pd

from data_layer import lake_cleanup as lc


def table(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"symbol": ["A"] * rows, "v": [1.0] * rows}).to_parquet(path, index=False)


def build(root):
    lake = root / "lake"
    table(lake / "fundamentals" / "income_statement.parquet", 3)          # has rows: stays
    table(lake / "fundamentals" / "ratios.parquet", 0)                    # empty: moves
    table(lake / "fundamentals" / "fund_meta.parquet", 5)                 # FMP leftover even with rows
    table(lake / "ticks" / "segment=eq" / "date=2026-11-10" / "ticks.parquet", 0)     # never touched
    (lake / "reference").mkdir(parents=True)
    (lake / "reference" / "broken.parquet").write_bytes(b"not parquet")
    (root / "data" / "fundamentals").mkdir(parents=True)
    (root / "data" / "fundamentals" / "cash_flow_in.csv").write_text("a\n")
    (root / "data" / "fund_meta.csv").write_text("a\n")
    return lake


def names(items):
    return {(i.path.name, i.reason, i.move) for i in items}


def test_scan_finds_empty_tables_and_fmp_leftovers_only(tmp_path):
    lake = build(tmp_path)
    got = names(lc.scan(tmp_path, lake))
    assert got == {("ratios.parquet", "empty table", True), ("fund_meta.parquet", "FMP leftover", True),
                   ("cash_flow_in.csv", "FMP leftover", True), ("fund_meta.csv", "FMP leftover", True),
                   ("broken.parquet", "unreadable", False)}


def test_dry_run_changes_nothing_and_apply_moves_with_paths_kept(tmp_path, capsys):
    lake = build(tmp_path)
    assert lc.main(["--root", str(tmp_path)]) == 1                         # 1: the unreadable file is flagged
    assert (lake / "fundamentals" / "ratios.parquet").exists() and "DRY RUN" in capsys.readouterr().out
    lc.main(["--root", str(tmp_path), "--apply"])
    assert not (lake / "fundamentals" / "ratios.parquet").exists()
    assert not (lake / "fundamentals" / "fund_meta.parquet").exists() and not (tmp_path / "data" / "fund_meta.csv").exists()
    assert (lake / "fundamentals" / "income_statement.parquet").exists()                 # rows: untouched
    assert (lake / "reference" / "broken.parquet").exists()                             # reported only
    assert (lake / "ticks" / "segment=eq" / "date=2026-11-10" / "ticks.parquet").exists()
    dest = next((tmp_path / "logs").glob("lake-cleanup-*"))
    assert (dest / "lake" / "fundamentals" / "ratios.parquet").exists() and (dest / "data" / "fund_meta.csv").exists()
    assert not (tmp_path / "data" / "fundamentals").exists()                           # emptied folder removed


def test_clean_lake_reports_nothing_to_do(tmp_path, capsys):
    table(tmp_path / "lake" / "fundamentals" / "income_statement.parquet", 2)
    assert lc.main(["--root", str(tmp_path), "--apply"]) == 0
    assert "nothing to clean" in capsys.readouterr().out
