"""
End-of-day diagnostics, read-only (SCRUM-115): the questions worth asking after a live session, answered from files.

  python -m data_layer.day_diag                       # today (UTC); report to logs/diag-<day>.md and the screen
  python -m data_layer.day_diag --day 2026-11-03 --probe probe_data/stream_fo

  1. silence in the feed's progress log: the "frames=... ticks=..." line is written every 15 s; a gap means the flush loop stopped
     (2026-11-03: the F&O feed went quiet at 08:18:51 UTC and its memory grew from 0.3 to 6 GB)
  2. every error line in the feed log, with context
  3. memory of the feed processes over the day, from logs/resources-*.jsonl
  4. rows that are identical in every column in the day's ticks, per class (equity, F&O)
  5. the stored option Greeks of a stream folder: units and ranges, distinct values, IV missing by delta bucket

Nothing is changed; the only file written is the report.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import duckdb

from . import lake as lake_mod

STAMP = re.compile(r"^(\d{4}-\d\d-\d\d) (\d\d):(\d\d):(\d\d)")
PROGRESS = re.compile(r"frames=\d+ ticks=\d+")
ERRORS = re.compile(r"Traceback|ERROR|CRITICAL|Exception|never retrieved")
MARKS = ("04:00", "06:00", "08:00", "08:15", "08:30", "09:00", "10:00")
FO = "regexp_matches(symbol, '_(CE|PE|FUT)_')"


def _seconds(line: str) -> Optional[int]:
    m = STAMP.match(line)
    return int(m[2]) * 3600 + int(m[3]) * 60 + int(m[4]) if m else None


def _clock(sec: int) -> str:
    return f"{sec // 3600:02d}:{sec % 3600 // 60:02d}:{sec % 60:02d}"


def progress_gaps(text: str, min_gap_s: int = 180) -> list[dict]:
    """Stretches in which the progress line is missing: between two progress lines further apart than `min_gap_s`, and from the
    last progress line to the last timestamped line of the file."""
    marks = [s for line in text.splitlines() if PROGRESS.search(line) and (s := _seconds(line)) is not None]
    stamps = [s for line in text.splitlines() if (s := _seconds(line)) is not None]
    out = []
    for a, b in zip(marks, marks[1:]):
        if b - a > min_gap_s:
            out.append({"after": _clock(a), "until": _clock(b), "gap_s": b - a})
    if marks and stamps and stamps[-1] - marks[-1] > min_gap_s:
        out.append({"after": _clock(marks[-1]), "until": _clock(stamps[-1]), "gap_s": stamps[-1] - marks[-1], "to_end_of_file": True})
    return out


def day_text(text: str, day: date) -> str:
    """The part of a log that belongs to `day`: from its first timestamped line to its last, untimestamped lines (tracebacks) included."""
    lines = text.splitlines()
    idx = [i for i, line in enumerate(lines) if line.startswith(f"{day} ")]
    return "\n".join(lines[idx[0]: idx[-1] + 1]) if idx else ""


def window(text: str, start: str, end: str) -> list[str]:
    """Log lines whose time of day lies between `start` and `end` (HH:MM:SS)."""
    def sec(t: str) -> int:
        h, m, s = (int(x) for x in t.split(":"))
        return h * 3600 + m * 60 + s
    lo, hi = sec(start), sec(end)
    return [line for line in text.splitlines() if (s := _seconds(line)) is not None and lo <= s <= hi]


def error_lines(text: str, context: int = 3, max_lines: int = 80) -> list[str]:
    lines, out, i = text.splitlines(), [], 0
    while i < len(lines) and len(out) < max_lines:
        if ERRORS.search(lines[i]):
            out += lines[i:i + 1 + context]
            i += 1 + context
        else:
            i += 1
    return out[:max_lines]


def rss_trend(jsonl: Path, day: date, match: str, exclude: tuple = (), marks: tuple = MARKS) -> list[tuple[str, Optional[float], Optional[float]]]:
    """(mark, RSS MB, peak RSS MB) of the first process whose command contains `match`, from the sample nearest each HH:MM of
    `day` (within 15 minutes); None where there is no sample."""
    samples = []
    if jsonl.exists():
        for line in jsonl.read_text().splitlines():
            try:
                rec = json.loads(line)
                t = datetime.strptime(rec["ts"], "%Y-%m-%dT%H:%M:%SZ")
            except (ValueError, KeyError):
                continue
            if t.date() == day:
                samples.append((t, rec.get("procs") or []))
    out = []
    for mark in marks:
        h, m = (int(x) for x in mark.split(":"))
        want = datetime(day.year, day.month, day.day, h, m)
        near = min(samples, key=lambda s: abs(s[0] - want), default=None)
        row = (mark, None, None)
        if near and abs(near[0] - want) <= timedelta(minutes=15):
            for p in near[1]:
                cmd = p.get("cmd", "")
                if match in cmd and not any(x in cmd for x in exclude):
                    row = (mark, p.get("rss_mb"), p.get("peak_rss_mb"))
                    break
        out.append(row)
    return out


def duplicate_counts(ticks_file: Path) -> dict[str, dict]:
    """Rows, rows after SELECT DISTINCT * (all columns) and the difference, for equity and F&O rows of one compacted day."""
    con = duckdb.connect()
    out = {}
    for name, cond in (("equity", f"NOT {FO}"), ("fo", FO)):
        total, distinct = con.execute(
            f"SELECT (SELECT count(*) FROM read_parquet('{ticks_file}') WHERE {cond}), "
            f"(SELECT count(*) FROM (SELECT DISTINCT * FROM read_parquet('{ticks_file}') WHERE {cond}))").fetchone()
        out[name] = {"total": total, "distinct": distinct, "duplicates": total - distinct,
                     "pct": round(100.0 * (total - distinct) / total, 1) if total else 0.0}
    return out


def greek_report(stream: Path) -> dict:
    """Units and ranges of the stored option Greeks (call options): what a calibration needs to know."""
    root = stream / "ticks" if (stream / "ticks").is_dir() else stream
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return {}
    src = f"read_parquet({[str(f) for f in files]!r}, union_by_name=true, hive_partitioning=false)"
    con = duckdb.connect()
    ce = f"contains(symbol, '_CE_')"
    row = con.execute(f"""SELECT count(*), count(iv), min(iv), quantile_cont(iv, 0.5), max(iv), count(DISTINCT delta), count(DISTINCT gamma),
                                 count(DISTINCT theta), count(DISTINCT vega), max(gamma), min(theta), max(vega)
                          FROM {src} WHERE {ce}""").fetchone()
    keys = ("ce_rows", "iv_rows", "iv_min", "iv_median", "iv_max", "distinct_delta", "distinct_gamma", "distinct_theta", "distinct_vega",
            "gamma_max", "theta_min", "vega_max")
    out = dict(zip(keys, row))
    out["iv_missing_by_delta"] = {
        label: (con.execute(f"SELECT count(*) FILTER (WHERE iv IS NULL), count(*) FROM {src} WHERE {ce} AND {cond}").fetchone())
        for label, cond in (("delta < 0.1", "delta < 0.1"), ("delta 0.1 to 0.9", "delta BETWEEN 0.1 AND 0.9"), ("delta > 0.9", "delta > 0.9"))}
    return out


def build_report(day: date, logs: Path, lake: Path, probe: Optional[Path]) -> str:
    body, summary = [], []
    fo_log = logs / "fo-feed.log"
    text = day_text(fo_log.read_text(errors="replace"), day) if fo_log.exists() else ""
    # 1 and 2: the progress log and errors
    gaps = progress_gaps(text)
    body += ["## 1. Silence in the F&O feed's progress log", ""]
    if not text:
        body.append(f"No lines for {day} in {fo_log}.")
        summary.append("No F&O feed log lines for the day.")
    elif not gaps:
        body.append("The progress line appears at least every 3 minutes all the way through the file.")
        summary.append("F&O progress log: no gaps.")
    else:
        for g in gaps:
            body.append(f"- no progress line from {g['after']} to {g['until']} UTC ({g['gap_s'] // 60} min)" + (" (to the end of the file)" if g.get("to_end_of_file") else ""))
        g = max(gaps, key=lambda x: x["gap_s"])
        summary.append(f"F&O progress log silent from {g['after']} to {g['until']} UTC ({g['gap_s'] // 60} min).")
        a = int(g["after"][:2]) * 3600 + int(g["after"][3:5]) * 60 + int(g["after"][6:])
        body += ["", f"Log lines from {_clock(max(0, a - 180))} to {_clock(a + 240)}:", "```", *window(text, _clock(max(0, a - 180)), _clock(a + 240)), "```"]
    errs = error_lines(text)
    body += ["", "## 2. Error lines in the F&O feed log", "", "```", *(errs or ["none"]), "```", ""]
    summary.append(f"{len([e for e in errs if ERRORS.search(e)])} error-looking line(s) in the F&O log.")
    # 3: memory
    body += ["## 3. Memory (RSS MB, peak) over the day", ""]
    for label, jsonl, match, exclude in (("F&O feed", logs / f"resources-fo-{day:%Y%m%d}.jsonl", "feed", ()),
                                         ("equity feed", next(iter(sorted(logs.glob("resources-eq-*.jsonl"))), logs / "resources-eq.jsonl"), "feed", ("segment fo",))):
        trend = rss_trend(jsonl, day, match, exclude)
        body.append(f"- {label}: " + ", ".join(f"{m} {('%.0f' % r) if r is not None else '-'} ({('%.0f' % p) if p is not None else '-'})" for m, r, p in trend))
        known = [r for _, r, _ in trend if r is not None]
        if len(known) >= 2 and known[-1] > 3 * known[0]:
            summary.append(f"{label} memory grew {known[-1] / known[0]:.0f}x over the day ({known[0]:.0f} to {known[-1]:.0f} MB).")
    # 4: duplicates
    ticks = lake / "ticks" / f"date={day}" / "ticks.parquet"
    body += ["", "## 4. Rows identical in every column (SELECT DISTINCT *)", ""]
    if ticks.exists():
        d = duplicate_counts(ticks)
        for k, v in d.items():
            body.append(f"- {k}: {v['total']:,} rows, {v['distinct']:,} distinct, {v['duplicates']:,} identical repeats ({v['pct']}%)")
        summary.append("Identical rows: " + ", ".join(f"{k} {v['pct']}%" for k, v in d.items()) + ".")
    else:
        body.append(f"{ticks} not found (not compacted yet?).")
    # 5: Greeks
    body += ["", "## 5. Stored option Greeks", ""]
    g = greek_report(probe) if probe else {}
    if g:
        body += [f"- call rows {g['ce_rows']:,}, with IV {g['iv_rows']:,}", f"- IV min {g['iv_min']:.4g}, median {g['iv_median']:.4g}, max {g['iv_max']:.4g}"
                 f" ({'a fraction' if g['iv_max'] is not None and g['iv_max'] < 10 else 'a percent'})",
                 f"- distinct values: delta {g['distinct_delta']:,}, gamma {g['distinct_gamma']:,}, theta {g['distinct_theta']:,}, vega {g['distinct_vega']:,}",
                 f"- gamma max {g['gamma_max']}, theta min {g['theta_min']}, vega max {g['vega_max']}"]
        body += [f"- IV missing, {k}: {a:,} of {b:,}" for k, (a, b) in g["iv_missing_by_delta"].items()]
        summary.append(f"IV range {g['iv_min']:.3g} to {g['iv_max']:.3g}; gamma has {g['distinct_gamma']} distinct values.")
    else:
        body.append("No stream folder given or it holds no data.")
    head = [f"# Read-only diagnostics {day}", "", "## Summary (read this first)", "", *[f"- {s}" for s in summary], "", "---", ""]
    return "\n".join(head + body) + "\n"


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Read-only end-of-day diagnostics")
    ap.add_argument("--day", type=date.fromisoformat, default=None, help="UTC day (default: today)")
    ap.add_argument("--logs", type=Path, default=lake_mod.ROOT / "logs")
    ap.add_argument("--lake", type=Path, default=lake_mod.LAKE_DIR)
    ap.add_argument("--probe", type=Path, default=lake_mod.ROOT / "probe_data" / "stream_fo", help="a stream folder (ticks/...) to read the stored Greeks from")
    ap.add_argument("--out", type=Path, default=None)
    a = ap.parse_args(argv)
    day = a.day or datetime.now(timezone.utc).date()
    report = build_report(day, a.logs, a.lake, a.probe if a.probe.exists() else None)
    out = a.out or a.logs / f"diag-{day:%Y%m%d}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report)
    print(report)
    print(f"saved to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
