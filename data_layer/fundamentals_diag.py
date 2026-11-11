"""
Why is the fundamentals and reference data incomplete?  A read-only analysis of everything the capture jobs leave behind.

  python -m data_layer.fundamentals_diag                    # report to the screen and logs/fundamentals-diag-<date>.md
  python -m data_layer.fundamentals_diag --days 14 --no-file

It reads, and never writes anything but its own report:
  lake/upstox_fundamentals/fetch_log.parquet, versions.parquet, raw/date=*/        (Upstox: one row per call, the values, the payloads)
  logs/fundamentals.log                                                              (Upstox runs: starts, progress, 429 warnings, summaries)
  data/universe.csv (or universe.local.csv), logs/fo-select.log                      (the reference data the jobs start from)

Every section ends in plain findings.  The sections that need a file skip themselves, and say so, when the file is not there.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from . import lake as lake_mod

LINE = re.compile(r"^(\d{4}-\d\d-\d\d)[ T](\d\d:\d\d:\d\d)(?:[,.]\d+)?\s+([A-Z]+)\s+(?:\[\w+\]\s+)?(.*)$")
RATE = re.compile(r"(/v2/fundamentals/(\S+?)/(\S+)) -> HTTP (\d+) \(attempt (\d+)/(\d+)\), waiting (\d+)s")
RUN_START = re.compile(r"fundamentals: (\d+) companies, endpoints (\S+), max age (\S+) days")
RUN_DONE = re.compile(r"fundamentals done: (\{.*\})")
EXPECTED_DAILY_UTC = "12:50"            # weekdays: key ratios;  Sunday 13:00: the full set (deploy/crontab)


# ---------------------------------------------------------------------------------------------- small helpers
def parse_log(path: Path) -> list[tuple[datetime, str, str]]:
    """(time, level, message) of every well-formed line of a log; the lines of a traceback keep the time of the line above."""
    out: list[tuple[datetime, str, str]] = []
    if not path.exists():
        return out
    last: datetime | None = None
    for raw in path.read_text(errors="replace").splitlines():
        m = LINE.match(raw)
        if m:
            last = datetime.fromisoformat(f"{m.group(1)} {m.group(2)}")
            out.append((last, m.group(3), m.group(4)))
        elif last is not None and raw.strip():
            out.append((last, "CONT", raw.strip()))
    return out


def _pct(a: int, b: int) -> str:
    return f"{100 * a / b:.1f} %" if b else "n/a"


def _table(rows: list[list], header: list[str]) -> list[str]:
    if not rows:
        return ["  (nothing)"]
    w = [max(len(str(x)) for x in col) for col in zip(header, *rows)]
    fmt = "  " + "  ".join(f"{{:<{n}}}" for n in w)
    return [fmt.format(*header), fmt.format(*["-" * n for n in w])] + [fmt.format(*[str(c) for c in r]) for r in rows]


class Report:
    def __init__(self) -> None:
        self.lines: list[str] = []
        self.findings: list[tuple[str, str]] = []                  # (severity, text): PROBLEM, WARN or INFO

    def h(self, title: str) -> None:
        self.lines += ["", f"## {title}"]

    def add(self, *lines: str) -> None:
        self.lines += list(lines)

    def find(self, severity: str, text: str) -> None:
        self.findings.append((severity, text))

    def text(self) -> str:
        order = {"PROBLEM": 0, "WARN": 1, "INFO": 2}
        fl = sorted(self.findings, key=lambda f: order.get(f[0], 3))
        head = ["# Fundamentals and reference data: why is it incomplete?", "", "## Findings (computed from the sections below)"]
        head += [f"{i}. [{sev}] {txt}" for i, (sev, txt) in enumerate(fl, 1)] or ["(nothing found)"]
        return "\n".join(head + self.lines) + "\n"


# ---------------------------------------------------------------------------------------------- Upstox: the fetch log
def upstox_fetch_log(rep: Report, base: Path, days: int) -> None:
    rep.h("1. Upstox fundamentals: the fetch log (one row per call)")
    p = base / "fetch_log.parquet"
    if not p.exists():
        rep.add("lake/upstox_fundamentals/fetch_log.parquet is missing: the Upstox fundamentals job has never stored a call.")
        rep.find("PROBLEM", "No Upstox fetch log at all: the job never ran, or never reached its first save.")
        return
    lg = pd.read_parquet(p)
    lg["day"] = lg["fetched_at"].astype(str).str[:10]
    lg["ok"] = lg["ok"].astype(bool)
    lg["empty"] = lg["ok"] & (lg["rows"].fillna(0) == 0)
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    rec = lg[lg["day"] >= cutoff]
    rep.add(f"{len(lg):,} calls logged in all, {len(rec):,} in the last {days} days, {lg['isin'].nunique()} companies, "
            f"first {lg['day'].min()}, last {lg['day'].max()}.")
    has_detail = "error_message" in lg.columns and lg["error_message"].notna().any()

    rep.add("", "Per day (last %d days):" % days)
    rows = []
    for d, g in rec.groupby("day"):
        errs = Counter(f"{int(r.status)}:{r.error_code}" for r in g[~g["ok"]].itertuples())
        rows.append([d, len(g), int(g["ok"].sum()), int((~g["ok"]).sum()), int(g["empty"].sum()), g["isin"].nunique(), g["endpoint"].nunique(),
                     ", ".join(f"{k} x{v}" for k, v in errs.most_common(3)) or "-"])
    rep.add(*_table(rows, ["day", "calls", "ok", "failed", "ok but 0 rows", "companies", "endpoints", "top errors"]))

    rep.add("", "Per endpoint (last %d days):" % days)
    rows = []
    for ep, g in rec.groupby("endpoint"):
        rows.append([ep, len(g), _pct(int(g["ok"].sum()), len(g)), _pct(int(g["empty"].sum()), len(g)),
                     g[g["ok"]]["isin"].nunique(), g["isin"].nunique()])
    rep.add(*_table(rows, ["endpoint", "calls", "ok", "ok but 0 rows", "companies ok", "companies tried"]))
    for ep, g in rec.groupby("endpoint"):
        if len(g) >= 50 and g["ok"].mean() < 0.9:
            rep.find("PROBLEM", f"Upstox endpoint {ep}: only {_pct(int(g['ok'].sum()), len(g))} of {len(g)} calls succeeded in the last {days} days.")
        if len(g) >= 50 and g["empty"].mean() > 0.2:
            rep.find("WARN", f"Upstox endpoint {ep}: {_pct(int(g['empty'].sum()), len(g))} of calls answered OK with zero rows (parser or empty response).")

    # how fresh is each company on each endpoint
    rep.add("", "Freshness: companies whose last successful call is at most N days old (of all companies seen):")
    total = lg["isin"].nunique()
    today = date.today()
    rows = []
    for ep, g in lg[lg["ok"]].groupby("endpoint"):
        last = g.groupby("isin")["day"].max()
        age = last.map(lambda d: (today - date.fromisoformat(d)).days)
        rows.append([ep, int((age <= 2).sum()), int((age <= 7).sum()), int((age <= 30).sum()), len(age), total])
    rep.add(*_table(rows, ["endpoint", "<= 2 d", "<= 7 d", "<= 30 d", "ever ok", "companies"]))
    for r in rows:
        if r[0] == "key_ratios" and total and r[2] < 0.9 * total:
            rep.find("PROBLEM", f"key_ratios: only {r[2]} of {total} companies have a snapshot from the last 7 days (the history this builds is the only way to get it).")

    # who fails: the same companies every time, or anyone?
    bad = rec[~rec["ok"]]
    if len(bad):
        by_isin = bad["isin"].value_counts()
        top10 = int(by_isin.head(10).sum())
        rep.add("", f"Failures: {len(bad):,} calls over {by_isin.size} companies; the 10 worst companies account for {_pct(top10, len(bad))}.")
        chronic = []
        for (isin, ep), g in rec.groupby(["isin", "endpoint"]):
            days_tried = g["day"].nunique()
            days_failed = g[~g["ok"]]["day"].nunique()
            if days_tried >= 3 and not g["ok"].any():
                chronic.append((isin, ep, days_tried, g["symbol"].iloc[0], Counter(f"{int(s)}:{c}" for s, c in zip(g["status"], g["error_code"])).most_common(1)[0][0]))
        rep.add(f"(company, endpoint) pairs that failed on every attempt on 3 or more days: {len(chronic)}")
        rep.add(*_table([[c[3], c[0], c[1], c[2], c[4]] for c in chronic[:15]], ["symbol", "isin", "endpoint", "days tried", "error"]))
        if chronic:
            rep.find("PROBLEM", f"{len(chronic)} (company, endpoint) pairs have failed on every attempt for 3 or more days "
                                f"(example: {chronic[0][3]} {chronic[0][1]}, {chronic[0][4]}): they are not a rate-limit problem, look at the error.")
        if top10 / len(bad) < 0.2 and len(bad) > 100:
            rep.add("Failures are spread over many companies (not a few bad ones): that points to a limit or an outage, not to bad data.")
        if has_detail:
            msgs = Counter(str(m)[:90] for m in bad["error_message"].fillna(""))
            rep.add("", "Error messages (since they were recorded):", *[f"  {n:>6} x {m}" for m, n in msgs.most_common(5)])
            att = bad["attempts"].fillna(1).astype(int)
            rep.add(f"Retries: failed calls used {att.mean():.1f} attempts on average (3 is the maximum, then the call is given up).")

    # where in a run do the failures start?  (rows are appended in call order, one stamp per run before 2026-11-05)
    rep.add("", "Where in a run do failures happen? (calls of one run in order, ten equal parts, failure rate in each):")
    rows = []
    for stamp, g in lg.groupby("fetched_at", sort=True) if not has_detail else [(d, g) for d, g in lg.groupby("day")]:
        if len(g) < 200 or str(stamp)[:10] < cutoff:
            continue
        flags = (~g["ok"]).to_numpy()
        parts = [flags[i * len(flags) // 10:(i + 1) * len(flags) // 10].mean() for i in range(10)]
        first = int(flags.argmax()) + 1 if flags.any() else 0
        rows.append([str(stamp)[:16], len(g), first or "-"] + [f"{x * 100:.0f}%" for x in parts])
        if flags.any() and first > 0.3 * len(flags) and parts[-1] > 0.3:
            rep.find("WARN", f"Run {str(stamp)[:16]}: calls ran clean for the first {first} and then failures took over "
                             f"({parts[-1] * 100:.0f} % in the last tenth): a quota that runs out part-way through, not bad companies.")
    rep.add(*_table(rows, ["run", "calls", "first failure #"] + [f"{i}" for i in range(1, 11)]))
    if not has_detail:
        rep.add("(Calls before 2026-11-05 carry the time the run started, so only their order is known, not their clock time.)")


# ---------------------------------------------------------------------------------------------- Upstox: versions and raw
def upstox_values(rep: Report, base: Path) -> None:
    rep.h("2. Upstox fundamentals: what is actually stored")
    p = base / "versions.parquet"
    if not p.exists():
        rep.add("versions.parquet is missing: no values have been stored.")
        rep.find("PROBLEM", "No Upstox versions table: nothing has ever been stored.")
        return
    v = pd.read_parquet(p)
    comp = v["isin"].nunique()
    rep.add(f"{len(v):,} value rows, {comp} companies, first_seen from {v['first_seen'].min()} to {v['first_seen'].max()}, "
            f"last_seen up to {v['last_seen'].max()}.")
    rows = []
    for ep, g in v.groupby("endpoint"):
        rows.append([ep, len(g), g["isin"].nunique(), _pct(g["isin"].nunique(), comp), g["last_seen"].max()])
    rep.add(*_table(rows, ["endpoint", "rows", "companies", "of all", "newest last_seen"]))
    for r in rows:
        if r[2] < 0.8 * comp:
            rep.find("WARN", f"Upstox {r[0]}: values for only {r[2]} of {comp} companies.")
    raw = base / "raw"
    if raw.exists():
        counts = {d.name.split("=")[1]: len(list(d.glob("*.json.gz"))) for d in sorted(raw.glob("date=*"))}
        rep.add("", "Raw payload files per day: " + ", ".join(f"{d} {n}" for d, n in list(counts.items())[-10:]))
        small = [d for d, n in counts.items() if n < 0.8 * comp]
        if small:
            rep.find("INFO", f"Raw payloads exist for fewer than 80 % of companies on {len(small)} day(s), e.g. {small[-1]} ({counts[small[-1]]}): "
                             f"days that only ran the key-ratios set or hit failures.")


# ---------------------------------------------------------------------------------------------- Upstox: the text log
def upstox_text_log(rep: Report, path: Path, days: int) -> None:
    rep.h("3. Upstox fundamentals: logs/fundamentals.log (runs and rate-limit warnings)")
    ev = parse_log(path)
    if not ev:
        rep.add(f"{path} is missing or empty.")
        rep.find("WARN", "logs/fundamentals.log is missing or empty: the cron job's output is not being kept.")
        return
    cutoff = datetime.now() - timedelta(days=days)
    runs: list[dict] = []
    cur: dict | None = None
    rates: list[tuple[datetime, str, str, int, int]] = []
    tracebacks = 0
    for t, lvl, msg in ev:
        if t < cutoff:
            continue
        m = RUN_START.search(msg)
        if m:
            cur = {"start": t, "companies": int(m.group(1)), "endpoints": m.group(2), "done": None, "summary": None, "rates": 0}
            runs.append(cur)
            continue
        m = RUN_DONE.search(msg)
        if m and cur is not None:
            cur["done"] = t
            try:
                cur["summary"] = json.loads(m.group(1))
            except ValueError:
                pass
            continue
        m = RATE.search(msg)
        if m:
            rates.append((t, m.group(2), m.group(3), int(m.group(4)), int(m.group(7))))
            if cur is not None:
                cur["rates"] += 1
        if "Traceback" in msg:
            tracebacks += 1
    rows = []
    for r in runs:
        s = r["summary"] or {}
        dur = f"{(r['done'] - r['start']).total_seconds() / 60:.0f} min" if r["done"] else "NO END LINE"
        rows.append([r["start"].strftime("%Y-%m-%d %H:%M"), r["endpoints"][:24], r["companies"], dur, s.get("calls", "-"), s.get("ok", "-"),
                     sum((s.get("failed") or {}).values()) if s else "-", r["rates"]])
        if not r["done"] and datetime.now() - r["start"] > timedelta(hours=4):        # a run still going is not a failure
            rep.find("PROBLEM", f"The run of {r['start']:%Y-%m-%d %H:%M} has no 'fundamentals done' line: it was killed or crashed before saving its summary.")
    rep.add(*_table(rows, ["start (UTC)", "endpoints", "companies", "duration", "calls", "ok", "failed", "429/5xx warnings"]))
    if tracebacks:
        rep.find("PROBLEM", f"{tracebacks} traceback(s) in fundamentals.log in the last {days} days.")

    # was every expected run there?
    seen = {r["start"].date() for r in runs}
    log_from = min(t for t, _, _ in ev).date()                           # days before the log begins say nothing
    missing = []
    for k in range(days):
        d = date.today() - timedelta(days=k)
        if d == date.today() or d.weekday() == 5 or d < log_from:
            continue                                                     # today may not have run yet; Saturday has no job
        if d not in seen:
            missing.append(d.isoformat())
    if missing:
        rep.add("", f"Days with no run at all (expected weekdays 12:50 UTC, Sunday 13:00 UTC): {', '.join(sorted(missing))}")
        rep.find("PROBLEM" if len(missing) > 2 else "WARN", f"No Upstox fundamentals run on {len(missing)} expected day(s): {', '.join(sorted(missing)[:6])}. "
                                                              f"Compare with `crontab -l` and logs/cron or syslog for why cron did not start it.")

    if rates:
        by_min = Counter(t.strftime("%Y-%m-%d %H:%M") for t, *_ in rates)
        peak = by_min.most_common(1)[0]
        waits = Counter(w for *_, w in rates)
        codes = Counter(c for *_, c, _ in rates)
        rep.add("", f"{len(rates)} rate-limit or server warnings; busiest minute {peak[0]} with {peak[1]}; wait before retry (s): "
                    + ", ".join(f"{w}s x{n}" for w, n in sorted(waits.items())) + "; HTTP: " + ", ".join(f"{c} x{n}" for c, n in codes.items()))
        companies = Counter(i for _, i, _, _, _ in rates)
        rep.add(f"They touched {len(companies)} companies; the 5 worst: " + ", ".join(f"{i} x{n}" for i, n in companies.most_common(5)))
        if len(waits) == 1 and 2 in waits:
            rep.find("INFO", "Every retry waited 2 s: the server sends no Retry-After, so the client only backs off briefly. "
                             "Three attempts of 2 s and 4 s do not outlast a rolling 30-minute quota.")
        clusters = sorted(by_min)
        if clusters:
            first, last = datetime.fromisoformat(clusters[0] + ":00"), datetime.fromisoformat(clusters[-1] + ":00")
            span = (last - first).total_seconds() / 60 + 1
            if len(by_min) < 0.5 * span and span > 10:
                rep.add(f"Warnings come in bursts: {len(by_min)} busy minutes within a {span:.0f}-minute window.")
    else:
        rep.add("", "No rate-limit warnings in the period.")


# ---------------------------------------------------------------------------------------------- reference data helpers
def _universe(data: Path) -> list[dict]:
    path = data / "universe.local.csv"
    path = path if path.exists() else data / "universe.csv"
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return [{k: (v or "").strip() for k, v in r.items()} for r in csv.DictReader(r for r in f if not r.startswith("#"))
                if (r.get("symbol") or "").strip()]


# ---------------------------------------------------------------------------------------------- reference data
def reference_section(rep: Report, root: Path) -> None:
    rep.h("4. Reference data the jobs start from")
    uni = _universe(root / "data")
    if not uni:
        rep.add("No universe file found.")
        rep.find("PROBLEM", "No data/universe.csv or universe.local.csv.")
    else:
        bad = [r["symbol"] for r in uni if not (len(r.get("isin", "")) == 12 and r.get("isin", "").startswith("IN"))]
        rep.add(f"Universe: {len(uni)} rows; {len(bad)} without a valid-looking ISIN" + (f" (first: {', '.join(bad[:8])})" if bad else "") + ".")
        if bad:
            rep.find("WARN", f"{len(bad)} universe stocks have no usable ISIN, so Upstox fundamentals and the equity feed skip them.")
        dup = [s for s, n in Counter(r["symbol"] for r in uni).items() if n > 1]
        if dup:
            rep.find("WARN", f"Duplicate symbols in the universe: {', '.join(dup[:8])}.")
    fo = parse_log(root / "logs" / "fo-select.log")
    if fo:
        errs = [(t, m) for t, lvl, m in fo if lvl in ("ERROR", "CRITICAL") or "Traceback" in m]
        last = fo[-1][0]
        rep.add(f"logs/fo-select.log: last line {last:%Y-%m-%d %H:%M}, {len(errs)} error line(s)" + (f", latest: {errs[-1][1][:120]}" if errs else "") + ".")
        if errs and errs[-1][0] > datetime.now() - timedelta(days=3):
            rep.find("WARN", f"The F&O instrument import or select logged an error recently ({errs[-1][0]:%Y-%m-%d}): {errs[-1][1][:100]}.")
    else:
        rep.add("logs/fo-select.log is missing or empty.")


# ---------------------------------------------------------------------------------------------- main
def analyse(lake: Path, root: Path, days: int = 30) -> Report:
    rep = Report()
    base = lake / "upstox_fundamentals"
    for fn, args in ((upstox_fetch_log, (rep, base, days)), (upstox_values, (rep, base)),
                     (upstox_text_log, (rep, root / "logs" / "fundamentals.log", days)),
                     (reference_section, (rep, root))):
        try:
            fn(*args)
        except Exception as exc:                                          # noqa: BLE001 -- one broken section must not hide the rest
            rep.add("", f"(section {fn.__name__} failed: {type(exc).__name__}: {exc})")
            rep.find("WARN", f"The analysis of {fn.__name__} itself failed ({type(exc).__name__}: {exc}); send this report anyway.")
    return rep


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Read-only analysis of why fundamentals and reference data are incomplete")
    ap.add_argument("--days", type=int, default=30, help="how far back to look (default 30)")
    ap.add_argument("--lake", type=Path, default=None)
    ap.add_argument("--root", type=Path, default=None, help="project root with data/ and logs/ (default: this checkout)")
    ap.add_argument("--no-file", action="store_true", help="print only, do not write logs/fundamentals-diag-<date>.md")
    a = ap.parse_args(argv)
    root = a.root or lake_mod.ROOT
    rep = analyse(a.lake or lake_mod.LAKE_DIR, root, a.days)
    text = rep.text()
    print(text)
    if not a.no_file:
        out = root / "logs" / f"fundamentals-diag-{date.today():%Y%m%d}.md"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text)
        print(f"written: {out}")
    return 1 if any(s == "PROBLEM" for s, _ in rep.findings) else 0


if __name__ == "__main__":
    raise SystemExit(main())
