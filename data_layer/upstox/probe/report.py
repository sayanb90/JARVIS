"""
The probe report: what works, how far back it goes, what a day of each dataset weighs, and what the box went through.

Built from files on disk plus the in-memory summaries, so `probe report` can rebuild it later and a partial report can be
written every hour while the run is still going.
"""

from __future__ import annotations

import json
import logging
import platform
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb
import pyarrow.parquet as pq

from .jobs import CATALOGUE
from .store import ProbeStore
from ...tick_store import EXT_COLUMNS

log = logging.getLogger(__name__)

GB = 1024 ** 3
TRADING_DAYS = 248
SESSION_SECONDS = 22_500                                                  # 09:15 to 15:30 IST
CLASSES = {
    "index": "symbol LIKE 'NSE_INDEX:%'",
    "global": "symbol LIKE 'GLOBAL:%'",
    "fo": "regexp_matches(symbol, '_(CE|PE|FUT)_')",
    "equity": "NOT (symbol LIKE 'NSE_INDEX:%' OR symbol LIKE 'GLOBAL:%' OR regexp_matches(symbol, '_(CE|PE|FUT)_'))",
}


# ----------------------------------------------------------------------------------------------------- tick storage
def _compressed_by_column(path: Path) -> dict[str, int]:
    md = pq.ParquetFile(path).metadata
    out: dict[str, int] = {}
    for rg in range(md.num_row_groups):
        for c in range(md.num_columns):
            col = md.row_group(rg).column(c)
            out[col.path_in_schema] = out.get(col.path_in_schema, 0) + col.total_compressed_size
    return out


def analyse_ticks(stream_root: Path, universe_scale: dict[str, float]) -> dict:
    """
    Size of one stream's compacted tick file: bytes per column (so the cost of each new field is visible), rows and
    bytes per instrument class, the extended fields' share, and the projection to a full year.
    """
    files = sorted(stream_root.glob("ticks/date=*/ticks.parquet")) or sorted(stream_root.glob("ticks/date=*/*.parquet"))
    if not files:
        return {"available": False}
    f = files[-1]
    cols = _compressed_by_column(f)
    total = sum(cols.values()) or 1
    rows = pq.ParquetFile(f).metadata.num_rows
    ext = [c for c in EXT_COLUMNS if c in cols]
    con = duckdb.connect()
    src = f"read_parquet('{f}')"
    tmp = Path(tempfile.mkdtemp(prefix="probe-size-"))
    classes: dict[str, dict] = {}
    try:
        for name, pred in CLASSES.items():
            n, t0, t1 = con.execute(f"SELECT count(*), min(ts), max(ts) FROM {src} WHERE {pred}").fetchone()
            if not n:
                continue
            sizes = {}
            for label, sel in (("all", "*"), ("base", f"* EXCLUDE ({', '.join(ext)})" if ext else "*")):
                out = tmp / f"{name}-{label}.parquet"
                con.execute(f"COPY (SELECT {sel} FROM {src} WHERE {pred} ORDER BY symbol, ts) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
                sizes[label] = out.stat().st_size
            span = max(1.0, (t1 - t0).total_seconds())
            coverage = min(1.0, span / SESSION_SECONDS)
            scale = universe_scale.get(name, 1.0)
            day_bytes = sizes["all"] / max(coverage, 0.05)                 # scale a short run up to a full session
            classes[name] = {"rows": int(n), "bytes": sizes["all"], "bytes_without_ext": sizes["base"],
                             "ext_bytes": sizes["all"] - sizes["base"], "bytes_per_row": round(sizes["all"] / n, 2),
                             "coverage_of_session": round(coverage, 2), "scale_to_plan": scale,
                             "gb_per_year": round(day_bytes * scale * TRADING_DAYS / GB, 2),
                             "gb_per_year_without_ext": round(sizes["base"] / max(coverage, 0.05) * scale * TRADING_DAYS / GB, 2)}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        con.close()
    return {"available": True, "file": str(f), "file_bytes": f.stat().st_size, "rows": rows,
            "columns": {c: {"bytes": b, "share_pct": round(100.0 * b / total, 1), "bytes_per_row": round(b / max(rows, 1), 3),
                            "extended": c in EXT_COLUMNS} for c, b in sorted(cols.items(), key=lambda kv: -kv[1])},
            "ext_share_pct": round(100.0 * sum(cols[c] for c in ext) / total, 1), "classes": classes}


# ----------------------------------------------------------------------------------------------------- REST projection
def project_rest(sizes: dict, results: list[dict]) -> list[dict]:
    """Yearly size of each REST dataset, from what one poll of the sample weighed."""
    out = []
    by_job: dict[str, list[dict]] = {}
    for r in results:
        by_job.setdefault(r["job"], []).append(r)
    for job, runs in by_job.items():
        spec = CATALOGUE.get(job)
        if not spec:
            continue
        code, title, p = spec
        runs_n = len(runs)
        total_bytes = sum(r.get("bytes", 0) for r in runs)
        per_poll = total_bytes / max(runs_n, 1)
        n_inst = max((r.get("notes", {}).get("n_inst") or 0) for r in runs) or None
        row = {"job": job, "catalogue": code, "title": title, "polls": runs_n, "bytes_total": total_bytes,
               "bytes_per_poll": round(per_poll), "rows_total": sum(r.get("rows", 0) for r in runs), "gb_per_year": None, "basis": ""}
        scale = (p["scale_to"] / n_inst) if p.get("scale_to") and n_inst else 1.0
        if p.get("one_off"):
            row["basis"] = "one-off sample; see the backfill section"
            if p.get("refreshes_per_year"):
                row["gb_per_year"] = round(per_poll * scale * p["refreshes_per_year"] / GB, 3)
                row["basis"] = f"{n_inst} sampled, scaled to {p['scale_to']} {p.get('scale_basis', '')}"
        elif p.get("polls_per_day"):
            row["gb_per_year"] = round(per_poll * scale * p["polls_per_day"] * TRADING_DAYS / GB, 3)
            row["basis"] = f"{p['polls_per_day']} poll(s) a day" + (f", scaled {n_inst} to {p['scale_to']}" if scale != 1.0 else "")
        out.append(row)
    return out


def backfill_estimate(results: list[dict]) -> Optional[dict]:
    """Request count, time and disk to backfill candles, from the sampled stocks."""
    runs = [r for r in results if r["job"] == "candles_sample" and r.get("notes", {}).get("daily")]
    if not runs:
        return None
    n = runs[-1]["notes"]
    stocks = max(1, n.get("n_inst", 1))
    d_rows = n["daily"].get("rows", 0) / stocks
    m_rows = n["minute"].get("rows_one_month_1min", 0) / stocks
    est = {"daily_rows_per_stock": round(d_rows), "minute_rows_per_stock_per_month": round(m_rows)}
    for universe in (500, 2000):
        reqs = universe * (3 + 58)                                         # 3 decades of daily, ~58 months of 1-minute
        est[str(universe)] = {"requests": reqs, "hours_at_limit": round(reqs / 4000, 1)}
    return est


# ----------------------------------------------------------------------------------------------------- the report
def build_report(root: Path, *, mode: str, monitor: dict, rest: dict, results: list[dict], streams: dict,
                 universe_scale: dict[str, float], meta: dict, partial: bool = False) -> dict:
    store = ProbeStore(root)
    sizes = store.sizes()
    ticks = {name: analyse_ticks(root / name, universe_scale) for name in streams if (root / name).exists()}
    rep = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ"), "mode": mode, "partial": partial,
           "host": platform.node(), "python": platform.python_version(), "meta": meta, "monitor": monitor, "rest": rest,
           "results": results, "streams": streams, "ticks": ticks, "sizes": sizes,
           "rest_projection": project_rest(sizes, results), "backfill": backfill_estimate(results)}
    rep["flags"] = flags(rep)
    return rep


def flags(rep: dict) -> list[str]:
    out = []
    pk = (rep.get("monitor") or {}).get("peaks", {})
    total = (rep.get("monitor") or {}).get("mem_total_gb") or 0
    if pk.get("swap_used_gb", 0) > 0.05:
        out.append(f"Swap was used (peak {pk['swap_used_gb']} GB): the box ran short of memory.")
    if total and pk.get("mem_available_min_gb") is not None and pk["mem_available_min_gb"] < 0.1 * total:
        out.append(f"Free memory fell to {pk['mem_available_min_gb']} GB of {total} GB.")
    if pk.get("cpu_pct", 0) > 80:
        out.append(f"CPU peaked at {pk['cpu_pct']}%.")
    if pk.get("iowait_pct", 0) > 20:
        out.append(f"Disk wait peaked at {pk['iowait_pct']}%: storage may be the bottleneck.")
    for tag, s in ((rep.get("rest") or {}).get("by_tag") or {}).items():
        for k, n in s.get("errors", {}).items():
            if "UDAPI1149" in k:
                out.append(f"{tag}: plan gating (UDAPI1149) x{n}: needs the Plus plan or a token that has it.")
            elif k.startswith("429"):
                out.append(f"{tag}: rate limited (HTTP 429) x{n}.")
    for r in rep.get("results", []):
        for t, e in (r.get("notes", {}).get("parse_errors") or {}).items():
            out.append(f"{r['job']}: parser '{t}' failed ({e}); see _samples for the real shape.")
    for name, s in (rep.get("streams") or {}).items():
        if s.get("error"):
            out.append(f"stream {name}: {s['error']}")
        if s.get("guard_stopped"):
            out.append(f"stream {name}: writes stopped by the disk guard ({s['guard_stopped']}).")
    return out


def _mb(n: float) -> str:
    return f"{n / 1024 ** 2:.1f} MB" if n >= 1024 ** 2 else f"{n / 1024:.1f} KB"


def render_markdown(rep: dict) -> str:
    L: list[str] = []
    add = L.append
    add(f"# Upstox probe report ({rep['mode']}{', PARTIAL' if rep['partial'] else ''})")
    add(f"\nGenerated {rep['generated_at']} on {rep['host']} (Python {rep['python']}).\n")
    if rep["flags"]:
        add("## Things to look at\n")
        L.extend(f"- {f}" for f in rep["flags"])
        add("")
    m = rep["monitor"] or {}
    pk = m.get("peaks", {})
    add("## The box\n")
    add(f"- CPUs: {m.get('cpus')}; memory: {m.get('mem_total_gb')} GB; samples every {m.get('interval_s')} s ({m.get('samples')} taken)")
    add(f"- CPU: average {m.get('cpu_avg_pct')}%, peak {pk.get('cpu_pct')}%, I/O wait peak {pk.get('iowait_pct')}%, load1 peak {pk.get('load1')}")
    add(f"- Memory: peak used {pk.get('mem_used_gb')} GB, lowest available {pk.get('mem_available_min_gb')} GB, swap peak {pk.get('swap_used_gb')} GB")
    for path, d in (m.get("disk_start_gb") or {}).items():
        end = (m.get("disk_end_gb") or {}).get(path, {})
        add(f"- Disk {path}: {d['used']} of {d['total']} GB used at start, {end.get('used')} at end; free {d['free']} to {end.get('free')} GB "
            f"(lowest {pk.get('disk_free_min_gb', {}).get(path)} GB)")
    for path, gb in (pk.get("dir_gb") or {}).items():
        add(f"- Directory {path}: peak {gb} GB")
    if pk.get("process_rss_mb"):
        add("\n| Process | Peak memory (MB) | Peak CPU (%) |\n| --- | --- | --- |")
        for cmd, rss in sorted(pk["process_rss_mb"].items(), key=lambda kv: -kv[1]):
            add(f"| {cmd} | {rss} | {pk.get('process_cpu_pct', {}).get(cmd, '')} |")
    add("\n## What answered (capability)\n")
    add("| Endpoint tag | Requests | OK | Errors | Avg latency (s) | Rate-limit headers |\n| --- | --- | --- | --- | --- | --- |")
    for tag, s in sorted(((rep.get("rest") or {}).get("by_tag") or {}).items()):
        avg = s["latency_s"] / max(s["requests"], 1)
        add(f"| {tag} | {s['requests']} | {s['ok']} | {json.dumps(s['errors']) if s['errors'] else ''} | {avg:.2f} | "
            f"{json.dumps(s['rate_headers']) if s['rate_headers'] else ''} |")
    r = rep.get("rest") or {}
    add(f"\nRequests: {r.get('requests')} (limiter waited {r.get('limiter_waited_s')} s in total).\n")
    add("## How far back it goes (from the sample jobs)\n")
    for res in rep["results"]:
        n = res.get("notes", {})
        for k in ("depth", "earliest", "periods", "daily", "minute", "expiries_count", "expiry_oldest", "expiry_newest", "contracts_per_expiry",
                  "index_keys_found", "global_keys_found", "holidays_path", "status_path", "timings_path", "stream", "n_inst",
                  "NSE_gz_bytes", "NSE_records", "global_records"):
            if n.get(k) not in (None, {}, []):
                add(f"- {res['job']} / {k}: `{json.dumps(n[k], default=str)}`")
    add("\n## What a day weighs\n")
    add("### Live stream (compacted ticks)\n")
    for name, t in rep["ticks"].items():
        s = (rep.get("streams") or {}).get(name, {})
        add(f"**{name}**: {s.get('keys', '?')} instruments, {s.get('frames', 0)} frames, {s.get('ticks', 0)} ticks")
        if not t.get("available"):
            add("\nNo tick file yet.\n")
            continue
        add(f"\nFile {_mb(t['file_bytes'])}, {t['rows']} rows; extended fields are {t['ext_share_pct']}% of the file.\n")
        add("| Class | Rows | Size | Bytes per row | Extended fields cost | GB per year (plan scale) | Without extended fields |\n| --- | --- | --- | --- | --- | --- | --- |")
        for cname, c in t["classes"].items():
            add(f"| {cname} | {c['rows']} | {_mb(c['bytes'])} | {c['bytes_per_row']} | {_mb(c['ext_bytes'])} | {c['gb_per_year']} | {c['gb_per_year_without_ext']} |")
        add("\n| Column | Compressed bytes | Share | Bytes per row | Extended |\n| --- | --- | --- | --- | --- |")
        for c, d in t["columns"].items():
            add(f"| {c} | {d['bytes']} | {d['share_pct']}% | {d['bytes_per_row']} | {'yes' if d['extended'] else ''} |")
        add("")
    add("### Everything else\n")
    add("| Dataset | Rows on disk | Size on disk | Bytes per row |\n| --- | --- | --- | --- |")
    for ds, s in rep["sizes"].items():
        if ds.startswith("stream_"):
            continue
        add(f"| {ds} | {s['rows']} | {_mb(s['bytes'])} | {round(s['bytes'] / s['rows'], 1) if s['rows'] else ''} |")
    add("\n| Job | Catalogue | Polls | Bytes per poll | GB per year | Basis |\n| --- | --- | --- | --- | --- | --- |")
    for p in rep["rest_projection"]:
        add(f"| {p['job']} | {p['catalogue']} | {p['polls']} | {p['bytes_per_poll']} | {p['gb_per_year'] if p['gb_per_year'] is not None else ''} | {p['basis']} |")
    if rep.get("backfill"):
        b = rep["backfill"]
        add(f"\n### Backfill cost (limit 4,000 requests an hour)\n\nPer stock: about {b['daily_rows_per_stock']} daily rows; about {b['minute_rows_per_stock_per_month']} one-minute rows a month.")
        for u in ("500", "2000"):
            add(f"- {u} stocks: about {b[u]['requests']} requests, about {b[u]['hours_at_limit']} hours")
    add("\n## Jobs\n")
    add("| Job | Started (UTC) | Seconds | Requests | Rows | Bytes | OK |\n| --- | --- | --- | --- | --- | --- | --- |")
    for res in rep["results"]:
        add(f"| {res['job']} | {res.get('started', '')} | {res.get('seconds')} | {res.get('requests')} | {res.get('rows')} | {res.get('bytes')} | {res.get('ok')} |")
    return "\n".join(L) + "\n"


def write_report(root: Path, rep: dict, name: str) -> tuple[Path, Path]:
    folder = Path(root) / "reports"
    folder.mkdir(parents=True, exist_ok=True)
    jp, mp = folder / f"{name}.json", folder / f"{name}.md"
    jp.write_text(json.dumps(rep, indent=1, default=str))
    mp.write_text(render_markdown(rep))
    return jp, mp
