"""
Pre-open anchors (17 Nov 2026): what the live engines compute once from earlier sessions, saved as small tables so an engine starts in seconds.

At its start every engine used to load 400 days of index and VIX minutes plus a week of option minutes, compute the daily realised measures and
the HAR forecast, the VIX regime, the time-of-day variance profile, the previous session's ATM rows and the 20-session volume baseline (twenty
loads of a session of option minutes), and keep 30 sessions of index minutes and one of options.  Four engines did that at once (about 100 s at
every core of the VPS, load average 6.05).  The numbers they need are small and known at the last close.

  python -m signals anchors --underlying NIFTY,BANKNIFTY [--day 2026-11-18]       cron, 03:30 UTC, after the 03:25 instrument refresh

writes <store>/anchors/date=D/underlying=U/ : one Parquet file per frame (hist.*, fixed.*, daily.*, prov.<cadence>.*, baseline.<cadence>.<signal>,
hist_atm, contract_table) and manifest.json (schema, day, underlying, signal versions, the day the history runs through, row counts).  An engine
loads them with `load()`, which returns None when the files are missing, from another day, another schema or other signal versions; then the engine
computes them from the history as before.  The nightly replay of a day uses the same code path, so live and replay start from the same anchors.
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from signals import cadence as cadence_mod
from signals.framework import REGISTRY

SCHEMA = 1
META_FILE = "manifest.json"


def folder(store: Path, day, underlying: str) -> Path:
    return Path(store) / "anchors" / f"date={pd.Timestamp(day).date()}" / f"underlying={underlying}"


def _versions(ids) -> dict:
    return {sid: REGISTRY[sid][0].version for sid in sorted(ids) if sid in REGISTRY}


def _write(df: Optional[pd.DataFrame], path: Path, counts: dict, name: str) -> None:
    if df is None:
        counts[name] = None
        return
    counts[name] = int(len(df))
    df.to_parquet(path, index=False)


def save(anc, baselines: dict, store: Path, day, ids: list, cadences: list, history_through: Optional[str] = None, log=print) -> Path:
    """Write `anc` (an engine.core.Anchors) and the baselines {cadence: {signal: frame}}; the manifest is written last, so a folder without
    one is never loaded."""
    out = folder(store, day, anc.underlying)
    tmp = out.with_name(out.name + ".tmp")
    if tmp.exists():
        import shutil
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    counts: dict = {}
    for k, v in anc.hist.items():
        _write(v, tmp / f"hist.{k}.parquet", counts, f"hist.{k}")
    for k, v in anc.fixed.items():
        _write(v, tmp / f"fixed.{k}.parquet", counts, f"fixed.{k}")
    for k, v in (anc.daily_signals or {}).items():
        _write(v, tmp / f"daily.{k}.parquet", counts, f"daily.{k}")
    for c in cadences:
        for k, v in anc.providers(c).items():
            _write(v, tmp / f"prov.{c}.{k}.parquet", counts, f"prov.{c}.{k}")
        for sid, v in (baselines.get(c) or {}).items():
            _write(v, tmp / f"baseline.{c}.{sid}.parquet", counts, f"baseline.{c}.{sid}")
    _write(anc.hist_atm, tmp / "hist_atm.parquet", counts, "hist_atm")
    _write(anc.contract_table, tmp / "contract_table.parquet", counts, "contract_table")
    meta = {"schema": SCHEMA, "underlying": anc.underlying, "day": str(pd.Timestamp(day).date()), "ids": sorted(ids), "cadences": list(cadences),
            "versions": _versions(ids), "history_through": history_through, "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "rows": counts}
    (tmp / META_FILE).write_text(json.dumps(meta, indent=1))
    if out.exists():
        import shutil
        shutil.rmtree(out)
    tmp.rename(out)
    log(f"anchors {anc.underlying} {meta['day']}: {len(counts)} tables -> {out}")
    return out


def _read(path: Path) -> Optional[pd.DataFrame]:
    return pd.read_parquet(path) if path.exists() else None


def load(store: Path, day, underlying: str, ids: list, cadences: list, log=None):
    """(Anchors, baselines {cadence: {signal: frame}}) from the saved tables, or None (nothing saved, or not valid for this day, these signals or
    this code).  `ids` are the signals of the process and `cadences` the ones it runs."""
    from signals.engine.core import Anchors
    from signals.live_runner import daily_dependencies
    f = folder(store, day, underlying)
    mp = f / META_FILE
    if not mp.exists():
        return None
    try:
        meta = json.loads(mp.read_text())
    except (OSError, ValueError):
        return None

    def refuse(why):
        if log:
            log(f"saved anchors in {f} not used: {why}")
        return None

    if meta.get("schema") != SCHEMA or meta.get("underlying") != underlying or meta.get("day") != str(pd.Timestamp(day).date()):
        return refuse("schema, underlying or day differ")
    if not set(ids) <= set(meta.get("ids", [])):
        return refuse(f"saved for other signals: {sorted(set(ids) - set(meta.get('ids', [])))}")
    if _versions(ids) != {k: v for k, v in meta.get("versions", {}).items() if k in set(ids)}:
        return refuse("signal versions differ")
    if not set(cadences) <= set(meta.get("cadences", [])):
        return refuse(f"saved for other cadences: {meta.get('cadences')}")
    rows = meta.get("rows", {})

    def group(prefix: str) -> dict:
        return {k[len(prefix):]: _read(f / f"{k}.parquet") for k in rows if k.startswith(prefix) and "." not in k[len(prefix):]}

    try:
        hist, fixed, daily = group("hist."), group("fixed."), group("daily.")
        need = set(daily_dependencies(ids))
        if not need <= set(daily):
            return refuse(f"daily signals missing: {sorted(need - set(daily))}")
        prov = {c: {k.split(".", 2)[2]: _read(f / f"{k}.parquet") for k in rows if k.startswith(f"prov.{c}.")} for c in cadences}
        baselines = {c: {k.split(".", 2)[2]: _read(f / f"{k}.parquet") for k in rows if k.startswith(f"baseline.{c}.")
                         and k.split(".", 2)[2] in set(ids)} for c in cadences}
        anc = Anchors.restore(underlying, day, ids, hist, fixed, _read(f / "hist_atm.parquet") if "hist_atm" in rows else pd.DataFrame(),
                              {k: v for k, v in daily.items()}, _read(f / "contract_table.parquet"), prov,
                              volume_slot_daily=fixed.get("volume_slot_daily"))
    except (OSError, ValueError, KeyError) as exc:               # a half-written or damaged folder: compute from the history instead
        return refuse(f"unreadable ({exc!r})")
    return anc, baselines


def build(lake: Path, store: Path, underlying: str, day, log=print) -> Path:
    """Compute the anchors of `underlying` for `day` from the lake, and save them.  Every live signal and every cadence of the cadence config."""
    from signals.engine.core import Anchors, live_signal_ids
    from signals.live_runner import history_for, load_baselines, slot_volume_history
    day_d = pd.Timestamp(day).date()
    t0 = time.perf_counter()
    ids = live_signal_ids()
    cad = cadence_mod.load(ids)
    groups = cadence_mod.groups(cad)
    hist = history_for(Path(lake), underlying, day_d)
    through = None
    ix = hist.get("index_1m")
    if ix is not None and len(ix):
        through = str(pd.to_datetime(ix["ts"]).max().date())
    log(f"anchors {underlying} {day_d}: history loaded in {time.perf_counter() - t0:.0f} s, through {through}")
    vsd = slot_volume_history(Path(lake), underlying, day_d)
    anc = Anchors(underlying, hist, pd.Timestamp(day_d), ids, volume_slot_daily=vsd)
    baselines = {c: load_baselines(Path(store), underlying, day_d, sids, c) for c, sids in groups.items()}
    out = save(anc, baselines, store, day_d, ids, list(groups), history_through=through, log=log)
    log(f"anchors {underlying} {day_d}: done in {time.perf_counter() - t0:.0f} s")
    return out


def main(argv=None) -> int:
    import argparse
    from signals.__main__ import default_lake, default_store
    ap = argparse.ArgumentParser(prog="python -m signals anchors")
    ap.add_argument("--underlying", default="NIFTY,BANKNIFTY")
    ap.add_argument("--day", type=date.fromisoformat, default=None, help="default: today (UTC)")
    ap.add_argument("--lake", type=Path, default=None)
    ap.add_argument("--root", type=Path, default=None)
    a = ap.parse_args(argv)
    day = a.day or datetime.now(timezone.utc).date()
    lake, store = a.lake or default_lake(), a.root or default_store()
    from data_layer.bars_from_ticks import build_day
    from signals.__main__ import newest_tick_day
    last = newest_tick_day(lake)                  # the last closed session's minutes from our own ticks, when the vendor's candles of it have not come yet
    if last is not None and last < day:
        try:
            build_day(lake, last)
        except Exception as exc:                  # noqa: BLE001 -- the anchors still run on what the lake has
            print(f"bars from ticks {last} failed: {exc!r}", flush=True)
    for u in [x.strip() for x in a.underlying.split(",") if x.strip()]:
        build(lake, store, u, day, log=lambda *x: print(*x, flush=True))
    return 0
