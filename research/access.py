"""
Open a research snapshot for querying, with resource limits, and record which snapshot a run used.

  from research.access import open_snapshot
  rs = open_snapshot()                       # the latest snapshot under $JARVIS_RESEARCH_ROOT (default /srv/jarvis-research)
  rs.lake.ohlc("RELIANCE.NS", "2022-01-01", "2026-11-10")        # the same `Lake` API the viewer uses
  rs.provenance                              # {"snapshot_id": ..., "created_at": ..., "files": ..., "manifest_sha256": ...} for the run record

A snapshot mirrors the lake's layout, so `data_layer.query.Lake` reads it unchanged.  What this adds:
  * limits for the DuckDB connection: memory, threads, a spill folder inside the research area and a cap on its size, so a heavy
    backtest cannot starve the collector (the operating-system limits in deploy/research/setup.sh are the hard stop; these keep one
    query from reaching them);
  * the configuration is locked after it is set, so a query cannot raise its own limits;
  * the snapshot is checked against its manifest (sizes) before use, so a half-removed snapshot is noticed;
  * `provenance`, which every signal run and backtest stores, so a result can be reproduced on exactly the same data.

Nothing here writes to a snapshot or to the lake.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from data_layer.query import Lake

from .snapshot import DEFAULT_ROOT, MANIFEST, SnapshotError, _load_manifest, resolve, verify

DEFAULT_MEMORY_GB = 4.0
DEFAULT_THREADS = 2
DEFAULT_TEMP_GB = 20.0


@dataclass
class ResearchLake:
    lake: Lake
    snapshot_dir: Path
    provenance: dict


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name) or default)
    except ValueError:
        return default


def provenance_of(snapshot: Path) -> dict:
    m = _load_manifest(snapshot)
    digest = hashlib.sha256((snapshot / MANIFEST).read_bytes()).hexdigest()
    return {"snapshot_id": m["snapshot_id"], "created_at": m["created_at"], "files": len(m["files"]), "bytes": m["bytes"],
            "manifest_sha256": digest, "mode": m.get("mode")}


def open_snapshot(snapshot: str = "latest", root: Optional[Path] = None, memory_gb: Optional[float] = None, threads: Optional[int] = None,
                  temp_dir: Optional[Path] = None, temp_gb: Optional[float] = None, check: bool = True) -> ResearchLake:
    """Open a snapshot as a `Lake` with limits.  Environment overrides: JARVIS_RESEARCH_MEMORY_GB, _THREADS, _TEMP_GB."""
    root = Path(root) if root else DEFAULT_ROOT
    snap = resolve(root, snapshot)
    if check:
        problems = verify(snap)
        if problems:
            raise SnapshotError(f"snapshot {snap.name} does not match its manifest ({len(problems)} problems, first: {problems[0]}): not opened")
    lake = Lake(snap)
    mem = memory_gb if memory_gb is not None else _env_float("JARVIS_RESEARCH_MEMORY_GB", DEFAULT_MEMORY_GB)
    thr = int(threads if threads is not None else _env_float("JARVIS_RESEARCH_THREADS", DEFAULT_THREADS))
    tmp = Path(temp_dir) if temp_dir else root / "work" / "duckdb-spill"
    tmp.mkdir(parents=True, exist_ok=True)
    cap = temp_gb if temp_gb is not None else _env_float("JARVIS_RESEARCH_TEMP_GB", DEFAULT_TEMP_GB)
    con = lake.con
    con.execute(f"SET memory_limit='{max(mem, 0.25):g}GB'")
    con.execute(f"SET threads={max(thr, 1)}")
    con.execute(f"SET temp_directory='{tmp}'")
    con.execute(f"SET max_temp_directory_size='{max(cap, 1):g}GB'")
    con.execute("SET lock_configuration=true")
    return ResearchLake(lake, snap, provenance_of(snap))
