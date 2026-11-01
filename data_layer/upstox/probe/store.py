"""Where the probe keeps what it collects: Parquet in the same layout production would use, so sizes are realistic."""

from __future__ import annotations

import json
import logging
import threading
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import pyarrow.parquet as pq

from ...tick_store import _write_segment

log = logging.getLogger(__name__)


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    """Make a frame safe for Parquet: nested values become JSON text, columns that are always empty are dropped."""
    for c in list(df.columns):
        col = df[c]
        if col.dtype == object:
            if col.map(lambda v: isinstance(v, (dict, list))).any():
                df[c] = col.map(lambda v: json.dumps(v, separators=(",", ":")) if isinstance(v, (dict, list)) else v)
            if df[c].isna().all():
                df = df.drop(columns=c)
                continue
            kinds = {type(v).__name__ for v in df[c].dropna()}
            if len(kinds) > 1:
                df[c] = df[c].map(lambda v: None if pd.isna(v) else str(v))
    return df


class ProbeStore:
    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.written: dict[str, dict] = {}
        self._samples: dict[str, int] = {}
        self._lock = threading.Lock()

    def total_bytes_written(self) -> int:
        with self._lock:
            return sum(v["bytes"] for v in self.written.values())

    def write(self, dataset: str, rows: Iterable[dict], day: Optional[date] = None) -> tuple[int, int]:
        """Append rows as a Parquet segment under <root>/<dataset>/date=<day>/.  Returns (rows, bytes)."""
        rows = list(rows)
        if not rows:
            return 0, 0
        day = day or datetime.now(timezone.utc).date()
        try:
            df = _clean(pd.DataFrame(rows))
            path = _write_segment(self.root / dataset / f"date={day}", df)
        except Exception:                                                # noqa: BLE001 -- keep the run going, keep the data
            log.exception("could not write %d %s rows as Parquet; saving raw JSON instead", len(rows), dataset)
            folder = self.root / "_failed" / dataset
            folder.mkdir(parents=True, exist_ok=True)
            path = folder / f"{datetime.now(timezone.utc):%H%M%S%f}.json"
            path.write_text(json.dumps(rows, default=str))
        size = path.stat().st_size
        with self._lock:
            w = self.written.setdefault(dataset, {"rows": 0, "bytes": 0, "writes": 0})
            w["rows"] += len(rows)
            w["bytes"] += size
            w["writes"] += 1
        return len(rows), size

    def write_blob(self, dataset: str, name: str, content: bytes, day: Optional[date] = None) -> int:
        day = day or datetime.now(timezone.utc).date()
        folder = self.root / dataset / f"date={day}"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / name).write_bytes(content)
        with self._lock:
            w = self.written.setdefault(dataset, {"rows": 0, "bytes": 0, "writes": 0})
            w["bytes"] += len(content)
            w["writes"] += 1
        return len(content)

    def save_sample(self, name: str, obj: object, limit: int = 2) -> None:
        """Keep the first responses of each endpoint as JSON, so parsers can be fixed against the real shape."""
        with self._lock:
            n = self._samples.get(name, 0)
            if n >= limit:
                return
            self._samples[name] = n + 1
        folder = self.root / "_samples"
        folder.mkdir(parents=True, exist_ok=True)
        text = json.dumps(obj, default=str)
        (folder / f"{name.replace('/', '_')}-{n + 1}.json").write_text(text[:200_000])

    def sizes(self) -> dict[str, dict]:
        """On-disk rows, files and bytes per dataset (reads Parquet footers; skips folders starting with underscore)."""
        out: dict[str, dict] = {}
        for d in sorted(p for p in self.root.iterdir() if p.is_dir() and not p.name.startswith("_") and p.name != "reports"):
            files = [f for f in d.rglob("*") if f.is_file()]
            rows = 0
            for f in files:
                if f.suffix == ".parquet":
                    try:
                        rows += pq.ParquetFile(f).metadata.num_rows
                    except Exception:                                    # noqa: BLE001
                        pass
            out[d.name] = {"files": len(files), "bytes": sum(f.stat().st_size for f in files), "rows": rows}
        return out
