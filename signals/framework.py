"""
Signal framework (SCRUM-146): named, versioned signal functions with declared inputs and fixed parameters, a context that builds shared
inputs once, a store outside the collectors' lake, a trial log, and the prefix-invariance guard.

  @register(Spec(id="vrp", version=1, family="vol_gate", grid="snap", inputs=("atm",), params={...}, doc="..."))
  def vrp(ctx: Context, p: dict) -> pd.DataFrame: ...

Rules every signal follows
  * Output columns: `underlying`, either `ts` (naive UTC, for minute grids) or `date` (for daily and snapshot grids), `known_at` (naive
    UTC: the earliest moment the value could have been computed live), then value columns.
  * Parameters are fixed in the Spec before any data is looked at; a change is a new version.
  * A signal reads raw inputs and providers through `ctx`; it may read another signal with `ctx.signal(id)`, never a label
    (Spec.kind == "label": forward-looking outcomes kept for research, e.g. the realised move to expiry).  `ctx.signal` refuses a label.
  * `live_only` signals need the quote feed (from 2026-11-03) and carry `min_sample`: the number of sessions before a value is used.
  * Prefix invariance: computing on data cut at time t gives the same values, for every row known by t, as computing on the full data
    (`check_prefix_invariance`).  The test suite runs it for every registered signal.

Store layout (under the research root, never under lake/):
  signals/<id>/v<version>/underlying=<U>/date=<YYYY-MM-DD>/part.parquet
  trials.jsonl                                       one line per run: signal, version, params hash, range, rows, commit, data provenance
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional

import numpy as np
import pandas as pd

GRIDS = ("1m", "snap", "daily", "expiry", "event")
KINDS = ("signal", "label", "study")


@dataclass(frozen=True)
class Spec:
    id: str
    version: int
    family: str
    grid: str
    inputs: tuple = ()
    params: dict = field(default_factory=dict)
    kind: str = "signal"
    warmup_days: int = 0
    live_only: bool = False
    min_sample: int = 0
    tickets: tuple = ()
    doc: str = ""

    def __post_init__(self):
        if self.grid not in GRIDS:
            raise ValueError(f"{self.id}: grid {self.grid!r} not in {GRIDS}")
        if self.kind not in KINDS:
            raise ValueError(f"{self.id}: kind {self.kind!r} not in {KINDS}")

    @property
    def params_hash(self) -> str:
        return hashlib.sha256(json.dumps(self.params, sort_keys=True, default=str).encode()).hexdigest()[:12]


REGISTRY: dict[str, tuple[Spec, Callable]] = {}
PROVIDERS: dict[str, Callable] = {}


def register(spec: Spec):
    def deco(fn):
        if spec.id in REGISTRY:
            raise ValueError(f"signal {spec.id} registered twice")
        REGISTRY[spec.id] = (spec, fn)
        return fn
    return deco


def provider(name: str):
    """A shared input built from raw inputs (and other providers), cached per context: the option chain, daily realised measures ..."""
    def deco(fn):
        PROVIDERS[name] = fn
        return fn
    return deco


class LabelAccessError(RuntimeError):
    pass


class Context:
    """Raw inputs for one underlying (DataFrames keyed by name) plus cached providers and signal outputs.

    Raw input names: index_1m, vix_1m, fut_1m, opt_1m, contracts, ticks_fo, participant, events.  Each minute frame has a naive-UTC `ts`;
    daily frames have `date` and `known_at`.  `cut` (optional) is the moment the data was cut at, used by the prefix test."""

    def __init__(self, underlying: str, raw: dict[str, pd.DataFrame], cut: Optional[pd.Timestamp] = None, r: float = 0.065,
                 cache: Optional[dict] = None, signals: Optional[dict] = None):
        """`cache` pre-sets providers (e.g. the ATM table read back from the store) and `signals` pre-sets signal outputs, so a long
        history can be computed in two passes without rebuilding the option chain."""
        self.underlying = underlying
        self.raw = raw
        self.cut = cut
        self.r = r
        self._cache: dict[str, object] = dict(cache or {})
        self._signals: dict[str, pd.DataFrame] = dict(signals or {})
        self._stack: list[str] = []

    def get(self, name: str):
        if name in self._cache:
            return self._cache[name]
        if name in self.raw:
            val = self.raw[name]
        elif name in PROVIDERS:
            val = PROVIDERS[name](self)
        else:
            raise KeyError(f"no raw input or provider named {name!r}")
        self._cache[name] = val
        return val

    def has(self, name: str) -> bool:
        df = self.raw.get(name)
        return df is not None and len(df) > 0

    def signal(self, sid: str) -> pd.DataFrame:
        spec, fn = REGISTRY[sid]
        if spec.kind != "signal":
            raise LabelAccessError(f"{sid} is a {spec.kind}: signals may not read forward-looking outputs")
        return self._run(sid)

    def _run(self, sid: str) -> pd.DataFrame:
        if sid in self._signals:
            return self._signals[sid]
        if sid in self._stack:
            raise RuntimeError(f"signal cycle: {' -> '.join(self._stack + [sid])}")
        spec, fn = REGISTRY[sid]
        self._stack.append(sid)
        try:
            out = fn(self, dict(spec.params))
        finally:
            self._stack.pop()
        out = normalise_output(spec, out, self.underlying)
        self._signals[sid] = out
        return out

    def compute(self, sid: str) -> pd.DataFrame:
        """Any registered output, labels and studies included (for the runner and research)."""
        return self._run(sid)


def normalise_output(spec: Spec, out: Optional[pd.DataFrame], underlying: str) -> pd.DataFrame:
    key = "ts" if spec.grid == "1m" else "date"
    if out is None or len(out) == 0:
        return pd.DataFrame(columns=["underlying", key, "known_at"])
    out = out.copy()
    if key not in out.columns or "known_at" not in out.columns:
        raise ValueError(f"{spec.id}: output needs columns {key!r} and 'known_at', has {list(out.columns)}")
    out["underlying"] = underlying
    out["known_at"] = pd.to_datetime(out["known_at"])
    cols = ["underlying", key, "known_at"] + [c for c in out.columns if c not in ("underlying", key, "known_at")]
    return out[cols].sort_values([key, "known_at"] + [c for c in ("snap", "expiry", "strike", "cp", "symbol", "bucket", "group", "participant", "hour", "leg") if c in out.columns]).reset_index(drop=True)


# ---------------------------------------------------------------------------------------------- prefix invariance
def cut_raw(raw: dict[str, pd.DataFrame], cut: pd.Timestamp) -> dict[str, pd.DataFrame]:
    """Every raw frame restricted to what was known at `cut`: tick frames (with an `fts` event time) by that time, minute frames by ts
    (a 1-minute bar stamped at its start is complete one minute later), daily frames by known_at, reference frames (contracts, events)
    unchanged."""
    out = {}
    for k, df in raw.items():
        if df is None or not len(df):
            out[k] = df
        elif "fts" in df.columns:
            out[k] = df[pd.to_datetime(df["fts"]) <= cut]
        elif "known_at" in df.columns:
            out[k] = df[pd.to_datetime(df["known_at"]) <= cut]
        elif "ts" in df.columns:
            out[k] = df[pd.to_datetime(df["ts"]) + pd.Timedelta(minutes=1) <= cut]
        else:
            out[k] = df
    return out


def check_prefix_invariance(sid: str, underlying: str, raw: dict[str, pd.DataFrame], cut: pd.Timestamp, rtol: float = 1e-9,
                            atol: float = 1e-9) -> tuple[bool, str]:
    """Compute `sid` on all of `raw` and on `raw` cut at `cut`; every row known by `cut` must be identical."""
    full = Context(underlying, raw).compute(sid)
    part = Context(underlying, cut_raw(raw, cut), cut=cut).compute(sid)
    full = full[pd.to_datetime(full["known_at"]) <= cut].reset_index(drop=True)
    part = part[pd.to_datetime(part["known_at"]) <= cut].reset_index(drop=True)
    if len(full) != len(part):
        return False, f"{sid}: {len(full)} rows known by the cut on full data, {len(part)} on cut data"
    if not len(full):
        return True, f"{sid}: no rows known by the cut"
    for c in full.columns:
        a, b = full[c], part[c]
        if pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(b) and not pd.api.types.is_bool_dtype(a):
            aa, bb = a.to_numpy(dtype=float), b.to_numpy(dtype=float)
            same = np.isclose(aa, bb, rtol=rtol, atol=atol, equal_nan=True)
            if not same.all():
                i = int(np.argmin(same))
                return False, f"{sid}.{c} row {i}: {aa[i]} on full data, {bb[i]} on cut data"
        elif not a.astype(str).equals(b.astype(str)):
            return False, f"{sid}.{c} differs"
    return True, f"{sid}: {len(full)} rows identical"


# ---------------------------------------------------------------------------------------------- store and trial log
def output_dir(root: Path, spec: Spec, underlying: str, day: date) -> Path:
    return Path(root) / "signals" / spec.id / f"v{spec.version}" / f"underlying={underlying}" / f"date={day}"


def write_output(root: Path, spec: Spec, df: pd.DataFrame) -> int:
    """Write one output, one folder per (underlying, day), replacing what was there for those days.  Returns rows written."""
    if df is None or not len(df):
        return 0
    key = "ts" if spec.grid == "1m" else "date"
    days = pd.to_datetime(df[key]).dt.date
    n = 0
    for (u, d), part in df.groupby([df["underlying"], days]):
        folder = output_dir(root, spec, u, d)
        folder.mkdir(parents=True, exist_ok=True)
        tmp = folder / "part.parquet.tmp"
        part.to_parquet(tmp, index=False)
        tmp.replace(folder / "part.parquet")
        n += len(part)
    return n


def read_output(root: Path, sid: str, underlying: str, version: Optional[int] = None) -> pd.DataFrame:
    spec = REGISTRY[sid][0]
    v = version or spec.version
    base = Path(root) / "signals" / sid / f"v{v}" / f"underlying={underlying}"
    files = sorted(base.glob("date=*/part.parquet"))
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True) if files else pd.DataFrame()


def git_commit() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, timeout=5,
                              cwd=Path(__file__).resolve().parents[1]).stdout.strip()
    except Exception:                                               # noqa: BLE001
        return ""


def log_trial(root: Path, spec: Spec, underlying: str, first: object, last: object, rows: int, provenance: Optional[dict] = None,
              note: str = "") -> dict:
    """Append one run to the trial log; the deflated Sharpe ratio later counts these."""
    rec = {"at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "signal": spec.id, "version": spec.version, "kind": spec.kind,
           "params": spec.params, "params_hash": spec.params_hash, "underlying": underlying, "first": str(first), "last": str(last),
           "rows": int(rows), "commit": git_commit(), "provenance": provenance or {}, "note": note}
    p = Path(root) / "trials.jsonl"
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a") as f:
        f.write(json.dumps(rec, default=str) + "\n")
    return rec


def read_trials(root: Path) -> list[dict]:
    p = Path(root) / "trials.jsonl"
    return [json.loads(x) for x in p.read_text().splitlines() if x.strip()] if p.exists() else []


def specs(kind: Optional[str] = None, family: Optional[str] = None) -> list[Spec]:
    return sorted((s for s, _ in REGISTRY.values() if (kind is None or s.kind == kind) and (family is None or s.family == family)),
                  key=lambda s: (s.family, s.id))


# ---------------------------------------------------------------------------------------------- small helpers shared by signals
def trailing_z(x: pd.Series, n: int, min_periods: Optional[int] = None) -> pd.Series:
    """(x - mean of the previous n values) / std of the previous n values: the current value is not in its own baseline."""
    prev = x.shift(1)
    mp = min_periods or max(5, n // 2)
    m = prev.rolling(n, min_periods=mp).mean()
    s = prev.rolling(n, min_periods=mp).std()
    return (x - m) / s.replace(0, np.nan)


def trailing_pct(x: pd.Series, n: int, min_periods: Optional[int] = None) -> pd.Series:
    """Share of the previous n values below the current one (0 to 1)."""
    mp = min_periods or max(5, n // 2)
    vals = x.to_numpy(dtype=float)
    out = np.full(len(vals), np.nan)
    for i in range(len(vals)):
        hist = vals[max(0, i - n):i]
        hist = hist[~np.isnan(hist)]
        if len(hist) >= mp and not np.isnan(vals[i]):
            out[i] = float((hist < vals[i]).mean())
    return pd.Series(out, index=x.index)


def iter_unique(xs: Iterable) -> list:
    seen, out = set(), []
    for x in xs:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out
