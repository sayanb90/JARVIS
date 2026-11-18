"""
Shadow parity (SCRUM-205): the vectorised native operators run next to the reference implementations they replaced (signals/engine/operators_ref.py)
on the same ticks, bucket by bucket, and every difference is counted and logged.  This is how an optimisation proves on the live feed that the
values did not change: the reference is the code that produced the stored history.

  JARVIS_SHADOW_PARITY=1        on (the crontab sets it for the first sessions after an optimisation; the cost is that of the old loops)
  JARVIS_SHADOW_RTOL=1e-9       relative tolerance of numbers (sums in another order differ in the last bits)

The status line of the engine carries `parity ok=N differ=M`; a difference is a WARNING with the signal, the column and the largest gap, once per
signal and 5 minutes.  `report()` returns the totals for the heartbeat and the end-of-session summary.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger("signals.engine.shadow")

DEFAULT_RTOL = 1e-9
ATOL = 1e-12
WARN_EVERY_S = 300.0


def enabled(env) -> bool:
    return str(env.get("JARVIS_SHADOW_PARITY", "")).strip().lower() in ("1", "true", "yes", "on")


def rtol(env) -> float:
    try:
        return float(env.get("JARVIS_SHADOW_RTOL", DEFAULT_RTOL))
    except ValueError:
        return DEFAULT_RTOL


def compare(a: Optional[pd.DataFrame], b: Optional[pd.DataFrame], rtol: float = DEFAULT_RTOL) -> Optional[dict]:
    """None when the two frames are the same rows (numbers within `rtol`, everything else equal); else what differs:
    {"what": "rows"|"columns"|column name, "gap": largest relative gap for numbers, "at": row}."""
    na, nb = 0 if a is None else len(a), 0 if b is None else len(b)
    if na != nb:
        return {"what": "rows", "gap": float(abs(na - nb)), "at": None, "detail": f"{na} vs {nb}"}
    if not na:
        return None
    if sorted(a.columns) != sorted(b.columns):
        return {"what": "columns", "gap": math_inf(), "at": None, "detail": f"{sorted(set(a.columns) ^ set(b.columns))}"}
    worst = None
    for c in a.columns:
        x, y = a[c].reset_index(drop=True), b[c].reset_index(drop=True)
        if pd.api.types.is_numeric_dtype(x) and pd.api.types.is_numeric_dtype(y):
            xv, yv = x.to_numpy(dtype=float), y.to_numpy(dtype=float)
            same_nan = np.isnan(xv) == np.isnan(yv)
            with np.errstate(invalid="ignore"):
                same_inf = ~np.isinf(xv) | (xv == yv)
                gap = np.abs(xv - yv) / np.maximum(np.abs(yv), ATOL)
                bad = ~same_nan | ~same_inf | (~np.isnan(xv) & ~np.isinf(xv) & (np.abs(xv - yv) > ATOL + rtol * np.abs(yv)))
            if bad.any():
                i = int(np.argmax(bad))
                g = float(np.nanmax(np.where(bad, np.where(np.isfinite(gap), gap, np.inf), 0.0)))
                if worst is None or g > worst["gap"]:
                    worst = {"what": c, "gap": g, "at": i, "detail": f"{xv[i]!r} vs {yv[i]!r}"}
        else:
            xs, ys = x.astype(object).where(x.notna(), None), y.astype(object).where(y.notna(), None)
            bad = ~np.array([p == q for p, q in zip(xs, ys)], dtype=bool)
            if bad.any():
                i = int(np.argmax(bad))
                if worst is None:
                    worst = {"what": c, "gap": math_inf(), "at": i, "detail": f"{xs.iloc[i]!r} vs {ys.iloc[i]!r}"}
    return worst


def math_inf() -> float:
    return float("inf")


class ShadowParity:
    """Per engine: a reference operator set per cadence group, fed the same ticks as the production one."""

    def __init__(self, underlying: str, rtol_: float = DEFAULT_RTOL):
        self.underlying = underlying
        self.rtol = rtol_
        self.ref: dict = {}                                    # cadence -> ReferenceNatives
        self.ok = defaultdict(int)                             # signal -> buckets equal
        self.differ = defaultdict(int)                         # signal -> buckets that differ
        self.worst: dict = {}                                  # signal -> (gap, column, detail)
        self.checked_s = 0.0                                   # time spent in the reference and the comparison
        self._warned: dict = {}

    def check(self, g, tp, chain_live, book, produced: dict) -> None:
        """Run the reference operators of group `g` on `tp` and compare with `produced` (signal -> frame of the production operators)."""
        from signals.engine.operators_ref import REFERENCE_IDS, ReferenceNatives
        ids = [s for s in g.natives.ids if s in REFERENCE_IDS]
        if not ids:
            return
        t0 = time.perf_counter()
        ref = self.ref.get(g.cadence)
        if ref is None:
            ref = self.ref[g.cadence] = ReferenceNatives(ids, g.td, g.natives.meta)
        ref.ids = ids
        want = ref.run(tp, chain_live=chain_live, book=book)
        for sid in ids:
            d = compare(produced.get(sid), want.get(sid), self.rtol)
            if d is None:
                self.ok[sid] += 1
                continue
            self.differ[sid] += 1
            if sid not in self.worst or d["gap"] > self.worst[sid][0]:
                self.worst[sid] = (d["gap"], d["what"], d.get("detail"))
            now = time.monotonic()
            if now - self._warned.get(sid, -1e9) > WARN_EVERY_S:
                self._warned[sid] = now
                log.warning("%s: PARITY %s at %s differs from the reference: %s (gap %.3g, %s); %d buckets so far",
                            self.underlying, sid, g.cadence, d["what"], d["gap"], d.get("detail"), self.differ[sid])
        self.checked_s += time.perf_counter() - t0

    def line(self) -> str:
        ok, bad = sum(self.ok.values()), sum(self.differ.values())
        s = f"parity ok={ok} differ={bad} ({1000 * self.checked_s:.0f} ms spent)"
        if bad:
            s += " in " + ", ".join(f"{k}:{v}" for k, v in sorted(self.differ.items()))
        return s

    def report(self) -> dict:
        return {"ok": dict(self.ok), "differ": dict(self.differ),
                "worst": {k: {"gap": v[0], "column": v[1], "detail": v[2]} for k, v in self.worst.items()}}
