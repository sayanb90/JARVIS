"""
How often each live signal is computed: 1 s, 10 s or 1 min (owner decision 14 Nov 2026; chosen per signal from the feasibility
measurements, see `python -m signals feasibility`).

Config: signals/cadence.toml (tracked), or the file named by JARVIS_SIGNAL_CADENCE_FILE.

    default = "1min"                 # any live signal not listed
    [signal]
    fut_ofi = "1s"

JARVIS_SIGNAL_CADENCE (1s, 10s or 1m), when set, overrides the file's default (not the per-signal lines).  A signal may not run faster
than its MIN_CADENCE: those are built on minute-of-day patterns of earlier sessions (1-minute candles), so a faster bucket would read
a pattern that does not exist at that resolution.  An invalid config stops the engine at start-up, never mid-session.
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Optional

import pandas as pd

ALLOWED = {"1s": "1s", "1sec": "1s", "10s": "10s", "10sec": "10s", "1m": "1min", "1min": "1min"}
ORDER = ["1s", "10s", "1min"]
CONFIG = Path(__file__).with_name("cadence.toml")

# Signals that cannot run below 1 minute, and why.
MIN_CADENCE: dict[str, tuple[str, str]] = {
    "noise_band": ("1min", "the band is a minute-of-day average of 14 sessions of 1-minute moves"),
    "fair_straddle": ("1min", "variance left in the day comes from the 1-minute time-of-day profile of earlier sessions"),
    "vrp_intraday": ("1min", "realised variance so far is scaled by the 1-minute time-of-day profile of earlier sessions"),
    "volume_burst": ("1min", "the baseline is earlier sessions' volume per 5-minute slot from 1-minute candles"),
    "residual_iv": ("1min", "reads the ATM IV exactly 5 minutes earlier on the 1-minute grid"),
}


def norm(v: str) -> str:
    k = str(v).strip().lower()
    if k not in ALLOWED:
        raise ValueError(f"cadence {v!r}: use 1s, 10s or 1min")
    return ALLOWED[k]


def load(ids: list[str], path: Optional[Path] = None, env=None) -> dict[str, str]:
    """Cadence per live signal id.  Raises ValueError on an unknown signal, an unknown cadence, or a cadence below MIN_CADENCE."""
    env = os.environ if env is None else env
    p = Path(path or env.get("JARVIS_SIGNAL_CADENCE_FILE") or CONFIG)
    cfg = tomllib.loads(p.read_text()) if p.exists() else {}
    default = norm(env.get("JARVIS_SIGNAL_CADENCE") or cfg.get("default", "1min"))
    per = {k: norm(v) for k, v in (cfg.get("signal") or {}).items()}
    unknown = set(per) - set(ids)
    if unknown:
        raise ValueError(f"{p}: not live signals: {sorted(unknown)}")
    out = {}
    for sid in ids:
        c = per.get(sid, default)
        floor = MIN_CADENCE.get(sid, ("1s", ""))[0]
        if ORDER.index(c) < ORDER.index(floor):
            if sid in per:
                raise ValueError(f"{sid}: {c} is faster than its minimum {floor} ({MIN_CADENCE[sid][1]})")
            c = floor                                    # the default is faster than this signal allows: run it at its minimum
        out[sid] = c
    return out


def groups(cadence: dict[str, str]) -> dict[str, list[str]]:
    """Signals by cadence, finest first."""
    out: dict[str, list[str]] = {}
    for sid, c in cadence.items():
        out.setdefault(c, []).append(sid)
    return {c: sorted(out[c]) for c in ORDER if c in out}


def td(c: str) -> pd.Timedelta:
    return pd.Timedelta(norm(c))


def parse_only(text: Optional[str]) -> Optional[list[str]]:
    """A comma list of cadences ("1s,10s", "1min") for a process that computes only those: None means all.  Raises ValueError on an unknown one."""
    if text is None or not str(text).strip():
        return None
    out = [norm(x.strip()) for x in str(text).split(",") if x.strip()]
    unknown = [x for x in out if x not in ORDER]
    if unknown:
        raise ValueError(f"unknown cadence {unknown}: use {list(ORDER)}")
    return sorted(set(out), key=ORDER.index)


def only(cadence: dict[str, str], cadences: Optional[list[str]]) -> dict[str, str]:
    """The signals of `cadence` that run at one of `cadences` (all of them when None)."""
    return dict(cadence) if cadences is None else {sid: c for sid, c in cadence.items() if c in cadences}


def tag(cadences: Optional[list[str]]) -> str:
    """"" for a process that computes the finest cadence (it keeps the plain names: signals-NIFTY, engine-NIFTY.heartbeat.json); otherwise
    "-<cadences>", so a second process of the same underlying (the one-minute signals) has its own bus endpoint and heartbeat file."""
    if cadences is None or ORDER[0] in cadences:
        return ""
    return "-" + "+".join(cadences)
