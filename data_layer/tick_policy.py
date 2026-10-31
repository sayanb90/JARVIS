"""
What the live feed keeps of the ticks it receives -- one setting, chosen by config, applied before anything is written.

  full      (option C, default)  every tick, nothing dropped
  throttle  (option B)           at most one tick per symbol per `min_interval_s` of exchange time
  changes   (option A)           a tick only when price, volume, quote or open interest differ from the last one kept
                                 (a tick that only carries new Greeks is dropped)

Orthogonal to the mode, `greeks` says which option Greeks are kept (all | iv | none) and `decimals` rounds them
(None keeps the float32 the feed delivers; fewer digits compress much better).

Config (`.env` or the process environment; the command line of `python -m data_layer.upstox.feed` overrides it):
  JARVIS_TICK_STORE            full | throttle | changes      (also accepts the letters c | b | a)
  JARVIS_TICK_STORE_EQ / _FO   the same, for one segment only (wins over JARVIS_TICK_STORE)
  JARVIS_TICK_MIN_INTERVAL_S   seconds for `throttle`, default 1
  JARVIS_GREEKS                all | iv | none, default all
  JARVIS_GREEKS_DECIMALS       digits to round the Greeks to, default: no rounding

Changing it only affects ticks written from then on; stored days are never rewritten.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping, Optional

from .ticks import Tick

MODES = ("full", "throttle", "changes")
GREEKS = ("all", "iv", "none")
_LETTERS = {"c": "full", "b": "throttle", "a": "changes"}
_GREEK_FIELDS = ("delta", "theta", "gamma", "vega", "rho")
_MARKET_FIELDS = ("ltp", "vol_today", "bid", "bid_qty", "ask", "ask_qty", "oi")


@dataclass
class TickPolicy:
    mode: str = "full"
    min_interval_s: float = 1.0
    greeks: str = "all"
    decimals: Optional[int] = None
    kept: int = field(default=0, init=False)
    dropped: int = field(default=0, init=False)
    _last: dict = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        self.mode = _LETTERS.get(str(self.mode).strip().lower(), str(self.mode).strip().lower())
        self.greeks = str(self.greeks).strip().lower()
        if self.mode not in MODES:
            raise ValueError(f"tick store mode must be one of {MODES} (or c, b, a), got {self.mode!r}")
        if self.greeks not in GREEKS:
            raise ValueError(f"greeks must be one of {GREEKS}, got {self.greeks!r}")
        if self.min_interval_s < 0:
            raise ValueError("min_interval_s must be >= 0")
        if self.decimals is not None and not 0 <= self.decimals <= 10:
            raise ValueError("decimals must be between 0 and 10")

    def describe(self) -> str:
        parts = [f"mode={self.mode}"]
        if self.mode == "throttle":
            parts.append(f"min_interval={self.min_interval_s:g}s")
        parts.append(f"greeks={self.greeks}")
        if self.decimals is not None:
            parts.append(f"decimals={self.decimals}")
        return " ".join(parts)

    def apply(self, t: Tick) -> Optional[Tick]:
        """The tick to store (greeks trimmed and rounded as configured), or None to drop it."""
        if self.mode != "full":
            prev = self._last.get(t.symbol)
            if self.mode == "throttle":
                if prev is not None and (t.ts - prev).total_seconds() < self.min_interval_s:
                    self.dropped += 1
                    return None
                self._last[t.symbol] = t.ts
            else:
                key = tuple(getattr(t, f) for f in _MARKET_FIELDS)
                if prev == key:
                    self.dropped += 1
                    return None
                self._last[t.symbol] = key
        self._shape_greeks(t)
        self.kept += 1
        return t

    def _shape_greeks(self, t: Tick) -> None:
        if self.greeks != "all":
            for f in _GREEK_FIELDS:
                setattr(t, f, None)
            if self.greeks == "none":
                t.iv = None
        if self.decimals is not None:
            for f in ("iv",) + _GREEK_FIELDS:
                v = getattr(t, f)
                if v is not None:
                    setattr(t, f, round(v, self.decimals))


def policy_from_env(segment: str, env: Optional[Mapping[str, str]] = None) -> TickPolicy:
    """The policy for one segment ('eq' or 'fo') from the environment (see the module docstring)."""
    env = os.environ if env is None else env
    def get(name: str) -> str:
        return (env.get(name) or "").strip()
    mode = get(f"JARVIS_TICK_STORE_{segment.upper()}") or get("JARVIS_TICK_STORE") or "full"
    dec = get("JARVIS_GREEKS_DECIMALS")
    return TickPolicy(mode=mode,
                      min_interval_s=float(get("JARVIS_TICK_MIN_INTERVAL_S") or 1.0),
                      greeks=get("JARVIS_GREEKS") or "all",
                      decimals=int(dec) if dec else None)
