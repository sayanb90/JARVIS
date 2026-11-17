"""
A sampling profiler for the live engine (17 Nov 2026), in the standard library: a daemon thread looks at the engine thread's stack every
`interval` seconds and counts, for each function on the stack, how many samples it was in (cumulative), and for the innermost frame (self).
`report()` returns the top of both as log lines and resets the counters, so the engine's log carries a profile of real data every status
interval.  No signals (a signal could interrupt a ZeroMQ call), no installed tools, about 0.2% of a core at 20 ms.

How to read it: a sample is taken whenever the sampler thread gets the interpreter lock, i.e. at most every 5 ms of the engine thread's
Python time and after each long C call, so many short calls (numpy, pandas) are represented fairly and one long C call that holds the
lock counts once.  Samples in `poll` are the engine waiting for ticks (idle).
"""

from __future__ import annotations

import collections
import sys
import threading
from typing import Optional


def _short(path: str) -> str:
    for marker in ("/JARVIS/", "/site-packages/", "/lib/python"):
        i = path.rfind(marker)
        if i >= 0:
            return path[i + len(marker):]
    return path


class StackSampler:
    def __init__(self, target_ident: Optional[int] = None, interval: float = 0.02):
        self.target = target_ident or threading.get_ident()
        self.interval = interval
        self._self: collections.Counter = collections.Counter()
        self._cum: collections.Counter = collections.Counter()
        self._n = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="stack-sampler", daemon=True)

    def start(self) -> "StackSampler":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        me = threading.get_ident()
        while not self._stop.wait(self.interval):
            frame = sys._current_frames().get(self.target)
            if frame is None or self.target == me:
                continue
            innermost = (frame.f_code.co_filename, frame.f_code.co_name, frame.f_code.co_firstlineno)
            seen = set()
            keys = []
            f = frame
            while f is not None:
                k = (f.f_code.co_filename, f.f_code.co_name, f.f_code.co_firstlineno)
                if k not in seen:
                    seen.add(k)
                    keys.append(k)
                f = f.f_back
            with self._lock:
                self._n += 1
                self._self[innermost] += 1
                self._cum.update(keys)

    def report(self, top_self: int = 10, top_cum: int = 18, reset: bool = True) -> list[str]:
        """Log lines: the samples taken, the innermost functions, and the functions with the most samples under them (own code first)."""
        with self._lock:
            n, own, cum = self._n, self._self.copy(), self._cum.copy()
            if reset:
                self._n, self._self, self._cum = 0, collections.Counter(), collections.Counter()
        if not n:
            return ["profile: no samples"]

        def fmt(k, c):
            return f"{100 * c / n:4.0f}%  {_short(k[0])}:{k[1]}:{k[2]}"

        project = [(k, c) for k, c in cum.items() if "/site-packages/" not in k[0] and "/lib/python" not in k[0]]
        out = [f"profile: {n} samples of {1000 * self.interval:.0f} ms (cumulative = functions the engine was inside; self = innermost frame)"]
        out.append("profile self: " + " | ".join(fmt(k, c) for k, c in own.most_common(top_self)))
        out.append("profile cumulative (our code): " + " | ".join(fmt(k, c) for k, c in sorted(project, key=lambda x: -x[1])[:top_cum]))
        return out
