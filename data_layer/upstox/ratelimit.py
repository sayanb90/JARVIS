"""
Request limiter for the Upstox REST API.

Upstox allows, per user and per API: 50 requests a second, 500 a minute and 2,000 per 30 minutes (rate-limiting page).
Going over "might result in temporary suspension of access", so every REST job shares one `RateLimiter` and waits
instead of finding out.  A small safety margin keeps us just below the published numbers.

`acquire()` blocks until a request may go; the clock and sleep are injectable so tests run instantly.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Callable

DEFAULT_WINDOWS = ((1.0, 50), (60.0, 500), (1800.0, 2000))


class RateLimiter:
    def __init__(self, windows=DEFAULT_WINDOWS, margin: float = 0.9, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.windows = [(span, max(1, int(limit * margin))) for span, limit in windows]
        self.clock, self.sleep = clock, sleep
        self._stamps: deque[float] = deque()
        self._lock = threading.Lock()
        self.requests = 0
        self.waited_s = 0.0

    def _wait_needed(self, now: float) -> float:
        longest = self.windows[-1][0]
        while self._stamps and now - self._stamps[0] > longest:
            self._stamps.popleft()
        need = 0.0
        for span, limit in self.windows:
            inside = [t for t in self._stamps if now - t < span]
            if len(inside) >= limit:
                need = max(need, inside[len(inside) - limit] + span - now)       # until the oldest counted request ages out
        return need

    def acquire(self) -> float:
        """Wait until a request is allowed, record it and return how long we waited."""
        waited = 0.0
        while True:
            with self._lock:
                now = self.clock()
                need = self._wait_needed(now)
                if need <= 0:
                    self._stamps.append(now)
                    self.requests += 1
                    self.waited_s += waited
                    return waited
            self.sleep(min(need, 5.0) + 0.001)
            waited += min(need, 5.0) + 0.001
