"""
Heavy work for the viewer, run in a short-lived child process with a memory cap (SCRUM-216, 22 Nov 2026).

The viewer is one long-lived process.  Every heavy request (the Market overview, an F&O chain, the first read of a signals day) used to build large pandas
frames inside it, and Python does not give that memory back: the viewer settled at the biggest peak it had ever seen (2.6 GB on 20 Nov, five incidents in
three days).  Here the heavy part runs in a child that exits, so the operating system takes every byte back, and the viewer stays small.

  run_child(args, timeout_s, mem_mb)   one child at a time (a slot); a second caller waits briefly and then gets QueryBusy (the viewer answers 503, Retry-After)
                                       the child is killed when it runs over `timeout_s` or its resident memory passes `mem_mb`

Children count against the viewer service's own memory cap (they share its cgroup), so the child cap plus the viewer's own size must stay under MemoryMax:
the defaults are 1400 MB for a child against 2 GB for the service (deploy/jarvis-viewer.service).
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from typing import Optional

from data_layer.query import QueryBusy

log = logging.getLogger("webapp.heavy")

_SLOT = threading.Lock()
POLL_S = 0.25


def child_mem_mb() -> float:
    try:
        return float(os.environ.get("JARVIS_VIEWER_CHILD_MB", 1400))
    except ValueError:
        return 1400.0


def children_enabled() -> bool:
    """JARVIS_VIEWER_CHILD=0 runs the heavy work in the viewer process again (what it did before 22 Nov 2026), as a fallback."""
    return os.environ.get("JARVIS_VIEWER_CHILD", "1") != "0"


@dataclass
class ChildResult:
    returncode: Optional[int]
    stdout: str
    stderr: str
    killed: Optional[str] = None          # "timeout" or "memory"
    seconds: float = 0.0
    peak_mb: float = 0.0

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and not self.killed


def _rss_mb(pid: int) -> float:
    try:
        with open(f"/proc/{pid}/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 2**20
    except (OSError, ValueError, IndexError):
        return 0.0


def slot_busy() -> bool:
    return _SLOT.locked()


def run_child(args: list, timeout_s: float = 300.0, mem_mb: Optional[float] = None, wait_slot_s: float = 5.0, env: Optional[dict] = None,
              cwd: Optional[str] = None, python: Optional[str] = None) -> ChildResult:
    """Run `python <args>` in a child; raises QueryBusy when another child holds the slot for longer than `wait_slot_s`."""
    mem_mb = mem_mb if mem_mb is not None else child_mem_mb()
    if not _SLOT.acquire(timeout=wait_slot_s):
        raise QueryBusy("another heavy request is running: try again in a few seconds")
    try:
        e = dict(os.environ)
        e.update(env or {})
        e.setdefault("JARVIS_VIEWER_MEM_GUARD_MB", str(int(mem_mb * 0.85)))          # the child's own loader stops before the watchdog below kills it
        e.setdefault("MALLOC_ARENA_MAX", "2")
        t0, peak = time.monotonic(), 0.0
        with tempfile.TemporaryFile("w+") as out, tempfile.TemporaryFile("w+") as err:
            proc = subprocess.Popen([python or sys.executable, *[str(a) for a in args]], stdout=out, stderr=err, env=e, cwd=cwd, text=True)
            killed = None
            while proc.poll() is None:
                time.sleep(POLL_S)
                peak = max(peak, _rss_mb(proc.pid))
                if peak > mem_mb:
                    killed = "memory"
                elif time.monotonic() - t0 > timeout_s:
                    killed = "timeout"
                if killed:
                    proc.kill()
                    proc.wait()
                    log.warning("heavy child %s killed (%s): %.0f MB after %.0f s", args[:3], killed, peak, time.monotonic() - t0)
                    break
            out.seek(0); err.seek(0)
            return ChildResult(proc.returncode, out.read(), err.read()[-4000:], killed, time.monotonic() - t0, peak)
    finally:
        _SLOT.release()
