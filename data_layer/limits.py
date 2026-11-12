"""
Raise the open-file limit for the jobs that fetch from Yahoo.

yfinance opens a connection per request from a pool of threads, and a nightly run over about two thousand tickers goes past the usual soft
limit of 1,024 open files: every market then failed with "[Errno 24] Too many open files" (the 23:00 UTC daily-candle cron job on 2026-11-11,
and the 1-minute backfill the same day).  A shell's `ulimit -n 65535` fixed a hand-started run but not cron, which starts jobs with the default.
Raising the soft limit to the hard limit inside the process makes the jobs independent of how they were started.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)
TARGET = 65536


def raise_open_files(target: int = TARGET) -> tuple[int, int] | None:
    """Raise this process's soft limit on open files to `target` (or the hard limit, if lower).  Returns (soft, hard), or None where the
    platform has no such limit.  Never raises: a job that cannot change the limit runs with what it has."""
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        want = target if hard == resource.RLIM_INFINITY else min(target, hard)
        if soft == resource.RLIM_INFINITY or soft >= want:
            return soft, hard
        resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))
        log.info("open-file limit raised from %d to %d", soft, want)
        return want, hard
    except (ImportError, ValueError, OSError) as exc:                  # noqa: BLE001
        log.warning("could not raise the open-file limit: %s", exc)
        return None
