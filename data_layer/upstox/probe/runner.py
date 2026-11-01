"""
Orchestration of the probe: streams, scheduled REST jobs, resource log, partial reports.

A run is safe by construction:
  * everything is written to the probe directory, never to the production lake
  * a disk guard stops writing when free space is below a floor or the probe directory is over its cap
  * every job and every stream is wrapped, so one failure never stops the others or the report
  * SIGINT and SIGTERM end the run cleanly: streams stop, ticks are compacted, the final report is written
"""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import signal
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

from .jobs import JOBS, Ctx
from .report import GB, build_report, write_report
from .store import ProbeStore
from .. import feed as feedmod
from ..rest import UpstoxRest
from ... import tick_store
from ...sysmon import ResourceMonitor, dir_size

log = logging.getLogger(__name__)

SETUP_JOBS = ["market_info", "expired_sample", "fundamentals_sample", "candles_sample", "option_stats", "flows"]
# (job, UTC times as HH:MM) for the session day; cadence jobs are expanded below
FIXED_TIMES = {"quotes": ["03:55", "10:05"], "flows": ["10:35"], "market_info": ["10:35"]}
EVERY = {"option_chain": 15, "option_stats": 60, "smartlists": 60, "news": 30, "global_cues": 5}


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass
class Guard:
    root: Path
    min_free_gb: float = 15.0
    max_gb: float = 25.0
    ttl: float = 30.0
    reason: str = ""
    _at: float = 0.0
    _ok: bool = True

    def ok(self) -> bool:
        now = time.monotonic()
        if now - self._at < self.ttl:
            return self._ok
        self._at = now
        free = shutil.disk_usage(self.root).free / GB
        size = dir_size(self.root) / GB
        if free < self.min_free_gb:
            self._ok, self.reason = False, f"free disk {free:.1f} GB is below the {self.min_free_gb} GB floor"
        elif size > self.max_gb:
            self._ok, self.reason = False, f"probe directory {size:.1f} GB is over its {self.max_gb} GB cap"
        else:
            self._ok = True
        if not self._ok:
            log.error("DISK GUARD: %s; probe writes stop", self.reason)
        return self._ok


class GuardedFeed(feedmod.UpstoxFeed):
    """The production feed with the extended fields on, whose writes stop when the disk guard says so."""

    def __init__(self, *a, guard: Optional[Guard] = None, **kw) -> None:
        super().__init__(*a, **kw)
        self.guard, self.guard_stopped = guard, ""

    def persist(self, final: bool = False) -> int:
        if self.guard and not self.guard.ok():
            self.proc.drain()
            self.proc.drain_events()
            self.guard_stopped = self.guard.reason
            return 0
        return super().persist(final)


class NullFeed(feedmod.UpstoxFeed):
    """Keeps ticks in memory instead of writing them: for the short stream check."""

    def persist(self, final: bool = False) -> int:
        return 0


# ----------------------------------------------------------------------------------------------------- the live streams
class ProbeStream:
    """One WebSocket connection in its own thread, writing ticks with the extended fields into <root>/<name>/."""

    def __init__(self, name: str, key_map: dict, root: Path, token_provider: Callable[[], str], start: datetime, end: datetime,
                 guard: Guard, url: str = feedmod.FEED_URL, clock=None, day: Optional[date] = None) -> None:
        self.name, self.key_map, self.start, self.end = name, key_map, start, end
        self.day = day or end.date()                                     # the UTC day whose ticks are compacted at the end
        self.root = Path(root) / name
        kw = {"clock": clock} if clock else {}
        self.feed = GuardedFeed(token_provider(), key_map, url=url, mode="full", lake_dir=self.root, ext=True, guard=guard,
                                token_provider=token_provider, auth_retry_s=60.0, write_s=15.0, **kw)
        self.error = ""
        self.compacted_rows = 0
        self.per_minute: list[tuple[str, int, int]] = []
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop: Optional[asyncio.Event] = None
        self._thread = threading.Thread(target=self._run, name=f"probe-stream-{name}", daemon=True)

    def begin(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        try:
            asyncio.run(self._main())
        except Exception as exc:                                         # noqa: BLE001
            self.error = f"{type(exc).__name__}: {exc}"[:300]
            log.exception("stream %s failed", self.name)

    async def _main(self) -> None:
        self._loop, self._stop = asyncio.get_running_loop(), asyncio.Event()
        wait = (self.start - utcnow()).total_seconds()
        if wait > 0:
            log.info("stream %s (%d instruments) starts at %s UTC", self.name, len(self.key_map), self.start)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=wait)
            except asyncio.TimeoutError:
                pass
        if self._stop.is_set():
            return

        async def stopper():
            await asyncio.sleep(max(0.0, (self.end - utcnow()).total_seconds()))
            self._stop.set()
        t = asyncio.create_task(stopper())
        try:
            await self.feed.run_session(self._stop)
        finally:
            t.cancel()
        self.compacted_rows = tick_store.compact_ticks_day(self.day, self.root)
        log.info("stream %s finished: %d frames, %d ticks, %d rows after compaction", self.name, self.feed.proc.frames,
                 self.feed.proc.ticks, self.compacted_rows)

    def request_stop(self) -> None:
        if self._loop and self._stop:
            self._loop.call_soon_threadsafe(self._stop.set)

    def join(self, timeout: float = 300.0) -> None:
        self._thread.join(timeout)

    def sample_rate(self) -> None:
        self.per_minute.append((utcnow().strftime("%H:%M"), self.feed.proc.frames, self.feed.proc.ticks))

    def info(self) -> dict:
        p = self.feed.proc
        return {"keys": len(self.key_map), "frames": p.frames, "ticks": p.ticks, "dropped_stale": p.dropped_stale,
                "compacted_rows": self.compacted_rows, "error": self.error, "guard_stopped": self.feed.guard_stopped,
                "per_minute": self.per_minute[-400:]}


def save_state(root: Path, mode: str, monitor: dict, rest: dict, results: list, streams: dict, universe_scale: dict, meta: dict,
               partial: bool) -> None:
    """Everything `probe report` needs to rebuild the report later."""
    (Path(root) / "_state.json").write_text(json.dumps({"mode": mode, "monitor": monitor, "rest": rest, "results": results,
                                                        "streams": streams, "universe_scale": universe_scale, "meta": meta,
                                                        "partial": partial}, default=str))


def stream_check(token: str, key_map: dict, seconds: float = 20.0) -> dict:
    """Connect for a few seconds with the extended decode and report which fields actually arrive."""
    feed = NullFeed(token, key_map, mode="full", ext=True, write_s=3600.0)
    out: dict = {"keys": len(key_map), "seconds": seconds, "error": ""}

    async def go():
        stop = asyncio.Event()
        asyncio.get_running_loop().call_later(seconds, stop.set)
        await feed.run_session(stop)
    try:
        asyncio.run(go())
    except Exception as exc:                                             # noqa: BLE001
        out["error"] = f"{type(exc).__name__}: {exc}"[:300]
    ticks = feed.proc.drain()
    out.update({"frames": feed.proc.frames, "ticks": len(ticks), "events": [e for e in feed.proc.drain_events()][:20]})
    fields = ("ltq", "atp", "tbq", "tsq", "cp", "iep", "ieq", "iiq_total", "iiq_market", "ref_price", "cas_eligible", "oi", "iv", "bid")
    out["field_coverage"] = {f: sum(1 for t in ticks if getattr(t, f, None) not in (None, 0)) for f in fields}
    out["symbols_seen"] = len({t.symbol for t in ticks})
    return out


# ----------------------------------------------------------------------------------------------------- the run
class ProbeRun:
    def __init__(self, ctx: Ctx, root: Path, logs_dir: Path, token_provider: Callable[[], str], *, start: datetime, end: datetime,
                 guard: Guard, monitor: ResourceMonitor, no_stream: bool = False, universe_scale: Optional[dict] = None,
                 meta: Optional[dict] = None, clock: Callable[[], datetime] = utcnow) -> None:
        self.ctx, self.root, self.logs = ctx, Path(root), Path(logs_dir)
        self.start, self.end, self.guard, self.monitor, self.clock = start, end, guard, monitor, clock
        self.token_provider, self.no_stream = token_provider, no_stream
        self.universe_scale = universe_scale or {}
        self.stream_keys: dict = {}
        self.meta = meta or {}
        self.results: list[dict] = []
        self.streams: dict[str, ProbeStream] = {}
        self.stop = threading.Event()
        self._jobs_log = self.logs / f"jobs-{start:%Y%m%d}.jsonl"

    # -- jobs
    def run_job(self, job: str, mode: str = "run") -> dict:
        if not self.guard.ok():
            log.error("job %s skipped: %s", job, self.guard.reason)
            return {}
        fn = JOBS[job]
        before_req, before_bytes = self.ctx.rest.limiter.requests, self.ctx.store.total_bytes_written()
        before_rows = sum(w["rows"] for w in self.ctx.store.written.values())
        t0, started = time.monotonic(), self.clock().strftime("%H:%M:%S")
        ok, notes = True, {}
        try:
            notes = fn(self.ctx, mode) or {}
        except Exception as exc:                                         # noqa: BLE001 -- one job never stops the run
            ok, notes = False, {"exception": f"{type(exc).__name__}: {exc}"[:300]}
            log.exception("job %s failed", job)
        rec = {"job": job, "mode": mode, "started": started, "seconds": round(time.monotonic() - t0, 1), "ok": ok,
               "requests": self.ctx.rest.limiter.requests - before_req,
               "bytes": self.ctx.store.total_bytes_written() - before_bytes,
               "rows": sum(w["rows"] for w in self.ctx.store.written.values()) - before_rows, "notes": notes}
        self.results.append(rec)
        self.logs.mkdir(parents=True, exist_ok=True)
        with self._jobs_log.open("a") as f:
            f.write(json.dumps(rec, default=str) + "\n")
        log.info("job %-20s %5.1fs  requests %4d  rows %7d  bytes %9d  %s", job, rec["seconds"], rec["requests"], rec["rows"], rec["bytes"],
                 "ok" if ok else "FAILED")
        return rec

    def _schedule(self) -> list[tuple[datetime, str]]:
        day = self.start.date()
        plan: list[tuple[datetime, str]] = []
        for job, times in FIXED_TIMES.items():
            for hhmm in times:
                h, m = map(int, hhmm.split(":"))
                plan.append((datetime.combine(day, dtime(h, m)), job))
        for job, every in EVERY.items():
            t = self.start + timedelta(minutes=30)                       # let the market open first
            while t <= self.end - timedelta(minutes=10):
                plan.append((t, job))
                t += timedelta(minutes=every)
        return sorted(plan)

    def _build_report(self, name: str, partial: bool) -> Optional[Path]:
        try:
            streams = {n: s.info() for n, s in self.streams.items()}
            rest = self.ctx.rest.summary()
            mon = self.monitor.summary()
            save_state(self.root, "run", mon, rest, self.results, streams, self.universe_scale, self.meta, partial)
            rep = build_report(self.root, mode="run", monitor=mon, rest=rest, results=self.results, streams=streams,
                               universe_scale=self.universe_scale, meta=self.meta, partial=partial)
            return write_report(self.root, rep, name)[1]
        except Exception:                                                # noqa: BLE001
            log.exception("report %s failed", name)
            return None

    def run(self) -> Optional[Path]:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, lambda *_: self.stop.set())
            except ValueError:                                           # not the main thread
                pass
        self.monitor.start()
        log.info("probe run: window %s to %s UTC; root %s", self.start, self.end, self.root)
        if not self.no_stream:                                           # streams first: they must not wait for the REST setup
            for name, keys in self.stream_keys.items():
                if keys and name.startswith("stream_"):
                    s = ProbeStream(name, keys, self.root, self.token_provider, self.start - timedelta(minutes=5), self.end, self.guard)
                    s.begin()
                    self.streams[name] = s
        for job in SETUP_JOBS:
            if self.stop.is_set():
                break
            self.run_job(job)
        plan = [(t, j) for t, j in self._schedule() if t >= self.clock() - timedelta(minutes=10)]
        last_partial, last_rate = self.clock(), self.clock()
        i = 0
        while not self.stop.is_set() and self.clock() < self.end + timedelta(minutes=45):
            now = self.clock()
            if i < len(plan) and plan[i][0] <= now:
                self.run_job(plan[i][1])
                i += 1
                continue
            if (now - last_rate).total_seconds() >= 60:
                for s in self.streams.values():
                    s.sample_rate()
                last_rate = now
            if (now - last_partial).total_seconds() >= 3600:
                self._build_report("report-partial", partial=True)
                last_partial = now
            if i >= len(plan) and now > self.end + timedelta(minutes=5) and all(not s._thread.is_alive() for s in self.streams.values()):
                break
            self.stop.wait(5.0)
        log.info("probe run: stopping streams")
        for s in self.streams.values():
            s.request_stop()
        for s in self.streams.values():
            s.join()
        self.monitor.stop()
        return self._build_report(f"report-{self.start:%Y%m%d}", partial=False)


def run_check(ctx: Ctx, root: Path, logs_dir: Path, monitor: ResourceMonitor, stream_keys: dict, token: str, stream_seconds: float = 20.0
              ) -> Path:
    """One pass over every dataset in small form, plus a short stream test; writes the capability report."""
    pr = ProbeRun(ctx, root, logs_dir, lambda: token, start=utcnow(), end=utcnow(), guard=Guard(root), monitor=monitor)
    monitor.start()
    for job in ("instrument_master", "market_info", "quotes", "option_chain", "option_stats", "flows", "smartlists", "news",
                "global_cues", "candles_sample", "expired_sample", "fundamentals_sample"):
        pr.run_job(job, "check")
    sc = stream_check(token, stream_keys.get("check", {}), stream_seconds) if stream_keys.get("check") else {"error": "no keys"}
    pr.results.append({"job": "stream_check", "mode": "check", "ok": not sc.get("error"), "seconds": stream_seconds, "requests": 0,
                       "rows": sc.get("ticks", 0), "bytes": 0, "started": utcnow().strftime("%H:%M:%S"), "notes": {"stream": sc}})
    mon = monitor.stop()
    save_state(root, "check", mon, ctx.rest.summary(), pr.results, {}, {}, {"stream_check": sc}, False)
    rep = build_report(root, mode="check", monitor=mon, rest=ctx.rest.summary(), results=pr.results, streams={}, universe_scale={},
                       meta={"stream_check": sc})
    return write_report(root, rep, f"check-{utcnow():%Y%m%d-%H%M}")[1]
