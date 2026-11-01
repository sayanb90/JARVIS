"""
Resource monitor: how much of the box JARVIS is using, written to a log as it happens.

Every `interval` seconds it records, as one JSON line:
  * system CPU busy and I/O-wait percent, load averages, memory used and available, swap used
  * for each process whose command line matches `patterns` (the feed, the viewer, the probe...): resident memory,
    the kernel's peak resident memory (VmHWM), CPU percent since the last sample, thread count
  * for each watched path: size of the path, and total, used and free space of its disk
and keeps the peaks, so a summary can say "peak CPU 63%, peak memory 2.1 GB, lowest free disk 41 GB".

It reads /proc directly (Linux, the VPS), so it needs no extra package.  `proc_root` lets tests point it at a fake tree.
A sampling error is logged and skipped; the monitor never takes the process it watches down.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional

log = logging.getLogger(__name__)

GB = 1024 ** 3
MB = 1024 ** 2


def dir_size(path: Path | str) -> int:
    """Total bytes of the files under `path` (0 if it does not exist).  Symlinks are not followed."""
    total, stack = 0, [str(path)]
    while stack:
        cur = stack.pop()
        try:
            with os.scandir(cur) as it:
                for e in it:
                    try:
                        if e.is_dir(follow_symlinks=False):
                            stack.append(e.path)
                        elif e.is_file(follow_symlinks=False):
                            total += e.stat(follow_symlinks=False).st_size
                    except OSError:
                        continue
        except OSError:
            continue
    return total


def _read(path: Path) -> str:
    try:
        return path.read_text()
    except OSError:
        return ""


class ResourceMonitor:
    def __init__(self, out_file: Path | str | None = None, interval: float = 30.0,
                 watch_paths: Iterable[Path | str] = (), patterns: Iterable[str] = ("data_layer.", "uvicorn", "webapp"),
                 dir_every: float = 120.0, proc_root: Path | str = "/proc", clk_tck: Optional[int] = None,
                 clock: Callable[[], float] = time.time) -> None:
        self.out_file = Path(out_file) if out_file else None
        self.interval, self.dir_every = interval, dir_every
        self.watch = [Path(p) for p in watch_paths]
        self.patterns = tuple(patterns)
        self.proc = Path(proc_root)
        self.clk_tck = clk_tck or (os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100)
        self.clock = clock
        self.cpus = os.cpu_count() or 1
        self._prev_stat: Optional[tuple[int, int, int]] = None          # busy, iowait, total jiffies
        self._prev_proc: dict[int, tuple[int, float]] = {}              # pid -> (cpu jiffies, time)
        self._dir_cache: dict[str, tuple[float, int]] = {}
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.samples = 0
        self._cpu_sum = 0.0
        self.peaks: dict = {"cpu_pct": 0.0, "iowait_pct": 0.0, "load1": 0.0, "mem_used_gb": 0.0, "mem_available_min_gb": None,
                            "swap_used_gb": 0.0, "disk_free_min_gb": {}, "dir_gb": {}, "process_rss_mb": {}, "process_cpu_pct": {}}
        self.first: dict = {}
        self.last: dict = {}

    # ------------------------------------------------------------------ sampling
    def _system_cpu(self) -> tuple[Optional[float], Optional[float]]:
        line = _read(self.proc / "stat").splitlines()[:1]
        if not line or not line[0].startswith("cpu "):
            return None, None
        v = [int(x) for x in line[0].split()[1:9]]                       # user nice system idle iowait irq softirq steal
        user, nice, system, idle, iowait, irq, softirq, steal = (v + [0] * 8)[:8]
        busy = user + nice + system + irq + softirq + steal
        total = busy + idle + iowait
        prev, self._prev_stat = self._prev_stat, (busy, iowait, total)
        if prev is None or total <= prev[2]:
            return None, None
        dt = total - prev[2]
        return 100.0 * (busy - prev[0]) / dt, 100.0 * (iowait - prev[1]) / dt

    def _memory(self) -> dict:
        m: dict[str, int] = {}
        for ln in _read(self.proc / "meminfo").splitlines():
            k, _, rest = ln.partition(":")
            parts = rest.split()
            if parts:
                m[k] = int(parts[0]) * 1024                               # kB -> bytes
        total, avail = m.get("MemTotal", 0), m.get("MemAvailable", m.get("MemFree", 0))
        swap = max(0, m.get("SwapTotal", 0) - m.get("SwapFree", 0))
        return {"mem_total_gb": round(total / GB, 3), "mem_used_gb": round((total - avail) / GB, 3),
                "mem_available_gb": round(avail / GB, 3), "swap_used_gb": round(swap / GB, 3)}

    def _processes(self, now: float) -> list[dict]:
        out, seen = [], set()
        try:
            pids = [int(p.name) for p in self.proc.iterdir() if p.name.isdigit()]
        except OSError:
            return out
        for pid in pids:
            base = self.proc / str(pid)
            cmd = _read(base / "cmdline").replace("\0", " ").strip()
            if not cmd or not any(p in cmd for p in self.patterns):
                continue
            status = {}
            for ln in _read(base / "status").splitlines():
                k, _, rest = ln.partition(":")
                status[k] = rest.strip()
            stat = _read(base / "stat")
            try:
                f = stat[stat.rindex(")") + 2:].split()                  # fields after the command name: state is f[0]
                jiff = int(f[11]) + int(f[12])                           # utime + stime
            except (ValueError, IndexError):
                jiff = 0
            seen.add(pid)
            prev = self._prev_proc.get(pid)
            cpu = None
            if prev and now > prev[1]:
                cpu = 100.0 * (jiff - prev[0]) / self.clk_tck / (now - prev[1])
            self._prev_proc[pid] = (jiff, now)

            def kb(key: str) -> float:
                try:
                    return int(status.get(key, "0 kB").split()[0]) / 1024.0
                except (ValueError, IndexError):
                    return 0.0
            label = cmd.split(" -m ")[-1] if " -m " in cmd else cmd
            out.append({"pid": pid, "cmd": label[:90], "rss_mb": round(kb("VmRSS"), 1), "peak_rss_mb": round(kb("VmHWM"), 1),
                        "cpu_pct": None if cpu is None else round(cpu, 1), "threads": int(status.get("Threads", "0") or 0)})
        for pid in set(self._prev_proc) - seen:
            self._prev_proc.pop(pid, None)
        return out

    def _disks(self, now: float) -> tuple[list[dict], dict]:
        disks, dirs = [], {}
        for p in self.watch:
            try:
                du = shutil.disk_usage(p if p.exists() else p.parent)
                disks.append({"path": str(p), "total_gb": round(du.total / GB, 2), "used_gb": round(du.used / GB, 2),
                              "free_gb": round(du.free / GB, 2)})
            except OSError:
                continue
            cached = self._dir_cache.get(str(p))
            if cached is None or now - cached[0] >= self.dir_every:
                cached = (now, dir_size(p))
                self._dir_cache[str(p)] = cached
            dirs[str(p)] = round(cached[1] / GB, 3)
        return disks, dirs

    def sample(self) -> dict:
        """Take one sample, update the peaks and return the record."""
        now = self.clock()
        cpu, iow = self._system_cpu()
        parts = _read(self.proc / "loadavg").split()
        if len(parts) >= 3:
            load = tuple(float(x) for x in parts[:3])
        else:
            try:
                load = os.getloadavg()
            except (OSError, AttributeError):
                load = (0.0, 0.0, 0.0)
        mem = self._memory()
        procs = self._processes(now)
        disks, dirs = self._disks(now)
        rec = {"ts": datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "cpus": self.cpus,
               "cpu_pct": None if cpu is None else round(cpu, 1), "iowait_pct": None if iow is None else round(iow, 1),
               "load1": round(load[0], 2), "load5": round(load[1], 2), "load15": round(load[2], 2), **mem,
               "procs": procs, "disks": disks, "dirs_gb": dirs}
        self._update_peaks(rec)
        self.samples += 1
        self.last = rec
        if not self.first:
            self.first = rec
        if self.out_file:
            self.out_file.parent.mkdir(parents=True, exist_ok=True)
            with self.out_file.open("a") as f:
                f.write(json.dumps(rec) + "\n")
        return rec

    def _update_peaks(self, r: dict) -> None:
        pk = self.peaks
        if r["cpu_pct"] is not None:
            pk["cpu_pct"] = max(pk["cpu_pct"], r["cpu_pct"])
            self._cpu_sum += r["cpu_pct"]
        if r["iowait_pct"] is not None:
            pk["iowait_pct"] = max(pk["iowait_pct"], r["iowait_pct"])
        pk["load1"] = max(pk["load1"], r["load1"])
        pk["mem_used_gb"] = max(pk["mem_used_gb"], r["mem_used_gb"])
        pk["swap_used_gb"] = max(pk["swap_used_gb"], r["swap_used_gb"])
        a = r["mem_available_gb"]
        pk["mem_available_min_gb"] = a if pk["mem_available_min_gb"] is None else min(pk["mem_available_min_gb"], a)
        for d in r["disks"]:
            cur = pk["disk_free_min_gb"].get(d["path"])
            pk["disk_free_min_gb"][d["path"]] = d["free_gb"] if cur is None else min(cur, d["free_gb"])
        for k, v in r["dirs_gb"].items():
            pk["dir_gb"][k] = max(pk["dir_gb"].get(k, 0.0), v)
        for p in r["procs"]:
            key = p["cmd"]
            pk["process_rss_mb"][key] = max(pk["process_rss_mb"].get(key, 0.0), p["rss_mb"], p["peak_rss_mb"])
            if p["cpu_pct"] is not None:
                pk["process_cpu_pct"][key] = max(pk["process_cpu_pct"].get(key, 0.0), p["cpu_pct"])

    def summary(self) -> dict:
        cpu_samples = max(1, self.samples - 1)                            # the first sample has no CPU delta
        return {"samples": self.samples, "cpus": self.cpus, "interval_s": self.interval,
                "mem_total_gb": self.last.get("mem_total_gb"), "cpu_avg_pct": round(self._cpu_sum / cpu_samples, 1),
                "peaks": self.peaks,
                "disk_start_gb": {d["path"]: {"free": d["free_gb"], "used": d["used_gb"], "total": d["total_gb"]}
                                  for d in self.first.get("disks", [])},
                "disk_end_gb": {d["path"]: {"free": d["free_gb"], "used": d["used_gb"], "total": d["total_gb"]}
                                for d in self.last.get("disks", [])},
                "dirs_start_gb": self.first.get("dirs_gb", {}), "dirs_end_gb": self.last.get("dirs_gb", {})}

    # ------------------------------------------------------------------ thread control
    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                rec = self.sample()
                if self.samples % 10 == 1:                                # a readable line in the normal log too
                    log.info("resources: cpu %s%% load %.2f mem used %.2f GB (available %.2f) swap %.2f GB; %s",
                             rec["cpu_pct"], rec["load1"], rec["mem_used_gb"], rec["mem_available_gb"], rec["swap_used_gb"],
                             "; ".join(f"{d['path']} free {d['free_gb']} GB" for d in rec["disks"]))
            except Exception:                                             # noqa: BLE001 -- never take the host process down
                log.exception("resource sample failed")
            self._stop.wait(self.interval)

    def start(self) -> "ResourceMonitor":
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="resource-monitor", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> dict:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.interval + 5)
        try:
            self.sample()                                                 # final reading
        except Exception:                                                 # noqa: BLE001
            log.exception("final resource sample failed")
        return self.summary()

    def __enter__(self) -> "ResourceMonitor":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()
