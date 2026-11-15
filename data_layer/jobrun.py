"""
Job heartbeat (SCRUM-142): record when every cron job started, ended and how it ended, and work out each job's state.

  python -m data_layer.jobrun run NAME -- COMMAND ...        # run COMMAND, recording start, end, exit code and duration
  python -m data_layer.jobrun run NAME --shell 'a && b'      # the same for a shell line (chains with && or |)
  python -m data_layer.jobrun status [--json]                # every job of deploy/crontab and its state now

Why: cron says nothing when a job fails or never starts.  Wrapping the command in `jobrun run` leaves one line at the start and one at
the end in logs/jobs/runs.jsonl (append-only, one small write each, so concurrent jobs cannot garble it).  The child's output and exit
code pass through unchanged, so the `>> logs/x.log 2>&1` redirect of the cron line keeps working and cron still sees the real exit code.

States of a job, from its schedule and its records:
  running        started and not ended (and the process still exists, or it started less than MAX_RUNNING_HOURS ago)
  ok             the last run ended with exit code 0
  failed         the last run ended with a non-zero exit code (or died without an end line: "lost")
  late           the schedule says it should have started (more than GRACE_MINUTES ago) and no run started since
  never          wrapped, but no run recorded yet and none due yet
  untracked      the cron line is not wrapped: only the log file's last write time is known
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

from . import lake as lake_mod

RUNS_FILE = "logs/jobs/runs.jsonl"
GRACE_MINUTES = 10                 # a job is "late" this long after its scheduled minute without a start
MAX_RUNNING_HOURS = 14             # a start without an end older than this is called lost, whether or not the pid exists
KEEP_DAYS = 45                     # records older than this are dropped when the file is compacted


def runs_path(root: Path | None = None) -> Path:
    return (root or lake_mod.ROOT) / RUNS_FILE


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _iso(t: datetime) -> str:
    return t.isoformat(timespec="seconds")


def _append(path: Path, rec: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(rec, separators=(",", ":")) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, line)                                    # one write: appends of this size do not interleave
    finally:
        os.close(fd)


# ---------------------------------------------------------------------------------------------- the wrapper
def run_job(name: str, argv: list[str] | None = None, shell: str | None = None, root: Path | None = None,
            now=_now) -> int:
    """Run the command and record it.  Returns the command's exit code (127 when it cannot be started)."""
    path = runs_path(root)
    run_id = f"{now():%Y%m%dT%H%M%S}-{os.getpid()}"
    started = now()
    _append(path, {"ev": "start", "job": name, "id": run_id, "at": _iso(started), "pid": os.getpid()})
    t0 = time.monotonic()
    try:
        code = subprocess.call(shell, shell=True, executable="/bin/bash") if shell else subprocess.call(argv)
    except OSError as exc:                                    # command not found, not executable
        print(f"jobrun: cannot start {name}: {exc}", file=sys.stderr)
        code = 127
    except BaseException:                                     # interrupted (SIGINT): record an end, then let it propagate
        _append(path, {"ev": "end", "job": name, "id": run_id, "at": _iso(now()), "code": 130, "secs": round(time.monotonic() - t0, 1)})
        raise
    _append(path, {"ev": "end", "job": name, "id": run_id, "at": _iso(now()), "code": int(code), "secs": round(time.monotonic() - t0, 1)})
    return int(code)


def load_runs(root: Path | None = None) -> list[dict]:
    p = runs_path(root)
    out: list[dict] = []
    try:
        for line in p.read_text().splitlines():
            try:
                rec = json.loads(line)
            except ValueError:
                continue                                      # a torn line is skipped, never fatal
            if isinstance(rec, dict) and rec.get("ev") in ("start", "end") and rec.get("job"):
                out.append(rec)
    except OSError:
        pass
    return out


def compact(root: Path | None = None, now: datetime | None = None) -> int:
    """Drop records older than KEEP_DAYS; returns how many remain."""
    now = now or _now()
    cutoff = _iso(now - timedelta(days=KEEP_DAYS))
    recs = [r for r in load_runs(root) if r.get("at", "") >= cutoff]
    p = runs_path(root)
    if p.exists():
        tmp = p.with_suffix(".tmp")
        tmp.write_text("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in recs))
        os.replace(tmp, p)
    return len(recs)


# ---------------------------------------------------------------------------------------------- cron expressions
_RANGES = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)]


def _field(spec: str, lo: int, hi: int) -> set[int]:
    out: set[int] = set()
    for part in spec.split(","):
        step = 1
        if "/" in part:
            part, s = part.split("/", 1)
            step = int(s)
        if part in ("*", ""):
            a, b = lo, hi
        elif "-" in part:
            x, y = part.split("-", 1)
            a, b = int(x), int(y)
        else:
            a = int(part)
            b = hi if step != 1 else a
        if not (lo <= a <= hi and lo <= b <= hi) or step < 1:
            raise ValueError(f"cron field out of range: {spec}")
        out.update(range(a, b + 1, step))
    return out


@dataclass(frozen=True)
class Schedule:
    minute: frozenset
    hour: frozenset
    dom: frozenset
    month: frozenset
    dow: frozenset                 # 0 = Sunday .. 6 (7 folded onto 0)
    dom_any: bool
    dow_any: bool
    text: str = ""

    @classmethod
    def parse(cls, expr: str) -> "Schedule":
        parts = expr.split()
        if len(parts) != 5:
            raise ValueError(f"not a 5-field cron expression: {expr!r}")
        sets = [_field(p, lo, hi) for p, (lo, hi) in zip(parts, _RANGES)]
        sets[4] = {d % 7 for d in sets[4]}
        return cls(*(frozenset(s) for s in sets), dom_any=parts[2].startswith("*"), dow_any=parts[4].startswith("*"), text=expr)

    def matches(self, t: datetime) -> bool:
        if t.minute not in self.minute or t.hour not in self.hour or t.month not in self.month:
            return False
        dom_ok, dow_ok = t.day in self.dom, ((t.weekday() + 1) % 7) in self.dow
        if self.dom_any or self.dow_any:                      # vixie cron: both restricted -> either may match
            return dom_ok and dow_ok
        return dom_ok or dow_ok

    def last_due(self, now: datetime, within_days: int = 8) -> Optional[datetime]:
        t = now.replace(second=0, microsecond=0)
        for _ in range(within_days * 1440):
            if self.matches(t):
                return t
            t -= timedelta(minutes=1)
        return None

    def next_due(self, now: datetime, within_days: int = 8) -> Optional[datetime]:
        t = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        for _ in range(within_days * 1440):
            if self.matches(t):
                return t
            t += timedelta(minutes=1)
        return None


# ---------------------------------------------------------------------------------------------- the crontab
@dataclass
class Job:
    name: str
    schedule: Schedule
    command: str
    log: Optional[str] = None                 # path relative to the project root, from the `>> logs/x.log` redirect
    wrapped: bool = False
    line: int = 0

    @property
    def schedule_text(self) -> str:
        return self.schedule.text


_WRAP = re.compile(r"python\S*\s+-m\s+data_layer\.jobrun\s+run\s+([A-Za-z0-9_.-]+)\s")
_LOG = re.compile(r">>\s*(logs/[A-Za-z0-9_./-]+)")


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def derive_name(command: str, log: Optional[str]) -> str:
    if log:
        return _slug(Path(log).stem)
    m = re.search(r"python\S*\s+(?:-m\s+)?([\w.]+)", command)
    return _slug(m.group(1)) if m else _slug(command)[:40]


def parse_crontab(text: str) -> list[Job]:
    """The jobs of a crontab: schedule lines only (variables, comments and blank lines are skipped).  Lines that cannot be read are skipped."""
    jobs: list[Job] = []
    seen: dict[str, int] = {}
    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#") or re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*=", line):
            continue
        parts = line.split(None, 5)
        if len(parts) < 6:
            continue
        try:
            sched = Schedule.parse(" ".join(parts[:5]))
        except ValueError:
            continue
        cmd = parts[5]
        log = (m.group(1) if (m := _LOG.search(cmd)) else None)
        w = _WRAP.search(cmd)
        name = w.group(1) if w else derive_name(cmd, log)
        if name in seen:                                      # two lines with one name (the two F&O shards): number them
            seen[name] += 1
            name = f"{name}-{seen[name]}"
        else:
            seen[name] = 1
        jobs.append(Job(name, sched, cmd, log, wrapped=bool(w), line=n))
    return jobs


def load_crontab(root: Path | None = None) -> list[Job]:
    p = (root or lake_mod.ROOT) / "deploy" / "crontab"
    try:
        return parse_crontab(p.read_text())
    except OSError:
        return []


# ---------------------------------------------------------------------------------------------- the state of a job
@dataclass
class State:
    name: str
    schedule: str
    command: str
    log: Optional[str]
    state: str
    last_start: Optional[str] = None
    last_end: Optional[str] = None
    exit_code: Optional[int] = None
    secs: Optional[float] = None
    next_run: Optional[str] = None
    due_at: Optional[str] = None
    log_written: Optional[str] = None
    history: list = field(default_factory=list)              # newest first: {"start", "end", "code", "secs"}

    def as_dict(self) -> dict:
        return self.__dict__.copy()


def _pid_alive(pid) -> bool:
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, TypeError, ValueError):
        return False


def _pair(runs: list[dict]) -> dict[str, list[dict]]:
    """Per job, its runs in start order: {"id", "start", "end", "code", "secs", "pid"}; a run with no end line has end None."""
    by_job: dict[str, dict[str, dict]] = {}
    for r in runs:
        run = by_job.setdefault(r["job"], {}).setdefault(r.get("id", ""), {"id": r.get("id", ""), "start": None, "end": None,
                                                                          "code": None, "secs": None, "pid": None})
        if r["ev"] == "start":
            run["start"], run["pid"] = r.get("at"), r.get("pid")
        else:
            run["end"], run["code"], run["secs"] = r.get("at"), r.get("code"), r.get("secs")
    return {j: sorted((x for x in d.values() if x["start"]), key=lambda x: x["start"]) for j, d in by_job.items()}


def states(jobs: Iterable[Job], runs: list[dict], now: datetime | None = None, root: Path | None = None,
           pid_alive=_pid_alive) -> list[State]:
    now = now or _now()
    root = root or lake_mod.ROOT
    paired = _pair(runs)
    out: list[State] = []
    for j in jobs:
        hist = paired.get(j.name, [])
        due = j.schedule.last_due(now)
        nxt = j.schedule.next_due(now)
        s = State(j.name, j.schedule_text, j.command, j.log, "untracked", next_run=_iso(nxt) if nxt else None,
                  due_at=_iso(due) if due else None)
        if j.log:
            try:
                s.log_written = _iso(datetime.fromtimestamp((root / j.log).stat().st_mtime, timezone.utc).replace(tzinfo=None))
            except OSError:
                pass
        s.history = [{"start": h["start"], "end": h["end"], "code": h["code"], "secs": h["secs"]} for h in reversed(hist[-10:])]
        if hist:
            last = hist[-1]
            s.last_start, s.last_end, s.exit_code, s.secs = last["start"], last["end"], last["code"], last["secs"]
            age_h = (now - datetime.fromisoformat(last["start"])).total_seconds() / 3600
            if last["end"] is None:
                s.state = "running" if age_h < MAX_RUNNING_HOURS and pid_alive(last["pid"]) else "lost"
            else:
                s.state = "ok" if last["code"] == 0 else "failed"
        elif j.wrapped:
            s.state = "never"
        if hist and s.state == "ok" and due:        # late: it should have started by now and nothing started since
            started_after = datetime.fromisoformat(hist[-1]["start"]) >= due - timedelta(minutes=1)
            if now - due > timedelta(minutes=GRACE_MINUTES) and not started_after:
                s.state = "late"
        out.append(s)
    return out


# ---------------------------------------------------------------------------------------------- the log tail
SECRET = re.compile(r"(?i)((?:token|secret|password|passwd|api[_-]?key|apikey|authorization|bearer)\S{0,20}?\s*[=:]\s*|bearer\s+)([^\s'\"&,;]+)")
LONG_TOKEN = re.compile(r"\b[A-Za-z0-9_\-]{32,}\b")
MAX_TAIL_LINES = 500
MAX_TAIL_BYTES = 256 * 1024


def redact(text: str) -> str:
    """Hide secrets in log text before it leaves the server: `token=...`-style assignments, bearer tokens, and long opaque strings."""
    text = SECRET.sub(lambda m: m.group(1) + "[hidden]", text)
    return LONG_TOKEN.sub("[hidden]", text)


def tail(log: str, lines: int = 200, root: Path | None = None) -> str:
    """The last `lines` lines (at most MAX_TAIL_LINES, MAX_TAIL_BYTES) of a project log file, redacted.  `log` must be a logs/ path."""
    root = (root or lake_mod.ROOT).resolve()
    p = (root / log).resolve()
    if not str(p).startswith(str(root / "logs") + os.sep) or p.suffix not in (".log", ".txt"):
        raise ValueError("not a log file")
    lines = max(1, min(int(lines), MAX_TAIL_LINES))
    with p.open("rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        f.seek(max(0, size - MAX_TAIL_BYTES))
        data = f.read().decode("utf-8", "replace")
    return redact("\n".join(data.splitlines()[-lines:]))


# ---------------------------------------------------------------------------------------------- command line
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Job heartbeat for cron jobs")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run a command and record it")
    r.add_argument("name")
    r.add_argument("--shell", default=None, help="a shell line instead of an argv")
    r.add_argument("command", nargs=argparse.REMAINDER)
    s = sub.add_parser("status", help="state of every job in deploy/crontab")
    s.add_argument("--json", action="store_true")
    sub.add_parser("compact", help=f"drop records older than {KEEP_DAYS} days")
    a = ap.parse_args(argv)
    if a.cmd == "run":
        cmd = a.command[1:] if a.command[:1] == ["--"] else a.command
        if not (cmd or a.shell):
            ap.error("no command given")
        return run_job(a.name, argv=cmd or None, shell=a.shell)
    if a.cmd == "compact":
        print(f"{compact()} records kept")
        return 0
    rows = states(load_crontab(), load_runs())
    if a.json:
        print(json.dumps([x.as_dict() for x in rows], indent=1))
    else:
        for x in rows:
            print(f"{x.state:<9} {x.name:<34} {x.schedule:<16} last {x.last_start or '-':<20} exit {x.exit_code if x.exit_code is not None else '-'}")
    return 1 if any(x.state in ("failed", "lost", "late") for x in rows) else 0


if __name__ == "__main__":
    sys.exit(main())
