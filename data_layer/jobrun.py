"""
Job heartbeat (SCRUM-142): record when every cron job started, ended and how it ended, and work out each job's state.

  python -m data_layer.jobrun run NAME -- COMMAND ...        # run COMMAND, recording start, end, exit code and duration
  python -m data_layer.jobrun run NAME --shell 'a && b'      # the same for a shell line (chains with && or |)
  python -m data_layer.jobrun status [--json]                # every job of deploy/crontab and its state now
  python -m data_layer.jobrun start NAME | stop NAME [--force]   # run a crontab job now / stop its running process (SCRUM-204)

Why: cron says nothing when a job fails or never starts.  Wrapping the command in `jobrun run` leaves one line at the start and one at
the end in logs/jobs/runs.jsonl (append-only, one small write each, so concurrent jobs cannot garble it).  The child's output and exit
code pass through unchanged, so the `>> logs/x.log 2>&1` redirect of the cron line keeps working and cron still sees the real exit code.

States of a job, from its schedule and its records:
  running        started and not ended (and the process still exists, or it started less than MAX_RUNNING_HOURS ago)
  ok             the last run ended with exit code 0
  stopped        the last run was stopped on purpose (from the Jobs tab or by `jobrun stop`)
  failed         the last run ended with a non-zero exit code (or died without an end line: "lost")
  late           the schedule says it should have started (more than GRACE_MINUTES ago) and no run started since
  never          wrapped, but no run recorded yet and none due yet
  untracked      the cron line is not wrapped: only the log file's last write time is known
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
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
    """Run the command and record it.  Returns the command's exit code (127 when it cannot be started).  A SIGTERM or SIGINT to this
    process (or its process group) is passed to the command and the run is recorded as stopped."""
    path = runs_path(root)
    run_id = f"{now():%Y%m%dT%H%M%S}-{os.getpid()}"
    started = now()
    trigger = os.environ.get("JARVIS_JOB_TRIGGER", "cron")
    _append(path, {"ev": "start", "job": name, "id": run_id, "at": _iso(started), "pid": os.getpid(), "pgid": os.getpgrp(), "by": trigger})
    t0 = time.monotonic()
    stopped = {"sig": None}
    child: dict = {"p": None}

    def on_signal(signum, _frame):
        stopped["sig"] = signum
        if child["p"] is not None and child["p"].poll() is None:
            try:
                child["p"].send_signal(signum)
            except OSError:
                pass

    old = {s: signal.signal(s, on_signal) for s in (signal.SIGTERM, signal.SIGINT)} if threading.current_thread() is threading.main_thread() else {}
    try:
        try:
            child["p"] = subprocess.Popen(shell, shell=True, executable="/bin/bash") if shell else subprocess.Popen(argv)
            code = child["p"].wait()
        except OSError as exc:                                # command not found, not executable
            print(f"jobrun: cannot start {name}: {exc}", file=sys.stderr)
            code = 127
    finally:
        for s, h in old.items():
            signal.signal(s, h)
    end = {"ev": "end", "job": name, "id": run_id, "at": _iso(now()), "code": int(code), "secs": round(time.monotonic() - t0, 1)}
    if stopped["sig"] is not None:
        end["stopped"] = True
    _append(path, end)
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
_LOG = re.compile(r">>?\s*[\"']?([^\s\"';&|<>]+\.(?:log|txt))")
ALIASES = {"@hourly": "0 * * * *", "@daily": "0 0 * * *", "@midnight": "0 0 * * *", "@weekly": "0 0 * * 0", "@monthly": "0 0 1 * *",
           "@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *"}              # @reboot has no schedule: such a line is not listed


def _log_of(command: str) -> Optional[str]:
    """The log file a cron line redirects to, as a path under the project's logs/ folder ("logs/x.log"), whether the line spells it
    relative (logs/x.log), with a variable ($JARVIS/logs/x.log) or absolute (/home/x/JARVIS/logs/x.log).  None when it is elsewhere."""
    m = _LOG.search(command)
    if not m:
        return None
    path = m.group(1)
    if path.startswith("logs/"):
        return path
    i = path.rfind("/logs/")
    return path[i + 1:] if i >= 0 else None


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def derive_name(command: str, log: Optional[str]) -> str:
    if log:
        return _slug(Path(log).stem)
    c = re.sub(r"\bcd\s+\S+\s*&&\s*", "", command)                 # cd $JARVIS &&
    c = re.sub(r"\s*\d*>>?\s*\S+|\s*\d*>&\d", "", c)                   # redirects, 2>&1
    m = re.search(r"python\S*\s+(?:-m\s+)?([\w.]+)", c)
    if m:
        return _slug(re.sub(r"^data_layer\.", "", m.group(1)))
    first = c.split()[0] if c.split() else command
    return _slug(Path(first).stem) or "job"


def parse_crontab(text: str) -> list[Job]:
    """The jobs of a crontab: schedule lines only (variables, comments and blank lines are skipped).  Lines that cannot be read are skipped."""
    jobs: list[Job] = []
    seen: dict[str, int] = {}
    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#") or re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*=", line):
            continue
        if line.startswith("@"):
            parts = line.split(None, 1)
            if len(parts) < 2 or parts[0].lower() not in ALIASES:
                continue
            try:
                sched = dataclasses.replace(Schedule.parse(ALIASES[parts[0].lower()]), text=parts[0].lower())
            except ValueError:
                continue
            cmd = parts[1]
        else:
            parts = line.split(None, 5)
            if len(parts) < 6:
                continue
            try:
                sched = Schedule.parse(" ".join(parts[:5]))
            except ValueError:
                continue
            cmd = parts[5]
        log = _log_of(cmd)
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
    """The jobs of the repo's deploy/crontab (the template).  The Jobs tab uses load_jobs, which prefers the crontab that is installed."""
    p = (root or lake_mod.ROOT) / "deploy" / "crontab"
    try:
        return parse_crontab(p.read_text())
    except OSError:
        return []


def installed_crontab(run=subprocess.run) -> Optional[str]:
    """The text of the crontab of the user this process runs as (`crontab -l`; read-only), or None when there is none or it cannot be read."""
    try:
        r = run(["crontab", "-l"], capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    return r.stdout if r.returncode == 0 and r.stdout.strip() else None


def load_jobs(root: Path | None = None, run=subprocess.run) -> tuple[list[Job], str]:
    """(jobs, where they came from).  In order: the file named by JARVIS_CRONTAB_FILE, the installed crontab (`crontab -l`), the repo's
    deploy/crontab.  Nothing here ever writes a crontab."""
    f = os.environ.get("JARVIS_CRONTAB_FILE", "").strip()
    if f:
        try:
            return parse_crontab(Path(f).read_text()), f"file {f}"
        except OSError:
            pass                                              # unreadable: fall through to the installed crontab
    text = installed_crontab(run)
    if text is not None:
        return parse_crontab(text), "installed crontab (crontab -l)"
    return load_crontab(root), "deploy/crontab in the repo (no installed crontab was found)"


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
    started_by: Optional[str] = None
    pid: Optional[int] = None                                # of the running wrapper, when state is running
    history: list = field(default_factory=list)              # newest first: {"start", "end", "code", "secs"}

    def as_dict(self) -> dict:
        return self.__dict__.copy()


def _pid_alive(pid) -> bool:
    try:
        os.kill(int(pid), 0)
    except (OSError, TypeError, ValueError):
        return False
    try:                                                      # a zombie (exited, not yet reaped by its parent) is not running
        return Path(f"/proc/{int(pid)}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except (OSError, IndexError, ValueError):
        return True


def _pair(runs: list[dict]) -> dict[str, list[dict]]:
    """Per job, its runs in start order: {"id", "start", "end", "code", "secs", "pid"}; a run with no end line has end None."""
    by_job: dict[str, dict[str, dict]] = {}
    for r in runs:
        run = by_job.setdefault(r["job"], {}).setdefault(r.get("id", ""), {"id": r.get("id", ""), "start": None, "end": None,
                                                                          "code": None, "secs": None, "pid": None, "pgid": None, "by": "", "stopped": False})
        if r["ev"] == "start":
            run["start"], run["pid"], run["pgid"], run["by"] = r.get("at"), r.get("pid"), r.get("pgid"), r.get("by", "")
        else:
            run["end"], run["code"], run["secs"], run["stopped"] = r.get("at"), r.get("code"), r.get("secs"), bool(r.get("stopped"))
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
        s.history = [{"start": h["start"], "end": h["end"], "code": h["code"], "secs": h["secs"], "stopped": h["stopped"]} for h in reversed(hist[-10:])]
        if hist:
            last = hist[-1]
            s.last_start, s.last_end, s.exit_code, s.secs = last["start"], last["end"], last["code"], last["secs"]
            s.started_by = last["by"] or None
            s.pid = last["pid"] if last["end"] is None else None
            age_h = (now - datetime.fromisoformat(last["start"])).total_seconds() / 3600
            if last["end"] is None:
                s.state = "running" if age_h < MAX_RUNNING_HOURS and pid_alive(last["pid"]) else "lost"
            else:
                s.state = "ok" if last["code"] == 0 else ("stopped" if last["stopped"] else "failed")
        elif j.wrapped:
            s.state = "never"
        if hist and s.state == "ok" and due:        # late: it should have started by now and nothing started since
            started_after = datetime.fromisoformat(hist[-1]["start"]) >= due - timedelta(minutes=1)
            if now - due > timedelta(minutes=GRACE_MINUTES) and not started_after:
                s.state = "late"
        out.append(s)
    return out


# ---------------------------------------------------------------------------------------------- start and stop (SCRUM-204)
class ControlError(Exception):
    """A start or stop that cannot be done, with a message fit to show the user."""


def _is_jobrun(pid) -> bool:
    """Whether `pid` is still one of our wrappers (a recycled pid must never be signalled).  Without /proc, only that the pid exists."""
    if not Path("/proc/self").exists():
        return _pid_alive(pid)
    try:
        return b"jobrun" in Path(f"/proc/{int(pid)}/cmdline").read_bytes()
    except (OSError, TypeError, ValueError):
        return False


def _find(name: str, root: Path | None) -> tuple[Job, State]:
    jobs, _ = load_jobs(root)
    job = next((j for j in jobs if j.name == name), None)
    if job is None:
        raise ControlError("no such job")                      # only a job named in deploy/crontab can be touched: the command is never from the caller
    st = states([job], load_runs(root), root=root)[0]
    return job, st


def start_job(name: str, root: Path | None = None, by: str = "admin", popen=subprocess.Popen) -> int:
    """Run a crontab job now, detached from the caller, exactly as cron would (same command, recorded by the heartbeat).  Returns the pid."""
    root = root or lake_mod.ROOT
    job, st = _find(name, root)
    if st.state == "running":
        raise ControlError("this job is already running")
    m = _WRAP.search(job.command)
    body = re.search(r"--shell\s+'(.*)'\s*$", job.command)
    if not (m and body):
        raise ControlError("this cron line is not wrapped by the heartbeat, so it cannot be controlled")
    repo = str(Path(__file__).resolve().parent.parent)           # so `-m data_layer.jobrun` is found wherever the caller runs
    env = {**os.environ, "JARVIS": os.environ.get("JARVIS") or str(root), "JARVIS_JOB_TRIGGER": f"admin:{by}"[:60],
           "PYTHONPATH": repo + (os.pathsep + os.environ["PYTHONPATH"] if os.environ.get("PYTHONPATH") else "")}
    p = popen([sys.executable, "-m", "data_layer.jobrun", "run", name, "--shell", body.group(1)], cwd=str(root), env=env,
              stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    threading.Thread(target=p.wait, daemon=True, name=f"reap-{name}").start()      # reap it when it ends: no zombies in a long-lived viewer
    return p.pid


def stop_job(name: str, root: Path | None = None, force: bool = False, killpg=os.killpg) -> int:
    """Stop the running process of a job: SIGTERM to its process group (SIGKILL with force).  Returns the group id signalled."""
    job, st = _find(name, root)
    if st.state != "running" or not st.pid:
        raise ControlError("this job is not running")
    if not _is_jobrun(st.pid):
        raise ControlError("the recorded process is no longer a job wrapper: not touching it")
    runs = [r for r in _pair(load_runs(root)).get(name, []) if r["end"] is None]
    pgid = (runs[-1]["pgid"] if runs else None) or st.pid
    if pgid in (os.getpgrp(), 0, 1):
        raise ControlError("refusing to signal this process group")
    killpg(int(pgid), signal.SIGKILL if force else signal.SIGTERM)
    if force and runs:                                        # SIGKILL leaves the wrapper no chance to write its end line: write it here
        started = datetime.fromisoformat(runs[-1]["start"])
        _append(runs_path(root), {"ev": "end", "job": name, "id": runs[-1]["id"], "at": _iso(_now()), "code": -9, "stopped": True,
                                  "secs": round((_now() - started).total_seconds(), 1)})
    return int(pgid)


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
    r.add_argument("command", nargs="*", help="argv of the command, after --")
    s = sub.add_parser("status", help="state of every job in deploy/crontab")
    s.add_argument("--json", action="store_true")
    for c, h in (("start", "run a crontab job now"), ("stop", "stop a running crontab job")):
        x = sub.add_parser(c, help=h)
        x.add_argument("name")
        if c == "stop":
            x.add_argument("--force", action="store_true", help="SIGKILL instead of SIGTERM")
    sub.add_parser("compact", help=f"drop records older than {KEEP_DAYS} days")
    a = ap.parse_args(argv)
    if a.cmd == "run":
        cmd = a.command[1:] if a.command[:1] == ["--"] else a.command            # argparse keeps or drops the "--"
        if not (cmd or a.shell):
            ap.error("no command given")
        return run_job(a.name, argv=cmd or None, shell=a.shell)
    if a.cmd in ("start", "stop"):
        try:
            if a.cmd == "start":
                print(f"started {a.name}, pid {start_job(a.name, by='cli')}")
            else:
                print(f"signalled process group {stop_job(a.name, force=a.force)}")
        except ControlError as exc:
            print(f"{a.cmd}: {exc}")
            return 1
        return 0
    if a.cmd == "compact":
        print(f"{compact()} records kept")
        return 0
    jobs, source = load_jobs()
    rows = states(jobs, load_runs())
    if not a.json:
        print(f"jobs from: {source}")
    if a.json:
        print(json.dumps([x.as_dict() for x in rows], indent=1))
    else:
        for x in rows:
            print(f"{x.state:<9} {x.name:<34} {x.schedule:<16} last {x.last_start or '-':<20} exit {x.exit_code if x.exit_code is not None else '-'}")
    return 1 if any(x.state in ("failed", "lost", "late") for x in rows) else 0


if __name__ == "__main__":
    sys.exit(main())
