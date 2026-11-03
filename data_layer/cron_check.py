"""
Is the scheduler set up to run what we expect, and when does each job fire?  (SCRUM-112)

  python -m data_layer.cron_check                       # installed crontab (crontab -l) against deploy/crontab, for tomorrow (UTC)
  python -m data_layer.cron_check --date 2026-11-03
  python -m data_layer.cron_check --crontab-file deploy/local/jarvis-crontab

Every job in the tracked template `deploy/crontab` must be in the installed crontab (the Upstox fundamentals jobs included).
Paths and log redirects may differ; the schedule and the command must match.  Extra jobs are listed, not failed.  Also shown:
the times each expected job fires on the chosen UTC date, its next run in UTC and IST, whether the cron daemon and the
jarvis-feed and jarvis-viewer services are active, and whether the server clock is UTC.
Exit code 0 only when nothing expected is missing, the clock is UTC and the cron daemon runs.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

TEMPLATE = Path(__file__).resolve().parent.parent / "deploy" / "crontab"
IST = timezone(timedelta(hours=5, minutes=30))
RANGES = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)]          # minute, hour, day of month, month, day of week (0 and 7 = Sunday)
ENV_LINE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
SERVICES = ("jarvis-feed", "jarvis-viewer")


@dataclass(frozen=True)
class Entry:
    schedule: str          # the five fields, single-spaced
    command: str           # as written
    key: str               # schedule + normalised command, for comparison


# ---------------------------------------------------------------------------------------------- parsing
def normalise(command: str) -> str:
    """The command without paths and log redirects, so a VPS that spells them differently still matches."""
    c = re.sub(r"\s*>>?\s*\S+(\s+2>&1)?", "", command)                 # log redirects
    c = re.sub(r"\bcd\s+\S+\s*&&\s*", "", c)                           # cd $JARVIS &&
    c = re.sub(r"\S*/python3?(?=\s)", "python", c)                      # .venv/bin/python, /usr/bin/python3
    c = re.sub(r"\bpython3(?=\s)", "python", c)
    return re.sub(r"\s+", " ", c).strip()


def parse_crontab(text: str) -> list[Entry]:
    out = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or ENV_LINE.match(line):
            continue
        parts = line.split(None, 5)
        if len(parts) < 6:
            continue
        schedule = " ".join(parts[:5])
        out.append(Entry(schedule, parts[5], schedule + " | " + normalise(parts[5])))
    return out


def _field(spec: str, lo: int, hi: int) -> set[int]:
    values: set[int] = set()
    for part in spec.split(","):
        step = 1
        if "/" in part:
            part, s = part.split("/", 1)
            step = int(s)
        if part in ("*", ""):
            a, b = lo, hi
        elif "-" in part:
            a, b = (int(x) for x in part.split("-", 1))
        else:
            a = b = int(part)
        values.update(range(a, b + 1, step))
    return values


class Schedule:
    def __init__(self, spec: str) -> None:
        f = spec.split()
        if len(f) != 5:
            raise ValueError(f"a cron schedule has five fields, got {spec!r}")
        self.minute, self.hour, self.dom, self.month = (_field(f[i], *RANGES[i]) for i in range(4))
        self.dow = {v % 7 for v in _field(f[4], 0, 7)}
        self.dom_any, self.dow_any = f[2] == "*", f[4] == "*"

    def matches(self, t: datetime) -> bool:
        if t.minute not in self.minute or t.hour not in self.hour or t.month not in self.month:
            return False
        dom_ok, dow_ok = t.day in self.dom, (t.isoweekday() % 7) in self.dow
        if self.dom_any or self.dow_any:                                # cron: restricted fields are ANDed with a star
            return dom_ok and dow_ok
        return dom_ok or dow_ok                                         # both restricted: either one

    def fires_on(self, day: date) -> list[str]:
        base = datetime(day.year, day.month, day.day)
        return [f"{t:%H:%M}" for t in (base + timedelta(minutes=m) for m in range(1440)) if self.matches(t)]

    def next_run(self, after: datetime, horizon_days: int = 400) -> Optional[datetime]:
        t = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
        for _ in range(horizon_days * 1440):
            if self.matches(t):
                return t
            t += timedelta(minutes=1)
        return None


# ---------------------------------------------------------------------------------------------- comparison and system state
def compare(expected: list[Entry], actual: list[Entry]) -> dict:
    have = {e.key for e in actual}
    want = {e.key for e in expected}
    return {"present": [e for e in expected if e.key in have], "missing": [e for e in expected if e.key not in have],
            "extra": [e for e in actual if e.key not in want]}


def _run(cmd: list[str]) -> Optional[str]:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    except (OSError, subprocess.SubprocessError):
        return None
    return r.stdout.strip() if r.returncode == 0 else (r.stdout.strip() or "")


def installed_crontab() -> Optional[str]:
    try:
        r = subprocess.run(["crontab", "-l"], capture_output=True, text=True, timeout=15)
    except (OSError, subprocess.SubprocessError):
        return None
    return r.stdout if r.returncode == 0 else None


def service_state(name: str, user_units: bool = True) -> str:
    """systemd state of a unit.  Units installed for a user (systemctl --user) are found too; the answer then reads
    "active (user)"."""
    out = _run(["systemctl", "is-active", name])
    if out is None:
        return "unknown (no systemctl)"
    if out == "active" or not user_units:
        return out or "inactive"
    user = _run(["systemctl", "--user", "is-active", name])
    return "active (user)" if user == "active" else (out or "inactive")


def cron_daemon_state() -> str:
    states = [service_state(n, user_units=False) for n in ("cron", "crond")]
    return "active" if "active" in states else ("unknown (no systemctl)" if all(s.startswith("unknown") for s in states) else "not running")


def clock_is_utc(now: Optional[datetime] = None) -> bool:
    return (now or datetime.now().astimezone()).utcoffset() == timedelta(0)


# ---------------------------------------------------------------------------------------------- report
def short(e: Entry) -> str:
    m = re.search(r"python\s+(?:-m\s+)?(\S+)(.*)", normalise(e.command))
    if not m:
        return normalise(e.command)[:70]
    args = re.sub(r"\s*&&.*", "", m.group(2)).strip()
    return f"{m.group(1)} {args}".strip()[:70]


def report(expected: list[Entry], actual: list[Entry], day: date, now: datetime, clock_utc: bool, daemon: str,
           services: dict[str, str]) -> tuple[str, bool]:
    cmp = compare(expected, actual)
    lines = [f"Scheduler check for {day} (UTC), now {now:%Y-%m-%d %H:%M} UTC", ""]
    lines.append(f"clock is UTC: {'yes' if clock_utc else 'NO'}   cron daemon: {daemon}   " +
                 "   ".join(f"{k}: {v}" for k, v in services.items()))
    lines += ["", f"{'status':<8}{'fires on ' + str(day) + ' (UTC)':<26}{'next run (UTC / IST)':<36}job"]
    for status, entries in (("present", cmp["present"]), ("MISSING", cmp["missing"])):
        for e in entries:
            s = Schedule(e.schedule)
            fires = ", ".join(s.fires_on(day)) or "-"
            nxt = s.next_run(now)
            nxt_s = f"{nxt:%a %m-%d %H:%M} / {(nxt.replace(tzinfo=timezone.utc).astimezone(IST)):%H:%M}" if nxt else "never"
            lines.append(f"{status:<8}{fires:<26}{nxt_s:<36}{short(e)}")
    if cmp["extra"]:
        lines += ["", "extra jobs in the installed crontab (not in deploy/crontab; check they are meant to be there):"]
        lines += [f"  {e.schedule}  {short(e)}" for e in cmp["extra"]]
    ok = not cmp["missing"] and clock_utc and daemon in ("active", "unknown (no systemctl)")
    lines += ["", ("OK: every expected job is installed." if ok else
                   "NOT OK: " + ("; ".join(filter(None, [
                       f"{len(cmp['missing'])} expected job(s) missing: install them from deploy/crontab" if cmp["missing"] else "",
                       "the server clock is not UTC" if not clock_utc else "",
                       "the cron daemon is not running" if daemon not in ("active", "unknown (no systemctl)") else ""]))))]
    return "\n".join(lines), ok


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Check the installed crontab against the tracked deploy/crontab")
    ap.add_argument("--crontab-file", type=Path, default=None, help="read this file instead of `crontab -l`")
    ap.add_argument("--template", type=Path, default=TEMPLATE)
    ap.add_argument("--date", type=date.fromisoformat, default=None, help="UTC date to list firing times for (default: tomorrow)")
    ap.add_argument("--now", default=None, help="override the current time (UTC, ISO) for testing")
    ap.add_argument("--no-services", action="store_true", help="skip the systemd checks")
    a = ap.parse_args(argv)
    now = datetime.fromisoformat(a.now) if a.now else datetime.now(timezone.utc).replace(tzinfo=None)
    day = a.date or (now + timedelta(days=1)).date()
    text = a.crontab_file.read_text() if a.crontab_file else installed_crontab()
    if text is None:
        print("No crontab installed for this user (crontab -l failed). Install one with: crontab deploy/crontab (then add your local extras).")
        return 1
    expected, actual = parse_crontab(a.template.read_text()), parse_crontab(text)
    services = {} if a.no_services else {n: service_state(n) for n in SERVICES}
    daemon = "unknown (no systemctl)" if a.no_services else cron_daemon_state()
    out, ok = report(expected, actual, day, now, clock_is_utc(), daemon, services)
    print(out)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
