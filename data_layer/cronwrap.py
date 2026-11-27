"""
Keep the installed crontab tracked and in step with deploy/crontab (SCRUM-219).

The Jobs tab knows a job's state only when its cron line runs through the heartbeat wrapper, `python -m data_layer.jobrun run NAME --shell '...'`.
On 27 Nov 2026 the VPS's installed crontab was an older copy with no wrapped line (33 jobs "untracked", "need attention" stuck at 0), and it lacked jobs
added to deploy/crontab since.  Two commands, neither of which writes a crontab:

  python -m data_layer.jobrun wrap  [--file F] [--add-missing] [--template-schedules]
      prints the installed crontab (or F) with every job line wrapped: comments, variables, paths and the `cd ... &&` prefix are kept; each job takes
      the name deploy/crontab gives it (matched on its log file, then its schedule or its program), else the name the Jobs tab derives; a line already
      wrapped is left as it is, so a second run changes nothing.  --add-missing appends deploy/crontab's jobs that are not installed; --template-schedules
      takes deploy/crontab's schedule for a matched job.  A summary goes to stderr; install the output with `crontab FILE` after reading the diff.
  python -m data_layer.jobrun check [--file F]
      the drift between the installed crontab and deploy/crontab: unwrapped lines, jobs missing on either side, schedules that differ.  Exit 1 on drift.
      data_layer.health prints the same as one line.

Rule (AGENTS.md): a job added to deploy/crontab is added wrapped, and reaches the VPS by `jobrun wrap --add-missing` plus `crontab`, checked by `jobrun check`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from data_layer.jobrun import _WRAP, Job, derive_name, parse_crontab

_CD = re.compile(r"^\s*(cd\s+\S+\s*&&\s*)(.*)$", re.S)
_SHELL = re.compile(r"--shell\s+'((?:[^']|'\\'')*)'")


def _sched_and_cmd(raw: str) -> tuple:
    """(schedule text, command) of a job line as written (@aliases kept)."""
    line = raw.strip()
    if line.startswith("@"):
        a, b = line.split(None, 1)
        return a, b
    parts = line.split(None, 5)
    return " ".join(parts[:5]), parts[5]


def _inner(cmd: str) -> str:
    """The command a line runs, without the `cd ... &&` prefix and without the wrapper."""
    m = _CD.match(cmd)
    body = m.group(2) if m else cmd
    s = _SHELL.search(body)
    if _WRAP.search(body) and s:
        return s.group(1).replace("'\\''", "'")
    return body


def _program(cmd: str) -> str:
    """What a line runs, for matching: the python module or script and a --job value (scheduler.py --job bars ...)."""
    c = _inner(cmd)
    m = re.search(r"python\S*\s+(?:-m\s+)?([\w./]+)", c)
    prog = m.group(1) if m else (c.split()[0] if c.split() else "")
    j = re.search(r"--job\s+(\S+)", c)
    sub = re.search(r"python\S*\s+-m\s+[\w.]+\s+([a-z][\w-]*)", c)          # a subcommand: python -m signals live / nightly ...
    und = re.search(r"--underlying\s+(\S+)", c)
    cad = re.search(r"--cadences\s+(\S+)", c)
    shard = re.search(r"--shard\s+(\S+)", c)
    return " ".join(x for x in (prog, j.group(1) if j else "", sub.group(1) if sub else "", und.group(1) if und else "",
                                cad.group(1) if cad else "", shard.group(1) if shard else "") if x)


def _quote(s: str) -> str:
    return "'" + s.replace("'", "'\\''") + "'"


def _wrap_cmd(cmd: str, name: str) -> Optional[str]:
    m = _CD.match(cmd)
    if not m:
        return None                                       # the wrapper needs the project as working directory: leave such a line, report it
    return f"{m.group(1)}python -m data_layer.jobrun run {name} --shell {_quote(m.group(2).strip())}"


@dataclass
class Report:
    wrapped: list = field(default_factory=list)           # names wrapped by this run
    already: list = field(default_factory=list)           # names that were wrapped before
    renamed: dict = field(default_factory=dict)           # derived name -> deploy/crontab's name
    unmatched: list = field(default_factory=list)         # installed jobs deploy/crontab does not have
    missing: list = field(default_factory=list)           # deploy/crontab jobs that are not installed
    added: list = field(default_factory=list)             # ... of which appended by --add-missing
    schedules: list = field(default_factory=list)         # (name, installed, template) that differ
    schedules_taken: list = field(default_factory=list)   # ... of which replaced by --template-schedules
    unwrappable: list = field(default_factory=list)       # line numbers without `cd ... &&`

    @property
    def drift(self) -> bool:
        return bool(self.unwrapped_left or self.missing or self.schedules or self.unmatched)

    unwrapped_left: list = field(default_factory=list)    # check: lines not wrapped

    def lines(self) -> list:
        out = []
        if self.wrapped:
            out.append(f"wrapped {len(self.wrapped)}: {', '.join(self.wrapped)}")
        if self.already:
            out.append(f"already wrapped {len(self.already)}")
        if self.renamed:
            out.append("named as deploy/crontab: " + ", ".join(f"{a} -> {b}" for a, b in self.renamed.items() if a != b))
        if self.unwrapped_left:
            out.append(f"NOT WRAPPED {len(self.unwrapped_left)}: {', '.join(self.unwrapped_left)}")
        if self.unwrappable:
            out.append(f"cannot wrap (no `cd ... &&`), lines: {self.unwrappable}")
        if self.missing:
            out.append(f"in deploy/crontab, not installed {len(self.missing)}: {', '.join(self.missing)}" + (" (appended)" if self.added else ""))
        if self.unmatched:
            out.append(f"installed, not in deploy/crontab {len(self.unmatched)}: {', '.join(self.unmatched)}")
        for n, a, b in self.schedules:
            out.append(f"schedule differs: {n}: installed '{a}', deploy/crontab '{b}'" + (" (taken from deploy/crontab)" if n in self.schedules_taken else ""))
        return out


def _match(installed: list, template: list) -> dict:
    """installed job name -> template Job.  On the log file first (the job's identity on the VPS); several with one log: the same schedule, then the
    same program; else on the program alone."""
    out, used = {}, set()
    for j in installed:
        same_log = [t for t in template if j.log and t.log == j.log and t.name not in used]
        cand = None
        if len(same_log) == 1:
            cand = same_log[0]
        elif same_log:
            prog = _program(j.command)
            cand = next((t for t in same_log if t.schedule_text == j.schedule_text and _program(t.command) == prog), None) \
                or next((t for t in same_log if _program(t.command) == prog), None) \
                or next((t for t in same_log if t.schedule_text == j.schedule_text), None)
        if cand is None:
            prog = _program(j.command)
            same_prog = [t for t in template if _program(t.command) == prog and t.name not in used]
            cand = same_prog[0] if len(same_prog) == 1 else None
        if cand is not None:
            out[j.name] = cand
            used.add(cand.name)
    return out


def wrap(installed_text: str, template_text: str, add_missing: bool = False, template_schedules: bool = False) -> tuple:
    """(new crontab text, Report).  Only job lines change; everything else is kept byte for byte."""
    inst, tmpl = parse_crontab(installed_text), parse_crontab(template_text)
    match = _match(inst, tmpl)
    rep = Report()
    raw_lines = installed_text.splitlines()
    names: set = set()
    by_line = {j.line: j for j in inst}
    for n, j in sorted(by_line.items()):
        t = match.get(j.name)
        name = t.name if t is not None else j.name
        if name in names:                                  # two installed lines matched one template job: keep them apart
            k = 2
            while f"{name}-{k}" in names:
                k += 1
            name = f"{name}-{k}"
        names.add(name)
        if t is None:
            rep.unmatched.append(name)
        elif t.name != j.name:
            rep.renamed[j.name] = t.name
        sched, cmd = _sched_and_cmd(raw_lines[n - 1])
        if t is not None and sched != t.schedule_text:
            rep.schedules.append((name, sched, t.schedule_text))
            if template_schedules:
                sched = t.schedule_text
                rep.schedules_taken.append(name)
        if j.wrapped:
            rep.already.append(name)
            new_cmd = cmd
        else:
            new_cmd = _wrap_cmd(cmd, name)
            if new_cmd is None:
                rep.unwrappable.append(n)
                new_cmd = cmd
            else:
                rep.wrapped.append(name)
        indent = raw_lines[n - 1][: len(raw_lines[n - 1]) - len(raw_lines[n - 1].lstrip())]
        raw_lines[n - 1] = f"{indent}{sched:<14} {new_cmd}" if not sched.startswith("@") else f"{indent}{sched} {new_cmd}"
    matched_template = {t.name for t in match.values()}
    rep.missing = [t.name for t in tmpl if t.name not in matched_template]
    if add_missing and rep.missing:
        tlines = template_text.splitlines()
        raw_lines += ["", "# --- added by `python -m data_layer.jobrun wrap --add-missing` from deploy/crontab ---"]
        for t in tmpl:
            if t.name in rep.missing:
                raw_lines.append(tlines[t.line - 1].strip())
                rep.added.append(t.name)
    text = "\n".join(raw_lines) + ("\n" if installed_text.endswith("\n") or not installed_text else "")
    return text, rep


def check(installed_text: str, template_text: str) -> Report:
    """The drift of the installed crontab against deploy/crontab (nothing is rewritten)."""
    _, rep = wrap(installed_text, template_text)
    inst = parse_crontab(installed_text)
    rep.unwrapped_left = [j.name for j in inst if not j.wrapped]
    rep.wrapped = []
    return rep


def template_text(root: Optional[Path] = None) -> str:
    from data_layer import lake as lake_mod
    return ((root or lake_mod.ROOT) / "deploy" / "crontab").read_text()
