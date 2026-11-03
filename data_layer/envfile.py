"""
Set keys in a .env file from a script without ever printing their values (SCRUM-109 rollout).

  python -m data_layer.envfile set KEY=VALUE [KEY=VALUE ...] [--file .env] [--no-backup]

An existing `KEY=...` line is replaced (everything else, comments included, stays as it is); a new key is appended.  A copy of the
file is saved first as `<file>.bak-<timestamp>` unless --no-backup, and the file keeps its permissions.  Only the key names are printed.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from . import lake as lake_mod

KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def set_keys(path: Path, values: dict[str, str], backup: bool = True, now: Optional[datetime] = None) -> tuple[list[str], Optional[Path]]:
    """Returns (the keys set, the backup path or None)."""
    for k in values:
        if not KEY.match(k):
            raise ValueError(f"not a valid variable name: {k!r}")
    path = Path(path)
    text = path.read_text() if path.exists() else ""
    saved = None
    if backup and path.exists():
        saved = path.with_name(f"{path.name}.bak-{(now or datetime.now()):%Y%m%d-%H%M%S}")
        shutil.copy2(path, saved)
    lines, done = text.splitlines(), set()
    for i, line in enumerate(lines):
        m = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)=", line)
        if m and m.group(1) in values and m.group(1) not in done:
            lines[i] = f"{m.group(1)}={values[m.group(1)]}"
            done.add(m.group(1))
    lines += [f"{k}={v}" for k, v in values.items() if k not in done]
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text("\n".join(lines) + "\n")
    if path.exists():
        shutil.copymode(path, tmp)
    else:
        tmp.chmod(0o600)
    os.replace(tmp, path)
    return list(values), saved


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Set keys in a .env file without printing the values")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("set")
    s.add_argument("pairs", nargs="+", metavar="KEY=VALUE")
    s.add_argument("--file", type=Path, default=None)
    s.add_argument("--no-backup", action="store_true")
    a = ap.parse_args(argv)
    values = {}
    for p in a.pairs:
        k, sep, v = p.partition("=")
        if not sep:
            raise SystemExit(f"expected KEY=VALUE, got {p!r}")
        values[k] = v
    keys, saved = set_keys(a.file or lake_mod.ENV_FILE, values, backup=not a.no_backup)
    print(f"set {', '.join(keys)}" + (f" (backup: {saved})" if saved else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
