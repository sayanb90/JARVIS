"""
Daily health report: one PASS / WARN / FAIL line per thing that must be true for data to be flowing.

  python -m data_layer.health                 # morning check (before the session)
  python -m data_layer.health --after-close   # evening check (expects today's bars, compacted)
  python -m data_layer.health --probe         # also try the Upstox feed handshake with the stored token (network)

Exit code 1 if anything FAILs.  Reads only; changes nothing.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import shutil
import subprocess
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from . import backup, fx, market_calendar, tick_store
from .fmp_client import read_universe
from .lake import BARS_MARKETS, LAKE_DIR, MARKETS, ROOT
from .query import Lake
from .verify import Report

SESSION_START, SESSION_END = time(3, 45), time(10, 0)
LOG_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def last_weekday(d: date) -> date:
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def prev_weekday(d: date) -> date:
    return last_weekday(d - timedelta(days=1))


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def in_session(now: datetime) -> bool:
    return now.weekday() < 5 and SESSION_START <= now.time() < SESSION_END


def _user_bus_env() -> dict:
    """The environment `systemctl --user` needs.  Under cron there is no XDG_RUNTIME_DIR, so the call cannot find the user's systemd
    instance, answers nothing, and a running user service was reported as "inactive" in every health report (SCRUM-144)."""
    env = dict(os.environ)
    runtime = env.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}"
    env["XDG_RUNTIME_DIR"] = runtime
    env.setdefault("DBUS_SESSION_BUS_ADDRESS", f"unix:path={runtime}/bus")
    return env


def service_state(service: str) -> tuple[str, str]:
    """(state, detail) of a system or user service: "active", "active (user)", "inactive" or "unknown" with the reason."""
    def run(args):
        return subprocess.run(args, capture_output=True, text=True, timeout=10, env=_user_bus_env())
    sysr = run(["systemctl", "is-active", service])
    state = sysr.stdout.strip()
    if state == "active":
        return "active", ""
    usr = run(["systemctl", "--user", "is-active", service])        # a service installed for a user is not seen above
    ustate = usr.stdout.strip()
    if ustate == "active":
        return "active (user)", ""
    if ustate in ("inactive", "failed", "activating", "deactivating"):
        return ustate, "user service"
    if "bus" in (usr.stderr or "").lower() or not ustate:
        return "unknown", "cannot reach the user systemd instance: " + ((usr.stderr or "").strip().splitlines() or ["no answer"])[0][:80]
    return state or ustate, ""


def check(now: datetime | None = None, after_close: bool = False, probe: bool = False, lake_dir: Path | None = None,
          root: Path | None = None, backup_path: str | Path | None = None, service: str = "jarvis-feed") -> Report:
    r, now = Report(), now or utcnow()
    lake_dir, root = lake_dir or LAKE_DIR, root or ROOT
    today = now.date()
    print(f"== JARVIS health {now:%Y-%m-%d %H:%M} UTC ({'after close' if after_close else 'morning'}) ==")

    # disk
    probe_path = lake_dir if lake_dir.exists() else root
    du = shutil.disk_usage(probe_path)
    free = du.free / du.total * 100
    r.line("FAIL" if free < 5 else "WARN" if free < 15 else "PASS", f"disk free {free:.0f}% ({du.free / 1e9:.1f} GB) on {probe_path}")

    # universe
    uni_in = read_universe("IN")
    with_isin = [u for u in uni_in if len(u.get("isin", "")) == 12 and u["isin"].startswith("IN")]
    counts = {m: len(read_universe(m)) for m in MARKETS}
    r.line("PASS" if uni_in else "FAIL", f"universe: {counts}")
    r.line("PASS" if len(with_isin) == len(uni_in) and uni_in else "WARN",
           f"India stocks with a usable ISIN (needed for the live feed): {len(with_isin)} of {len(uni_in)}")

    # market calendar (SCRUM-141): is today a day the exchange should have produced data?
    cal = market_calendar.load(lake_dir)
    today_cal = market_calendar.status(today, "NSE", cal, today)
    if today_cal.state == "unknown":
        r.line("WARN", f"market calendar: {today_cal.reason}")
    elif today_cal.state == "closed":
        r.line("PASS", f"market calendar: NSE is closed today ({today_cal.reason}): no data is expected")
    elif today_cal.state == "special":
        r.line("PASS", f"market calendar: special session today ({today_cal.reason})")
    else:
        r.line("PASS", "market calendar: NSE is open today")

    # FX rates (SCRUM-160): needed to compare earnings in one currency with prices in another
    rates = fx.load_rates(lake_dir)
    if rates is None:
        r.line("WARN", "no FX rates yet (run: python -m data_layer.fx refresh)")
    else:
        newest = rates.newest()
        age = (today - newest).days
        lacking = [c for c in fx.REQUIRED if c not in rates.currencies()]
        r.line("WARN" if age > 4 or lacking else "PASS", f"FX rates: newest {newest} ({age} days old), {len(rates.currencies())} currencies"
               + (f"; missing {', '.join(lacking)}" if lacking else ""))

    lake, tables, lake_error = None, [], None
    try:
        lake = Lake(lake_dir)
        tables = lake.tables()
    except Exception as exc:                                  # one damaged Parquet file must not hide every other check
        lake_error = str(exc).splitlines()[0][:200]
        r.line("FAIL", f"the lake cannot be opened: {lake_error}")

    # daily OHLC freshness
    if lake is None:
        pass
    elif "ohlc" not in tables:
        r.line("FAIL", "no daily OHLC in the lake")
    else:
        latest = lake.sql("SELECT market, max(date) AS d, count(DISTINCT symbol) AS n FROM ohlc GROUP BY 1 ORDER BY 1")
        for row in latest.itertuples():
            age = (today - row.d.date()).days
            r.line("WARN" if age > 4 else "PASS", f"daily OHLC {row.market}: latest {row.d.date()} ({age} days old), {row.n} symbols")

    # Yahoo 1-minute bars for US, UK and EU (built forward from the day the cron job started; nothing to say before that)
    ybars = lake_dir / "bars" / "interval=1m"
    ydays = sorted(p.name.split("=", 1)[1] for p in ybars.glob("date=*") if any(p.glob("*.parquet"))) if ybars.exists() else []
    if ydays:
        age = (today - date.fromisoformat(ydays[-1])).days
        try:
            import duckdb
            per = duckdb.connect().execute(
                f"SELECT market, count(DISTINCT symbol), count(*) FROM read_parquet('{ybars}/date={ydays[-1]}/*.parquet') GROUP BY 1 ORDER BY 1"
            ).fetchall()
            detail = ", ".join(f"{m} {n} symbols {rows:,} bars" for m, n, rows in per)
        except Exception as exc:                                  # noqa: BLE001
            detail = f"unreadable: {str(exc).splitlines()[0][:80]}"
        r.line("WARN" if age > 4 else "PASS", f"Yahoo 1-minute bars: {len(ydays)} days stored, newest {ydays[-1]} ({age} days old); {detail}")

    # intraday bars
    days = lake.bar_days("1s") if lake is not None else []
    expected_day = today if (after_close and today_cal.state != "closed") else market_calendar.previous_open_day(today, "NSE", cal)
    if lake is None:
        pass
    elif not days:
        r.line("WARN", "no 1-second bars stored yet (feed has not run)")
    else:
        newest = days[0]
        stale_days = [d for d in days
                     if d < today.isoformat() and any(f.name.startswith("seg-") for f in tick_store.day_files(lake_dir, d))]
        r.line("PASS" if newest >= expected_day.isoformat() else "WARN", f"latest bar day {newest} (expected {expected_day} or later)")
        syms = lake.bar_symbols(newest, "1s")
        want = max(len(with_isin), 1)
        newest_cal = market_calendar.status(date.fromisoformat(newest), "NSE", cal, today)
        if newest_cal.state == "closed":                      # a holiday the feed sat through: a few stray ticks are not a missing day
            r.line("PASS", f"bars on {newest}: {len(syms)} instruments; the market was closed that day ({newest_cal.reason})")
        else:
            r.line("PASS" if len(syms) >= 0.8 * want else "WARN", f"bars on {newest}: {len(syms)} instruments of {len(with_isin)} subscribable")
        r.line("WARN" if stale_days else "PASS", f"days with uncompacted segment files: {stale_days or 'none'}"
               + (" (run: python -c \"from data_layer.tick_store import compact_ticks_day; ...\")" if stale_days else ""))

    # is the feed writing right now?
    if in_session(now) and today_cal.state != "closed":
        files = tick_store.day_files(lake_dir, today)                 # every segment's folder, and the older single folder
        newest_age = (now.timestamp() - max((f.stat().st_mtime for f in files), default=0)) if files else None
        # file mtimes are real wall-clock; only compare when `now` is the real now
        if abs((utcnow() - now).total_seconds()) < 120:
            ok = newest_age is not None and newest_age < 120
            r.line("PASS" if ok else "FAIL", "live feed is writing ticks (newest file under 2 minutes old)" if ok else
                   f"live feed is NOT writing: newest tick file {'none today' if newest_age is None else f'{newest_age:.0f}s old'}")

    # service
    if shutil.which("systemctl"):
        try:
            state, detail = service_state(service)
            if state:
                r.line("PASS" if state.startswith("active") else "WARN", f"systemd service {service}: {state}" + (f" ({detail})" if detail else ""))
        except Exception:
            pass

    # scheduler log
    log = root / "logs" / "scheduler.log"
    if log.exists():
        ok_ts, bad_ts = None, None
        for line in log.read_text(errors="replace").splitlines():
            m = LOG_TS.match(line)
            if not m:
                continue
            t = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            if "Scheduler finished successfully" in line:
                ok_ts = t
            elif "Scheduler finished with errors" in line:
                bad_ts = t
        if ok_ts is None and bad_ts is None:
            r.line("WARN", "scheduler log has no finished runs yet")
        else:
            failing = bad_ts is not None and (ok_ts is None or bad_ts > ok_ts)
            r.line("FAIL" if failing else "PASS", f"scheduler: last success {ok_ts or 'never'}, last error {bad_ts or 'never'}")
            if ok_ts and (now - ok_ts) > timedelta(days=3) and today.weekday() < 5:
                r.line("WARN", f"scheduler has not succeeded for {(now - ok_ts).days} days")
    else:
        r.line("WARN", "no logs/scheduler.log yet (cron not run)")

    # token
    tok = os.getenv("UPSTOX_FEED_TOKEN") or os.getenv("UPSTOX_ACCESS_TOKEN")
    r.line("PASS" if tok else "WARN", "Upstox token present in the environment" if tok else
           "no Upstox token set: run python -m data_layer.upstox.auth (the daily token expires)")
    if probe and tok:
        from .upstox.auth import probe_feed
        res = asyncio.run(probe_feed(tok))
        r.line("PASS" if res.startswith("accepted") else "FAIL", f"Upstox feed handshake: {res}")

    # backup
    bdir = backup_path or os.getenv("JARVIS_BACKUP_DIR")
    if not bdir:
        r.line("WARN", "no backup destination configured (JARVIS_BACKUP_DIR; backup is parked: SCRUM-121)")
    else:
        snaps = backup.list_snapshots(Path(bdir))
        if not snaps:
            r.line("WARN", f"no backup snapshots in {bdir} (backup is parked: SCRUM-121)")
        else:
            age = (today - date.fromisoformat(snaps[-1])).days
            r.line("PASS" if age <= 1 else "WARN", f"newest backup {snaps[-1]} ({age} days old), {len(snaps)} snapshots")
    return r


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--after-close", action="store_true")
    ap.add_argument("--probe", action="store_true")
    a = ap.parse_args(argv)
    from dotenv import load_dotenv
    from .lake import ENV_FILE
    load_dotenv(ENV_FILE)
    rep = check(after_close=a.after_close, probe=a.probe)
    print(f"\n{'FAILED: ' + str(rep.fails) + ' check(s)' if rep.fails else 'no FAIL checks'}")
    return 1 if rep.fails else 0


if __name__ == "__main__":
    sys.exit(main())
