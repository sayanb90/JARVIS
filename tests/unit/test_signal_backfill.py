"""The signal history backfill job: what can be backfilled, and the resumable, gated, time- and memory-limited run (runners stubbed)."""
from __future__ import annotations

import fcntl
from datetime import date, datetime

import pytest

from signals import backfill_job as bj

NOW = datetime(2026, 11, 18, 12, 0)             # a Wednesday, after the session
YDAY = date(2026, 11, 17)


class Harness:
    def __init__(self, tmp_path, gate_pass=True, fail=(), secs=1.0):
        self.lake, self.root = tmp_path / "lake", tmp_path / "store"
        self.lake.mkdir(parents=True)
        self.calls, self.ranges, self.gates, self.fail, self.gate_pass, self.t, self.secs = [], [], [], set(fail), gate_pass, 0.0, secs

    def hist(self, lake, root, u, a, b):
        self.calls.append(("history", u, f"{a:%Y-%m}", b))
        self.ranges.append(("history", a, b))
        if (u, f"{a:%Y-%m}") in self.fail:
            raise RuntimeError("boom")
        return {"vrp": 10, "pin": 5}

    def minute(self, lake, root, u, a, b):
        self.calls.append(("minute", u, f"{a:%Y-%m}", b))
        self.ranges.append(("minute", a, b))
        return {"vrp": 100}

    def gate(self, lake, root, u, d0, d1):
        self.gates.append(u)
        return {"pass": self.gate_pass, "median_abs": 0.003, "p95_abs": 0.01, "n": 500, "reason": None if self.gate_pass else "too far apart"}

    def clock(self):
        self.t += self.secs
        return self.t

    def run(self, **kw):
        args = dict(lake=self.lake, root=self.root, underlyings=("NIFTY",), d0=date(2026, 9, 1), now=lambda: NOW, run_history=self.hist,
                    run_minute=self.minute, gate=self.gate, memory=lambda: 99.0, clock=self.clock, log=lambda *_: None)
        args.update(kw)
        return bj.run_job(**args)


# -- what can be backfilled ----------------------------------------------------------------------
def test_plan_marks_live_only_signals_as_not_backfillable_and_names_proxies():
    df = bj.plan().set_index("signal")
    assert df.loc["book_pressure", "backfill"] == "no" and "5-level book" in df.loc["book_pressure", "how"]
    assert df.loc["signed_flow", "backfill"] == "no" and "vol_demand_proxy" in df.loc["signed_flow", "how"]
    assert df.loc["vrp", "backfill"] == "yes" and "every minute" in df.loc["vrp", "how"]
    assert df.loc["noise_band", "how"] == "per minute"
    assert df.loc["basket_fair_value", "backfill"] == "partly" and "weights" in df.loc["basket_fair_value", "how"]
    assert set(df["backfill"]) == {"yes", "no", "partly"}


def test_plan_covers_every_registered_signal():
    import signals.catalogue  # noqa: F401
    from signals.framework import REGISTRY
    assert set(bj.plan()["signal"]) == set(REGISTRY)


# -- the run ---------------------------------------------------------------------------------------
def test_months_run_oldest_first_and_the_minute_layer_stops_before_the_tick_era(tmp_path):
    h = Harness(tmp_path)
    r = h.run()
    assert r.code == 0 and "finished" in r.message
    hist = [c[2] for c in h.calls if c[0] == "history"]
    assert hist == ["2026-09", "2026-10", "2026-11"]                             # oldest first, through yesterday
    assert [c[2] for c in h.calls if c[0] == "minute"] == ["2026-09", "2026-10", "2026-11"]
    assert [c[3] for c in h.calls if c[0] == "minute"][-1] == date(2026, 11, 2)    # from 2026-11-03 the live engine covers every bucket
    assert [c[3] for c in h.calls if c[0] == "history"][-1] == YDAY


def test_a_rerun_the_same_day_does_nothing_and_the_next_day_adds_only_the_new_days(tmp_path):
    h = Harness(tmp_path)
    h.run()
    h.calls.clear()
    r = h.run()
    assert h.calls == [] and r.chunks == 0 and "nothing left to do" in r.message
    h.ranges.clear()
    r = h.run(now=lambda: datetime(2026, 11, 19, 12, 0))                               # a day later: yesterday is now 18 Nov
    assert h.ranges == [("history", date(2026, 11, 18), date(2026, 11, 18))]            # one day, not the whole month again
    assert bj.load_state(h.root)["NIFTY"]["history"]["2026-11"]["through"] == "2026-11-18"


def test_state_records_rows_and_signal_counts(tmp_path):
    h = Harness(tmp_path)
    h.run()
    st = bj.load_state(h.root)
    m = st["NIFTY"]["history"]["2026-10"]
    assert m["ok"] and m["rows"] == 15 and m["signals"] == {"vrp": 10, "pin": 5}


def test_failed_month_is_recorded_retried_and_makes_the_exit_code_one(tmp_path):
    h = Harness(tmp_path, fail=[("NIFTY", "2026-10")])
    r = h.run()
    assert r.code == 1 and "failed" in r.message
    st = bj.load_state(h.root)["NIFTY"]["history"]
    assert st["2026-10"]["ok"] is False and "boom" in st["2026-10"]["error"] and st["2026-09"]["ok"] and st["2026-11"]["ok"]
    h.fail.clear()
    h.calls.clear()
    h.ranges.clear()
    r = h.run()
    assert h.ranges == [("history", date(2026, 10, 1), date(2026, 10, 31))] and r.code == 0      # retried from the start of the month


def test_three_failures_in_a_row_stop_the_run(tmp_path):
    h = Harness(tmp_path, fail=[("NIFTY", m) for m in ("2026-07", "2026-08", "2026-09")])
    r = h.run(d0=date(2026, 7, 1))                                                      # Oct and Nov would work, but the run stops first
    assert "failed in a row" in r.message
    assert len([c for c in h.calls if c[0] == "history"]) == 3


def test_a_failed_gate_skips_the_underlying_and_a_pass_is_cached(tmp_path):
    h = Harness(tmp_path, gate_pass=False)
    r = h.run(underlyings=("NIFTY", "BANKNIFTY"))
    assert h.calls == [] and r.chunks == 0 and h.gates == ["NIFTY", "BANKNIFTY"]
    assert bj.load_state(h.root)["NIFTY"]["gate"]["pass"] is False
    h2 = Harness(tmp_path / "ok")
    h2.run()
    h2.run()
    assert h2.gates == ["NIFTY"]                                                      # asked once, not again within 14 days


def test_time_limit_stops_between_months_and_the_next_run_continues(tmp_path):
    h = Harness(tmp_path, secs=60)                                                     # every clock reading is a minute later
    r = h.run(max_minutes=3)
    assert "time limit" in r.message and r.code == 0 and 0 < r.chunks < 6
    done = r.chunks
    h.t, h.calls = 0.0, []
    r2 = h.run(max_minutes=999)
    assert r2.code == 0 and "stopped" not in r2.message
    assert len(h.calls) == 6 - done                                                    # exactly what was left


def test_low_memory_stops_the_run(tmp_path):
    h = Harness(tmp_path)
    r = h.run(memory=lambda: 2.0, min_free_gb=5)
    assert h.calls == [] and "free memory" in r.message


def test_refuses_during_the_session_and_when_another_run_holds_the_lock(tmp_path):
    h = Harness(tmp_path)
    r = h.run(now=lambda: datetime(2026, 11, 18, 5, 0))                                # Wednesday 05:00 UTC
    assert r.code == 3 and "session" in r.message and h.calls == []
    assert h.run(now=lambda: datetime(2026, 11, 21, 5, 0)).code == 0                  # a Saturday is fine
    (h.root / "backfill").mkdir(parents=True, exist_ok=True)
    with open(h.root / "backfill" / "lock", "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        r = h.run()
    assert r.code == 3 and "lock" in r.message


def test_the_store_must_be_outside_the_lake(tmp_path):
    h = Harness(tmp_path)
    with pytest.raises(SystemExit):
        h.run(root=h.lake / "inside")


def test_status_text_reports_progress_and_failures(tmp_path):
    h = Harness(tmp_path, fail=[("NIFTY", "2026-10")])
    h.run()
    t = bj.status_text(h.root, d0=date(2026, 9, 1), today=NOW.date())
    assert "NIFTY: IV gate passed" in t and "history  2/3 months" in t and "FAILED: 2026-10" in t and "next 2026-10" in t
    assert "minute" in t
    assert "nothing has run yet" in bj.status_text(tmp_path / "empty")


def test_command_line_plan_and_status(tmp_path, capsys):
    from signals.__main__ import main
    assert main(["backfill", "plan"]) == 0
    assert "book_pressure" in capsys.readouterr().out
    assert main(["backfill", "status", "--root", str(tmp_path)]) == 0
    assert "nothing has run yet" in capsys.readouterr().out
