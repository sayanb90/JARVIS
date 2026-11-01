"""Resource monitor (fake /proc), rate limiter and the Upstox REST client (fake HTTP session).  No network."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from data_layer.sysmon import ResourceMonitor, dir_size
from data_layer.upstox.ratelimit import RateLimiter
from data_layer.upstox.rest import UpstoxRest


# ---------------------------------------------------------------------------------- resource monitor
def make_proc(root: Path, cpu: str, procs=(), avail_kb=2_000_000, swap_free_kb=1_000_000) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "stat").write_text(f"cpu  {cpu}\ncpu0 1 1 1 1 1 1 1 1\n")
    (root / "meminfo").write_text(f"MemTotal:        8000000 kB\nMemFree:          100000 kB\nMemAvailable:   {avail_kb} kB\n"
                                  f"SwapTotal:       1000000 kB\nSwapFree:       {swap_free_kb} kB\n")
    (root / "loadavg").write_text("1.50 1.20 1.00 2/300 999\n")
    for pid, cmd, rss, hwm, utime, stime in procs:
        d = root / str(pid)
        d.mkdir(exist_ok=True)
        (d / "cmdline").write_bytes(cmd.replace(" ", "\0").encode())
        (d / "status").write_text(f"Name:\tpython\nVmRSS:\t{rss} kB\nVmHWM:\t{hwm} kB\nThreads:\t7\n")
        (d / "stat").write_text(f"{pid} (python) S 1 1 1 0 -1 0 0 0 0 0 {utime} {stime} 0 0 20 0 7 0 100 0 0\n")


def test_monitor_reports_cpu_memory_processes_disk_and_keeps_peaks(tmp_path):
    proc, watched = tmp_path / "proc", tmp_path / "data"
    watched.mkdir()
    (watched / "f.bin").write_bytes(b"x" * (3 * 1024 * 1024))
    ticks = iter([100.0, 130.0, 160.0])
    out = tmp_path / "res.jsonl"
    mon = ResourceMonitor(out, interval=30, watch_paths=[watched], patterns=("data_layer.upstox.feed",), proc_root=proc, clk_tck=100,
                          clock=lambda: next(ticks))
    make_proc(proc, "100 0 100 800 0 0 0 0", [(11, "python -m data_layer.upstox.feed --segment eq", 400_000, 450_000, 60, 40),
                                              (12, "python -m other.thing", 1000, 1000, 1, 1)])
    first = mon.sample()
    assert first["cpu_pct"] is None and [p["pid"] for p in first["procs"]] == [11]          # only matching processes are tracked
    make_proc(proc, "400 0 200 1100 100 0 0 0", [(11, "python -m data_layer.upstox.feed --segment eq", 500_000, 620_000, 250, 150, )],
              avail_kb=500_000, swap_free_kb=900_000)
    second = mon.sample()
    assert second["cpu_pct"] == pytest.approx(50.0)                                         # 400 busy of 800 jiffies
    assert second["iowait_pct"] == pytest.approx(12.5)
    p = second["procs"][0]
    assert p["cpu_pct"] == pytest.approx(10.0) and p["rss_mb"] == pytest.approx(488.3, abs=0.1) and p["threads"] == 7
    assert p["cmd"].startswith("data_layer.upstox.feed")
    assert second["load1"] == 1.5 and second["swap_used_gb"] == pytest.approx(0.095, abs=0.01)
    assert second["mem_available_gb"] < first["mem_available_gb"]
    d = second["disks"][0]
    assert d["free_gb"] > 0 and second["dirs_gb"][str(watched)] == pytest.approx(0.003, abs=0.001)
    s = mon.summary()
    assert s["peaks"]["cpu_pct"] == 50.0 and s["peaks"]["mem_available_min_gb"] == second["mem_available_gb"]
    assert s["peaks"]["process_rss_mb"]["data_layer.upstox.feed --segment eq"] > 600           # the kernel's own peak (VmHWM) counts
    lines = out.read_text().splitlines()
    assert len(lines) == 2 and json.loads(lines[1])["cpu_pct"] == 50.0


def test_monitor_thread_writes_and_stops_and_survives_missing_proc(tmp_path):
    mon = ResourceMonitor(tmp_path / "r.jsonl", interval=0.05, proc_root=tmp_path / "nothing", watch_paths=[tmp_path])
    mon.start()
    import time
    time.sleep(0.2)
    s = mon.stop()
    assert s["samples"] >= 2 and (tmp_path / "r.jsonl").exists()


def test_dir_size_counts_files_and_ignores_missing(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "x").write_bytes(b"1234")
    (tmp_path / "y").write_bytes(b"12")
    assert dir_size(tmp_path) == 6 and dir_size(tmp_path / "nope") == 0


# ---------------------------------------------------------------------------------- rate limiter
class Clock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


def test_limiter_waits_for_the_per_second_and_per_minute_windows():
    c = Clock()
    lim = RateLimiter(windows=((1.0, 5), (10.0, 8), (1800.0, 100)), margin=1.0, clock=c.now, sleep=c.sleep)
    for _ in range(5):
        assert lim.acquire() == 0.0                          # five in the first second go straight through
    w = lim.acquire()                                        # the sixth waits for the second to pass
    assert 0.9 <= w <= 1.1 and 1.0 <= c.t <= 1.1
    for _ in range(2):
        lim.acquire()                                        # 8 in the first 10 seconds so far
    w = lim.acquire()                                        # the ninth waits for the ten-second window
    assert c.t >= 10.0 and w > 5 and lim.requests == 9


def test_limiter_default_stays_below_the_published_limits():
    lim = RateLimiter()
    assert [limit for _, limit in lim.windows] == [45, 450, 1800]


# ---------------------------------------------------------------------------------- REST client
class FakeResp:
    def __init__(self, status=200, body=None, headers=None, text=""):
        self.status_code, self._body, self.headers, self.text = status, body, headers or {}, text
        self.content = json.dumps(body).encode() if body is not None else text.encode()

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


class FakeSession:
    def __init__(self, *responses):
        self.responses, self.calls = list(responses), []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        r = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        if isinstance(r, Exception):
            raise r
        return r


def client(*responses, **kw):
    sleeps = []
    c = UpstoxRest(lambda: "SECRET-TOKEN", session=FakeSession(*responses), limiter=RateLimiter(sleep=lambda s: None),
                   sleep=sleeps.append, **kw)
    return c, sleeps


def test_rest_success_sends_the_token_but_never_records_it():
    c, _ = client(FakeResp(200, {"status": "success", "data": {"x": 1}}, {"X-RateLimit-Remaining": "49"}))
    r = c.get("/v3/market-quote/ltp", {"instrument_key": "A"}, tag="ltp")
    assert r.ok and r.data["data"]["x"] == 1 and r.rate_headers == {"x-ratelimit-remaining": "49"}
    assert c.session.calls[0]["headers"]["Authorization"] == "Bearer SECRET-TOKEN"
    s = c.summary()
    assert s["by_tag"]["ltp"]["ok"] == 1 and "SECRET-TOKEN" not in json.dumps(s)


def test_rest_parses_upstox_error_codes_and_status_error_bodies():
    body = {"status": "error", "errors": [{"errorCode": "UDAPI1149", "message": "available exclusively with Upstox Plus"}]}
    c, _ = client(FakeResp(403, body))
    r = c.get("/v2/expired-instruments/expiries", tag="expired")
    assert not r.ok and r.status == 403 and r.error_code == "UDAPI1149" and "Plus" in r.error_message
    assert c.stats["expired"]["errors"] == {"403:UDAPI1149": 1}
    c2, _ = client(FakeResp(200, {"status": "error", "errors": [{"errorCode": "UDAPI1206", "message": "Invalid ISIN"}]}))
    assert not c2.get("/v2/fundamentals/X/profile").ok            # an error body on HTTP 200 is still an error


def test_rest_retries_429_honouring_retry_after_then_succeeds():
    c, sleeps = client(FakeResp(429, {"status": "error"}, {"Retry-After": "3"}), FakeResp(200, {"status": "success"}))
    r = c.get("/x", tag="x")
    assert r.ok and r.attempts == 2 and sleeps == [3.0] and c.stats["x"]["retries"] == 1


def test_rest_network_errors_become_a_status_zero_response_not_an_exception():
    c, _ = client(ConnectionError("boom"), max_attempts=2)
    r = c.get("/x")
    assert not r.ok and r.status == 0 and "ConnectionError" in r.error_message and r.attempts == 2


def test_rest_does_not_retry_client_errors():
    c, sleeps = client(FakeResp(400, {"status": "error", "errors": [{"errorCode": "UDAPI1203", "message": "bad date"}]}))
    r = c.get("/x")
    assert r.status == 400 and r.attempts == 1 and sleeps == []
