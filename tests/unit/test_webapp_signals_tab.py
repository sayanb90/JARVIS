"""The viewer's Signals tab, built out (SCRUM-206): signals by family, a signal's day, per-contract signals by strike, chain views, history across
days, the IV smile; read from a store that the engines fill with many small files; read once, then only the new files."""

import resource
import time
from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from signals.options.stream import write_rows
from tests.signals_helpers import write_fake_lake
from webapp import signals_store, signals_view
from webapp.app import create_app

DAY = pd.Timestamp("2026-11-13")
T0 = DAY + pd.Timedelta(hours=4)                      # 04:00 UTC
EXPS = ["2026-11-17", "2026-11-24"]
STRIKES = [24000.0, 24100.0, 24200.0]


def contract_rows(sid, seconds, bucket="1s", fut=True, start=0):
    rows = []
    for i in range(start, start + seconds):
        ts = T0 + pd.Timedelta(seconds=i)
        ka = ts + pd.Timedelta(seconds=1)
        for e in EXPS:
            for k in STRIKES:
                for cp in ("CE", "PE"):
                    rows.append({"signal": sid, "bucket": bucket, "underlying": "NIFTY", "ts": ts, "known_at": ka,
                                 "symbol": f"NIFTY_{int(k)}_{cp}_{e}", "expiry": e, "strike": k, "cp": cp,
                                 "qi": np.sin(i / 30 + k / 1000) * (1 if cp == "CE" else -1), "micro_rel": 0.001 * (i % 7)})
        if fut:
            rows.append({"signal": sid, "bucket": bucket, "underlying": "NIFTY", "ts": ts, "known_at": ka, "symbol": "NIFTY_FUT_24_NOV_26",
                         "expiry": "2026-11-24", "strike": np.nan, "cp": "FUT", "qi": 0.5 + 0.1 * np.sin(i / 20), "micro_rel": 0.0})
    return rows


def single_rows(sid, minutes, bucket="1min", col="skew1_s", start=0):
    rows = []
    for i in range(start, start + minutes):
        ts = T0 + pd.Timedelta(minutes=i)
        rows.append({"signal": sid, "bucket": bucket, "underlying": "NIFTY", "ts": ts, "known_at": ts + pd.Timedelta(minutes=1), "date": ts.date(),
                     col: 0.02 + 0.001 * i, col + "_tod_z": 0.05 * i, "other": float(i)})
    return rows


def fill(store, minutes=10):
    write_rows(contract_rows("book_pressure", minutes * 60), store, prefix="engine-NIFTY-1s")
    write_rows(contract_rows("spread_gate", minutes * 60, fut=False), store, prefix="engine-NIFTY-1s")
    write_rows(single_rows("skew", minutes), store, prefix="engine-NIFTY-1m")
    write_rows(single_rows("pcr_full", minutes, bucket="10s", col="pcr_oi"), store, prefix="engine-NIFTY-1s")
    ts = T0 + pd.Timedelta(minutes=3)
    write_rows([{"signal": "block_prints", "bucket": "1s", "underlying": "NIFTY", "ts": ts, "known_at": ts, "symbol": "NIFTY_24100_CE_2026-11-17",
                 "instrument_type": "CE", "ltp": 120.0, "lots": 150.0, "sign": 1.0, "oi_jump": 5000.0}], store, prefix="engine-NIFTY-1s")


def nightly(store, sid, day, col="skew1_s", minutes=30, bucket="1m"):
    from webapp.signals_view import _spec
    spec = _spec(sid)
    folder = store / "signals" / sid / f"v{spec.version}" / "intraday" / f"bucket={bucket}" / "underlying=NIFTY" / f"date={day}"
    folder.mkdir(parents=True, exist_ok=True)
    d = pd.Timestamp(day)
    pd.DataFrame({"known_at": [d + pd.Timedelta(hours=4, minutes=i + 1) for i in range(minutes)], col: [0.01 + 0.001 * i for i in range(minutes)],
                  "other": range(minutes)}).to_parquet(folder / "part.parquet", index=False)


@pytest.fixture()
def store(tmp_path):
    signals_store.forget()
    fill(tmp_path)
    yield tmp_path
    signals_store.forget()


NOW = datetime(2026, 11, 13, 4, 10, 5)


def test_summary_groups_signals_by_family_and_reads_every_kind(store):
    s = signals_view.summary(store, "NIFTY", DAY.date(), NOW)
    by = {(x["signal"], x["bucket"]): x for x in s["signals"]}
    assert set(by) == {("book_pressure", "1s"), ("spread_gate", "1s"), ("skew", "1min"), ("pcr_full", "10s"), ("block_prints", "1s")}
    assert by[("skew", "1min")]["family"] == "surface" and by[("skew", "1min")]["family_label"] == "Volatility surface" and by[("skew", "1min")]["doc"]
    assert by[("book_pressure", "1s")]["family"] == "live" and by[("block_prints", "1s")]["family"] == "flow"
    assert by[("skew", "1min")]["kind"] == "single" and by[("book_pressure", "1s")]["kind"] == "contract" and by[("block_prints", "1s")]["kind"] == "events"
    assert by[("skew", "1min")]["headline"] == "skew1_s" and abs(by[("skew", "1min")]["z"]["skew1_s"] - 0.45) < 1e-9
    assert by[("book_pressure", "1s")]["front_future"] is True and by[("book_pressure", "1s")]["contracts"] == 12 and len(by[("book_pressure", "1s")]["series"]) > 100
    sg = by[("spread_gate", "1s")]                                   # options only: the cross-section median of each snapshot
    assert sg["kind"] == "contract" and sg["front_future"] is False and sg["headline"] == "qi" and len(sg["series"]) >= 10
    assert by[("block_prints", "1s")]["events"] == 1
    assert [x["family"] for x in s["signals"]] == sorted((x["family"] for x in s["signals"]), key=lambda f: ["live", "flow", "surface", "vol_gate", "positioning"].index(f))
    assert s["days"] == ["2026-11-13"] and s["load"]["loaded"] is True and s["cadence"] == ["1s", "10s", "1min"]


def test_series_gives_every_column_with_z_and_the_previous_session(store):
    nightly(store, "skew", "2026-11-12")
    s = signals_view.series(store, "NIFTY", "skew", "1min", DAY.date())
    assert s["kind"] == "single" and s["headline"] == "skew1_s" and set(s["columns"]) == {"skew1_s", "other"} and len(s["t"]) == 10
    assert abs(s["series"]["skew1_s"][-1] - 0.029) < 1e-12 and abs(s["z"]["skew1_s"][-1] - 0.45) < 1e-9
    p = s["prev"]
    assert p["date"] == "2026-11-12" and len(p["t"]) == 30 and p["t"][0].startswith("2026-11-13T04:01")        # shifted onto the day
    assert signals_view.series(store, "NIFTY", "skew", "1min", DAY.date(), max_points=4)["t"].__len__() == 4
    sub = signals_view.series(store, "NIFTY", "skew", "1min", DAY.date(), columns=["other"])
    assert list(sub["series"]) == ["other"]
    fut = signals_view.series(store, "NIFTY", "book_pressure", "1s", DAY.date())               # per contract: the future's series
    assert fut["kind"] == "contract" and len(fut["t"]) == 600 and fut["prev"] is None
    ev = signals_view.series(store, "NIFTY", "block_prints", "1s", DAY.date())
    assert ev["kind"] == "events" and ev["events"][0]["lots"] == 150.0
    assert signals_view.series(store, "NIFTY", "nope", "1s", DAY.date())["t"] == []


def test_contracts_by_strike_and_as_strike_by_time(store):
    c = signals_view.contracts(store, "NIFTY", "book_pressure", "1s", DAY.date())
    assert c["expiries"] == EXPS and c["expiry"] == EXPS[0] and c["column"] == "qi" and {"qi", "micro_rel"} <= set(c["columns"])
    rows = c["snapshot"]["rows"]
    assert len(rows) == 6 and [r["strike"] for r in rows][:2] == [24000.0, 24000.0] and {r["cp"] for r in rows} == {"CE", "PE"}
    h = c["heat"]
    assert h["strikes"] == STRIKES and len(h["times"]) == 10 and len(h["call"]) == 3 and len(h["call"][0]) == 10 and len(h["put"]) == 3
    assert abs(h["call"][0][0] - np.sin(59 / 30 + 24.0)) < 1e-5                                   # the snapshot at the first full minute
    c2 = signals_view.contracts(store, "NIFTY", "book_pressure", "1s", DAY.date(), expiry=EXPS[1], column="micro_rel")
    assert c2["expiry"] == EXPS[1] and c2["column"] == "micro_rel"
    assert signals_view.contracts(store, "NIFTY", "skew", "1min", DAY.date())["snapshot"] is None


def test_chain_panels_and_history(store):
    ch = signals_view.chain(store, "NIFTY", DAY.date())
    titles = {p["title"]: {c["signal"]: c for c in p["charts"]} for p in ch["panels"]}
    assert "skew" in titles["Skew and shape"] and "pcr_full" in titles["Positioning"]
    assert titles["Skew and shape"]["skew"]["column"] == "skew1_s" and len(titles["Skew and shape"]["skew"]["t"]) == 10
    for i, d in enumerate(("2026-11-10", "2026-11-11", "2026-11-12")):
        nightly(store, "skew", d, minutes=20 + i)
    h = signals_view.history(store, "NIFTY", "skew", "1min")
    assert h["available"] and h["headline"] == "skew1_s" and [d["date"] for d in h["days"]] == ["2026-11-10", "2026-11-11", "2026-11-12"]
    day = h["days"][0]["columns"]["skew1_s"]
    assert abs(day["open"] - 0.01) < 1e-12 and abs(day["close"] - 0.029) < 1e-12 and day["low"] <= day["mean"] <= day["high"]
    assert len(h["paths"]) == 3 and h["paths"][0]["minutes"][0] == 241.0
    assert signals_view.history(store, "NIFTY", "vrp", "1min")["available"] is False


def test_new_files_are_read_incrementally_and_duplicates_dropped(store):
    s1 = signals_view.summary(store, "NIFTY", DAY.date(), NOW)
    ld = signals_store.get_day(store, DAY.date())
    seen = len(ld.seen)
    write_rows(single_rows("skew", 5, start=10), store, prefix="engine-NIFTY-1m")           # five newer minutes
    write_rows(single_rows("skew", 2, start=8), store, prefix="engine-NIFTY-1m")            # two minutes already there (a restarted engine)
    signals_store.get_day(store, DAY.date()).last_refresh = 0                                # do not wait for the refresh interval
    s2 = signals_view.summary(store, "NIFTY", DAY.date(), NOW)
    k1 = next(x for x in s1["signals"] if x["signal"] == "skew")
    k2 = next(x for x in s2["signals"] if x["signal"] == "skew")
    assert k1["n"] == 10 and k2["n"] == 15 and k2["known_at"] > k1["known_at"]
    assert len(signals_store.get_day(store, DAY.date()).seen) == seen + 2


def test_api_endpoints_validate_and_never_write(store, monkeypatch, tmp_path):
    monkeypatch.setenv("JARVIS_SIGNAL_STORE", str(store))
    before = sorted(str(p) for p in store.rglob("*"))
    c = TestClient(create_app(tmp_path / "lake"))
    p = {"underlying": "NIFTY", "date": "2026-11-13"}
    live = c.get("/api/signals/live", params=p).json()
    assert {x["signal"] for x in live["signals"]} >= {"skew", "book_pressure"} and live["days"] == ["2026-11-13"]
    assert c.get("/api/signals/series", params={**p, "signal": "skew", "bucket": "1min"}).json()["headline"] == "skew1_s"
    assert c.get("/api/signals/contracts", params={**p, "signal": "book_pressure", "bucket": "1s", "expiry": EXPS[1]}).json()["expiry"] == EXPS[1]
    assert c.get("/api/signals/chain", params=p).json()["panels"]
    assert c.get("/api/signals/history", params={"underlying": "NIFTY", "signal": "skew"}).json()["signal"] == "skew"
    for path, bad in (("series", {"signal": "skew;x", "bucket": "1min"}), ("series", {"signal": "skew", "bucket": "1h"}), ("contracts", {"signal": "skew", "bucket": "1s", "expiry": "x"}),
                      ("contracts", {"signal": "skew", "bucket": "1s", "column": "a b"}), ("chain", {"date": "nope"}), ("history", {"signal": "A"})):
        assert c.get("/api/signals/" + path, params={"underlying": "NIFTY", **bad}).status_code == 400, (path, bad)
    assert sorted(str(p) for p in store.rglob("*")) == before


def test_a_big_day_loads_in_bounded_time_and_memory(tmp_path):
    """The first version read every file of the day into one frame (about 500 MB of files by 06:15 UTC on 18 Nov; the viewer is capped at 1.5 GB).
    300 files of 3,000 per-contract rows a second apart (about 900,000 rows) are read once and reduced."""
    signals_store.forget()
    syms = [(f"NIFTY_{24000 + 50 * j}_{cp}_2026-11-17", 24000.0 + 50 * j, cp) for j in range(30) for cp in ("CE", "PE")]
    base = T0
    for f in range(300):
        rows = []
        for s in range(50):
            ts = base + pd.Timedelta(seconds=f * 50 + s)
            for sym, k, cp in syms:
                rows.append((ts, ts + pd.Timedelta(seconds=1), sym, "2026-11-17", k, cp, 0.1, 0.2))
        df = pd.DataFrame(rows, columns=["ts", "known_at", "symbol", "expiry", "strike", "cp", "qi", "micro_rel"])
        df.insert(0, "signal", "book_pressure"); df.insert(1, "bucket", "1s"); df.insert(2, "underlying", "NIFTY")
        write_rows(df, tmp_path, prefix=f"engine-NIFTY-1s-{f:04d}")
    rss0 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    t0 = time.perf_counter()
    s = signals_view.summary(tmp_path, "NIFTY", DAY.date(), NOW, wait_s=120)
    took = time.perf_counter() - t0
    grown_mb = (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - rss0) / 1024
    assert s["load"]["loaded"] and s["signals"][0]["contracts"] == 60
    assert took < 60 and grown_mb < 900, (took, grown_mb)
    t0 = time.perf_counter()
    signals_view.summary(tmp_path, "NIFTY", DAY.date(), NOW)                                  # the second call reads no file again
    assert time.perf_counter() - t0 < 2
    signals_store.forget()


def test_iv_smile_from_the_option_ticks(tmp_path):
    from tests.unit.test_signal_ops import DAY as TICK_DAY
    write_fake_lake(tmp_path)
    s = signals_view.smile(tmp_path, "NIFTY", TICK_DAY, minutes=24 * 60)
    assert s["expiries"] and s["expiry"] in s["expiries"]
    lat = s["latest"]
    assert len(lat["strikes"]) == len(lat["call"]) == len(lat["put"]) > 3 and 0.02 < np.nanmean([v for v in lat["call"] if v is not None]) < 1.0
    ot = s["over_time"]
    assert len(ot["iv"]) == len(ot["strikes"]) and len(ot["iv"][0]) == len(ot["times"]) and s["atm"]
    assert signals_view.smile(tmp_path, "BANKNIFTY", TICK_DAY)["latest"] is None


def test_the_store_is_found_from_the_checkout_when_the_viewer_has_no_environment(tmp_path, monkeypatch):
    """18 Nov 2026: the viewer service reads only .env, so JARVIS_SIGNAL_STORE (set in the crontab) was missing and it read a folder that does not
    exist.  It now uses <working directory>/store when that holds live rows, and says which store it reads."""
    signals_store.forget()
    monkeypatch.delenv("JARVIS_SIGNAL_STORE", raising=False)
    fill(tmp_path / "store")
    monkeypatch.chdir(tmp_path)
    c = TestClient(create_app(tmp_path / "lake"))
    j = c.get("/api/signals/live", params={"underlying": "NIFTY", "date": "2026-11-13"}).json()
    assert j["store"] == {"path": str(tmp_path / "store"), "exists": True, "from_env": False} and j["signals"]
    monkeypatch.chdir(tmp_path / "store")                                                   # no store/ here: the research default, reported as missing
    j2 = c.get("/api/signals/live", params={"underlying": "NIFTY", "date": "2026-11-13"}).json()
    assert j2["signals"] == [] and j2["store"]["exists"] in (True, False) and j2["store"]["from_env"] is False
    signals_store.forget()


def test_thinning_inside_duckdb_keeps_exactly_what_the_tab_shows(tmp_path, monkeypatch):
    """The first read of a day drops the per-contract option rows that are not at a full minute before they reach pandas (nine in ten); what the
    tab shows must not change: the same summary, strike snapshot and heatmap as when every row is read."""
    signals_store.forget()
    for k in range(6):                                                    # six files of 100 s each, so the thinned part and the whole-read tail both occur
        write_rows(contract_rows("book_pressure", 100, start=100 * k), tmp_path, prefix=f"engine-NIFTY-1s-{k}")
        write_rows(single_rows("skew", 2, start=2 * k), tmp_path, prefix=f"engine-NIFTY-1m-{k}")
    out = {}
    for name, tail in (("thin", 1), ("whole", 10_000)):
        signals_store.forget()
        monkeypatch.setattr(signals_store, "TAIL_FILES", tail)
        s = signals_view.summary(tmp_path, "NIFTY", DAY.date(), NOW)
        c = signals_view.contracts(tmp_path, "NIFTY", "book_pressure", "1s", DAY.date())
        sr = signals_view.series(tmp_path, "NIFTY", "book_pressure", "1s", DAY.date())
        out[name] = (s["signals"], c["snapshot"], c["heat"], sr["t"], sr["series"])
    assert out["thin"] == out["whole"]
    assert len(out["thin"][2]["times"]) == 10 and len(out["thin"][3]) == 600
    signals_store.forget()


def test_every_api_call_is_timed_and_the_perf_route_and_sweep_report_it(store, monkeypatch, tmp_path, capsys):
    """SCRUM-206: "the viewer takes forever" needed numbers: /api/perf lists calls, mean and slowest per route (and the slowest query), a request over
    a second is logged, every response carries Server-Timing, and python -m webapp.perf times each tab's requests twice."""
    monkeypatch.setenv("JARVIS_SIGNAL_STORE", str(store))
    c = TestClient(create_app(tmp_path / "lake"))
    r = c.get("/api/signals/live", params={"underlying": "NIFTY", "date": "2026-11-13"})
    assert r.headers["Server-Timing"].startswith("app;dur=")
    c.get("/api/signals/chain", params={"underlying": "NIFTY", "date": "2026-11-13"})
    routes = {x["route"]: x for x in c.get("/api/perf").json()["routes"]}
    assert routes["/api/signals/live"]["calls"] == 1 and routes["/api/signals/live"]["max_s"] >= 0 and "underlying=NIFTY" in routes["/api/signals/live"]["slowest_query"]
    assert "/api/perf" not in routes
    from webapp import perf
    import threading, socket, time, uvicorn
    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    write_fake_lake(tmp_path / "lake")
    server = uvicorn.Server(uvicorn.Config(create_app(tmp_path / "lake"), host="127.0.0.1", port=port, log_level="warning"))
    th = threading.Thread(target=server.run, daemon=True); th.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.1)
    try:
        assert perf.main(["--base", f"http://127.0.0.1:{port}", "--report", str(tmp_path / "perf.txt")]) == 0
    finally:
        server.should_exit = True; th.join(10)
    text = (tmp_path / "perf.txt").read_text()
    assert "first" in text and "/api/signals/live" in text and "the viewer's own timings" in text


def test_the_loader_stops_at_a_memory_guard_instead_of_being_killed(store, monkeypatch):
    """18 Nov 2026: the viewer (capped at 1500 MB) was OOM-killed seven times in 25 minutes by the first read of a day.  Above the guard the loader stops,
    keeps what it has, and says so; the page still answers."""
    signals_store.forget()
    monkeypatch.setattr(signals_store, "rss_mb", lambda: 5000.0)
    s = signals_view.summary(store, "NIFTY", DAY.date(), NOW, wait_s=30)
    assert s["load"]["loaded"] is True and "memory guard" in s["load"]["error"] and s["signals"] == []
    monkeypatch.setattr(signals_store, "rss_mb", lambda: 100.0)
    signals_store.get_day(store, DAY.date()).last_refresh = 0
    s2 = signals_view.summary(store, "NIFTY", DAY.date(), NOW)
    assert s2["load"]["error"] is None and len(s2["signals"]) == 5                          # the next refresh reads the rest
    signals_store.forget()


def test_the_viewers_duckdb_limits_fit_inside_its_service_cap(tmp_path, monkeypatch):
    """18 Nov 2026: each query connection was allowed 2 GB while the service is capped at 1.5 GB, so one heavy day query got the viewer OOM-killed (seven
    times in 25 minutes).  Now 0.6 GB a connection, two queries at once, two threads; JARVIS_VIEWER_MEMORY_GB still overrides."""
    from data_layer import query
    monkeypatch.delenv("JARVIS_VIEWER_MEMORY_GB", raising=False)
    assert query.VIEWER_MEMORY_GB * query.VIEWER_QUERIES < 1.5 and query._memory_limit() == "0.6GB"
    q = query.Lake(tmp_path)
    assert q.con.execute("SELECT current_setting('threads')").fetchone()[0] == 2
    assert q.con.execute("SELECT current_setting('memory_limit')").fetchone()[0].startswith("572.")          # 0.6 GB as DuckDB prints it
    monkeypatch.setenv("JARVIS_VIEWER_MEMORY_GB", "1.5")
    assert query._memory_limit() == "1.5GB"


def test_lake_opens_without_reading_every_tick_file_and_creates_the_bar_views_when_a_query_names_them(tmp_path):
    """18 Nov 2026: every viewer request opened a Lake, which created the bars_* views over the footers of every parquet file of every day (about 1 s and
    200 MB held per connection for 12,000 tick files); a page load made several at once and the viewer was OOM-killed.  The views now appear when a
    query names them; the results are the same."""
    from data_layer import query
    write_fake_lake(tmp_path)
    q = query.Lake(tmp_path)
    assert q._pending and "bars_1s" in q._pending                                   # nothing bound yet
    assert q.bar_days("1s") and q.candle_days("1s") is not None                       # listings need no view
    assert q._pending and "bars_1s" in q._pending
    n = q.sql("SELECT count(*) AS n FROM bars_1s")["n"][0]                            # naming the view creates it
    assert n > 0 and "bars_1s" not in q._pending
    assert "bars_1s" in q.tables()
    eager = query.Lake(tmp_path)
    eager._views_for("SHOW TABLES")
    assert not eager._pending and set(eager.tables()) == set(q.tables())


def test_a_busy_viewer_answers_503_quickly_and_the_direct_timing_mode_reports_each_call(tmp_path, monkeypatch, capsys):
    """18 Nov 2026: two slow queries held both query slots, every other request waited behind them and the proxy answered 504.  A request that cannot get a
    slot in QUERY_WAIT_S now gets a 503.  perf --direct times each heavy lake call and the Signals load in a process of its own (seconds, peak memory)."""
    from data_layer import query
    monkeypatch.setattr(query, "QUERY_WAIT_S", 0.2)
    write_fake_lake(tmp_path / "lake")
    held = [query._QUERY_SLOTS.acquire() for _ in range(query.VIEWER_QUERIES)]
    try:
        c = TestClient(create_app(tmp_path / "lake"))
        r = c.get("/api/symbols", params={"date": "2026-03-31"})
        assert r.status_code == 503 and "busy" in r.json()["detail"] and r.headers["Retry-After"]
    finally:
        for _ in held:
            query._QUERY_SLOTS.release()
    assert TestClient(create_app(tmp_path / "lake")).get("/api/perf").json()["rss_mb_now"] > 0
    monkeypatch.setenv("JARVIS_LAKE", str(tmp_path / "lake")); monkeypatch.setenv("JARVIS_SIGNAL_STORE", str(tmp_path / "store"))
    from webapp import perf
    assert perf.main(["--direct", "--limit", "120", "--report", str(tmp_path / "direct.txt")]) == 0
    text = (tmp_path / "direct.txt").read_text()
    assert "direct timings" in text and "F&O chain" in text and "Signals tab: the first read" in text and "peak MB" in text


def test_the_market_overview_in_batches_gives_the_same_rows_and_the_direct_report_survives_noise(tmp_path, monkeypatch):
    """18 Nov 2026: the overview built the windows over every equity's second bars at once (25-110 s, 2-3 GB of RSS).  It now runs OVERVIEW_BATCH symbols at a
    time; a row depends on one symbol only, so the result is the same whatever the batch size."""
    from data_layer import query
    write_fake_lake(tmp_path)
    from tests.unit.test_signal_ops import DAY as TICK_DAY
    day = str(TICK_DAY)
    monkeypatch.setattr(query.Lake, "OVERVIEW_BATCH", 10_000)
    whole = query.Lake(tmp_path).day_overview(day)
    monkeypatch.setattr(query.Lake, "OVERVIEW_BATCH", 3)
    batched = query.Lake(tmp_path).day_overview(day)
    assert len(whole) > 3
    pd.testing.assert_frame_equal(batched.reset_index(drop=True), whole.reset_index(drop=True))
    # a heavy call that prints a progress bar before the marker, and a result line glued to it, must still be read
    monkeypatch.setattr("webapp.perf.DIRECT_CALLS", [("noisy", "import sys\nsys.stdout.write('50% |==|\\r100% |====| (00:00:01)')\nrows = 7")])
    monkeypatch.setenv("JARVIS_LAKE", str(tmp_path)); monkeypatch.setenv("JARVIS_SIGNAL_STORE", str(tmp_path / "store"))
    from webapp import perf
    assert perf.main(["--direct", "--date", day, "--limit", "60", "--report", str(tmp_path / "d.txt")]) == 0
    text = (tmp_path / "d.txt").read_text()
    assert "noisy" in text and "7" in text and "failed" not in text


def test_a_compacted_day_in_one_wide_file_loads_in_chunks_and_gives_the_same_tab(tmp_path, monkeypatch):
    """18 Nov 2026 (VPS): the day's live rows had been compacted into ONE file, 1.1 GB, 60.5 M rows, 136 columns (every signal's columns side by side).  Even
    thinned, the result as one pandas frame was about 2.7 GB and every read was OOM-killed.  The rows now stream in chunks and each signal keeps only its
    own columns; the tab must be the same as from the engines' small files, and a big file is always thinned."""
    import duckdb
    from webapp import signals_store as ss
    small, wide = tmp_path / "small", tmp_path / "wide"
    fill(small)
    folder = wide / "live" / f"date={DAY.date()}"
    folder.mkdir(parents=True)
    extra = ", ".join(f"CAST(NULL AS DOUBLE) AS pad{i}" for i in range(100))
    duckdb.connect().execute(f"COPY (SELECT *, {extra} FROM read_parquet('{small}/live/date={DAY.date()}/*.parquet', union_by_name=true)) TO '{folder}/live-compacted.parquet' (FORMAT PARQUET)")
    monkeypatch.setattr(ss, "BATCH_ROWS", 4096)                                  # several chunks, so the streaming is exercised
    monkeypatch.setattr(ss, "TAIL_MAX_BYTES", 1)                                 # the one file counts as big: thinned
    out = {}
    for name, store_ in (("small", small), ("wide", wide)):
        ss.forget()
        s = signals_view.summary(store_, "NIFTY", DAY.date(), NOW)
        c = signals_view.contracts(store_, "NIFTY", "book_pressure", "1s", DAY.date())
        sr = signals_view.series(store_, "NIFTY", "skew", "1min", DAY.date())
        out[name] = ([{k: v for k, v in x.items() if k not in ("n",)} for x in s["signals"]], c["heat"], sr["series"], sr["z"])
    assert out["small"][1:] == out["wide"][1:]
    a, b = out["small"][0], out["wide"][0]
    assert [x["signal"] for x in a] == [x["signal"] for x in b] and all("pad0" not in x["columns"] for x in b)      # a signal keeps its own columns only
    assert [x["values"] for x in a if x["kind"] != "contract"] == [x["values"] for x in b if x["kind"] != "contract"]
    ss.forget()


def test_a_finished_day_is_read_once_and_then_from_its_cache(tmp_path, monkeypatch):
    """18 Nov 2026: the first read of a compacted day took 54 s (60 M rows).  After one read the reduced day is saved next to the store; the next read
    (the viewer restarted, or another process) takes it from there, gives the same tab, and a changed source file makes it read the rows again."""
    import duckdb
    from webapp import signals_store as ss
    small, wide = tmp_path / "small", tmp_path / "wide"
    fill(small)
    folder = wide / "live" / f"date={DAY.date()}"
    folder.mkdir(parents=True)
    duckdb.connect().execute(f"COPY (SELECT * FROM read_parquet('{small}/live/date={DAY.date()}/*.parquet', union_by_name=true)) TO '{folder}/live-compacted.parquet' (FORMAT PARQUET)")
    monkeypatch.setattr(ss, "TAIL_MAX_BYTES", 1)
    ss.forget()
    first = signals_view.contracts(wide, "NIFTY", "book_pressure", "1s", DAY.date())
    cache = wide / "viewer-cache" / f"signals-date={DAY.date()}.pkl"
    assert cache.exists()
    ss.forget()
    monkeypatch.setattr(ss.LiveDay, "_batches", staticmethod(lambda *a, **k: (_ for _ in ()).throw(AssertionError("the rows were read again"))))
    second = signals_view.contracts(wide, "NIFTY", "book_pressure", "1s", DAY.date())
    assert second["heat"] == first["heat"] and second["snapshot"] == first["snapshot"] and second["load"]["loaded"]
    # a source file that changed is not served from the old cache
    (folder / "live-compacted.parquet").touch()
    ss.forget()
    assert "read again" in (signals_view.contracts(wide, "NIFTY", "book_pressure", "1s", DAY.date())["load"]["error"] or "")        # it tried to read the rows
    # a running day (the engines' small files) is never cached
    ss.forget()
    fill(tmp_path / "live_day")
    signals_view.summary(tmp_path / "live_day", "NIFTY", DAY.date(), NOW)
    assert not (tmp_path / "live_day" / "viewer-cache").exists()
    ss.forget()


def test_by_strike_shows_the_newest_row_of_each_contract_when_the_last_second_has_only_a_few(tmp_path):
    """18 Nov 2026 (live): at 1 s only the contracts that traded in the last second have a row, so a snapshot of that instant had none of the nearest
    expiry and the tab said 'No snapshot' (and the strike x time charts were hidden behind that message)."""
    signals_store.forget()
    write_rows(contract_rows("book_pressure", 600), tmp_path, prefix="engine-NIFTY-1s")
    ts = T0 + pd.Timedelta(seconds=600)
    write_rows([{"signal": "book_pressure", "bucket": "1s", "underlying": "NIFTY", "ts": ts, "known_at": ts + pd.Timedelta(seconds=1), "symbol": "NIFTY_24100_CE_2026-11-24",
                 "expiry": "2026-11-24", "strike": 24100.0, "cp": "CE", "qi": 0.3, "micro_rel": 0.0}] * 2, tmp_path, prefix="engine-NIFTY-1s")
    c = signals_view.contracts(tmp_path, "NIFTY", "book_pressure", "1s", DAY.date(), expiry=EXPS[0])
    assert len(c["snapshot"]["rows"]) == 6 and all(r["expiry"] == EXPS[0] for r in c["snapshot"]["rows"])
    assert c["heat"] is not None and len(c["heat"]["strikes"]) == 3
    signals_store.forget()
