"""Fixtures shared by unit tests and BDD scenarios.  Nothing here touches the network or the real repo dirs."""
from __future__ import annotations

import importlib
import time
from types import SimpleNamespace

import pytest

from tests.helpers import UNI_HEADER, universe_rows, write_csv


@pytest.fixture()
def env(tmp_path, monkeypatch):
    """Legacy fixture used by the first tests: fresh lake module rooted at tmp_path."""
    monkeypatch.setenv("JARVIS_ROOT", str(tmp_path))
    import data_layer.lake as lake
    importlib.reload(lake)
    (tmp_path / "data/fundamentals").mkdir(parents=True)
    (tmp_path / "data/ohlc").mkdir(parents=True)
    return tmp_path, lake


@pytest.fixture()
def world(tmp_path, monkeypatch):
    """
    Redirect every module-level path (lake, query, tick_store, fmp_client, yf_client) into tmp_path
    and return handles.  Uses monkeypatch.setattr, so no module reloads and no leakage between tests.
    """
    import data_layer.fmp_client as fmp
    import data_layer.lake as lake
    import data_layer.query as query
    import data_layer.tick_store as tick_store
    import data_layer.yf_client as yf

    data, lakedir = tmp_path / "data", tmp_path / "lake"
    for d in (data / "fundamentals", data / "ohlc", data / "bars", tmp_path / "logs"):
        d.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("JARVIS_ROOT", str(tmp_path))
    for mod, attrs in (
        (lake, {"ROOT": tmp_path, "DATA_DIR": data, "LAKE_DIR": lakedir}),
        (query, {"LAKE_DIR": lakedir}),
        (tick_store, {"LAKE_DIR": lakedir}),
        (fmp, {"LOG_DIR": tmp_path / "logs", "DATA_DIR": data, "FUND_DIR": data / "fundamentals",
               "UNIVERSE_CSV": data / "universe.csv", "FUND_META_CSV": data / "fund_meta.csv"}),
        (yf, {"DATA_DIR": data, "OHLC_DIR": data / "ohlc", "BARS_DIR": data / "bars"}),
    ):
        for k, v in attrs.items():
            monkeypatch.setattr(mod, k, v)

    def set_universe(spec: dict[str, list[str]]):
        write_csv(data / "universe.csv", UNI_HEADER, universe_rows(spec))

    return SimpleNamespace(root=tmp_path, data=data, lakedir=lakedir, lake=lake, query=query,
                           tick_store=tick_store, fmp=fmp, yf=yf, set_universe=set_universe)


@pytest.fixture()
def no_sleep(monkeypatch):
    """Make time.sleep instant but record the requested delays."""
    calls: list[float] = []
    monkeypatch.setattr(time, "sleep", lambda s: calls.append(s))
    return calls


@pytest.fixture()
def sched(world, monkeypatch):
    """scheduler imported fresh with its ROOT/LOCK/log paths inside tmp_path."""
    import sys
    monkeypatch.setenv("JARVIS_ROOT", str(world.root))
    sys.modules.pop("scheduler", None)
    s = importlib.import_module("scheduler")
    monkeypatch.setattr(s, "LOCK_FILE", world.root / "logs" / "scheduler.lock")
    yield s
    sys.modules.pop("scheduler", None)


@pytest.fixture(scope="session", autouse=True)
def _tests_never_touch_the_repo_data_dirs():
    """Fail the run if any test wrote a lake/ folder or CSVs into the real repository."""
    from pathlib import Path
    repo = Path(__file__).resolve().parent.parent
    watched = [repo / "lake", repo / "db", repo / "data" / "bars"]
    before = {p: p.exists() for p in watched}
    csvs = set((repo / "data" / "ohlc").glob("*.csv")) | set((repo / "data" / "fundamentals").glob("*.csv"))
    yield
    for p in watched:
        assert p.exists() == before[p], f"a test created or removed {p} in the real repo"
    after = set((repo / "data" / "ohlc").glob("*.csv")) | set((repo / "data" / "fundamentals").glob("*.csv"))
    assert after == csvs, f"a test wrote CSVs into the real repo: {sorted(after ^ csvs)}"


@pytest.fixture(scope="session")
def demo_lake(tmp_path_factory):
    """A synthetic 3-day, 10-stock 1-second lake (see webapp/demo.py), built once per test session."""
    from webapp.demo import write_demo
    out = tmp_path_factory.mktemp("demo_lake")
    write_demo(out, days=3)
    return out


@pytest.fixture(scope="session")
def demo_lake_fo(tmp_path_factory):
    """The synthetic lake plus futures and option chains for NIFTY and BANKNIFTY (for the real-browser test)."""
    from webapp.demo import write_demo
    out = tmp_path_factory.mktemp("demo_lake_fo")
    write_demo(out, days=3, derivatives=True)
    return out


@pytest.fixture(autouse=True)
def _isolated_env_file(tmp_path, monkeypatch):
    """No test may read or write a developer's real .env: point every ENV_FILE at a path that does not exist."""
    import data_layer.lake as lake
    import data_layer.upstox.auth as auth
    import data_layer.upstox.feed as feed
    nowhere = tmp_path / "no-such.env"
    for mod, attr in ((lake, "ENV_FILE"), (feed, "ENV_FILE"), (auth, "ENV_PATH")):
        monkeypatch.setattr(mod, attr, nowhere)
