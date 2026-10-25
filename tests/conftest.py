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
