"""Step definitions shared by every feature file."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from pytest_bdd import given, parsers, then, when

from tests.helpers import bday_frame

SUFFIX = {"IN": ".NS", "UK": ".L", "EU": ".PA", "US": ""}


def symbols_for(market: str, n: int) -> list[str]:
    return [f"{market}{i}{SUFFIX[market]}" for i in range(n)]


@pytest.fixture()
def ctx(world):
    """Mutable scenario context."""
    return SimpleNamespace(w=world, universe={}, rowcount=None)


@given("an isolated JARVIS workspace")
def _workspace(ctx):
    return ctx


@given(parsers.parse("a universe of {n:d} stocks in each market of {markets}"))
def _universe(ctx, n, markets):
    ctx.universe = {m: symbols_for(m, n) for m in markets.split()}
    ctx.w.set_universe(ctx.universe)


class Yahoo:
    """Programmable stand-in for yfinance.download via yf_client._batch_download."""
    def __init__(self, periods=1300):
        self.periods, self.fail_for, self.unknown, self.corrections = periods, set(), set(), {}

    def __call__(self, tickers, **kw):
        if self.fail_for & set(tickers):
            raise RuntimeError("yahoo 429")
        frames = bday_frame([t for t in tickers if t not in self.unknown], self.periods)
        for t, close in self.corrections.items():
            if t in frames:
                frames[t].iloc[-1, frames[t].columns.get_loc("Close")] = close
        return frames


@pytest.fixture()
def yahoo(ctx, monkeypatch):
    y = Yahoo()
    monkeypatch.setattr(ctx.w.yf, "_batch_download", y)
    return y


@pytest.fixture()
def yahoo(ctx, monkeypatch):
    y = Yahoo()
    monkeypatch.setattr(ctx.w.yf, "_batch_download", y)
    return y


@given(parsers.parse("Yahoo serves {days:d} business days of prices for every ticker"))
def _serves(yahoo, days):
    yahoo.periods = days


@then(parsers.parse("the lake holds daily prices for markets {markets}"))
def _markets(ctx, markets):
    got = set(ctx.w.query.Lake().sql("SELECT DISTINCT market FROM ohlc").market)
    assert got == set(markets.split())
