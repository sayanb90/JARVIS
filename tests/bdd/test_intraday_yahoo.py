import pandas as pd
import pytest
from pytest_bdd import given, parsers, scenarios, then, when

scenarios("../features/intraday_yahoo.feature")
pytestmark = pytest.mark.bdd


def session_frame(day, minutes_total=75, forming=False):
    start = pd.Timestamp(f"{day} 09:15", tz="Asia/Kolkata")
    idx = pd.DatetimeIndex([start + pd.Timedelta(minutes=5 * i) for i in range(minutes_total)])
    c = pd.Series(range(len(idx)), index=idx, dtype=float) + 100
    return pd.DataFrame({"Open": c, "High": c + 1, "Low": c - 1, "Close": c, "Volume": 1000}, index=idx)


@given(parsers.parse("Yahoo serves 5-minute bars for the last {n:d} sessions with the latest bar still forming"))
def _bars(ctx, monkeypatch, n):
    now = pd.Timestamp.now(tz="UTC")
    days = [(now - pd.Timedelta(days=k)).strftime("%Y-%m-%d") for k in range(n, 0, -1)]
    ctx.forming_ts = now.floor("5min")                         # a bar that started this slot has not closed yet

    def fake(tickers, **kw):
        out = {}
        for t in tickers:
            full = pd.concat([session_frame(d) for d in days])
            forming = pd.DataFrame({"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": 1},
                                   index=pd.DatetimeIndex([ctx.forming_ts]))
            out[t] = pd.concat([full, forming])
        return out
    monkeypatch.setattr(ctx.w.yf, "_batch_download", fake)


@when("I collect the Yahoo intraday bars for India with the deepest window and load the lake")
def _collect(ctx):
    ctx.w.yf.run_bars("IN", interval="5m", backfill=True)
    ctx.w.lake.load_bars("5m", since=pd.Timestamp("2000-01-01").date(), markets=("IN",))


@when(parsers.parse("I try to collect Yahoo intraday bars for {market}"))
def _try(ctx, market):
    try:
        ctx.w.yf.run_bars(market, tickers=["X"], interval="5m")
        ctx.refused = False
    except ValueError as exc:
        ctx.refused = "India" in str(exc) or "intraday" in str(exc)


@when("I plan a deepest-window collection for each bar size")
def _plan(ctx):
    ctx.windows = {iv: ctx.w.yf.INTRADAY_MAX_DAYS[iv] - 1 for iv in ("5m", "1m", "60m")}


@then("the first stored bar of each day starts at 03:45 UTC")
def _first(ctx):
    df = ctx.w.query.Lake().sql("SELECT CAST(ts AS DATE) d, min(ts) first FROM bars_5m GROUP BY 1")
    assert len(df) >= 2 and all(t.strftime("%H:%M") == "03:45" for t in df["first"] if t.date() != ctx.forming_ts.date())


@then("the bar that was still forming is not stored")
def _not_forming(ctx):
    df = ctx.w.query.Lake().sql("SELECT ts FROM bars_5m")
    assert pd.Timestamp(ctx.forming_ts).tz_localize(None) not in set(pd.to_datetime(df.ts))


@then("each symbol has no duplicate bar timestamps")
def _dups(ctx):
    df = ctx.w.query.Lake().sql("SELECT symbol, ts, count(*) n FROM bars_5m GROUP BY 1, 2 HAVING count(*) > 1")
    assert df.empty


@then("the request is refused because intraday is India only")
def _refused(ctx):
    assert ctx.refused is True


@then("the windows are 5m 59 days, 1m 6 days and 60m 729 days")
def _windows(ctx):
    assert ctx.windows == {"5m": 59, "1m": 6, "60m": 729}
