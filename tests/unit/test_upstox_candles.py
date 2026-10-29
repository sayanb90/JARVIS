"""Upstox candle REST client: parsing, endpoint choice, retries, gap-filling pull."""
import json
from datetime import date, datetime, timedelta

import pytest

from data_layer.upstox import candles
from data_layer.upstox.candles import CandleClient, candle_ts, candles_to_bars, pull
from tests.helpers import ms

KEY = "NSE_EQ|INE002A01018"
DAY = date(2026, 10, 26)


def session_candles(day=DAY, minutes=375, px=100.0, vol=10):
    """Candles as the API presents them: IST ISO timestamps, newest first (the API returns descending)."""
    out = []
    for k in range(minutes):
        t = datetime(day.year, day.month, day.day, 9, 15) + timedelta(minutes=k)
        out.append([f"{t:%Y-%m-%dT%H:%M:%S}+05:30", px + k * 0.01, px + k * 0.01 + 0.5, px + k * 0.01 - 0.5,
                    px + k * 0.01, vol, 0])
    return out[::-1]


class Resp:
    def __init__(self, status=200, body=None, text=""):
        self.status_code, self._b, self.text = status, body, text

    def json(self):
        return self._b


class FakeHttp:
    def __init__(self, script=None, fn=None):
        self.script, self.fn, self.urls, self.headers = list(script or []), fn, [], []

    def get(self, url, headers, timeout):
        self.urls.append(url); self.headers.append(headers)
        return self.fn(url) if self.fn else self.script.pop(0)


def ok(cs):
    return Resp(200, {"status": "success", "data": {"candles": cs}})


# -- parsing ----------------------------------------------------------------------------------
@pytest.mark.parametrize("raw", ["2026-10-26T09:15:00+05:30", "2026-10-26T03:45:00+00:00", "2026-10-26 09:15:00",
                                 ms(3, 45, 0, (2026, 10, 26))])
def test_candle_ts_formats_all_land_on_the_same_utc_minute(raw):
    assert candle_ts(raw) == datetime(2026, 10, 26, 3, 45)


def test_candles_to_bars_keeps_session_only_and_skips_malformed():
    cs = session_candles(minutes=3) + [["2026-10-26T08:00:00+05:30", 1, 1, 1, 1, 1, 0],        # before open
                                      ["2026-10-26T15:30:00+05:30", 1, 1, 1, 1, 1, 0],        # at close: excluded
                                      ["2026-10-26T09:20:00+05:30", "x", 1, 1, 1, 1, 0], ["short"]]
    bars = candles_to_bars(cs, "A.NS")
    assert len(bars) == 3 and {b.src for b in bars} == {"rest"}
    assert min(b.ts for b in bars) == datetime(2026, 10, 26, 3, 45) and bars[0].vwap is None and bars[0].volume == 10


def test_blank_volume_becomes_zero():
    (b,) = candles_to_bars([["2026-10-26T09:15:00+05:30", 1, 2, 0.5, 1.5, None, 0]], "A.NS")
    assert b.volume == 0


# -- client -----------------------------------------------------------------------------------
def client(http, **kw):
    return CandleClient("TOK", session=http, delay=0, **kw)


def test_historical_url_orders_to_before_from_and_encodes_the_key():
    http = FakeHttp([ok(session_candles(minutes=2))])
    client(http).candles(KEY, date(2026, 10, 20), date(2026, 10, 26), 1, "historical")
    assert http.urls[0] == "https://api.upstox.com/v3/historical-candle/NSE_EQ%7CINE002A01018/minutes/1/2026-10-26/2026-10-20"
    assert http.headers[0]["Authorization"] == "Bearer TOK"


def test_auto_uses_intraday_for_today_and_falls_back_when_empty(monkeypatch):
    monkeypatch.setattr(candles, "ist_today", lambda: DAY)
    http = FakeHttp([ok([]), ok(session_candles(minutes=2))])
    assert len(client(http).candles(KEY, DAY, DAY)) == 2
    assert "/intraday/" in http.urls[0] and "/intraday/" not in http.urls[1] and http.urls[1].endswith("/2026-10-26/2026-10-26")


def test_auto_uses_historical_for_past_days(monkeypatch):
    monkeypatch.setattr(candles, "ist_today", lambda: date(2026, 10, 27))
    http = FakeHttp([ok(session_candles(minutes=2))])
    client(http).candles(KEY, DAY, DAY)
    assert len(http.urls) == 1 and "/intraday/" not in http.urls[0]


def test_intraday_only_source_never_falls_back(monkeypatch):
    http = FakeHttp([ok([])])
    assert client(http).candles(KEY, DAY, DAY, source="intraday") == []
    assert len(http.urls) == 1


@pytest.mark.parametrize("status", [401, 403])
def test_token_rejection_is_fatal(status):
    with pytest.raises(RuntimeError, match="expires daily"):
        client(FakeHttp([Resp(status)])).candles(KEY, DAY, DAY, source="historical")


def test_retries_429_and_5xx_with_backoff_then_succeeds(monkeypatch):
    slept = []
    monkeypatch.setattr(candles.time, "sleep", slept.append)
    http = FakeHttp([Resp(429), Resp(503), ok(session_candles(minutes=1))])
    assert len(client(http).candles(KEY, DAY, DAY, source="historical")) == 1
    assert 2.0 in slept and 4.0 in slept


def test_gives_up_after_retries_and_unknown_instrument_is_just_empty(monkeypatch):
    monkeypatch.setattr(candles.time, "sleep", lambda s: None)
    assert client(FakeHttp([Resp(500)] * 3)).candles(KEY, DAY, DAY, source="historical") == []
    assert client(FakeHttp([Resp(400, text="UDAPI100011 invalid instrument")])).candles(KEY, DAY, DAY, source="historical") == []


# -- pull -----------------------------------------------------------------------------------------
def api_for(by_key):
    def fn(url):
        for k, cs in by_key.items():
            if k.replace("|", "%7C") in url:
                return ok(cs)
        return Resp(400, text="invalid instrument")
    return FakeHttp(fn=fn)


def test_pull_stores_full_session_compacts_and_marks_source(world):
    km = {KEY: "RELIANCE.NS", "NSE_EQ|INE009A01021": "INFY.NS"}
    http = api_for({KEY: session_candles(), "NSE_EQ|INE009A01021": session_candles(px=1500)})
    st = pull(client(http), km, DAY, DAY, source="historical", lake_dir=world.lakedir)
    assert st["bars_written"] == 750 and st["no_data"] == []
    folder = world.lakedir / "bars/interval=1m/date=2026-10-26"
    assert [p.name for p in folder.iterdir()] == ["bars.parquet"]
    df = world.query.Lake().sql("SELECT * FROM bars_1m")
    assert set(df.src) == {"rest"} and df.groupby("symbol").size().eq(375).all()
    assert df.ts.min().strftime("%H:%M") == "03:45" and df.ts.max().strftime("%H:%M") == "09:59"


def test_pull_fills_gaps_but_never_overwrites_live_bars(world):
    from data_layer.ticks import BarAggregator, Tick
    ag, live = BarAggregator(60), []
    for k in range(100):
        live += ag.add(Tick(datetime(2026, 10, 26, 3, 45) + timedelta(minutes=k), "RELIANCE.NS", 999.0, 1000 + k))
    world.tick_store.write_bars(live + ag.flush_all(), "1m")
    st = pull(client(api_for({KEY: session_candles()})), {KEY: "RELIANCE.NS"}, DAY, DAY, source="historical",
              lake_dir=world.lakedir)
    assert st["skipped_existing"] == 100 and st["bars_written"] == 275
    df = world.query.Lake().sql("SELECT * FROM bars_1m ORDER BY ts")
    assert len(df) == 375 and df.ts.is_unique
    assert df[df.src == "live"].close.eq(999.0).all() and (df.src == "rest").sum() == 275


def test_pull_overwrite_replaces_live_bars(world):
    from data_layer.ticks import BarAggregator, Tick
    ag = BarAggregator(60)
    world.tick_store.write_bars(ag.add(Tick(datetime(2026, 10, 26, 3, 45), "RELIANCE.NS", 999.0, 1)) + ag.flush_all(), "1m")
    pull(client(api_for({KEY: session_candles(minutes=2)})), {KEY: "RELIANCE.NS"}, DAY, DAY, source="historical",
         overwrite=True, lake_dir=world.lakedir)
    df = world.query.Lake().sql("SELECT * FROM bars_1m ORDER BY ts")
    assert len(df) == 2 and df.src.tolist() == ["rest", "rest"]


def test_pull_reports_instruments_without_data_and_dumps_raw(world, tmp_path):
    km = {KEY: "RELIANCE.NS", "NSE_EQ|INE999A01010": "GHOST.NS"}
    st = pull(client(api_for({KEY: session_candles(minutes=5)})), km, DAY, DAY, source="historical",
              lake_dir=world.lakedir, dump_raw=1, dump_dir=tmp_path / "raw")
    assert st["no_data"] == ["GHOST.NS"] and st["bars_written"] == 5
    (f,) = (tmp_path / "raw").glob("*.json")
    assert len(json.loads(f.read_text())) == 5


def test_pull_multi_day_range_groups_by_day(world):
    cs = session_candles(date(2026, 10, 23), 3) + session_candles(date(2026, 10, 26), 3)
    pull(client(api_for({KEY: cs})), {KEY: "RELIANCE.NS"}, date(2026, 10, 23), date(2026, 10, 26), source="historical",
         lake_dir=world.lakedir)
    days = sorted(p.name for p in (world.lakedir / "bars/interval=1m").iterdir())
    assert days == ["date=2026-10-23", "date=2026-10-26"]


def test_pull_ignores_candles_outside_the_requested_days(world):
    cs = session_candles(date(2026, 10, 23), 3) + session_candles(DAY, 3)
    st = pull(client(api_for({KEY: cs})), {KEY: "RELIANCE.NS"}, DAY, DAY, source="historical", lake_dir=world.lakedir)
    assert st["bars_written"] == 3


def test_cli_pulls_a_day_end_to_end(world, monkeypatch, capsys):
    import sys
    world.set_universe({"IN": ["A.NS", "B.NS"]})
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", "TOK"); monkeypatch.delenv("UPSTOX_FEED_TOKEN", raising=False)
    monkeypatch.chdir(world.root)
    monkeypatch.setattr(candles, "ist_today", lambda: date(2026, 10, 27))
    monkeypatch.setattr(candles.time, "sleep", lambda s: None)
    monkeypatch.setattr(candles.requests, "Session", lambda: FakeHttp(fn=lambda url: ok(session_candles(minutes=10))))
    monkeypatch.setattr(sys, "argv", ["candles", "--date", "2026-10-26", "--limit", "1"])
    candles.main()
    assert "wrote 10 bars" in capsys.readouterr().out
    monkeypatch.setattr(sys, "argv", ["candles", "--from", "2026-10-26", "--to", "2026-10-26", "--chunk-days", "5"])
    candles.main()
    assert "already present" in capsys.readouterr().out


def test_cli_requires_token_and_dates(world, monkeypatch):
    import sys
    monkeypatch.delenv("UPSTOX_ACCESS_TOKEN", raising=False); monkeypatch.delenv("UPSTOX_FEED_TOKEN", raising=False)
    monkeypatch.chdir(world.root)
    monkeypatch.setattr(sys, "argv", ["candles", "--date", "2026-10-26"])
    with pytest.raises(SystemExit, match="no token"):
        candles.main()
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", "T")
    monkeypatch.setattr(sys, "argv", ["candles"])
    with pytest.raises(SystemExit, match="--date"):
        candles.main()
