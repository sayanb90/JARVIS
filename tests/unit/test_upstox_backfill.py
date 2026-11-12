"""History backfill (SCRUM-102, SCRUM-101): month chunks, resume state, strike band, spot and expired flows against a fake Upstox."""
import json
from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from data_layer import lake as lake_mod
from data_layer.query import Lake
from data_layer.upstox import backfill as bf
from data_layer.upstox.ratelimit import RateLimiter
from data_layer.upstox.rest import UpstoxRest


class R:
    def __init__(self, status=200, body=None):
        self.status_code, self._b, self.text, self.headers = status, body, json.dumps(body), {}
        self.content = self.text.encode()

    def json(self):
        return self._b


class FakeUpstox:
    """Answers the five endpoints from small tables; records every call."""

    def __init__(self, candle_days=None, expiries=None, options=None, futures=None, index_daily=None, fail=None):
        self.candle_days, self.expiries, self.options, self.futures = candle_days or {}, expiries or [], options or {}, futures or {}
        self.index_daily, self.fail, self.calls = index_daily or [], fail or {}, []

    def get(self, url, params=None, headers=None, timeout=None):
        path = url.split("api.upstox.com")[1]
        self.calls.append((path, params))
        for frag, resp in self.fail.items():
            if frag in path:
                return resp
        if path.startswith("/v2/expired-instruments/expiries"):
            return ok(self.expiries)
        if path.startswith("/v2/expired-instruments/option/contract"):
            return ok(self.options.get(params["expiry_date"], []))
        if path.startswith("/v2/expired-instruments/future/contract"):
            return ok(self.futures.get(params["expiry_date"], []))
        if "/days/1/" in path:
            return ok({"candles": self.index_daily})
        key, to, frm = path.split("/")[-4 if "/v3/" in path else -4], path.split("/")[-2], path.split("/")[-1]
        key = path.split("historical-candle/")[1].split("/")[0]
        days = [d for d in self.candle_days.get(key, []) if frm <= d <= to]
        return ok({"candles": [c for d in days for c in minute_candles(date.fromisoformat(d))]})


def ok(data):
    return R(200, {"status": "success", "data": data})


def err(status, code, msg="nope"):
    return R(status, {"status": "error", "errors": [{"errorCode": code, "message": msg}]})


def minute_candles(day, n=3, px=100.0):
    t0 = datetime(day.year, day.month, day.day, 9, 15)
    return [[f"{t0 + timedelta(minutes=k):%Y-%m-%dT%H:%M:%S}+05:30", px, px + 1, px - 1, px + 0.5, 10, 1000 + k] for k in range(n)]


def fetcher(fake, **kw):
    rest = UpstoxRest(lambda: "tok", session=fake, limiter=RateLimiter(windows=((1.0, 10**9),)), sleep=lambda s: None)
    return bf.Fetcher(rest, **kw)


@pytest.fixture
def lake_dir(tmp_path, monkeypatch):
    d = tmp_path / "lake"
    monkeypatch.setattr(lake_mod, "LAKE_DIR", d)
    monkeypatch.setattr(bf, "free_gb", lambda p: 100.0)
    return d


def state(lake_dir):
    return bf.State(lake_dir / "backfill" / "state.json")


# -- helpers --------------------------------------------------------------------------------------
def test_month_chunks_follow_the_calendar_and_clip_both_ends():
    assert bf.month_chunks(date(2024, 1, 15), date(2024, 3, 10)) == [
        (date(2024, 1, 15), date(2024, 1, 31)), (date(2024, 2, 1), date(2024, 2, 29)), (date(2024, 3, 1), date(2024, 3, 10))]
    assert bf.month_chunks(date(2024, 5, 2), date(2024, 5, 1)) == []


def test_keys_are_fully_url_encoded():
    assert bf.enc("NSE_INDEX|Nifty 50") == "NSE_INDEX%7CNifty%2050"


def test_state_survives_a_restart_and_only_ok_and_empty_count_as_done(tmp_path):
    s = bf.State(tmp_path / "x" / "state.json")
    s.mark("spot|A|2024-01", bf.OK, 10); s.mark("spot|B|2024-01", bf.EMPTY); s.mark("spot|C|2024-01", bf.ERROR, msg="HTTP 500")
    s.save()
    s2 = bf.State(tmp_path / "x" / "state.json")
    assert s2.done("spot|A|2024-01") and s2.done("spot|B|2024-01") and not s2.done("spot|C|2024-01") and not s2.done("spot|D|2024-01")
    assert s2.summary()["spot"] == {"ok": 1, "empty": 1, "error": 1, "rows": 10}


def test_strike_band_widens_the_index_range_by_n_strikes_each_side():
    ladder = range(22000, 26001, 50)
    assert bf.strike_band(ladder, 24010, 24490, 5) == (23750, 24750)        # 24000 - 5*50 .. 24500 + 5*50
    assert bf.strike_band(ladder, 24000, 24000, 0) == (24000, 24000)
    assert bf.strike_band([100.0], 90, 110, 5) == (100.0, 100.0) and bf.strike_band([], 1, 2, 5) == (0.0, 0.0)


def test_parse_contracts_reads_tolerantly_and_drops_what_it_cannot_use():
    recs = [{"instrument_key": "NSE_FO|1|28-11-2024", "instrument_type": "CE", "strike_price": 24000, "trading_symbol": "NIFTY 24000 CE 28 NOV 24",
             "lot_size": 25, "expiry": "2024-11-28"},
            {"expired_instrument_key": "NSE_FO|2|28-11-2024", "option_type": "pe", "strike": "24000", "minimum_lot": 25},
            {"instrument_key": "NSE_FO|3|28-11-2024", "instrument_type": "FUT", "trading_symbol": "NIFTY FUT 28 NOV 24"},
            {"instrument_type": "CE", "strike_price": 1}, {"instrument_key": "k", "instrument_type": "CE", "strike_price": "x"},
            {"instrument_key": "k2", "instrument_type": "EQ"}, "junk"]
    df = bf.parse_contracts(recs, "NIFTY", date(2024, 11, 28))
    assert list(df["symbol"]) == ["NIFTY_24000_CE_28_NOV_24", "NIFTY_24000_PE_28_NOV_24", "NIFTY_FUT_28_NOV_24"]
    assert list(df["lot_size"]) == [25, 25, 0] and df["expiry"].iloc[1] == date(2024, 11, 28) and pd.isna(df["strike"].iloc[2])


# -- spot ---------------------------------------------------------------------------------------
KM = {"NSE_INDEX|Nifty 50": "NSE_INDEX:Nifty 50", "NSE_EQ|INE002A01018": "RELIANCE.NS"}


def test_spot_stores_bars_newest_month_first_and_a_second_run_fetches_nothing(lake_dir):
    fake = FakeUpstox(candle_days={"NSE_EQ%7CINE002A01018": ["2024-01-10", "2024-02-12"], "NSE_INDEX%7CNifty%2050": ["2024-02-12"]})
    st = bf.run_spot(fetcher(fake), KM, date(2024, 1, 1), date(2024, 2, 29), state(lake_dir), lake_dir)
    assert st["calls"] == 4 and st["bars"] == 9 and st["empty"] == 1 and st["errors"] == 0
    assert "/2024-02-29/2024-02-01" in fake.calls[0][0] and "/minutes/1/" in fake.calls[0][0]
    df = Lake(lake_dir).sql("SELECT symbol, count(*) n, min(src) src, max(oi) oi FROM bars_1m GROUP BY 1 ORDER BY 1")
    assert dict(zip(df.symbol, df.n)) == {"NSE_INDEX:Nifty 50": 3, "RELIANCE.NS": 6} and set(df.src) == {"rest"} and df.oi.max() == 1002
    assert not list((lake_dir / "bars" / "interval=1m").glob("date=*/seg-*.parquet"))          # every touched day was compacted
    fake2 = FakeUpstox()
    assert bf.run_spot(fetcher(fake2), KM, date(2024, 1, 1), date(2024, 2, 29), state(lake_dir), lake_dir)["calls"] == 0 and not fake2.calls


def test_spot_never_asks_for_minutes_before_2022_and_dry_run_only_counts(lake_dir):
    fake = FakeUpstox()
    st = bf.run_spot(fetcher(fake), KM, date(2021, 11, 1), date(2022, 2, 28), state(lake_dir), lake_dir, dry_run=True)
    assert st["calls_planned"] == 4 and st["calls"] == 0 and not fake.calls and not (lake_dir / "bars").exists()


def test_spot_error_is_recorded_and_retried_by_the_next_run(lake_dir):
    fake = FakeUpstox(candle_days={"NSE_EQ%7CINE002A01018": ["2024-01-10"]}, fail={"NSE_INDEX": err(500, "UDAPI100500")})
    st = bf.run_spot(fetcher(fake), KM, date(2024, 1, 1), date(2024, 1, 31), state(lake_dir), lake_dir)
    assert st["errors"] == 1 and st["bars"] == 3
    s = state(lake_dir)
    assert s.tasks["spot|NSE_INDEX:Nifty 50|2024-01"]["status"] == "error" and s.done("spot|RELIANCE.NS|2024-01")
    fake2 = FakeUpstox(candle_days={"NSE_INDEX%7CNifty%2050": ["2024-01-10"]})
    assert bf.run_spot(fetcher(fake2), KM, date(2024, 1, 1), date(2024, 1, 31), s, lake_dir)["bars"] == 3 and len(fake2.calls) == 1


def test_rejected_token_stops_the_run_but_keeps_what_was_fetched(lake_dir):
    class Flaky(FakeUpstox):
        def get(self, url, **kw):
            return err(401, "UDAPI100050", "Invalid token") if len(self.calls) >= 1 else super().get(url, **kw)
    fake = Flaky(candle_days={"NSE_INDEX%7CNifty%2050": ["2024-01-10"]})
    with pytest.raises(bf.Stop, match="rejected the token"):
        bf.run_spot(fetcher(fake), KM, date(2024, 1, 1), date(2024, 1, 31), state(lake_dir), lake_dir)
    assert state(lake_dir).done("spot|NSE_INDEX:Nifty 50|2024-01")
    assert Lake(lake_dir).sql("SELECT count(*) n FROM bars_1m").n[0] == 3


def test_low_disk_stops_after_storing(lake_dir, monkeypatch):
    monkeypatch.setattr(bf, "free_gb", lambda p: 1.0)
    fake = FakeUpstox(candle_days={"NSE_INDEX%7CNifty%2050": ["2024-01-10"]})
    with pytest.raises(bf.Stop, match="GB free"):
        bf.run_spot(fetcher(fake), KM, date(2024, 1, 1), date(2024, 1, 31), state(lake_dir), lake_dir)
    assert state(lake_dir).done("spot|NSE_INDEX:Nifty 50|2024-01")


def test_sink_flushes_when_the_buffer_is_full(lake_dir, monkeypatch):
    monkeypatch.setattr(bf, "FLUSH_BARS", 3)
    fake = FakeUpstox(candle_days={"NSE_EQ%7CINE002A01018": ["2024-01-10"], "NSE_INDEX%7CNifty%2050": ["2024-01-10"]})
    assert bf.run_spot(fetcher(fake), KM, date(2024, 1, 1), date(2024, 1, 31), state(lake_dir), lake_dir)["bars"] == 6


def test_free_gb_reads_the_nearest_existing_folder(tmp_path):
    assert bf.free_gb(tmp_path / "not" / "there" / "yet") > 0


# -- expired ------------------------------------------------------------------------------------
EXP = "2024-11-28"


def chain(expiry=EXP, strikes=range(23000, 25001, 50)):
    d = date.fromisoformat(expiry)
    out = []
    for i, k in enumerate(strikes):
        for t in ("CE", "PE"):
            out.append({"instrument_key": f"NSE_FO|{k}{t}|{d:%d-%m-%Y}", "instrument_type": t, "strike_price": k, "lot_size": 25,
                        "trading_symbol": f"NIFTY {k} {t} {d:%d %b %y}".upper(), "expiry": expiry})
    return out


def expired_world(**kw):
    daily = [["2024-11-27T00:00:00+05:30", 24100, 24290, 24010, 24200, 0, 0], ["2024-11-28T00:00:00+05:30", 24200, 24240, 24120, 24150, 0, 0],
             ["bad"]]
    fut = [{"instrument_key": "NSE_FO|FUT|28-11-2024", "instrument_type": "FUT", "trading_symbol": "NIFTY FUT 28 NOV 24", "lot_size": 25}]
    days = {"NSE_FO%7C24000CE%7C28-11-2024": ["2024-11-27", "2024-11-28"], "NSE_FO%7CFUT%7C28-11-2024": ["2024-10-15", "2024-11-28"]}
    return FakeUpstox(expiries=[EXP, "2024-11-21", "2020-01-30"], options={EXP: chain()}, futures={EXP: fut}, index_daily=daily,
                      candle_days=days, **kw)


def test_expired_pulls_the_band_around_the_money_and_the_future(lake_dir):
    fake = expired_world()
    st = bf.run_expired(fetcher(fake), ["NIFTY"], date(2024, 11, 25), date(2024, 11, 30), state(lake_dir), lake_dir, strikes=5, window_days=7)
    meta = pd.read_parquet(lake_dir / "instruments" / "fo_expired.parquet")
    opts = meta[meta.instrument_type != "FUT"]
    assert st["expiries"] == 1 and opts.strike.min() == 23750 and opts.strike.max() == 24550          # 24000-250 .. 24300+250
    assert len(opts) == 17 * 2 and list(meta[meta.instrument_type == "FUT"].symbol) == ["NIFTY_FUT_28_NOV_24"]
    df = Lake(lake_dir).sql("SELECT symbol, count(*) n, max(oi) oi FROM bars_1m GROUP BY 1 ORDER BY 1")
    assert dict(zip(df.symbol, df.n)) == {"NIFTY_24000_CE_28_NOV_24": 6, "NIFTY_FUT_28_NOV_24": 6} and df.oi.max() == 1002
    assert any("/1minute/2024-11-28/2024-11-21" in p for p, _ in fake.calls)                           # option window: 7 days
    assert any("/1minute/2024-08-31/2024-08-25" in p for p, _ in fake.calls)                           # future window: 95 days, by month
    n = len(fake.calls)
    bf.run_expired(fetcher(fake), ["NIFTY"], date(2024, 11, 25), date(2024, 11, 30), state(lake_dir), lake_dir, strikes=5, window_days=7)
    assert len(fake.calls) - n == 4                                # a second run only re-reads the lists: expiries, index, options, future


def test_expired_dry_run_reads_lists_only_and_stores_nothing(lake_dir):
    fake = expired_world()
    st = bf.run_expired(fetcher(fake), ["NIFTY"], date(2024, 11, 25), date(2024, 11, 30), state(lake_dir), lake_dir, window_days=7, dry_run=True)
    assert st["contracts"] == 35 and st["calls"] == 34 + 4 and st["bars"] == 0
    assert not (lake_dir / "bars").exists() and not (lake_dir / "instruments").exists()
    assert not any("historical-candle/NSE_FO" in p for p, _ in fake.calls)


def test_expired_without_plus_stops_with_a_clear_message(lake_dir):
    fake = expired_world(fail={"/expiries": err(403, "UDAPI1149", "Plus required")})
    with pytest.raises(bf.Stop, match="Upstox Plus"):
        bf.run_expired(fetcher(fake), ["NIFTY"], date(2024, 11, 25), date(2024, 11, 30), state(lake_dir), lake_dir)


def test_expired_list_and_candle_errors_are_counted_and_other_work_continues(lake_dir):
    fake = expired_world(fail={"/option/contract": err(500, "X"), "FUT%7C28-11-2024/1minute/2024-11-28": err(400, "UDAPI100011")})
    st = bf.run_expired(fetcher(fake), ["NIFTY"], date(2024, 11, 25), date(2024, 11, 30), state(lake_dir), lake_dir, window_days=7)
    assert st["errors"] == 2 and st["contracts"] == 1 and st["bars"] == 3                            # the October future candles still land
    s = state(lake_dir)
    assert s.tasks["expired-list|NIFTY|2024-11-28"]["status"] == "error" and not s.done("expired|NIFTY_FUT_28_NOV_24|2024-11-01")


def test_expired_with_no_index_range_skips_options_and_no_expiries_is_quiet(lake_dir):
    fake = expired_world()
    fake.index_daily = []
    st = bf.run_expired(fetcher(fake), ["NIFTY"], date(2024, 11, 25), date(2024, 11, 30), state(lake_dir), lake_dir, window_days=7)
    assert st["no_range"] == 1 and st["contracts"] == 1
    assert bf.run_expired(fetcher(expired_world()), ["NIFTY"], date(2019, 1, 1), date(2019, 2, 1), state(lake_dir), lake_dir)["expiries"] == 0


def test_expired_stops_when_the_expiry_list_or_the_index_cannot_be_read(lake_dir):
    with pytest.raises(bf.Stop, match="could not list"):
        bf.run_expired(fetcher(expired_world(fail={"/expiries": err(500, "X")})), ["NIFTY"], date(2024, 11, 25), date(2024, 11, 30),
                       state(lake_dir), lake_dir)
    with pytest.raises(bf.Stop, match="daily candles"):
        bf.run_expired(fetcher(expired_world(fail={"/days/1/": err(500, "X")})), ["NIFTY"], date(2024, 11, 25), date(2024, 11, 30),
                       state(lake_dir), lake_dir)


def test_dump_raw_saves_the_first_responses(lake_dir, tmp_path):
    f = fetcher(expired_world(), dump_raw=1, dump_dir=tmp_path / "dump")
    f.expiries("NSE_INDEX|Nifty 50"); f.expiries("NSE_INDEX|Nifty 50")
    assert [p.name for p in (tmp_path / "dump").iterdir()] == ["expiries.json"]


# -- command line -------------------------------------------------------------------------------
def rest_for(fake):
    return UpstoxRest(lambda: "tok", session=fake, limiter=RateLimiter(windows=((1.0, 10**9),)), sleep=lambda s: None)


def test_cli_status_spot_expired_and_stop(lake_dir, monkeypatch, capsys):
    assert bf.main(["status"]) == 0 and "nothing backfilled" in capsys.readouterr().out
    monkeypatch.setattr(bf, "build_key_map", lambda: ({"NSE_EQ|INE002A01018": "RELIANCE.NS", "NSE_EQ|X": "X.NS"}, []))
    fake = FakeUpstox(candle_days={"NSE_EQ%7CINE002A01018": ["2024-01-10"]})
    args = ["--from", "2024-01-01", "--to", "2024-01-31"]
    assert bf.main(["spot", *args, "--limit", "1", "--no-index", "--dry-run"], rest=rest_for(fake)) == 0
    assert "1 instruments" in capsys.readouterr().out and not fake.calls
    assert bf.main(["spot", *args, "--limit", "1", "--no-index"], rest=rest_for(fake)) == 0 and "3 bars stored" in capsys.readouterr().out
    assert len(bf.spot_key_map()) == 2 + len(bf.INDEX_KEYS) and list(bf.spot_key_map())[0].startswith("NSE_INDEX")
    ew = expired_world()
    assert bf.main(["expired", "--from", "2024-11-25", "--to", "2024-11-30", "--underlying", "NIFTY", "--window-days", "7", "--dry-run"],
                   rest=rest_for(ew)) == 0 and "35 contracts" in capsys.readouterr().out
    assert bf.main(["expired", "--from", "2024-11-25", "--to", "2024-11-30", "--underlying", "NIFTY", "--window-days", "7"],
                   rest=rest_for(ew)) == 0 and "expired done: 1 expiries" in capsys.readouterr().out
    assert bf.main(["status"]) == 0
    out = capsys.readouterr().out
    assert "spot" in out and "expired" in out
    bad = FakeUpstox(fail={"historical-candle": err(401, "UDAPI100050")})
    assert bf.main(["spot", "--from", "2024-03-01", "--to", "2024-03-31", "--limit", "1", "--no-index"], rest=rest_for(bad)) == 1
    assert "STOPPED" in capsys.readouterr().err
    monkeypatch.setattr(bf, "build_key_map", lambda: ({}, []))
    assert bf.main(["spot", *args, "--no-index"], rest=rest_for(fake)) == 2


def test_cli_without_a_token_says_so(lake_dir, monkeypatch, capsys):
    for v in ("UPSTOX_FEED_TOKEN", "UPSTOX_ACCESS_TOKEN"):
        monkeypatch.delenv(v, raising=False)
    assert bf.main(["expired", "--dry-run"]) == 2 and "no token" in capsys.readouterr().err


# -- one lock for every writer of a bars day (the backfill, the nightly Yahoo load, the candle tool) -------------------------------
def test_compact_day_and_load_bars_take_the_same_per_day_lock(lake_dir, monkeypatch):
    from data_layer import tick_store
    taken = []
    real = tick_store._folder_lock
    import contextlib

    @contextlib.contextmanager
    def spy(folder):
        taken.append(folder.name)
        with real(folder):
            yield
    monkeypatch.setattr(tick_store, "_folder_lock", spy)
    fake = FakeUpstox(candle_days={"NSE_EQ%7CINE002A01018": ["2024-01-10"]})
    bf.run_spot(fetcher(fake), {"NSE_EQ|INE002A01018": "RELIANCE.NS"}, date(2024, 1, 1), date(2024, 1, 31), state(lake_dir), lake_dir)
    assert taken == ["date=2024-01-10"]
    assert tick_store.compact_day("1m", date(2024, 1, 11), lake_dir) == 0 and taken == ["date=2024-01-10"]       # no folder: nothing to lock
    monkeypatch.setattr(lake_mod, "DATA_DIR", lake_dir.parent / "data")
    csv = lake_dir.parent / "data" / "bars" / "1m" / "us" / "2024-01-10.csv"
    csv.parent.mkdir(parents=True)
    csv.write_text("ts,symbol,market,open,high,low,close,volume\n2024.01.10D14:30:00.000000000,AAPL,US,1,2,0,1,5\n")
    assert lake_mod.load_bars("1m", date(2024, 1, 1), ("US",)) == 1 and taken == ["date=2024-01-10", "date=2024-01-10"]
    df = Lake(lake_dir).sql("SELECT DISTINCT symbol FROM bars_1m ORDER BY symbol")
    assert list(df.symbol) == ["AAPL", "RELIANCE.NS"]                        # the load added a market to the day, it did not replace the backfill's rows


# -- call budget, time limit, anonymous candles ------------------------------------------------------------------------------------
def test_anonymous_requests_carry_no_token_and_expired_refuses_to_run_anonymously(lake_dir):
    sent = []

    class Spy(FakeUpstox):
        def get(self, url, params=None, headers=None, timeout=None):
            sent.append((headers or {}).get("Authorization"))
            return super().get(url, params=params, headers=headers, timeout=timeout)
    fake = Spy(candle_days={"NSE_EQ%7CINE002A01018": ["2024-01-10"]})
    bf.run_spot(bf.Fetcher(UpstoxRest(lambda: "secret", session=fake, limiter=RateLimiter(windows=((1.0, 10**9),)), sleep=lambda s: None),
                           anonymous=True), {"NSE_EQ|INE002A01018": "RELIANCE.NS"}, date(2024, 1, 1), date(2024, 1, 31), state(lake_dir), lake_dir)
    assert sent == [None]
    with pytest.raises(bf.Stop, match="needs a token"):
        bf.Fetcher(rest_for(fake), anonymous=True).expired_candles("k", date(2024, 1, 1), date(2024, 1, 2))
    refused = FakeUpstox(fail={"historical-candle": err(401, "UDAPI100050")})
    with pytest.raises(bf.Stop, match="without a token"):
        bf.run_spot(bf.Fetcher(rest_for(refused), anonymous=True), KM, date(2024, 2, 1), date(2024, 2, 29), state(lake_dir), lake_dir)


def test_the_time_limit_stops_cleanly_after_storing_and_the_next_run_continues(lake_dir, capsys, monkeypatch):
    ticks = iter(range(0, 10**6, 100))                                  # the clock advances 100 s per look
    fake = FakeUpstox(candle_days={"NSE_EQ%7CINE002A01018": ["2024-01-10", "2024-02-12", "2024-03-11"]})
    f = bf.Fetcher(rest_for(fake), max_minutes=5, clock=lambda: next(ticks))             # 300 s: about two calls
    km = {"NSE_EQ|INE002A01018": "RELIANCE.NS"}
    with pytest.raises(bf.TimeUp, match="run the same command again"):
        bf.run_spot(f, km, date(2024, 1, 1), date(2024, 3, 31), state(lake_dir), lake_dir)
    done = [t for t in state(lake_dir).tasks if state(lake_dir).done(t)]
    assert 1 <= len(done) < 3 and Lake(lake_dir).sql("SELECT count(*) n FROM bars_1m").n[0] == 3 * len(done)
    monkeypatch.setattr(bf, "build_key_map", lambda: (km, []))
    monkeypatch.setattr(bf, "Fetcher", lambda rest, dump_raw=0, **kw: bf.Fetcher.__new__(bf.Fetcher) if False else _TimeUpFetcher())
    assert bf.main(["spot", "--from", "2024-01-01", "--to", "2024-03-31", "--no-index", "--max-minutes", "1"], rest=rest_for(fake)) == bf.EX_TEMPFAIL
    assert "TIME LIMIT" in capsys.readouterr().err


class _TimeUpFetcher:
    def minute_candles(self, *a):
        raise bf.TimeUp("the time limit (--max-minutes) is reached")


def test_the_call_budget_reaches_the_limiter_and_the_estimate_follows_it(lake_dir, monkeypatch, capsys):
    monkeypatch.setenv("UPSTOX_FEED_TOKEN", "tok")
    seen = {}
    real = bf.RateLimiter
    monkeypatch.setattr(bf, "RateLimiter", lambda windows, **kw: seen.setdefault("w", windows) and real(windows=windows, **kw))
    monkeypatch.setattr(bf, "build_key_map", lambda: ({"NSE_EQ|X": "X.NS"}, []))
    assert bf.main(["spot", "--from", "2024-01-01", "--to", "2024-03-31", "--no-index", "--budget-30min", "900", "--dry-run"]) == 0
    assert seen["w"][-1] == (1800.0, 900) and "900 calls per 30 minutes" in capsys.readouterr().out
    assert "1,500 calls per 30 minutes" in bf.estimate(3000) and bf.estimate(2700, 1500).startswith("2,700 calls, about 1.0 hours")


def test_spot_runs_without_a_token_but_expired_does_not(lake_dir, monkeypatch, capsys):
    for v in ("UPSTOX_FEED_TOKEN", "UPSTOX_ACCESS_TOKEN"):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setattr(bf, "build_key_map", lambda: ({"NSE_EQ|X": "X.NS"}, []))
    assert bf.main(["spot", "--from", "2024-01-01", "--to", "2024-01-31", "--no-index", "--dry-run"]) == 0
    assert "without one" in capsys.readouterr().err
    assert bf.main(["expired", "--dry-run"]) == 2 and "no token" in capsys.readouterr().err


def test_recent_fetches_only_the_missing_open_days_and_a_second_run_finds_nothing(lake_dir, monkeypatch, capsys):
    from data_layer import gaps, market_calendar
    day = date(2026, 11, 11)
    market_calendar.save([], lake_dir, now=datetime(2026, 11, 12, 3, 0))
    monkeypatch.setattr(bf, "ist_today", lambda: date(2026, 11, 12))
    monkeypatch.setattr(bf, "build_key_map", lambda: ({"NSE_EQ|INE002A01018": "RELIANCE.NS"}, []))
    monkeypatch.setattr(bf, "INDEX_KEYS", {})
    fake = FakeUpstox(candle_days={"NSE_EQ%7CINE002A01018": ["2026-11-10", "2026-11-11"]})
    assert gaps.india_missing(lake_dir, date(2026, 11, 12), datetime(2026, 11, 12, 9, 0, tzinfo=__import__("datetime").timezone.utc), span=3) == \
        [date(2026, 11, 9), date(2026, 11, 10), day]
    st = bf.run_recent(fetcher(fake), {"NSE_EQ|INE002A01018": "RELIANCE.NS"}, [day], bf.State(lake_dir / "backfill" / "recent_state.json"), lake_dir)
    assert st["calls"] == 1 and st["bars"] == 3 and st["errors"] == 0 and "/2026-11-11/2026-11-11" in fake.calls[0][0]
    df = Lake(lake_dir).sql("SELECT symbol, count(*) n, min(CAST(ts AS DATE)) d FROM bars_1m GROUP BY 1")
    assert list(df.symbol) == ["RELIANCE.NS"] and list(df.n) == [3] and str(df.d[0])[:10] == "2026-11-11"           # the 10th was not asked to be stored
    assert "IN" in gaps.days_with_rows(lake_dir, "1m", day, day)[day]


def test_recent_cli_says_so_when_nothing_is_missing_and_keeps_going_when_some_calls_fail(lake_dir, monkeypatch, capsys):
    from data_layer import market_calendar
    market_calendar.save([], lake_dir, now=datetime(2026, 11, 12, 3, 0))
    monkeypatch.setattr(bf, "ist_today", lambda: date(2026, 11, 14))                         # a Saturday after a week with no rows: every weekday is missing
    monkeypatch.setattr(bf, "build_key_map", lambda: ({"NSE_EQ|INE002A01018": "RELIANCE.NS"}, []))
    monkeypatch.setattr(bf, "INDEX_KEYS", {})
    fake = FakeUpstox(candle_days={"NSE_EQ%7CINE002A01018": ["2026-11-09", "2026-11-10", "2026-11-11", "2026-11-12", "2026-11-13"]})
    assert bf.main(["recent", "--days", "5", "--dry-run"], rest=rest_for(fake)) == 0 and "1 instruments" in capsys.readouterr().out and not fake.calls
    assert bf.main(["recent", "--days", "5"], rest=rest_for(fake)) == 0
    assert "15 bars stored" in capsys.readouterr().out
    assert bf.main(["recent", "--days", "5"], rest=rest_for(FakeUpstox())) == 0 and "nothing to fetch" in capsys.readouterr().out
    bad = FakeUpstox(fail={"historical-candle": err(500, "UDAPI100500")})
    monkeypatch.setattr(bf, "ist_today", lambda: date(2026, 12, 4))
    assert bf.main(["recent", "--days", "3"], rest=rest_for(bad)) == 1                      # every call failed and nothing was stored: cron sees a failure


def test_an_instrument_whose_call_failed_is_asked_for_again_on_the_next_run_even_though_the_day_has_rows(lake_dir, monkeypatch, capsys):
    from data_layer import gaps, market_calendar
    market_calendar.save([], lake_dir, now=datetime(2026, 11, 12, 3, 0))
    monkeypatch.setattr(bf, "ist_today", lambda: date(2026, 11, 14))                    # a Saturday: the open days are 12 and 13 Nov whatever the clock says
    keys = {"NSE_EQ|INE002A01018": "RELIANCE.NS", "NSE_EQ|INE467B01029": "TCS.NS"}
    monkeypatch.setattr(bf, "build_key_map", lambda: (keys, []))
    monkeypatch.setattr(bf, "INDEX_KEYS", {})
    from data_layer import tick_store
    from data_layer.ticks import Bar
    for sym in keys.values():                                                          # both instruments had rows on the 11th (the earlier backfill)
        tick_store.write_bars([Bar(datetime(2026, 11, 11, 4, 0), sym, "IN", 1, 2, 0.5, 1.5, 10, None, None, None, None, None, None, "rest")], "1m", lake_dir)
    days = {"NSE_EQ%7CINE002A01018": ["2026-11-12", "2026-11-13"], "NSE_EQ%7CINE467B01029": ["2026-11-12", "2026-11-13"]}
    limited = FakeUpstox(candle_days=days, fail={"INE467B01029": err(429, "UDAPI10005", "slow down")})
    args = ["recent", "--days", "3", "--budget-30min", "400"]
    assert bf.main(args, rest=rest_for(limited)) == 0
    out = capsys.readouterr().out
    assert "6 bars stored" in out and "1 errors" in out and "asked for again next run" in out
    assert gaps.india_holes(lake_dir, ["RELIANCE.NS", "TCS.NS"], date(2026, 11, 14), span=3) == {"TCS.NS": {date(2026, 11, 12), date(2026, 11, 13)}}
    assert "IN" in gaps.days_with_rows(lake_dir, "1m", date(2026, 11, 12), date(2026, 11, 12))[date(2026, 11, 12)]      # present: only the failure list asks again
    again = FakeUpstox(candle_days=days)
    assert bf.main(args, rest=rest_for(again)) == 0
    out = capsys.readouterr().out
    assert len(again.calls) == 1 and "6 bars stored" in out and "again next run" not in out                            # only the failed instrument, one call
    df = Lake(lake_dir).sql("SELECT symbol, count(*) n FROM bars_1m GROUP BY 1 ORDER BY 1")
    assert dict(zip(df.symbol, df.n)) == {"RELIANCE.NS": 7, "TCS.NS": 7}            # 6 fetched plus the bar seeded on the 11th
    assert bf.main(args, rest=rest_for(FakeUpstox())) == 0 and "nothing to fetch" in capsys.readouterr().out
