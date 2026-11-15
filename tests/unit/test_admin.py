"""JARVIS /admin: access, the table catalog and paging, the read-only SQL sandbox, and the subscription list."""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from data_layer import tick_store, universe_files
from data_layer import universe as uni
from data_layer.ticks import Bar, Tick
from data_layer.upstox import feed
from data_layer.upstox import fo_instruments as fo
from webapp import admin
from webapp.app import create_app

DAY = date(2026, 10, 23)
T0 = datetime(2026, 10, 23, 3, 45)
A = ("boss", "s3cret")
H = {"x-jarvis-admin": "1"}
EQ_RECORDS = [{"segment": "NSE_EQ", "instrument_type": "EQ", "trading_symbol": s, "name": f"{s} Ltd",
               "isin": isin, "instrument_key": f"NSE_EQ|{isin}", "tick_size": 5, "lot_size": 1}
              for s, isin in [("RELIANCE", "INE002A01018"), ("TCS", "INE467B01029"), ("INFY", "INE009A01021"),
                              ("SBIN", "INE062A01020"), ("M&M", "INE101A01026"), ("BAJAJ-AUTO", "INE917I01010")]]


@pytest.fixture()
def lake(world, monkeypatch):
    """A lake with raw ticks for 3 symbols on 2 days, REST minute bars, the equity table and a small universe."""
    monkeypatch.setenv("ADMIN_USER", A[0]); monkeypatch.setenv("ADMIN_PASSWORD", A[1])
    monkeypatch.delenv("WEBAPP_USER", raising=False); monkeypatch.delenv("WEBAPP_PASSWORD", raising=False)
    for d in (DAY - timedelta(days=1), DAY):
        base = datetime.combine(d, T0.time())
        tick_store.write_ticks([Tick(base + timedelta(milliseconds=250 * i), sym, 100.0 + i * 0.01, 1000 + i, 99.95, 10, 100.05, 12)
                                for sym in ("RELIANCE.NS", "TCS.NS", "INFY.NS") for i in range(250)], world.lakedir)
        tick_store.compact_ticks_day(d, world.lakedir)
    tick_store.write_bars([Bar(T0 + timedelta(minutes=i), "TCS.NS", "IN", 1, 2, 0.5, 1.5, 3, None, None, None, None, None, None, src="rest")
                           for i in range(10)], "1m", world.lakedir)
    fo.save_eq_table(fo.parse_equities(EQ_RECORDS), world.lakedir)
    world.set_universe({"IN": ["RELIANCE.NS", "TCS.NS", "INFY.NS"], "US": ["AAPL"]})
    rows = universe_files.read_universe(None)
    for r in rows:                                   # give the India rows the real ISINs from the instrument file
        isin = {e["trading_symbol"] + ".NS": e["isin"] for e in EQ_RECORDS}.get(r["symbol"])
        if isin:
            r["isin"] = isin
    uni.write_universe(universe_files.UNIVERSE_CSV, [], rows)
    return world


@pytest.fixture()
def client(lake):
    return TestClient(create_app(lake.lakedir))


# -- access --------------------------------------------------------------------------------------
def test_admin_is_absent_without_credentials(world, monkeypatch):
    monkeypatch.delenv("ADMIN_USER", raising=False); monkeypatch.delenv("ADMIN_PASSWORD", raising=False)
    c = TestClient(create_app(world.lakedir))
    assert c.get("/admin").status_code == 404 and c.get("/admin/api/catalog").status_code == 404


def test_every_admin_path_needs_the_admin_password(client):
    for path in ("/admin", "/admin/", "/admin/api/catalog", "/admin/static/admin.js", "/admin/api/universe"):
        r = client.get(path)
        assert r.status_code == 401 and r.headers["www-authenticate"] == 'Basic realm="JARVIS admin"', path
        assert client.get(path, auth=("boss", "wrong")).status_code == 401
    page = client.get("/admin", auth=A)
    assert page.status_code == 200 and "JARVIS" in page.text and "noindex" in page.headers["x-robots-tag"]
    assert client.get("/admin/static/style.css", auth=A).status_code == 200
    assert client.get("/admin/static/../app.py", auth=A).status_code == 404
    assert client.get("/admin/static/index.html", auth=A).status_code == 404


def test_one_login_covers_viewer_and_admin(lake, monkeypatch):
    monkeypatch.setenv("WEBAPP_USER", "viewer"); monkeypatch.setenv("WEBAPP_PASSWORD", "view")
    c = TestClient(create_app(lake.lakedir))
    V = ("viewer", "view")
    assert c.get("/api/meta").status_code == 401 and c.get("/admin/api/catalog").status_code == 401        # no login: neither
    assert c.get("/api/meta", auth=V).status_code == 200 and c.get("/admin/api/catalog", auth=V).status_code == 200
    assert c.get("/api/meta", auth=V).json()["admin"] is True
    assert c.get("/admin/api/catalog", auth=A).status_code == 401 and c.get("/api/meta", auth=A).status_code == 401   # the shared login wins


def test_admin_alone_keeps_working_with_the_older_admin_login(lake):
    c = TestClient(create_app(lake.lakedir))                                    # fixture: ADMIN_* set, WEBAPP_* not
    assert c.get("/api/meta").json()["admin"] is True                          # viewer open, as before
    assert c.get("/admin/api/catalog").status_code == 401 and c.get("/admin/api/catalog", auth=A).status_code == 200


def test_open_viewer_never_serves_admin(world, monkeypatch):
    for k in ("ADMIN_USER", "ADMIN_PASSWORD", "WEBAPP_USER", "WEBAPP_PASSWORD"):
        monkeypatch.delenv(k, raising=False)
    c = TestClient(create_app(world.lakedir))
    assert c.get("/api/meta").json()["admin"] is False
    assert c.get("/admin").status_code == 404 and c.post("/admin/api/universe/apply", json={}).status_code == 404


def test_half_a_login_does_not_enable_admin(world, monkeypatch):
    monkeypatch.setenv("WEBAPP_USER", "viewer"); monkeypatch.delenv("WEBAPP_PASSWORD", raising=False)
    monkeypatch.delenv("ADMIN_USER", raising=False); monkeypatch.delenv("ADMIN_PASSWORD", raising=False)
    assert TestClient(create_app(world.lakedir)).get("/admin").status_code == 404


# -- catalog and paging ----------------------------------------------------------------------------
def test_catalog_groups_tables_into_services(client):
    cat = client.get("/admin/api/catalog", auth=A).json()
    assert [s["service"] for s in cat] == ["reference", "market", "aggregates", "fundamentals", "indices", "operations", "analytics"]
    tables = {(s["service"], t["name"]): t for s in cat for t in s["tables"]}
    assert tables[("market", "ticks")]["days"] == ["2026-10-23", "2026-10-22"] and tables[("market", "ticks")]["available"]
    assert tables[("reference", "eq_instruments")]["available"] and tables[("reference", "universe")]["available"]
    assert not tables[("aggregates", "ohlc_daily")]["available"] and tables[("aggregates", "bars_1m")]["query_name"] is None


def test_ticks_page_through_200_rows_at_a_time_with_a_count_from_the_file_footers(client, lake):
    r = client.get("/admin/api/table", params={"service": "market", "name": "ticks"}, auth=A).json()
    assert r["day"] == "2026-10-23" and r["total"] == 750 and r["pages"] == 4 and len(r["rows"]) == 200
    assert "date" not in r["columns"] and r["columns"][:3] == ["ts", "symbol", "ltp"]
    last = client.get("/admin/api/table", params={"service": "market", "name": "ticks", "page": 4}, auth=A).json()
    assert len(last["rows"]) == 150 and last["page"] == 4
    over = client.get("/admin/api/table", params={"service": "market", "name": "ticks", "page": 99}, auth=A).json()
    assert over["page"] == 4                                                   # clamped, not an empty page
    files = list((lake.lakedir / "ticks").rglob("date=2026-10-23/*.parquet"))                  # the stocks' file and the options' file, whichever exist
    assert sum(pq.ParquetFile(f).metadata.num_rows for f in files) == 750


def test_filter_order_page_size_and_earlier_days(client):
    r = client.get("/admin/api/table", params={"service": "market", "name": "ticks", "symbol": "TCS.NS", "order": "desc", "size": 50},
                   auth=A).json()
    assert r["total"] == 250 and r["pages"] == 5 and {row[1] for row in r["rows"]} == {"TCS.NS"}
    ts = [row[0] for row in r["rows"]]
    assert ts == sorted(ts, reverse=True) and ts[0].endswith(".250")         # milliseconds, newest first
    d = client.get("/admin/api/table", params={"service": "market", "name": "ticks", "day": "2026-10-22"}, auth=A).json()
    assert d["day"] == "2026-10-22" and d["total"] == 750
    for bad in ({"size": 7}, {"order": "random"}, {"symbol": "x;drop"}, {"day": "yesterday"}):
        assert client.get("/admin/api/table", params={"service": "market", "name": "ticks", **bad}, auth=A).status_code == 422
    assert client.get("/admin/api/table", params={"service": "market", "name": "nope"}, auth=A).status_code == 404
    assert client.get("/admin/api/table", params={"service": "../etc", "name": "ticks"}, auth=A).status_code == 404


def test_derived_bars_reference_tables_and_empty_tables(client):
    m = client.get("/admin/api/table", params={"service": "aggregates", "name": "bars_1m", "symbol": "RELIANCE.NS"}, auth=A).json()
    assert m["total"] == 2 and m["columns"][:3] == ["symbol", "market", "ts"]  # 250 ticks at 4 a second span two minutes
    rest = client.get("/admin/api/table", params={"service": "market", "name": "candles_1m"}, auth=A).json()
    assert rest["total"] == 10 and "vendor" in rest["columns"] and {r[rest["columns"].index("vendor")] for r in rest["rows"]} == {"Upstox"}      # India candles come from Upstox
    u = client.get("/admin/api/table", params={"service": "reference", "name": "universe"}, auth=A).json()
    assert u["total"] == 4 and "source" in u["columns"]
    eq = client.get("/admin/api/table", params={"service": "reference", "name": "eq_instruments", "symbol": "M&M"}, auth=A).json()
    assert eq["total"] == 1 and eq["rows"][0][2] == "INE101A01026"
    empty = client.get("/admin/api/table", params={"service": "aggregates", "name": "ohlc_daily"}, auth=A).json()
    assert empty["total"] == 0 and empty["rows"] == []
    assert client.get("/admin/api/table", params={"service": "reference", "name": "fo_watchlist", "symbol": "X"}, auth=A).json()["total"] == 0


# -- read-only SQL -----------------------------------------------------------------------------------
def q(client, sql, page=1, headers=H):
    return client.post("/admin/api/query", json={"sql": sql, "page": page}, auth=A, headers=headers)


def test_query_reads_service_tables_and_pages(client):
    r = q(client, "SELECT symbol, count(*) AS n FROM market.ticks WHERE date = '2026-10-23' GROUP BY 1 ORDER BY 1;").json()
    assert r["columns"] == ["symbol", "n"] and r["rows"] == [["INFY.NS", 250], ["RELIANCE.NS", 250], ["TCS.NS", 250]]
    p1 = q(client, "SELECT * FROM market.ticks ORDER BY ts, symbol").json()
    p8 = q(client, "SELECT * FROM market.ticks ORDER BY ts, symbol", page=8).json()
    assert p1["has_more"] and len(p1["rows"]) == 200 and not p8["has_more"] and len(p8["rows"]) == 100     # 1,500 rows
    s = client.get("/admin/api/schema", auth=A).json()
    assert "market.ticks" in s and "ltp" in s["market.ticks"] and "reference.eq_instruments" in s


@pytest.mark.parametrize("sql,why", [
    ("COPY (SELECT 1) TO 'x.csv'", "COPY"), ("DROP VIEW market.ticks", "DROP"), ("CREATE TABLE t AS SELECT 1", "CREATE"),
    ("SELECT 1; SELECT 2", "one statement"), ("", "empty"), ("SELEC 1", "syntax"),
    ("SELECT * FROM read_csv('/etc/passwd')", "Permission"), ("ATTACH 'other.db'", "ATTACH"),
    ("SET enable_external_access = true", "SET"), ("SELECT * FROM nosuch.table_x", "Catalog"),
])
def test_query_refuses_anything_but_a_read_inside_the_lake(client, sql, why):
    r = q(client, sql)
    assert r.status_code == 422 and why.lower() in r.json()["detail"].lower(), r.json()


def test_writes_need_the_admin_header_so_another_site_cannot_post(client):
    assert q(client, "SELECT 1", headers={}).status_code == 403
    assert client.post("/admin/api/universe/apply", json={"tickers": ["TCS"]}, auth=A).status_code == 403


def test_a_runaway_query_is_stopped(lake):
    sb = admin.Sandbox(admin.Catalog(lake.lakedir, lake.data))
    with pytest.raises(admin.HTTPException) as e:
        sb.run("SELECT count(*) FROM range(1000000000000) a", timeout=0.3)
    assert e.value.status_code == 408


# -- subscription list ---------------------------------------------------------------------------------
def test_snapshot_lists_the_india_universe_with_keys_metadata_and_todays_ticks(client):
    u = client.get("/admin/api/universe", auth=A).json()
    assert u["source"] == "universe.csv" and u["tick_day"] == "2026-10-23" and u["eq_table"] and u["eq_listings"] == 6
    rows = {r["symbol"]: r for r in u["equities"]}
    assert set(rows) == {"RELIANCE.NS", "TCS.NS", "INFY.NS"}
    assert rows["TCS.NS"]["instrument_key"] == "NSE_EQ|INE467B01029" and rows["TCS.NS"]["listed"] and rows["TCS.NS"]["ticks"] == 250
    assert rows["TCS.NS"]["last_price"] == pytest.approx(102.49)


def test_upload_preview_maps_tickers_and_reports_unknowns_without_writing(client, lake):
    p = client.post("/admin/api/universe/preview", json={"tickers": "Symbol\nnse:sbin\nRELIANCE.NS\nM&M\nRELIANC\nFOOBAR\n", "mode": "replace"},
                    auth=A, headers=H).json()
    assert [r["symbol"] for r in p["india"]] == ["M&M.NS", "RELIANCE.NS", "SBIN.NS"]
    assert p["added"] == ["M&M.NS", "SBIN.NS"] and p["removed"] == ["INFY.NS", "TCS.NS"] and p["kept"] == ["RELIANCE.NS"]
    assert {u["ticker"]: u["suggestions"] for u in p["unknown"]} == {"RELIANC": ["RELIANCE"], "FOOBAR": []}
    assert not (lake.data / "universe.local.csv").exists()


def test_apply_writes_the_local_list_keeps_other_markets_and_can_be_undone(client, lake):
    r = client.post("/admin/api/universe/apply", json={"tickers": ["SBIN", "BAJAJ-AUTO", "NOPE"], "mode": "add"}, auth=A, headers=H).json()
    assert r["india"] == 5 and r["added"] == ["BAJAJ-AUTO.NS", "SBIN.NS"] and r["skipped"] == ["NOPE"]
    assert universe_files.active_universe_csv().name == "universe.local.csv"
    syms = {x["symbol"] for x in universe_files.read_universe(None)}
    assert {"SBIN.NS", "BAJAJ-AUTO.NS", "AAPL", "TCS.NS"} <= syms                # US row untouched
    assert universe_files.UNIVERSE_CSV.read_text().count(",IN,") == 3              # the tracked file is never edited
    hist = client.get("/admin/api/universe", auth=A).json()["history"]
    assert len(hist) == 1 and hist[0]["india"] == 3
    client.post("/admin/api/universe/restore", json={"name": hist[0]["name"]}, auth=A, headers=H)
    assert {x["symbol"] for x in universe_files.read_universe("IN")} == {"RELIANCE.NS", "TCS.NS", "INFY.NS"}
    assert client.post("/admin/api/universe/restore", json={"name": "../../etc/passwd"}, auth=A, headers=H).status_code == 404


def test_apply_refuses_a_list_with_nothing_mappable_and_bad_input(client):
    assert client.post("/admin/api/universe/apply", json={"tickers": ["NOPE"]}, auth=A, headers=H).status_code == 422
    assert client.post("/admin/api/universe/preview", json={"tickers": []}, auth=A, headers=H).status_code == 422
    assert client.post("/admin/api/universe/preview", json={"tickers": ["TCS"], "mode": "merge"}, auth=A, headers=H).status_code == 422


# -- building blocks -------------------------------------------------------------------------------------
def test_ticker_text_parsing_and_normalising():
    assert uni.parse_ticker_text("Ticker,weight\n reliance ,1\n\"TCS\";x\nNSE:INFY\nHDFCBANK-EQ\nreliance\n\t\n") == \
        ["RELIANCE", "TCS", "INFY", "HDFCBANK"]
    assert uni.normalise_ticker("NSE_EQ|SBIN") == "SBIN" and uni.normalise_ticker("symbol") == ""


def test_mapping_without_the_instrument_file_uses_the_current_list(lake):
    m = uni.map_tickers(["TCS", "SBIN", "bad ticker!"], pd.DataFrame(), universe_files.read_universe(None))
    assert [r["symbol"] for r in m["rows"]] == ["TCS.NS"] and m["unknown"][0]["ticker"] == "SBIN" and m["invalid"] == ["bad ticker!"]
    with pytest.raises(ValueError):
        uni.plan_india([], [], "merge")


def test_equities_parsed_from_the_instrument_file_prefer_the_eq_series():
    recs = EQ_RECORDS + [{"segment": "NSE_EQ", "instrument_type": "BE", "trading_symbol": "TCS", "instrument_key": "NSE_EQ|INE467B01029"},
                         {"segment": "NSE_EQ", "trading_symbol": "NOKEY"},
                         {"segment": "NSE_FO", "instrument_type": "FUT", "trading_symbol": "X"},
                         {"segment": "NSE_EQ", "instrument_type": "EQ", "trading_symbol": "noisin", "instrument_key": "NSE_EQ|INE000000001"}]
    df = fo.parse_equities(recs)
    assert list(df.columns) == fo.EQ_COLUMNS and df.symbol.is_unique
    assert df.set_index("symbol").loc["TCS", "instrument_type"] == "EQ"
    assert df.set_index("symbol").loc["NOISIN", "isin"] == "INE000000001"       # ISIN taken from the key
    assert fo.parse_equities([]).empty


def test_fo_import_also_stores_the_equity_table(world, tmp_path, capsys):
    import json
    from tests.unit.test_fo_instruments import master_records
    f = tmp_path / "NSE.json"
    f.write_text(json.dumps(master_records() + EQ_RECORDS))
    fo.main(["import", "--file", str(f), "--asof", "2026-10-23"])
    assert "stored 6 NSE equities" in capsys.readouterr().out and len(fo.load_eq_table()) == 6


def test_ticks_compaction_does_not_store_the_partition_folder_as_a_column(world):
    tick_store.write_ticks([Tick(T0, "A.NS", 1.0, 1)], world.lakedir)
    tick_store.write_ticks([Tick(T0 + timedelta(seconds=1), "A.NS", 1.1, 2)], world.lakedir)
    assert tick_store.compact_ticks_day(DAY, world.lakedir) == 2
    f = world.lakedir / "ticks/segment=eq/date=2026-10-23/ticks.parquet"
    assert "date" not in pq.ParquetFile(f).schema_arrow.names
    # a file compacted by the earlier code (with a stored date column) loses it on the next compaction
    pd.read_parquet(f).assign(date=pd.Timestamp("2026-10-23")).to_parquet(f)
    tick_store.write_ticks([Tick(T0 + timedelta(seconds=2), "A.NS", 1.2, 3)], world.lakedir)
    assert tick_store.compact_ticks_day(DAY, world.lakedir) == 3 and "date" not in pq.ParquetFile(f).schema_arrow.names


def test_feed_rereads_the_subscription_list_at_each_session(monkeypatch, tmp_path):
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", "TOK")
    monkeypatch.chdir(tmp_path)
    lists = [{"NSE_EQ|A": "A.NS"}, {"NSE_EQ|A": "A.NS", "NSE_EQ|B": "B.NS"}, {}]
    calls = iter(lists)

    def fake_keys(seg, fo_file=None):
        v = next(calls)
        if v == {}:
            raise SystemExit("file gone")
        return v
    seen = []

    async def fake_run_forever(make, once):
        seen.extend(sorted(make().keys) for _ in range(2))      # two sessions: second picks up the new list
    monkeypatch.setattr(feed, "segment_key_map", fake_keys)
    monkeypatch.setattr(feed, "run_forever", fake_run_forever)
    monkeypatch.setattr(sys, "argv", ["feed"])
    feed.main()
    assert seen == [["NSE_EQ|A", "NSE_EQ|B"], ["NSE_EQ|A", "NSE_EQ|B"]]      # third read failed: last good list kept


# -- a vendor-neutral catalog --------------------------------------------------------------------------------------
def write_vendor_files(lake_dir):
    """What the Yahoo and Upstox fundamentals jobs store, in their own column layouts."""
    yf, up = lake_dir / "yf_fundamentals", lake_dir / "upstox_fundamentals"
    yf.mkdir(parents=True), up.mkdir(parents=True)
    d = date(2026, 11, 10)
    pd.DataFrame([{"symbol": "AAPL", "statement": "income", "item": "revenue", "period_end": date(2025, 9, 30), "value": 391.0, "currency": "USD",
                   "first_seen": d, "last_seen": d}]).to_parquet(yf / "versions.parquet")
    pd.DataFrame([{"isin": "INE002A01018", "endpoint": "ratios", "item": "pe", "period": "2025-03", "value": 24.5, "text": None, "change": 1.2,
                   "first_seen": d, "last_seen": d}]).to_parquet(up / "versions.parquet")
    pd.DataFrame([{"symbol": "AAPL", "part": "income", "fetched_at": datetime(2026, 11, 10, 1), "ok": True, "error_class": "ok", "rows": 12,
                   "attempts": 1, "latency_s": 0.4, "message": ""}]).to_parquet(yf / "fetch_log.parquet")
    pd.DataFrame([{"isin": "INE002A01018", "symbol": "RELIANCE.NS", "endpoint": "ratios", "fetched_at": datetime(2026, 11, 10, 12), "ok": False, "status": 429,
                   "error_code": "UDAPI100072", "error_message": "too many requests", "attempts": 3, "latency_s": 1.1, "rows": 0}]).to_parquet(up / "fetch_log.parquet")


def test_table_names_never_carry_a_vendor_and_every_table_names_its_vendor():
    for t in admin.CATALOG:
        assert not any(v in t.name.lower() for v in ("yahoo", "yf_", "upstox", "refinitiv", "thomson")), t.name
        assert t.vendor, t.name
    assert {v for t in admin.CATALOG for v in t.vendor.replace("·", ",").split(",") if v.strip()} >= {"Upstox", "Yahoo", "Internal"}


def test_vendors_with_different_layouts_share_one_table_with_a_vendor_column(client, lake):
    write_vendor_files(lake.lakedir)
    items = client.get("/admin/api/table", params={"service": "fundamentals", "name": "items"}, auth=A).json()
    assert items["total"] == 2 and items["columns"][:2] == ["vendor", "symbol"]
    by = {r[0]: dict(zip(items["columns"], r)) for r in items["rows"]}
    assert by["Yahoo"]["symbol"] == "AAPL" and by["Yahoo"]["isin"] is None and by["Yahoo"]["section"] == "income" and by["Yahoo"]["period"] == "2025-09-30"
    assert by["Upstox"]["isin"] == "INE002A01018" and by["Upstox"]["symbol"] is None and by["Upstox"]["change"] == 1.2
    log = client.get("/admin/api/table", params={"service": "operations", "name": "fetch_log"}, auth=A).json()
    rows = {r[0]: dict(zip(log["columns"], r)) for r in log["rows"]}
    assert log["total"] == 2 and rows["Yahoo"]["endpoint"] == "income" and rows["Upstox"]["error_class"] == "UDAPI100072" and rows["Upstox"]["status"] == 429
    assert client.get("/admin/api/table", params={"service": "fundamentals", "name": "items", "symbol": "AAPL"}, auth=A).json()["total"] == 1
    r = q(client, "SELECT vendor, count(*) AS n FROM fundamentals.items GROUP BY 1 ORDER BY 1").json()
    assert r["rows"] == [["Upstox", 1], ["Yahoo", 1]]
    assert "operations.fetch_log" in client.get("/admin/api/schema", auth=A).json()


def test_the_catalog_lists_each_tables_vendors_and_works_with_one_vendor_missing(client, lake):
    cat = client.get("/admin/api/catalog", auth=A).json()
    tables = {(s["service"], t["name"]): t for s in cat for t in s["tables"]}
    assert tables[("fundamentals", "items")]["vendors"] == ["Upstox", "Yahoo"] and tables[("market", "ticks")]["vendors"] == ["Upstox"]
    assert not tables[("fundamentals", "items")]["available"]                                  # no vendor file yet
    write_vendor_files(lake.lakedir)
    (lake.lakedir / "yf_fundamentals" / "versions.parquet").unlink()                           # only Upstox has stored anything
    one = client.get("/admin/api/table", params={"service": "fundamentals", "name": "items"}, auth=A).json()
    assert one["total"] == 1 and one["rows"][0][0] == "Upstox"


# -- jobs (Autosys style) ---------------------------------------------------------------------------
def jobs_world(lake):
    root = lake.root
    (root / "deploy").mkdir(exist_ok=True)
    (root / "deploy" / "crontab").write_text(
        "0 1 * * * cd $JARVIS && python -m data_layer.jobrun run fx -- python -m data_layer.fx refresh >> logs/fx.log 2>&1\n"
        "30 2 * * * cd $JARVIS && python -m data_layer.health >> logs/health.log 2>&1\n"
        "0 3 * * * cd $JARVIS && python -m data_layer.rates daily\n")
    (root / "logs").mkdir(exist_ok=True)
    (root / "logs" / "fx.log").write_text("ok line\ntoken=SECRETVALUE\n")
    (root / ".env").write_text("UPSTOX_FEED_TOKEN=abc\n")
    from data_layer import jobrun
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    t = (now - timedelta(minutes=30)).isoformat(timespec="seconds")
    p = jobrun.runs_path(root)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"ev": "start", "job": "fx", "id": "a", "at": t, "pid": 1}) + "\n"
                 + json.dumps({"ev": "end", "job": "fx", "id": "a", "at": t, "code": 2, "secs": 4.0}) + "\n")


def test_jobs_listing_states_and_counts(client, lake):
    jobs_world(lake)
    body = client.get("/admin/api/jobs", auth=A).json()
    by = {j["name"]: j for j in body["jobs"]}
    assert set(by) == {"fx", "health", "rates"}
    assert by["fx"]["state"] == "failed" and by["fx"]["exit_code"] == 2 and by["fx"]["next_run"]
    assert by["health"]["state"] == "untracked" and by["rates"]["log"] is None
    assert body["counts"]["failed"] == 1 and body["counts"]["untracked"] == 2


def test_job_log_tail_is_redacted_and_only_for_crontab_jobs(client, lake):
    jobs_world(lake)
    r = client.get("/admin/api/jobs/log", params={"name": "fx", "lines": 5}, auth=A)
    assert r.status_code == 200 and "ok line" in r.json()["text"] and "SECRETVALUE" not in r.json()["text"]
    assert client.get("/admin/api/jobs/log", params={"name": "../.env"}, auth=A).status_code == 404       # not a crontab job
    assert client.get("/admin/api/jobs/log", params={"name": "rates"}, auth=A).status_code == 404         # writes no log
    assert client.get("/admin/api/jobs/log", params={"name": "health"}, auth=A).status_code == 404         # log file not there yet
    assert client.get("/admin/api/jobs/log", params={"name": "fx", "lines": 100000}, auth=A).status_code == 422


def test_jobs_need_the_login(client):
    assert client.get("/admin/api/jobs").status_code == 401 and client.get("/admin/api/jobs/log", params={"name": "fx"}).status_code == 401


# -- job control (SCRUM-204) ----------------------------------------------------------------------------
H = {"x-jarvis-admin": "1"}


def control_world(lake, monkeypatch, on=True):
    root = lake.root
    monkeypatch.setenv("JARVIS_ROOT", str(root))
    if on:
        monkeypatch.setenv("JARVIS_JOB_CONTROL", "1")
    else:
        monkeypatch.delenv("JARVIS_JOB_CONTROL", raising=False)
    (root / "deploy").mkdir(exist_ok=True)
    (root / "logs").mkdir(exist_ok=True)
    (root / "deploy" / "crontab").write_text(
        "0 1 * * * cd $JARVIS && python -m data_layer.jobrun run quick --shell 'echo done >> logs/quick.log 2>&1'\n"
        "0 2 * * * cd $JARVIS && python -m data_layer.jobrun run slow --shell 'sleep 30 >> logs/slow.log 2>&1'\n"
        "0 3 * * * cd $JARVIS && python -m data_layer.fx refresh >> logs/fx.log 2>&1\n")


def wait_state(client, name, want, secs=10):
    import time
    end = time.time() + secs
    while time.time() < end:
        s = {j["name"]: j["state"] for j in client.get("/admin/api/jobs", auth=A).json()["jobs"]}
        if s.get(name) in want:
            return s[name]
        time.sleep(0.2)
    return s.get(name)


def test_job_control_is_off_unless_enabled(client, lake, monkeypatch):
    control_world(lake, monkeypatch, on=False)
    assert client.get("/admin/api/jobs", auth=A).json()["control"] is False
    r = client.post("/admin/api/jobs/start", json={"name": "quick"}, auth=A, headers=H)
    assert r.status_code == 403 and "JARVIS_JOB_CONTROL" in r.json()["detail"]


def test_job_control_needs_login_and_the_admin_header(client, lake, monkeypatch):
    control_world(lake, monkeypatch)
    assert client.post("/admin/api/jobs/start", json={"name": "quick"}, headers=H).status_code == 401
    assert client.post("/admin/api/jobs/start", json={"name": "quick"}, auth=A).status_code == 403          # no x-jarvis-admin header
    assert client.post("/admin/api/jobs/stop", json={"name": "quick"}, auth=A).status_code == 403


def test_start_a_job_from_the_api_and_refuse_bad_requests(client, lake, monkeypatch):
    control_world(lake, monkeypatch)
    assert client.get("/admin/api/jobs", auth=A).json()["control"] is True
    r = client.post("/admin/api/jobs/start", json={"name": "quick"}, auth=A, headers=H)
    assert r.status_code == 200 and r.json()["action"] == "start"
    assert wait_state(client, "quick", ("ok",)) == "ok"
    assert (lake.root / "logs" / "quick.log").read_text().strip() == "done"
    assert client.post("/admin/api/jobs/start", json={"name": "nope"}, auth=A, headers=H).status_code == 404
    assert client.post("/admin/api/jobs/start", json={"name": "../../bin/sh"}, auth=A, headers=H).status_code == 404
    assert client.post("/admin/api/jobs/start", json={"name": "fx"}, auth=A, headers=H).status_code == 409        # not wrapped
    assert client.post("/admin/api/jobs/stop", json={"name": "quick"}, auth=A, headers=H).status_code == 409      # not running


def test_start_then_stop_a_running_job(client, lake, monkeypatch):
    control_world(lake, monkeypatch)
    assert client.post("/admin/api/jobs/start", json={"name": "slow"}, auth=A, headers=H).status_code == 200
    assert wait_state(client, "slow", ("running",)) == "running"
    assert client.post("/admin/api/jobs/start", json={"name": "slow"}, auth=A, headers=H).status_code == 409      # already running
    assert client.post("/admin/api/jobs/stop", json={"name": "slow"}, auth=A, headers=H).status_code == 200
    assert wait_state(client, "slow", ("stopped",)) == "stopped"
