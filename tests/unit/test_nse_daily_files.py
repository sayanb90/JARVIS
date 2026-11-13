"""NSE daily market files (SCRUM-187): one parser per file on real (trimmed) files, checks, storage and the shared run."""

from datetime import date
from pathlib import Path

import pytest

from data_layer import nse_daily_files as n
from data_layer.nse_archive import OlderFile, ParseError

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "market_files"


def body(name: str) -> bytes:
    return (FIX / name).read_bytes()


# ---------------------------------------------------------------------------------------------- delivery
def test_delivery_parses_and_converts_units():
    df = n.parse_delivery(body("sec_bhavdata_full_12112026.csv"), date(2026, 11, 12))
    assert len(df) == 50 and (df["date"] == date(2026, 11, 12)).all()
    r = df[df["symbol"] == "20MICRONS"].iloc[0]
    assert r["series"] == "EQ" and r["volume"] == 90907 and r["delivery_qty"] == 38434 and r["delivery_pct"] == pytest.approx(42.28)
    assert r["turnover"] == pytest.approx(173.10e5)
    be = df[df["symbol"] == "21STCENMGM"].iloc[0]
    assert be["series"] == "BE" and be["delivery_qty"] is not None and str(be["delivery_qty"]) == "<NA>"


def test_delivery_file_with_another_days_data_is_rejected():
    # NSE's file named 30-Sep-2019 holds 27-Jun-2019's prices: the date check is what keeps it out
    with pytest.raises(OlderFile, match="27"):
        n.parse_delivery(body("sec_bhavdata_full_30092019.csv"), date(2019, 9, 30))


def test_a_holiday_served_with_the_previous_days_delivery_file_counts_as_missing(tmp_path):
    fake = lambda url: (200, body("sec_bhavdata_full_12112026.csv"))           # noqa: E731 -- what NSE does for 2026-10-02
    st = n.run("delivery", [date(2026, 11, 13)], lake_dir=tmp_path, fetch=fake, sleep_s=0, today=date(2026, 11, 13), sleeper=lambda s: None)
    assert st["holiday"] == 1 and st["bad"] == 0 and not n.table_path("delivery", tmp_path).exists()


def test_delivery_above_volume_is_rejected():
    bad = body("sec_bhavdata_full_12112026.csv").replace(b"90907, 173.10, 2783, 38434", b"90907, 173.10, 2783, 98434")
    with pytest.raises(ParseError, match="delivery above volume"):
        n.parse_delivery(bad, date(2026, 11, 12))


# ---------------------------------------------------------------------------------------------- ban list
@pytest.mark.parametrize("name,day,symbols", [
    ("fo_secban_12112026.csv", date(2026, 11, 12), ["KAYNES"]),
    ("fo_secban_02012015.csv", date(2015, 1, 2), []),                     # "...: NIL"
    ("fo_secban_02012012.csv", date(2012, 1, 2), ["RUCHISOYA"]),          # "02-January-2012"
])
def test_ban_list(name, day, symbols):
    assert list(n.parse_ban_list(body(name), day)["symbol"]) == symbols


def test_ban_list_wrong_date_is_rejected():
    with pytest.raises(ParseError, match="file is for"):
        n.parse_ban_list(body("fo_secban_12112026.csv"), date(2026, 11, 11))


# ---------------------------------------------------------------------------------------------- combined OI / MWPL
@pytest.mark.parametrize("name,day,fut_eq", [
    ("combineoi_12112026.zip", date(2026, 11, 12), True),
    ("combineoi_02012015.zip", date(2015, 1, 2), False),
    ("combineoi_02012012.zip", date(2012, 1, 2), False),
])
def test_combined_oi(name, day, fut_eq):
    df = n.parse_combined_oi(body(name), day)
    assert len(df) == 50 and (df["date"] == day).all()
    assert df["future_equivalent_oi"].notna().all() == fut_eq
    assert df.loc[df["no_fresh_positions"], "limit_next_day"].isna().all()
    assert df.loc[~df["no_fresh_positions"], "limit_next_day"].notna().all()


def test_combined_oi_values_and_wrong_date():
    df = n.parse_combined_oi(body("combineoi_12112026.zip"), date(2026, 11, 12)).set_index("symbol")
    assert df.loc["360ONE", "mwpl"] == 57218515 and df.loc["360ONE", "limit_next_day"] == 49811431 and df.loc["KAYNES", "isin"] == "INE918Z01012"
    with pytest.raises(OlderFile):                                  # an earlier day's file: treated as not published / holiday
        n.parse_combined_oi(body("combineoi_12112026.zip"), date(2026, 11, 13))
    with pytest.raises(ParseError, match="file is for"):
        n.parse_combined_oi(body("combineoi_12112026.zip"), date(2026, 11, 11))


# ---------------------------------------------------------------------------------------------- short selling
@pytest.mark.parametrize("name,day,rows,trade_day", [
    ("shortselling_13112026.csv", date(2026, 11, 13), 105, date(2026, 11, 12)),
    ("shortselling_11112026.csv", date(2026, 11, 11), 198, date(2026, 11, 9)),
    ("shortselling_02012023.csv", date(2023, 1, 2), 4, date(2022, 12, 26)),
    ("shortselling_02012020.csv", date(2020, 1, 2), 1, date(2019, 12, 26)),   # older header: "Security Symbol", no quotes
    ("shortselling_02012015.csv", date(2015, 1, 2), 0, None),                 # "No records."
])
def test_short_selling(name, day, rows, trade_day):
    df = n.parse_short_selling(body(name), day)
    assert len(df) == rows
    if rows:
        assert set(df["date"]) == {trade_day} and (df["report_date"] == day).all() and (df["quantity"] > 0).all()


def test_short_selling_trades_after_the_report_are_rejected():
    with pytest.raises(ParseError, match="trade dates"):
        n.parse_short_selling(body("shortselling_13112026.csv"), date(2026, 11, 10))


# ---------------------------------------------------------------------------------------------- bulk and block deals
def test_bulk_and_block_deals():
    bulk = n.parse_deals(body("bulk_2026-11-12.csv"), recent_from=date(2026, 11, 3))
    assert len(bulk) == 50 and set(bulk["date"]) == {date(2026, 11, 12)} and set(bulk["side"]) <= {"BUY", "SELL"}
    block = n.parse_deals(body("block_2026-11-12.csv"), date(2026, 11, 12))
    assert list(block["side"]) == ["BUY", "SELL"] and (block["quantity"] == 4000000).all() and (block["price"] == 503.0).all()


def test_stale_last_session_file_is_rejected():
    with pytest.raises(ParseError, match="not one recent weekday"):
        n.parse_deals(body("bulk_2026-11-12.csv"), recent_from=date(2026, 11, 20))


def test_website_history_download_imports_with_its_own_headers(tmp_path):
    hist = ('"Date","Symbol","Security Name","Client Name","Buy / Sell","Quantity Traded","Trade Price / Wght. Avg. Price","Remarks"\n'
            '"10-Nov-2026","ABC","Abc Ltd","X LLP","BUY","1,00,000","12.50","-"\n'
            '"11-Nov-2026","ABC","Abc Ltd","X LLP","SELL","50,000","13.00","-"\n')
    p = tmp_path / "bulk-history.csv"
    p.write_text(hist)
    assert n.import_file("bulk_deals", p, lake_dir=tmp_path) == 2
    df = n.load("bulk_deals", tmp_path)
    assert list(df["quantity"]) == [100000, 50000] and set(df["date"]) == {date(2026, 11, 10), date(2026, 11, 11)}


def test_urls():
    d = date(2026, 11, 12)
    assert n.FILES["delivery"].url(d).endswith("/products/content/sec_bhavdata_full_12112026.csv")
    assert n.FILES["ban_list"].url(d).endswith("/archives/fo/sec_ban/fo_secban_12112026.csv")
    assert n.FILES["combined_oi"].url(d).endswith("/archives/nsccl/mwpl/combineoi_12112026.zip")
    assert n.FILES["short_selling"].url(d).endswith("/archives/equities/shortSelling/shortselling_12112026.csv")
    assert n.FILES["bulk_deals"].url(d).endswith("/content/equities/bulk.csv")


# ---------------------------------------------------------------------------------------------- runs
class FakeNSE:
    SERVE = {"sec_bhavdata_full_12112026": "sec_bhavdata_full_12112026.csv", "fo_secban_12112026": "fo_secban_12112026.csv",
             "fo_secban_02012015": "fo_secban_02012015.csv", "combineoi_12112026": "combineoi_12112026.zip",
             "shortselling_13112026": "shortselling_13112026.csv", "shortselling_02012015": "shortselling_02012015.csv",
             "bulk.csv": "bulk_2026-11-12.csv", "block.csv": "block_2026-11-12.csv"}

    def __init__(self, status: int = 0):
        self.calls: list[str] = []
        self.status = status

    def __call__(self, url: str):
        self.calls.append(url)
        if self.status:
            return self.status, b""
        for k, v in self.SERVE.items():
            if k in url:
                return 200, body(v)
        return 404, b""


def run(tmp_path, name, days, fake, **kw):
    return n.run(name, days, lake_dir=tmp_path, fetch=fake, sleep_s=0, today=date(2026, 11, 13), sleeper=lambda s: None, **kw)


def test_run_each_dated_file_and_resume(tmp_path):
    fake = FakeNSE()
    for name in ("delivery", "ban_list", "combined_oi"):
        st = run(tmp_path, name, [date(2026, 11, 12)], fake)
        assert st["stored"] == 1 and st["calls"] == 1, name
        assert len(n.load(name, tmp_path)) > 0
        assert run(tmp_path, name, [date(2026, 11, 12)], fake)["calls"] == 0
    st = run(tmp_path, "short_selling", [date(2026, 11, 13)], fake)
    assert st["stored"] == 1 and set(n.load("short_selling", tmp_path)["date"]) == {date(2026, 11, 12)}
    assert run(tmp_path, "short_selling", [date(2026, 11, 13)], fake)["calls"] == 0          # done by report date


def test_days_with_no_rows_are_remembered_as_empty(tmp_path):
    fake = FakeNSE()
    run(tmp_path, "ban_list", [date(2015, 1, 2)], fake)
    run(tmp_path, "short_selling", [date(2015, 1, 2)], fake)
    assert not n.table_path("ban_list", tmp_path).exists()
    assert run(tmp_path, "ban_list", [date(2015, 1, 2)], fake)["calls"] == 0
    assert run(tmp_path, "short_selling", [date(2015, 1, 2)], fake)["calls"] == 0
    assert n.status(tmp_path)["ban_list"]["days"] == 1 and n.status(tmp_path)["ban_list"]["empty_days"] == 1


def test_holidays_and_first_day(tmp_path):
    fake = FakeNSE()
    st = run(tmp_path, "delivery", [date(2019, 9, 27), date(2026, 10, 2)], fake)
    assert st["calls"] == 1 and st["holiday"] == 1                  # nothing before the delivery archive starts is asked for
    assert run(tmp_path, "delivery", [date(2026, 10, 2)], fake)["calls"] == 0


def test_refusal_page_counts_as_an_error_and_stops_the_run(tmp_path):
    st = run(tmp_path, "combined_oi", n.weekdays(date(2026, 9, 1), date(2026, 9, 30)), FakeNSE(status=-1))
    assert "stopped" in st and st["calls"] == 5 and "refused" in st["stopped"]


def test_last_session_deals(tmp_path):
    fake = FakeNSE()
    st = run(tmp_path, "bulk_deals", [], fake)
    assert st["stored"] == 1 and len(n.load("bulk_deals", tmp_path)) == 50
    run(tmp_path, "bulk_deals", [], fake)                            # the same file again: upserted, not doubled
    assert len(n.load("bulk_deals", tmp_path)) == 50
    assert run(tmp_path, "block_deals", [], fake)["stored"] == 1


def test_shared_limit_and_status(tmp_path):
    from data_layer.nse_archive import Polite
    polite = Polite(FakeNSE(), 0)
    n.run("ban_list", n.weekdays(date(2026, 9, 1), date(2026, 9, 30)), lake_dir=tmp_path, today=date(2026, 11, 13), limit=3, polite=polite)
    assert polite.calls == 3
    run(tmp_path, "delivery", [date(2026, 11, 12)], FakeNSE())
    s = n.status(tmp_path)
    assert s["delivery"]["days"] == 1 and s["delivery"]["bytes"] > 0 and s["ban_list"]["holidays_known"] == 3
