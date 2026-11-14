"""NSE F&O bhavcopy (SCRUM-184): both formats parsed from real (trimmed) files onto one schema, checks, per-day storage and the run."""

import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from data_layer import eod_fo as e
from data_layer.nse_archive import ParseError

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "eod_fo"


def body(name: str) -> bytes:
    return (FIX / name).read_bytes()


def csv_text(name: str) -> str:
    with zipfile.ZipFile(FIX / name) as z:
        return z.read(z.namelist()[0]).decode()


@pytest.mark.parametrize("name,day,fmt", [
    ("new_2026-11-12.csv.zip", date(2026, 11, 12), "udiff"),
    ("new_2024-07-05.csv.zip", date(2024, 7, 5), "udiff"),
    ("old_2024-07-05.csv.zip", date(2024, 7, 5), "legacy"),
    ("old_2015-01-02.csv.zip", date(2015, 1, 2), "legacy"),
])
def test_both_formats_parse_to_one_schema(name, day, fmt):
    df = e.parse(body(name), day)
    assert list(df.columns) == e.COLUMNS
    assert (df["date"] == day).all() and (df["source_format"] == fmt).all()
    assert set(df["option_type"]) <= {"CE", "PE", "XX"}
    assert df.loc[df["instrument_type"].str.startswith("FUT"), "strike"].isna().all()
    assert (df.loc[df["instrument_type"].str.startswith("FUT"), "option_type"] == "XX").all()
    assert ((df["instrument_type"] == "FUTIDX") & (df["symbol"] == "NIFTY")).any()


def test_new_format_values_and_type_mapping():
    df = e.parse(body("new_2026-11-12.csv.zip"), date(2026, 11, 12))
    assert set(df["instrument_type"]) == {"OPTSTK", "FUTIDX"}
    row = df[(df["symbol"] == "ABCAPITAL") & (df["strike"] == 360.0) & (df["option_type"] == "CE")].iloc[0]
    assert row["expiry"] == date(2026, 11, 23) and row["volume"] == 23 and row["oi"] == 647900 and row["oi_change"] == -4650
    assert row["lot_size"] == 1550 and row["underlying_price"] == pytest.approx(419.43) and row["turnover"] == pytest.approx(14684467.50)
    assert row["settle"] == pytest.approx(60.08) and row["trades"] == 17


def test_old_and_new_files_of_the_overlap_day_agree():
    a = e.parse(body("new_2024-07-05.csv.zip"), date(2024, 7, 5))
    b = e.parse(body("old_2024-07-05.csv.zip"), date(2024, 7, 5))
    m = a.merge(b, on=e.KEY, suffixes=("_new", "_old"))
    assert len(m) >= 4                                         # the NIFTY futures and the 24000 call are in both trimmed files
    assert (m["volume_new"] == m["volume_old"]).all() and (m["oi_new"] == m["oi_old"]).all()
    assert (m["turnover_new"] - m["turnover_old"]).abs().max() < 1e4          # the old file rounds to a thousandth of a lakh
    assert (m["settle_new"] - m["settle_old"]).abs().max() < 0.01
    fut = m[(m["instrument_type"] == "FUTIDX") & (m["expiry"] == date(2024, 7, 25))].iloc[0]
    assert fut["volume_new"] == 230381 and fut["oi_new"] == 14171325


def test_old_format_has_no_underlying_or_lot_size_and_turnover_in_rupees():
    df = e.parse(body("old_2015-01-02.csv.zip"), date(2015, 1, 2))
    assert df["underlying_price"].isna().all() and df["lot_size"].isna().all()
    bank = df[(df["symbol"] == "BANKNIFTY") & (df["expiry"] == date(2015, 1, 29))].iloc[0]
    assert bank["turnover"] == pytest.approx(527447.85e5) and bank["volume"] == 110103
    assert "FUTIVX" in set(df["instrument_type"])


def test_wrong_date_is_rejected():
    with pytest.raises(ParseError, match="file is for"):
        e.parse(body("new_2026-11-12.csv.zip"), date(2026, 11, 11))
    with pytest.raises(ParseError, match="file is for"):
        e.parse(body("old_2015-01-02.csv.zip"), date(2015, 1, 5))


def test_missing_column_duplicates_and_no_index_future_are_rejected():
    text = csv_text("new_2026-11-12.csv.zip")
    with pytest.raises(ParseError, match="missing columns"):
        e.parse(text.replace("SttlmPric", "Settle").encode(), date(2026, 11, 12))
    lines = text.splitlines(keepends=True)
    with pytest.raises(ParseError, match="more than once"):
        e.parse("".join(lines + [lines[1]]).encode(), date(2026, 11, 12))
    with pytest.raises(ParseError, match="NIFTY index future"):
        e.parse("".join(l for l in lines if ",IDF," not in l).encode(), date(2026, 11, 12))


def test_urls_and_format_choice():
    assert e.new_url(date(2026, 11, 12)).endswith("BhavCopy_NSE_FO_0_0_0_20261112_F_0000.csv.zip")
    assert e.old_url(date(2023, 1, 2)).endswith("/DERIVATIVES/2023/JAN/fo02JAN2023bhav.csv.zip")
    assert [f for f, _ in e.urls_for(date(2023, 12, 29))] == ["legacy"]
    assert [f for f, _ in e.urls_for(date(2024, 3, 1))] == ["udiff", "legacy"]
    assert [f for f, _ in e.urls_for(date(2024, 7, 8))] == ["udiff"]


class FakeNSE:
    """Serves the 2026-11-12 and 2015-01-02 fixtures; the new file for 2024-07-05 is 'missing' so the old one is used; 404 otherwise."""

    def __init__(self, fail: bool = False):
        self.calls: list[str] = []
        self.fail = fail

    def __call__(self, url: str):
        self.calls.append(url)
        if self.fail:
            return 503, b""
        if "20261112" in url:
            return 200, body("new_2026-11-12.csv.zip")
        if "fo02JAN2015" in url:
            return 200, body("old_2015-01-02.csv.zip")
        if "fo05JUL2024" in url:
            return 200, body("old_2024-07-05.csv.zip")
        return 404, b""


def run(tmp_path, days, fake, **kw):
    return e.run(days, lake_dir=tmp_path, fetch=fake, sleep_s=0, today=date(2026, 11, 13), sleeper=lambda s: None, **kw)


def test_run_stores_one_file_per_day_and_is_resumable(tmp_path):
    fake = FakeNSE()
    st = run(tmp_path, [date(2026, 11, 12), date(2015, 1, 2)], fake)
    assert st["stored"] == 2 and st["calls"] == 2
    assert e.day_path(date(2026, 11, 12), tmp_path).exists() and e.day_path(date(2015, 1, 2), tmp_path).exists()
    assert e.stored_days(tmp_path) == [date(2015, 1, 2), date(2026, 11, 12)]
    both = e.read(tmp_path)
    assert set(both["source_format"]) == {"udiff", "legacy"}
    assert len(e.read(tmp_path, first=date(2026, 1, 1))) == len(e.parse(body("new_2026-11-12.csv.zip"), date(2026, 11, 12)))
    st2 = run(tmp_path, [date(2026, 11, 12), date(2015, 1, 2)], fake)
    assert st2["calls"] == 0 and st2["skipped"] == 2


def test_overlap_day_falls_back_to_the_old_file(tmp_path):
    fake = FakeNSE()
    st = run(tmp_path, [date(2024, 7, 5)], fake)
    assert st["stored"] == 1 and st["calls"] == 2 and "BhavCopy" in fake.calls[0] and "fo05JUL2024" in fake.calls[1]
    assert set(e.read(tmp_path)["source_format"]) == {"legacy"}


def test_holiday_is_remembered_and_final_when_old(tmp_path):
    fake = FakeNSE()
    st = run(tmp_path, [date(2026, 10, 2)], fake)
    assert st["holiday"] == 1 and st["calls"] == 1
    assert run(tmp_path, [date(2026, 10, 2)], fake)["calls"] == 0
    run(tmp_path, [date(2026, 11, 11)], fake)
    assert run(tmp_path, [date(2026, 11, 11)], fake)["calls"] == 1        # recent: retried, NSE may publish late


def test_bad_file_is_reported_not_stored(tmp_path):
    st = e.run([date(2026, 11, 11)], lake_dir=tmp_path, fetch=lambda u: (200, body("new_2026-11-12.csv.zip")), sleep_s=0,
               today=date(2026, 11, 13), sleeper=lambda s: None)
    assert st["bad"] == 1 and not e.stored_days(tmp_path)
    assert "2026-11-11" in e.status(tmp_path)["bad"]


def test_run_stops_after_repeated_errors_and_limit_caps_requests(tmp_path):
    days = e.weekdays(date(2026, 10, 1), date(2026, 10, 31))
    st = run(tmp_path, days, FakeNSE(fail=True))
    assert "stopped" in st and st["calls"] == 5
    st = run(tmp_path, days, FakeNSE(), limit=3)
    assert st["calls"] == 3


def test_import_and_status(tmp_path):
    src = tmp_path / "manual.zip"
    src.write_bytes(body("new_2026-11-12.csv.zip"))
    n = e.import_file(src, date(2026, 11, 12), tmp_path)
    s = e.status(tmp_path)
    assert n > 0 and s["days"] == 1 and s["bytes"] > 0 and s["bytes_per_day"] == s["bytes"] and s["last"] == "2026-11-12"
    assert isinstance(pd.read_parquet(e.day_path(date(2026, 11, 12), tmp_path))["expiry"].iloc[0], date)


def test_admin_catalog_reads_the_day_files(tmp_path):
    from webapp import admin
    e.run([date(2026, 11, 12), date(2015, 1, 2)], lake_dir=tmp_path, fetch=FakeNSE(), sleep_s=0, today=date(2026, 11, 13), sleeper=lambda s: None)
    cat = admin.Catalog(tmp_path, tmp_path)
    t = admin.BY_KEY[("market", "eod_fo")]
    assert len(cat.files(t)) == 2 and t.vendor == "NSE"
    box = admin.Sandbox(cat)
    n = box.con.execute("SELECT count(DISTINCT date) FROM market.eod_fo").fetchone()[0]
    assert n == 2
