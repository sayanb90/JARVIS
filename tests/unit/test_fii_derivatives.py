"""FII derivative statistics (SCRUM-185): real .xls files from 2014 to 2026, checks, storage and the resumable run."""

from datetime import date
from pathlib import Path

import pytest

from data_layer import fii_derivatives as f
from data_layer.nse_archive import ParseError

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "fii_derivatives"


def body(day: str) -> bytes:
    return (FIX / f"fii_stats_{day}.xls").read_bytes()


@pytest.mark.parametrize("name,day,rows", [
    ("01-Dec-2014", date(2014, 12, 1), 4),
    ("02-Jan-2015", date(2015, 1, 2), 4),
    ("02-Jan-2023", date(2023, 1, 2), 4),
    ("12-Nov-2026", date(2026, 11, 12), 16),
])
def test_parse_real_files(name, day, rows):
    df = f.parse(body(name), day)
    assert list(df.columns) == f.COLUMNS and len(df) == rows and (df["date"] == day).all()
    assert set(df.loc[df["underlying"] == "ALL", "instrument"]) == set(f.INSTRUMENTS.values())


def test_values_and_index_split_2026():
    df = f.parse(body("12-Nov-2026"), date(2026, 11, 12)).set_index(["instrument", "underlying"])
    assert df.loc[("index_options", "NIFTY"), "oi_contracts"] == 2807553
    assert df.loc[("index_futures", "ALL"), "buy_amount_cr"] == pytest.approx(5223.25)
    assert df.loc[("stock_options", "ALL"), "sell_contracts"] == 314597
    assert {u for i, u in df.index if i == "index_futures"} >= {"ALL", "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}


def test_values_2014():
    df = f.parse(body("01-Dec-2014"), date(2014, 12, 1)).set_index("instrument")
    assert df.loc["index_futures", "oi_contracts"] == 781042
    assert df.loc["stock_options", "oi_amount_cr"] == pytest.approx(1522.6438)


def test_wrong_date_is_rejected():
    with pytest.raises(ParseError, match="file is for"):
        f.parse(body("12-Nov-2026"), date(2026, 11, 11))


def test_not_an_xls_is_rejected():
    with pytest.raises(ParseError, match="readable"):
        f.parse(b"<html>Service Temporarily Unavailable</html>", date(2026, 11, 12))


def test_index_rows_that_do_not_add_up_are_rejected(monkeypatch):
    real = f._read_sheet

    def tampered(b):
        sheet = real(b)
        sheet = sheet.astype(object)
        sheet.iloc[7, 1] = 28803                             # NIFTY FUTURES bought, 10,000 more than the file says
        return sheet
    monkeypatch.setattr(f, "_read_sheet", tampered)
    with pytest.raises(ParseError, match="indices add to"):
        f.parse(body("12-Nov-2026"), date(2026, 11, 12))


def test_url_format():
    assert f.url_for(date(2026, 11, 12)).endswith("/content/fo/fii_stats_12-Nov-2026.xls")
    assert f.url_for(date(2015, 1, 2)).endswith("fii_stats_02-Jan-2015.xls")


class FakeNSE:
    def __init__(self, fail: bool = False):
        self.calls: list[str] = []
        self.fail = fail

    def __call__(self, url: str):
        self.calls.append(url)
        if self.fail:
            return 503, b""
        for name in ("12-Nov-2026", "02-Jan-2015"):
            if name in url:
                return 200, body(name)
        return 404, b""


def run(tmp_path, days, fake, **kw):
    return f.run(days, lake_dir=tmp_path, fetch=fake, sleep_s=0, today=date(2026, 11, 13), sleeper=lambda s: None, **kw)


def test_run_stores_and_skips_done_days(tmp_path):
    fake = FakeNSE()
    st = run(tmp_path, [date(2026, 11, 12), date(2015, 1, 2)], fake)
    assert st["stored"] == 2 and st["calls"] == 2 and len(f.load(tmp_path)) == 20
    assert run(tmp_path, [date(2026, 11, 12), date(2015, 1, 2)], fake)["calls"] == 0


def test_holidays_days_before_the_first_file_and_errors(tmp_path):
    fake = FakeNSE()
    st = run(tmp_path, [date(2014, 11, 28), date(2026, 10, 2)], fake)
    assert st["calls"] == 1 and st["holiday"] == 1                    # before 2014-12-01 nothing is asked for
    assert run(tmp_path, [date(2026, 10, 2)], fake)["calls"] == 0
    st = run(tmp_path, f.weekdays(date(2026, 9, 1), date(2026, 9, 30)), FakeNSE(fail=True))
    assert "stopped" in st and st["calls"] == 5


def test_import_and_status(tmp_path):
    p = tmp_path / "x.xls"
    p.write_bytes(body("12-Nov-2026"))
    assert f.import_file(p, date(2026, 11, 12), tmp_path) == 16
    s = f.status(tmp_path)
    assert s["days"] == 1 and s["bytes"] > 0 and s["days_with_index_split"] == 1 and s["last"] == "2026-11-12"
