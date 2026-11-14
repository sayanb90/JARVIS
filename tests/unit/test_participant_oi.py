"""NSE participant-wise open interest and volume (SCRUM-175): parsing real files, checks, storage and the resumable run."""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from data_layer import participant_oi as p

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "participant"


def text(name: str) -> str:
    return (FIX / name).read_text()


@pytest.mark.parametrize("name,kind,day", [
    ("oi_2026-11-11.csv", "oi", date(2026, 11, 11)),
    ("oi_2013-01-02.csv", "oi", date(2013, 1, 2)),
    ("oi_2012-01-03.csv", "oi", date(2012, 1, 3)),
    ("vol_2026-11-11.csv", "volume", date(2026, 11, 11)),
])
def test_parse_real_files(name, kind, day):
    df = p.parse(text(name), kind, day)
    assert list(df["participant"]) == list(p.PARTICIPANTS)
    assert set(p.COLUMNS) <= set(df.columns)
    assert (df["date"] == day).all()


def test_parse_values_2026():
    df = p.parse(text("oi_2026-11-11.csv"), "oi", date(2026, 11, 11)).set_index("participant")
    assert df.loc["FII", "option_index_call_short"] == 958257
    assert df.loc["Client", "option_index_put_long"] == 2722311


def test_early_file_na_is_null():
    df = p.parse(text("oi_2012-01-03.csv"), "oi", date(2012, 1, 3)).set_index("participant")
    assert pd.isna(df.loc["DII", "option_index_call_short"])
    assert df.loc["DII", "future_index_long"] == 60612


def test_wrong_date_is_rejected():
    with pytest.raises(p.ParseError, match="file is for"):
        p.parse(text("oi_2026-11-11.csv"), "oi", date(2026, 11, 10))


def test_wrong_kind_is_rejected():
    with pytest.raises(p.ParseError, match="title"):
        p.parse(text("vol_2026-11-11.csv"), "oi", date(2026, 11, 11))


def test_totals_that_do_not_add_up_are_rejected():
    bad = text("oi_2026-11-11.csv").replace("FII,57226,", "FII,97226,")
    with pytest.raises(p.ParseError):
        p.parse(bad, "oi", date(2026, 11, 11))


def test_off_by_one_total_is_accepted():
    assert p._close(4878934, 4878935)
    assert not p._close(4878934, 4879935)


class FakeNSE:
    """Serves the fixture for 2026-11-11, 404 for 2026-11-10 (as a holiday would), and counts calls."""

    def __init__(self, fail: int = 0):
        self.calls: list[str] = []
        self.fail = fail

    def __call__(self, url: str):
        self.calls.append(url)
        if self.fail:
            return 503, ""
        if "11112026" in url:
            return 200, text("oi_2026-11-11.csv" if "participant_oi" in url else "vol_2026-11-11.csv")
        return 404, ""


def test_run_stores_both_kinds_and_skips_done_days(tmp_path):
    fake = FakeNSE()
    st = p.run([date(2026, 11, 11)], lake_dir=tmp_path, fetch=fake, sleep_s=0, today=date(2026, 11, 12), sleeper=lambda s: None)
    assert st["stored"] == 2 and st["calls"] == 2
    assert len(p.load("oi", tmp_path)) == 4 and len(p.load("volume", tmp_path)) == 4
    st2 = p.run([date(2026, 11, 11)], lake_dir=tmp_path, fetch=fake, sleep_s=0, today=date(2026, 11, 12), sleeper=lambda s: None)
    assert st2["calls"] == 0 and st2["skipped"] == 1


def test_holiday_asks_once_and_old_ones_are_final(tmp_path):
    fake = FakeNSE()
    st = p.run([date(2026, 10, 2)], lake_dir=tmp_path, fetch=fake, sleep_s=0, today=date(2026, 11, 12), sleeper=lambda s: None)
    assert st["holiday"] == 1 and st["calls"] == 1                     # a 404 for one kind: the other is not asked for
    assert p.load_missing(tmp_path)["2026-10-02"]["final"] is True
    st2 = p.run([date(2026, 10, 2)], lake_dir=tmp_path, fetch=fake, sleep_s=0, today=date(2026, 11, 12), sleeper=lambda s: None)
    assert st2["calls"] == 0


def test_recent_missing_day_is_retried(tmp_path):
    fake = FakeNSE()
    p.run([date(2026, 11, 10)], lake_dir=tmp_path, fetch=fake, sleep_s=0, today=date(2026, 11, 12), sleeper=lambda s: None)
    assert p.load_missing(tmp_path)["2026-11-10"]["final"] is False
    st = p.run([date(2026, 11, 10)], lake_dir=tmp_path, fetch=fake, sleep_s=0, today=date(2026, 11, 12), sleeper=lambda s: None)
    assert st["calls"] == 1


def test_run_stops_after_repeated_errors(tmp_path):
    fake = FakeNSE(fail=1)
    days = p.weekdays(date(2026, 10, 1), date(2026, 10, 31))
    st = p.run(days, lake_dir=tmp_path, fetch=fake, sleep_s=0, today=date(2026, 11, 12), sleeper=lambda s: None)
    assert "stopped" in st and st["calls"] == p.MAX_CONSECUTIVE_ERRORS


def test_limit_caps_requests(tmp_path):
    fake = FakeNSE()
    st = p.run(p.weekdays(date(2026, 9, 1), date(2026, 9, 30)), lake_dir=tmp_path, fetch=fake, sleep_s=0, today=date(2026, 11, 12),
               limit=3, sleeper=lambda s: None)
    assert st["calls"] == 3


def test_status_reports_size(tmp_path):
    p.run([date(2026, 11, 11)], lake_dir=tmp_path, fetch=FakeNSE(), sleep_s=0, today=date(2026, 11, 12), sleeper=lambda s: None)
    s = p.status(tmp_path)
    assert s["oi"]["days"] == 1 and s["oi"]["bytes"] > 0 and s["volume"]["last"] == "2026-11-11"


def test_url_format():
    assert p.url_for("oi", date(2026, 1, 5)).endswith("fao_participant_oi_05012026.csv")
    assert p.url_for("volume", date(2026, 1, 5)).endswith("fao_participant_vol_05012026.csv")
