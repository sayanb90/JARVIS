"""Index constituents -> Yahoo symbols -> data/universe_global.csv.  Pages are small fixtures; nothing touches the network."""
from __future__ import annotations

import csv

import pytest

from data_layer import index_universe as iu


def page(headers, rows) -> str:
    th = "".join(f"<th>{h}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f"<html><body><table><tr>{th}</tr>{body}</table></body></html>"


@pytest.mark.parametrize("fn,code,ex,expected", [
    (iu.y_us, "BRK.B", "", "BRK-B"), (iu.y_us, "aapl", "", "AAPL"),
    (iu.y_uk, "BT.A", "", "BT-A.L"), (iu.y_uk, "BP.", "", "BP.L"), (iu.y_uk, "SHEL.L", "", "SHEL.L"),
    (iu.y_jp, "7203", "", "7203.T"), (iu.y_jp, "285A", "", "285A.T"), (iu.y_jp, "TSE: 7203", "", None),
    (iu.y_kr, "005930", "", "005930.KS"), (iu.y_kr, "5930", "", "005930.KS"),
    (iu.y_tw, "2330", "", "2330.TW"), (iu.y_tw, "TWSE: 2330", "", "2330.TW"),
    (iu.y_sg, "SGX: D05", "", "D05.SI"), (iu.y_sg, "Z74", "", "Z74.SI"),
    (iu.y_cn, "600519", "", "600519.SS"), (iu.y_cn, "000858", "", "000858.SZ"), (iu.y_cn, "300750", "", "300750.SZ"), (iu.y_cn, "12", "", None),
    (iu.y_hk, "700", "", "0700.HK"), (iu.y_hk, "SEHK: 5", "", "0005.HK"), (iu.y_hk, "09988", "", "9988.HK"),
    (iu.y_eu, "SAP", "Xetra Germany", "SAP.DE"), (iu.y_eu, "ASML", "Euronext Amsterdam", "ASML.AS"),
    (iu.y_eu, "VOLV B", "Nasdaq Omx Nordic Sweden", "VOLV-B.ST"), (iu.y_eu, "NOVN.SW", "", "NOVN.SW"), (iu.y_eu, "XYZ", "Unknown Exchange", None),
])
def test_codes_become_yahoo_symbols(fn, code, ex, expected):
    assert fn(code, ex) == expected


def test_a_wikipedia_table_is_read_with_citations_dropped():
    html = page(["Symbol", "Security", "GICS Sector"], [["AAPL", "Apple Inc.<sup>[1]</sup>", "Information Technology"], ["BRK.B", "Berkshire", "Financials"]])
    ix = iu.BY_KEY["sp500"]
    rows = iu.members_from_tables(ix, iu.parse_tables(html))
    assert [(r["code"], r["name"], r["sector"]) for r in rows] == [("AAPL", "Apple Inc.", "Information Technology"), ("BRK.B", "Berkshire", "Financials")]


def test_the_table_with_a_plausible_member_count_wins_and_split_tables_are_merged():
    ix = iu.BY_KEY["nikkei225"]
    many = [[f"{1000 + i}", f"Co {i}"] for i in range(225)]
    html = page(["Code", "Company"], many[:100]) + page(["Code", "Company"], many[100:]) + page(["Year", "Event"], [["2020", "x"]])
    assert len(iu.members_from_tables(ix, iu.parse_tables(html))) == 225                        # two sector tables, merged
    html = page(["Code", "Company"], many) + page(["Code", "Company"], many[:5])
    assert len(iu.members_from_tables(ix, iu.parse_tables(html))) == 225                        # the big one, not the small one


def test_a_list_with_the_wrong_member_count_is_not_used():
    ix = iu.BY_KEY["sp500"]
    html = page(["Symbol", "Security"], [[f"S{i}", "x"] for i in range(40)])
    members, note = iu.fetch_index(ix, lambda url: html)
    assert members == [] and "parsed 40 members, expected 480 to 520" in note
    members, note = iu.fetch_index(ix, lambda url: (_ for _ in ()).throw(OSError("blocked")))
    assert members == [] and "download failed" in note


def test_the_ishares_holdings_file_is_read_and_only_equities_are_kept():
    text = ("iShares STOXX Europe 600\nFund Holdings as of,Nov 05\n\n"
            "Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Location,Exchange\n"
            'SAP,SAP SE,Information Technology,Equity,1,1.0,Germany,Xetra\n'
            'NOVO B,Novo Nordisk,Health Care,Equity,1,1.0,Denmark,Nasdaq Omx Nordic\n'
            '-,Cash,Cash,Cash,1,0.1,-,-\n')
    rows = iu.members_from_ishares(text)
    assert [r["code"] for r in rows] == ["SAP", "NOVO B"] and rows[1]["country"] == "Denmark"


def write_source(root, key, rows, header="code,name,exchange,country,sector,yahoo"):
    d = root / "data" / "index_sources"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{key}.csv").write_text(header + "\n" + "\n".join(rows) + "\n")


def test_build_maps_dedupes_and_assigns_markets(tmp_path):
    write_source(tmp_path, "sp500", [f"S{i},Co {i},,," for i in range(500)])
    write_source(tmp_path, "ftse100", [f"T{i},Uk {i},,," for i in range(100)])
    eu = [f"E{i},Eu {i},Xetra,Germany,," for i in range(480)] + ["SHEL,Shell,London Stock Exchange,United Kingdom,,", "T5,Dup of the FTSE name,London,UK,,"]
    eu += ["BAD,Nowhere,Unknown,Mars,,", "ODD,Odd,Unknown,Mars,,ODD.PA"] + [f"F{i},Eu {i},Euronext Paris,France,," for i in range(40)]
    write_source(tmp_path, "stoxx600", eu)
    built = iu.assemble(tmp_path)
    c = iu.counts(built.rows)
    assert c["US"] == 500 and c["UK"] == 101                                          # 100 FTSE names plus Shell from the STOXX list
    by = {r["symbol"]: r for r in built.rows}
    assert by["SHEL.L"]["market"] == "UK" and by["E0.DE"]["market"] == "EU" and by["ODD.PA"]["market"] == "EU"        # a `yahoo` value wins
    assert "BAD" in {r[0] for r in built.rejects} and sum(1 for r in built.rows if r["symbol"] == "T5.L") == 1       # one T5.L, not two
    assert by["S0"]["currency"] == "USD" and by["T0.L"]["currency"] == "GBp"
    assert any("no source file" in n for n in built.notes) and built.problems >= 1                                  # the other indices are missing


def test_an_index_file_with_the_wrong_count_is_skipped_with_a_note(tmp_path):
    write_source(tmp_path, "hsi", [f"{i},Co {i},,," for i in range(1, 11)])
    built = iu.assemble(tmp_path, only=["hsi"])
    assert built.rows == [] and "10 members, expected 70 to 100: not used" in "\n".join(built.notes)


def test_validate_rejects_symbols_yahoo_does_not_know_but_trusts_nothing_from_a_silent_chunk(tmp_path):
    write_source(tmp_path, "taiwan50", [f"{2300 + i},Co {i},,," for i in range(50)])
    built = iu.assemble(tmp_path, only=["taiwan50"])
    assert len(built.rows) == 50
    rejected, unchecked = iu.validate(built, batch=lambda chunk: {s: object() for s in chunk if s != "2305.TW"})
    assert (rejected, unchecked) == (1, 0) and len(built.rows) == 49 and ("2305.TW", "TW", "Yahoo has no daily bars for this symbol") in built.rejects
    rejected, unchecked = iu.validate(built, batch=lambda chunk: {})                        # Yahoo answered nothing: nothing is dropped
    assert (rejected, unchecked) == (0, 49) and len(built.rows) == 49


def test_command_line_fetch_build_and_status(tmp_path, capsys):
    html = page(["Ticker", "Company"], [[f"{2300 + i}", f"Co {i}"] for i in range(50)])
    assert iu.main(["fetch", "--only", "taiwan50"], root=tmp_path, getter=lambda url: html) == 0
    assert (tmp_path / "data" / "index_sources" / "taiwan50.csv").exists()
    assert iu.main(["build", "--only", "taiwan50"], root=tmp_path) == 0
    rows = list(csv.DictReader(l for l in (tmp_path / "data" / "universe_global.csv").read_text().splitlines() if not l.startswith("#")))
    assert len(rows) == 50 and rows[0]["market"] == "TW" and rows[0]["symbol"].endswith(".TW")
    assert iu.main(["fetch", "--only", "hsi"], root=tmp_path, getter=lambda url: "<html></html>") == 1
    assert iu.main(["build", "--only", "nonsense"], root=tmp_path) == 2
    assert "FAILED" in capsys.readouterr().out


def test_read_universe_uses_the_global_file_for_covered_markets_and_the_active_file_for_the_rest(world):
    world.set_universe({"IN": ["A.NS"], "US": ["AAPL"], "UK": ["SHEL.L"], "EU": ["SAP.DE"]})
    assert [r["symbol"] for r in world.universe_files.read_universe("US")] == ["AAPL"]                   # no global file yet
    glob = world.data / "universe_global.csv"
    glob.write_text(",".join(iu.HEADER) + "\nMSFT,Microsoft,US,,,,,,,USD\nNVDA,Nvidia,US,,,,,,,USD\n7203.T,Toyota,JP,,,,,,,JPY\n")
    assert [r["symbol"] for r in world.universe_files.read_universe("US")] == ["MSFT", "NVDA"]            # the index build replaces the placeholder rows
    assert [r["symbol"] for r in world.universe_files.read_universe("JP")] == ["7203.T"]
    assert [r["symbol"] for r in world.universe_files.read_universe("UK")] == ["SHEL.L"]                  # not covered by the build: unchanged
    assert [r["symbol"] for r in world.universe_files.read_universe("IN")] == ["A.NS"]
    assert {r["symbol"] for r in world.universe_files.read_universe()} == {"A.NS", "SHEL.L", "SAP.DE", "MSFT", "NVDA", "7203.T"}


def test_a_downloaded_ishares_file_and_a_plain_csv_both_import(tmp_path, capsys):
    holdings = ("iShares STOXX Europe 600 UCITS ETF\nFund Holdings as of,Nov 10\n\n"
                "Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Location,Exchange\n"
                + "".join(f"E{i},Eu {i},Health Care,Equity,1,0.1,Germany,Xetra\n" for i in range(520))
                + "-,Cash,Cash,Cash,1,0.1,-,-\nEURUSD,FX,FX,FX Contract,1,0.1,-,-\n")
    f = tmp_path / "exsa.csv"
    f.write_text(holdings)
    assert iu.main(["import", "stoxx600", str(f)], root=tmp_path) == 0
    rows = iu.read_source(iu.BY_KEY["stoxx600"], tmp_path)
    assert len(rows) == 520 and rows[0]["code"] == "E0" and rows[0]["exchange"] == "Xetra" and rows[0]["country"] == "Germany"
    plain = tmp_path / "tw.csv"
    plain.write_text("Code,Company Name\n" + "".join(f"{2300 + i},Co {i}\n" for i in range(50)))
    assert iu.main(["import", "taiwan50", str(plain)], root=tmp_path) == 0
    assert iu.main(["build", "--only", "stoxx600,taiwan50"], root=tmp_path) == 0
    out = capsys.readouterr().out
    assert "stoxx600" in out and "520 members -> 520 symbols" in out


def test_an_imported_file_with_the_wrong_count_or_an_unknown_key_is_refused(tmp_path, capsys):
    short = tmp_path / "short.csv"
    short.write_text("Ticker,Name\nA,a\nB,b\n")
    assert iu.main(["import", "taiwan50", str(short)], root=tmp_path) == 1
    assert "2 members found in the file, expected 48 to 52: not imported" in capsys.readouterr().out
    assert iu.main(["import", "nope", str(short)], root=tmp_path) == 2
    assert iu.main(["import", "taiwan50", str(tmp_path / "missing.csv")], root=tmp_path) == 1


def test_the_nikkei_and_kospi_pages_found_on_the_vps_are_read():
    nik = "".join(page(["Code", "Company Name"], [[f"{1000 + i + 10 * s}", f"Co {i}"] for i in range(7)]) for s in range(32))      # 224 members in 32 tables
    nik += page(["Code", "Company Name"], [["9999", "Last Co"]])
    members, note = iu.fetch_index(iu.BY_KEY["nikkei225"], lambda url: nik)
    assert "indexes.nikkei.co.jp" in iu.BY_KEY["nikkei225"].url and len(members) >= 215, note
    kospi = page(["Company", "Symbol", "GICS Sector"], [[f"Co {i}", f"{5930 + i:06d}", "Information Technology"] for i in range(200)])
    members, note = iu.fetch_index(iu.BY_KEY["kospi200"], lambda url: kospi)
    assert iu.BY_KEY["kospi200"].url.endswith("/KOSPI_200") and len(members) == 200 and members[0]["sector"] == "Information Technology"
