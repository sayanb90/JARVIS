"""
What the viewer needs to know about each market's exchange: its time zone, regular trading hours, quote currency and display name.

The charts show every instrument in its own exchange's local time and only over that exchange's session, in the currency it is quoted in.
Hours are the regular continuous session (auctions and early closes are ignored); a lunch break is drawn as a gap.  The bars in the lake are
stored in UTC.

  market_of(symbol)          the market code of a symbol, from its Yahoo suffix (".L" -> UK, ".T" -> JP, no suffix -> US ...)
  exchange_for(symbol, mk)   the exchange facts for a symbol (Europe is several exchanges: the suffix picks the right one)
  to_local(ts, tz)           UTC timestamps -> local wall-clock strings
  tz_label(tz, day)          "GMT", "CET", "JST" ... for the title of a chart
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd

# code: (market name, exchange label, time zone, open, close, lunch break or None, currency)
MARKETS: dict[str, dict] = {
    "IN": dict(name="India", exchange="NSE", tz="Asia/Kolkata", open="09:15", close="15:30", lunch=None, currency="INR"),
    "US": dict(name="United States", exchange="NYSE / Nasdaq", tz="America/New_York", open="09:30", close="16:00", lunch=None, currency="USD"),
    "UK": dict(name="United Kingdom", exchange="London", tz="Europe/London", open="08:00", close="16:30", lunch=None, currency="GBp"),
    "EU": dict(name="Europe", exchange="Europe", tz="Europe/Paris", open="09:00", close="17:30", lunch=None, currency="EUR"),
    "JP": dict(name="Japan", exchange="Tokyo", tz="Asia/Tokyo", open="09:00", close="15:30", lunch=("11:30", "12:30"), currency="JPY"),
    "KR": dict(name="South Korea", exchange="Seoul", tz="Asia/Seoul", open="09:00", close="15:30", lunch=None, currency="KRW"),
    "TW": dict(name="Taiwan", exchange="Taipei", tz="Asia/Taipei", open="09:00", close="13:30", lunch=None, currency="TWD"),
    "SG": dict(name="Singapore", exchange="Singapore", tz="Asia/Singapore", open="09:00", close="17:00", lunch=None, currency="SGD"),
    "CN": dict(name="China", exchange="Shanghai / Shenzhen", tz="Asia/Shanghai", open="09:30", close="15:00", lunch=("11:30", "13:00"), currency="CNY"),
    "HK": dict(name="Hong Kong", exchange="Hong Kong", tz="Asia/Hong_Kong", open="09:30", close="16:00", lunch=("12:00", "13:00"), currency="HKD"),
    # the global cues (data/global_cues.csv): futures and FX trade nearly round the clock, so the whole UTC day is the session
    "GLOBAL": dict(name="Global cues", exchange="CME · CBOE · FX", tz="UTC", open="00:00", close="23:59", lunch=None, currency="USD"),
}

# Yahoo suffix -> (market, exchange label, time zone, open, close, currency); the European exchanges differ in hours and currency
SUFFIXES: dict[str, tuple] = {
    ".NS": ("IN", None), ".BO": ("IN", None), ".L": ("UK", None), ".T": ("JP", None), ".KS": ("KR", None), ".KQ": ("KR", None),
    ".TW": ("TW", None), ".TWO": ("TW", None), ".SI": ("SG", None), ".SS": ("CN", None), ".SZ": ("CN", None), ".HK": ("HK", None),
    ".DE": ("EU", dict(exchange="Xetra", tz="Europe/Berlin", currency="EUR")),
    ".F": ("EU", dict(exchange="Frankfurt", tz="Europe/Berlin", currency="EUR")),
    ".PA": ("EU", dict(exchange="Paris", tz="Europe/Paris", currency="EUR")),
    ".AS": ("EU", dict(exchange="Amsterdam", tz="Europe/Amsterdam", currency="EUR")),
    ".MI": ("EU", dict(exchange="Milan", tz="Europe/Rome", currency="EUR")),
    ".MC": ("EU", dict(exchange="Madrid", tz="Europe/Madrid", currency="EUR")),
    ".BR": ("EU", dict(exchange="Brussels", tz="Europe/Brussels", currency="EUR")),
    ".VI": ("EU", dict(exchange="Vienna", tz="Europe/Vienna", currency="EUR")),
    ".LS": ("EU", dict(exchange="Lisbon", tz="Europe/Lisbon", open="08:00", close="16:30", currency="EUR")),
    ".IR": ("EU", dict(exchange="Dublin", tz="Europe/Dublin", open="08:00", close="16:30", currency="EUR")),
    ".HE": ("EU", dict(exchange="Helsinki", tz="Europe/Helsinki", open="10:00", close="18:30", currency="EUR")),
    ".AT": ("EU", dict(exchange="Athens", tz="Europe/Athens", open="10:15", close="17:20", currency="EUR")),
    ".ST": ("EU", dict(exchange="Stockholm", tz="Europe/Stockholm", currency="SEK")),
    ".CO": ("EU", dict(exchange="Copenhagen", tz="Europe/Copenhagen", close="17:00", currency="DKK")),
    ".OL": ("EU", dict(exchange="Oslo", tz="Europe/Oslo", close="16:20", currency="NOK")),
    ".SW": ("EU", dict(exchange="Zurich", tz="Europe/Zurich", currency="CHF")),
    ".WA": ("EU", dict(exchange="Warsaw", tz="Europe/Warsaw", close="17:00", currency="PLN")),
}


_CUES: dict = {}


def _cues() -> set[str]:
    """Symbols of data/global_cues.csv, re-read only when the file changes (market_of runs once per symbol of the universe)."""
    try:
        from data_layer import universe_files as uf
        path = uf.global_cues_csv()
        key = (str(path), path.stat().st_mtime_ns if path.exists() else 0)
        if _CUES.get("key") != key:
            _CUES.update(key=key, symbols={c.upper() for c in uf.cue_symbols()})
        return _CUES["symbols"]
    except Exception:                                                     # noqa: BLE001 -- no file: no cues
        return set()


def market_of(symbol: str) -> str:
    """The market a symbol belongs to, from its Yahoo suffix; a symbol without one is a US listing."""
    s = (symbol or "").upper()
    if s.startswith("NSE_INDEX:"):
        return "IN"
    if s.endswith(("=F", "=X")) or s in _cues():
        return "GLOBAL"
    for suffix, (market, _) in SUFFIXES.items():
        if s.endswith(suffix):
            return market
    return "US"


def exchange_for(symbol: str, market: Optional[str] = None) -> dict:
    """Name, exchange, time zone, open, close, lunch break and currency for one symbol (the market's defaults, refined by its suffix)."""
    market = market or market_of(symbol)
    base = dict(MARKETS.get(market) or MARKETS["IN"])
    s = (symbol or "").upper()
    for suffix, (mk, over) in SUFFIXES.items():
        if over and mk == market and s.endswith(suffix):
            base.update(over)
            break
    base["market"] = market
    return base


def to_local(ts: pd.Series, tz: str) -> list[str]:
    """UTC timestamps -> 'YYYY-MM-DD HH:MM:SS' wall-clock strings in `tz`."""
    t = pd.to_datetime(ts)
    t = t.dt.tz_localize("UTC") if t.dt.tz is None else t.dt.tz_convert("UTC")
    return t.dt.tz_convert(tz).dt.strftime("%Y-%m-%d %H:%M:%S").tolist()


def tz_label(tz: str, day: Optional[date | str] = None) -> str:
    """Short zone name on `day` (daylight saving included): GMT/BST, CET/CEST, EST/EDT, JST ..."""
    d = date.fromisoformat(day) if isinstance(day, str) else (day or date.today())
    name = datetime(d.year, d.month, d.day, 12, tzinfo=ZoneInfo(tz)).tzname() or tz
    return {"Asia/Singapore": "SGT"}.get(tz, name)                    # the zone database names Singapore "+08"


def session_minutes(info: dict) -> int:
    """Minutes of continuous trading in the regular session, a lunch break taken out."""
    def mins(hhmm: str) -> int:
        h, m = hhmm.split(":")
        return int(h) * 60 + int(m)
    total = mins(info["close"]) - mins(info["open"])
    if info.get("lunch"):
        total -= mins(info["lunch"][1]) - mins(info["lunch"][0])
    return total


def public(info: dict, day: Optional[str] = None) -> dict:
    """The facts the page needs, without internals."""
    return {"market": info["market"], "market_name": info["name"], "exchange": info["exchange"], "tz": info["tz"],
            "tz_label": tz_label(info["tz"], day), "open": info["open"], "close": info["close"],
            "lunch": list(info["lunch"]) if info.get("lunch") else None, "currency": info["currency"],
            "session_minutes": session_minutes(info)}


def universe_info() -> dict[str, dict]:
    """{symbol: {name, market, sector}} from the universe files (India from universe.csv, the other markets from the index lists); best effort."""
    try:
        from data_layer.universe_files import read_universe
        return {r["symbol"]: {"name": r.get("name", "") or "", "market": r.get("market", "") or "", "sector": r.get("sector", "") or ""}
                for r in read_universe(None) if r.get("symbol")}
    except Exception:                                                     # noqa: BLE001 -- a missing file just means no names
        return {}
