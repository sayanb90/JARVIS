"""
Universe symbol <-> Upstox instrument key.

NSE equities use `NSE_EQ|<ISIN>`, so the key comes straight from the ISIN column
of data/universe.csv.  Optionally validate against Upstox's instrument master
(a list of dicts with an `instrument_key`) saved locally; the master download
needs no code here because it has been reported to refuse some automated clients.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from ..universe_files import read_universe

log = logging.getLogger(__name__)


def equity_key(isin: str) -> str:
    return f"NSE_EQ|{isin.strip()}"


def build_key_map(rows: list[dict] | None = None, master: Path | str | None = None
                  ) -> tuple[dict[str, str], list[str]]:
    """
    Return ({instrument_key: symbol}, [symbols skipped]) for the India universe.
    Rows without a valid-looking ISIN, or absent from `master` when given, are skipped.
    """
    rows = read_universe("IN") if rows is None else rows
    known: set[str] | None = None
    if master:
        known = {r["instrument_key"] for r in json.loads(Path(master).read_text())}
    keys: dict[str, str] = {}
    skipped: list[str] = []
    for r in rows:
        isin = (r.get("isin") or "").strip()
        if len(isin) != 12 or not isin.startswith("IN"):
            skipped.append(r["symbol"])
            continue
        k = equity_key(isin)
        if known is not None and k not in known:
            skipped.append(r["symbol"])
            continue
        keys[k] = r["symbol"]
    if skipped:
        log.warning("%d universe symbols have no usable Upstox key: %s",
                    len(skipped), ", ".join(skipped[:10]) + ("..." if len(skipped) > 10 else ""))
    return keys, skipped


# Index keys (SCRUM-100).  The lake symbol is "NSE_INDEX:<name>", which is how tick_store.is_index tells an index from a stock.  The names
# are Upstox's own index names; a key that never sends a frame is reported at the end of the session (see feed.silent_keys).
INDEX_KEYS: dict[str, str] = {f"NSE_INDEX|{n}": f"NSE_INDEX:{n}" for n in (
    "Nifty 50", "Nifty Bank", "India VIX", "Nifty Fin Service", "Nifty Next 50", "NIFTY MID SELECT")}


def index_keys_enabled(env=None) -> bool:
    import os
    return (os.environ if env is None else env).get("JARVIS_INDEX_KEYS", "").lower() in ("1", "true", "yes", "on")


def segment_key_map(segment: str = "eq", fo_file: Path | str | None = None) -> dict[str, str]:
    """{instrument_key: symbol} for the collectors: the equity universe ("eq", plus the index keys when JARVIS_INDEX_KEYS is on) or the
    selected contracts ("fo")."""
    if segment == "fo":
        from .fo_instruments import fo_key_map
        return fo_key_map(Path(fo_file) if fo_file else None)
    keys = build_key_map()[0]
    return {**keys, **INDEX_KEYS} if index_keys_enabled() else keys
