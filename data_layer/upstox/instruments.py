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

from ..fmp_client import read_universe

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
