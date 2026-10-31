"""
Decode Upstox v3 market-data frames into `Tick`s.

Schema is the one in the vendored MarketDataFeedV3_pb2 (from the official SDK):
FeedResponse{type, feeds{instrument_key -> Feed}, currentTs, marketInfo}.
A Feed carries one of: ltpc (LTPC mode), fullFeed (full / full_d30: marketFF for
stocks and derivatives, indexFF for indices), firstLevelWithGreeks (option_greeks).

ASSUMPTION to confirm against a captured live frame (runbook Gate U1): `ltt` is
epoch MILLISECONDS in UTC.  The processor logs a clock-skew warning if not.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterator, Optional

from ..ticks import Tick
from .proto import MarketDataFeedV3_pb2 as pb

_MARKET_STATUS = {v.number: v.name for v in pb.MarketStatus.DESCRIPTOR.values}


def parse_frame(data: bytes) -> "pb.FeedResponse":
    return pb.FeedResponse.FromString(data)


def _ts(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def _greeks(holder) -> dict:
    """iv, delta, theta, gamma, vega, rho from a feed message that carries `optionGreeks` (options only)."""
    if not holder.HasField("optionGreeks"):
        return {}
    g = holder.optionGreeks
    return {"iv": holder.iv or None, "delta": g.delta, "theta": g.theta, "gamma": g.gamma, "vega": g.vega, "rho": g.rho}


def _tick(symbol: str, ltpc, vtt: Optional[int], quote, oi: Optional[float], greeks: Optional[dict] = None
          ) -> Optional[Tick]:
    if not ltpc.ltt or not ltpc.ltp:                 # no trade yet today
        return None
    bid = ask = bq = aq = None
    if quote is not None:
        bid, bq, ask, aq = quote.bidP or None, quote.bidQ, quote.askP or None, quote.askQ
    return Tick(_ts(ltpc.ltt), symbol, ltpc.ltp, vtt, bid, bq, ask, aq,
                int(oi) if oi else None, **(greeks or {}))


def ticks_from_response(resp: "pb.FeedResponse", key_to_symbol: dict[str, str]
                        ) -> Iterator[Tick]:
    """Yield one Tick per known instrument in a feed frame; unknown keys are ignored."""
    for key, feed in resp.feeds.items():
        sym = key_to_symbol.get(key)
        if sym is None:
            continue
        t: Optional[Tick] = None
        if feed.HasField("fullFeed"):
            ff = feed.fullFeed
            if ff.HasField("marketFF"):
                m = ff.marketFF
                q = m.marketLevel.bidAskQuote[0] if m.marketLevel.bidAskQuote else None
                t = _tick(sym, m.ltpc, m.vtt, q, m.oi, _greeks(m))
            elif ff.HasField("indexFF"):
                t = _tick(sym, ff.indexFF.ltpc, None, None, None)
        elif feed.HasField("firstLevelWithGreeks"):
            g = feed.firstLevelWithGreeks
            t = _tick(sym, g.ltpc, g.vtt, g.firstDepth if g.HasField("firstDepth") else None, g.oi, _greeks(g))
        elif feed.HasField("ltpc"):
            t = _tick(sym, feed.ltpc, None, None, None)
        if t is not None:
            yield t


def market_status(resp: "pb.FeedResponse") -> dict[str, str]:
    """{segment: status name} from a market_info frame (empty otherwise)."""
    return {k: _MARKET_STATUS.get(v, str(v)) for k, v in resp.marketInfo.segmentStatus.items()}
