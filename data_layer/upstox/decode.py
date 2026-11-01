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


def _ltpc_iep(ltpc) -> Optional[float]:
    """ltpc.iep is a DoubleValue wrapper: set only while a pre-open or closing auction is running."""
    return (ltpc.iep.value or None) if ltpc.HasField("iep") else None


def _ext_ltpc(ltpc) -> dict:
    """Extended fields every mode carries: last trade size, previous close, indicative auction price.  Zero means absent."""
    return {"ltq": int(ltpc.ltq) or None, "cp": ltpc.cp or None, "iep": _ltpc_iep(ltpc)}


def _ext_full(m) -> dict:
    """Extended fields of the full feed: average price, whole-book totals, auction fields.  Zero means absent."""
    return {"atp": m.atp or None, "tbq": int(m.tbq) or None, "tsq": int(m.tsq) or None, "iep": m.iep or _ltpc_iep(m.ltpc),
            "ieq": int(m.ieq) or None, "iiq_total": int(m.iiqTotal) or None, "iiq_market": int(m.iiqM) or None,
            "ref_price": m.rp or None, "cas_eligible": True if m.casEligible else None}


def _tick(symbol: str, ltpc, vtt: Optional[int], quote, oi: Optional[float], greeks: Optional[dict] = None,
          extra: Optional[dict] = None) -> Optional[Tick]:
    if not ltpc.ltt or not ltpc.ltp:                 # no trade yet today
        return None
    bid = ask = bq = aq = None
    if quote is not None:
        bid, bq, ask, aq = quote.bidP or None, quote.bidQ, quote.askP or None, quote.askQ
    return Tick(_ts(ltpc.ltt), symbol, ltpc.ltp, vtt, bid, bq, ask, aq,
                int(oi) if oi else None, **(greeks or {}), **(extra or {}))


def ticks_from_response(resp: "pb.FeedResponse", key_to_symbol: dict[str, str], ext: bool = False
                        ) -> Iterator[Tick]:
    """Yield one Tick per known instrument in a feed frame; unknown keys are ignored.  `ext` adds the extended fields."""
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
                t = _tick(sym, m.ltpc, m.vtt, q, m.oi, _greeks(m), {**_ext_ltpc(m.ltpc), **_ext_full(m)} if ext else None)
            elif ff.HasField("indexFF"):
                t = _tick(sym, ff.indexFF.ltpc, None, None, None, None, _ext_ltpc(ff.indexFF.ltpc) if ext else None)
        elif feed.HasField("firstLevelWithGreeks"):
            g = feed.firstLevelWithGreeks
            t = _tick(sym, g.ltpc, g.vtt, g.firstDepth if g.HasField("firstDepth") else None, g.oi, _greeks(g),
                      _ext_ltpc(g.ltpc) if ext else None)
        elif feed.HasField("ltpc"):
            t = _tick(sym, feed.ltpc, None, None, None, None, _ext_ltpc(feed.ltpc) if ext else None)
        if t is not None:
            yield t


def books_from_response(resp: "pb.FeedResponse", key_to_symbol: dict[str, str]
                       ) -> Iterator[tuple[str, datetime, float, list, list]]:
    """(symbol, exchange time, last price, bids, asks) for every `full` frame that carries market depth.
    bids/asks are [[price, quantity], ...] best first, empty levels dropped.  Live display only: never stored."""
    for key, feed in resp.feeds.items():
        sym = key_to_symbol.get(key)
        if sym is None or not feed.HasField("fullFeed") or not feed.fullFeed.HasField("marketFF"):
            continue
        m = feed.fullFeed.marketFF
        quotes = m.marketLevel.bidAskQuote
        if not quotes:
            continue
        bids = [[q.bidP, q.bidQ] for q in quotes if q.bidP]
        asks = [[q.askP, q.askQ] for q in quotes if q.askP]
        if bids or asks:
            yield sym, _ts(m.ltpc.ltt) if m.ltpc.ltt else datetime.utcnow(), m.ltpc.ltp or None, bids, asks


def events_from_response(resp: "pb.FeedResponse") -> list[dict]:
    """Session events from a market_info frame: segment status, closing-auction phase, pre-open phase."""
    if not resp.HasField("marketInfo"):
        return []
    mi = resp.marketInfo
    ts = _ts(resp.currentTs) if resp.currentTs else datetime.now(timezone.utc).replace(tzinfo=None)
    out = [{"ts": ts, "kind": "segment", "segment": k, "status": _MARKET_STATUS.get(v, str(v)), "updated": None}
           for k, v in mi.segmentStatus.items()]
    for kind, entries in (("cas", mi.casMarketStatus), ("preopen", mi.preOpenSessionStatus)):
        for k, v in entries.items():
            status = v.status if hasattr(v, "status") else _MARKET_STATUS.get(v, str(v))
            upd = _ts(v.updatedTime) if getattr(v, "updatedTime", 0) else None
            out.append({"ts": ts, "kind": kind, "segment": k, "status": status, "updated": upd})
    return out


def market_status(resp: "pb.FeedResponse") -> dict[str, str]:
    """{segment: status name} from a market_info frame (empty otherwise)."""
    return {k: _MARKET_STATUS.get(v, str(v)) for k, v in resp.marketInfo.segmentStatus.items()}
