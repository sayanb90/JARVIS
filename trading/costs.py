"""
Transaction costs of NSE index options and futures, per fill, in rupees.  Rates are dated: each applies from its date on, so a backtest over 2023-2026 pays
the rates of each trade day.

  brokerage   per executed order, by broker (BROKERS): Zerodha futures 0.03 % or Rs 20 whichever is lower, options flat Rs 20;
              Upstox futures 0.05 % or Rs 20 whichever is lower, options flat Rs 20
  STT         options: on the premium of the sell side; futures: on the sell-side value
  exchange    NSE transaction charge on premium (options) or value (futures)
  IPFT        NSE investor protection fund charge, on value; folded into the transaction charge from 1 March 2026
  SEBI        turnover fee, Rs 10 per crore
  stamp       buy side only
  GST         18 % on brokerage + exchange charge + IPFT + SEBI fee

Sources, read 25 Nov 2026: zerodha.com/charges and upstox.com/brokerage-charges.  Both list today: STT 0.15 % options (sell, premium) and 0.05 % futures
(sell), from 1 April 2026; NSE transaction 0.03553 % options (premium) and 0.00183 % futures, from 1 March 2026 (0.03503 % and 0.00173 % from 1 October
2024); stamp 0.003 % options and 0.002 % futures (buy); SEBI Rs 10 per crore; GST 18 %.  Earlier STT steps (1 April 2023, before) are statutory.
The NSE transaction charge before October 2024 is NOT on either page (marked below): check one contract note from 2023 or early 2024 and correct it here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional

# (from date, rate as a fraction).  The last entry dated on or before the trade date applies.
STT = {
    "opt_sell": [(date(2016, 6, 1), 0.0005), (date(2023, 4, 1), 0.000625), (date(2024, 10, 1), 0.001), (date(2026, 4, 1), 0.0015)],
    "fut_sell": [(date(2016, 6, 1), 0.0001), (date(2023, 4, 1), 0.000125), (date(2024, 10, 1), 0.0002), (date(2026, 4, 1), 0.0005)],
}
EXCHANGE = {
    # before 1 Oct 2024: NSE's slab rates (about 0.05 % options, 0.002 % futures) -- NOT verified from a source, see the docstring
    "opt": [(date(2016, 1, 1), 0.0005), (date(2024, 10, 1), 0.0003503), (date(2026, 3, 1), 0.0003553)],
    "fut": [(date(2016, 1, 1), 0.00002), (date(2024, 10, 1), 0.0000173), (date(2026, 3, 1), 0.0000183)],
}
IPFT = {"opt": [(date(2016, 1, 1), 50 / 1e7), (date(2026, 3, 1), 0.0)],          # Rs 0.50 a lakh of premium; part of the transaction charge from March 2026
        "fut": [(date(2016, 1, 1), 10 / 1e7), (date(2026, 3, 1), 0.0)]}          # Rs 0.10 a lakh of value
SEBI_FEE = 10 / 1e7
STAMP = {"opt": 0.00003, "fut": 0.00002}
GST = 0.18
BROKERAGE = 20.0
# broker -> kind -> (flat rupees per order, percentage cap of the order value or None): the lower of the two is charged
BROKERS = {"upstox": {"opt": (20.0, None), "fut": (20.0, 0.0005)},
           "zerodha": {"opt": (20.0, None), "fut": (20.0, 0.0003)}}


def _rate(table: list, day: date) -> float:
    r = table[0][1]
    for d, v in table:
        if d <= day:
            r = v
    return r


@dataclass
class Cost:
    brokerage: float
    stt: float
    exchange: float
    sebi: float
    stamp: float
    gst: float
    ipft: float = 0.0

    @property
    def total(self) -> float:
        return self.brokerage + self.stt + self.exchange + self.sebi + self.stamp + self.gst + self.ipft


def brokerage_of(kind: str, value: float, broker: str = "upstox") -> float:
    flat, pct = BROKERS[broker][kind]
    return min(flat, value * pct) if pct is not None else flat


def fill_cost(kind: str, side: int, price: float, qty: int, day: Optional[date] = None, brokerage: Optional[float] = None,
              broker: str = "upstox") -> Cost:
    """Costs of one fill: kind "opt" or "fut", side +1 buy / -1 sell, price per unit, qty in units (lots x lot size).  `brokerage` overrides the broker's
    schedule (rupees per order)."""
    day = day or date.today()
    value = abs(price * qty)
    brk = brokerage if brokerage is not None else brokerage_of(kind, value, broker)
    stt = value * _rate(STT[f"{kind}_sell"], day) if side < 0 else 0.0
    exch = value * _rate(EXCHANGE[kind], day)
    ipft = value * _rate(IPFT[kind], day)
    sebi = value * SEBI_FEE
    stamp = value * STAMP[kind] if side > 0 else 0.0
    gst = GST * (brk + exch + ipft + sebi)
    return Cost(brk, stt, exch, sebi, stamp, gst, ipft)
