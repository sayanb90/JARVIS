"""
Transaction costs of NSE index options and futures, per fill, in rupees.  Rates are dated: each applies from its date on.

Verify against a broker contract note before trusting paper P&L to the rupee; the table is the place to correct a rate.

  brokerage   flat per executed order (Upstox: Rs 20)
  STT         options: on the premium of the sell side; futures: on the sell-side value
  exchange    NSE transaction charge on premium (options) or value (futures)
  SEBI        turnover fee, Rs 10 per crore
  stamp       buy side only
  GST         18 % on brokerage + exchange charge + SEBI fee
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional

# (from date, rate as a fraction).  The last entry dated on or before the trade date applies.
STT = {
    "opt_sell": [(date(2023, 4, 1), 0.000625), (date(2024, 10, 1), 0.001), (date(2026, 4, 1), 0.0015)],     # 2026: Union Budget 2026
    "fut_sell": [(date(2023, 4, 1), 0.000125), (date(2024, 10, 1), 0.0002), (date(2026, 4, 1), 0.0005)],
}
EXCHANGE = {"opt": [(date(2024, 10, 1), 0.0003503)], "fut": [(date(2024, 10, 1), 0.0000173)]}
SEBI_FEE = 10 / 1e7
STAMP = {"opt": 0.00003, "fut": 0.00002}
GST = 0.18
BROKERAGE = 20.0


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

    @property
    def total(self) -> float:
        return self.brokerage + self.stt + self.exchange + self.sebi + self.stamp + self.gst


def fill_cost(kind: str, side: int, price: float, qty: int, day: Optional[date] = None, brokerage: float = BROKERAGE) -> Cost:
    """Costs of one fill: kind "opt" or "fut", side +1 buy / -1 sell, price per unit, qty in units (lots x lot size)."""
    day = day or date.today()
    value = abs(price * qty)
    stt = value * _rate(STT[f"{kind}_sell"], day) if side < 0 else 0.0
    exch = value * _rate(EXCHANGE[kind], day)
    sebi = value * SEBI_FEE
    stamp = value * STAMP[kind] if side > 0 else 0.0
    gst = GST * (brokerage + exch + sebi)
    return Cost(brokerage, stt, exch, sebi, stamp, gst)
