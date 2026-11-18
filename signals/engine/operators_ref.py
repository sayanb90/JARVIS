"""
The first implementations of the native operators that loop over symbols (17 Nov 2026), kept unchanged as the reference for their vectorised
versions in signals/engine/operators.py (SCRUM-205).

Two uses: the parity tests compare the vectorised operators with these on dense ticks, and the engine can run them next to the vectorised ones
on the live feed (JARVIS_SHADOW_PARITY=1: signals/engine/shadow.py) and log every difference.  They are slow on purpose (a Python loop per
symbol); do not call them on the live path.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from signals.engine.operators import BLOCK_LOTS_FUT, BLOCK_LOTS_OPT, SIZE_LABELS, Natives, _div, _last

REFERENCE_IDS = ("signed_flow", "trade_size_flow", "opening_flow", "block_prints", "fair_value_gap")


class ReferenceNatives(Natives):
    """Natives with the loop implementations of the per-symbol operators (the others are inherited and not run: pass ids from REFERENCE_IDS)."""

    def _greek_table(self, chain_live, book):
        greek = {}
        if chain_live is not None and len(chain_live) and book is not None:
            s = chain_live.slots
            for j, slot in enumerate(s):
                greek[book.symbols[slot]] = (chain_live.cols["delta"][j], chain_live.cols["vega"][j], chain_live.cols["k"][j])
        return greek

    def _lee_ready(self, tp: TapeTable, rows: np.ndarray):
        """dv and sign for `rows` (in the bucket), against each symbol's previous tick; updates the tick-rule memory."""
        pv = tp.shifted(tp.vol_today)
        dv = np.nan_to_num(np.clip(tp.vol_today - pv, 0, None), nan=0.0)
        prev_mid = (tp.shifted(tp.bid) + tp.shifted(tp.ask)) / 2
        pl = tp.shifted(tp.ltp)
        with np.errstate(invalid="ignore"):
            d = np.sign(tp.ltp - pl)
        sign = np.zeros(tp.n)
        for s, idx in tp.groups(np.ones(tp.n, bool)):
            mem = self.tick_sign.get(s, 0.0)
            for i in idx:
                if d[i] == d[i] and d[i] != 0:
                    mem = d[i]
                pm = prev_mid[i]
                if pm != pm:
                    sign[i] = mem
                elif tp.ltp[i] > pm:
                    sign[i] = 1.0
                elif tp.ltp[i] < pm:
                    sign[i] = -1.0
                else:
                    sign[i] = mem
            self.tick_sign[s] = mem
        return dv, sign

    def _carry(self, tp: TapeTable) -> None:
        for s, idx in tp.groups(tp.is_opt):
            v = _last(tp.oi, idx)
            if v == v:
                self.prev_oi[s] = v

    def _signed_flow(self, tp, ts, ka, dv=None, sign=None, greek=None, **_):
        o = np.flatnonzero(tp.is_opt & tp.in_b)
        if not len(o) or not greek:
            return pd.DataFrame()
        sv = df_ = vf = 0.0
        for s, idx in tp.groups(tp.is_opt):
            signed = float((sign[idx] * dv[idx]).sum())
            d_, v_, _ = greek.get(s, (math.nan, math.nan, math.nan))
            lot = tp.lot[idx[0]]
            lot = 1.0 if lot != lot else lot
            sv += signed
            df_ += signed * (0.0 if d_ != d_ else d_) * lot
            vf += signed * (0.0 if v_ != v_ else v_) * lot / 100.0
        return pd.DataFrame([{"ts": ts, "signed_volume": sv, "delta_flow": df_, "vega_flow": vf, "known_at": ka}])

    def _lots(self, tp, rows):
        with np.errstate(divide="ignore", invalid="ignore"):
            lot = np.where(tp.lot[rows] == 0, np.nan, tp.lot[rows])
            return tp.ltq[rows] / lot

    def _trade_size_flow(self, tp, ts, ka, dv=None, sign=None, **_):
        if not tp.has_ltq or np.isnan(tp.ltq).all():
            return pd.DataFrame()
        rows = np.flatnonzero(tp.in_b & (tp.is_opt | tp.is_fut) & (dv > 0))
        if not len(rows):
            return pd.DataFrame()
        lots = self._lots(tp, rows)
        lots = np.where(np.isnan(lots), 1.0, lots)
        out = {f"{k}_{b}": 0.0 for k in ("opt", "fut") for b in SIZE_LABELS}
        edges = [0, 1, 5, 20, np.inf]
        for j, r in enumerate(rows):
            x = lots[j]
            lab = next((SIZE_LABELS[i] for i in range(4) if edges[i] < x <= edges[i + 1]), None)
            if lab is None:
                continue
            kind = "fut" if tp.is_fut[r] else "opt"
            out[f"{kind}_{lab}"] += sign[r] * x
        return pd.DataFrame([{"ts": ts, **out, "known_at": ka}])

    def _opening_flow(self, tp, ts, ka, dv=None, sign=None, **_):
        cls_names = ("open_buy", "open_sell", "close_short", "close_long", "none")
        tot = {f"{c}_{s}": 0.0 for s in ("calls", "puts") for c in cls_names}
        any_row = False
        for s, idx in tp.groups(tp.is_opt):
            any_row = True
            signed = float((sign[idx] * dv[idx]).sum())
            vol = float(dv[idx].sum())
            oi = _last(tp.oi, idx)
            prev = self.prev_oi.get(s)
            d_oi = oi - prev if prev is not None and oi == oi else 0.0
            d_oi = 0.0 if d_oi != d_oi else d_oi
            c = ("open_buy" if d_oi > 0 and signed > 0 else "open_sell" if d_oi > 0 and signed < 0 else
                 "close_short" if d_oi < 0 and signed > 0 else "close_long" if d_oi < 0 and signed < 0 else "none")
            side = "calls" if self.meta[s][0] == "CE" else "puts"
            tot[f"{c}_{side}"] += vol
        if not any_row:
            return pd.DataFrame()
        for side in ("calls", "puts"):
            tot[f"net_open_buy_{side}"] = tot[f"open_buy_{side}"] - tot[f"open_sell_{side}"]
        return pd.DataFrame([{"ts": ts, **tot, "known_at": ka}])

    def _block_prints(self, tp, ts, ka, dv=None, sign=None, **_):
        if not tp.has_ltq:
            return pd.DataFrame()
        rows = np.flatnonzero(tp.in_b & (tp.is_opt | tp.is_fut))
        if not len(rows):
            return pd.DataFrame()
        lots = self._lots(tp, rows)
        big = (tp.is_fut[rows] & (lots >= BLOCK_LOTS_FUT)) | (tp.is_opt[rows] & (lots >= BLOCK_LOTS_OPT))
        ev = rows[big & (dv[rows] > 0)]
        if not len(ev):
            return pd.DataFrame()
        top = None
        for s, idx in tp.groups(tp.is_opt):
            oi = _last(tp.oi, idx)
            prev = self.prev_oi.get(s)
            if prev is None or oi != oi:
                continue
            d = oi - prev
            if top is None or abs(d) > abs(top[3]):
                e, k, cp = self._info(s)
                top = (s, k, cp, d)
        lot_of = dict(zip(rows, lots))
        out = []
        for r in ev:
            out.append({"ts": ts, "symbol": tp.sym[r], "instrument_type": tp.itype[r], "ltp": tp.ltp[r], "lots": lot_of[r],
                        "sign": sign[r], "oi_symbol": top[0] if top else None, "oi_strike": top[1] if top else math.nan,
                        "oi_cp": top[2] if top else None, "oi_jump": top[3] if top else math.nan,
                        "known_at": pd.Timestamp(int(tp.t[r]))})
        return pd.DataFrame(out)

    def _fair_value_gap(self, tp, ts, ka, greek=None, **_):
        fut = np.flatnonzero(tp.is_fut & tp.in_b)
        fmp = math.nan
        if len(fut):
            e = min(tp.expiry[fut])
            fr = fut[tp.expiry[fut] == e]
            fmp = _last(tp.microprice, fr)
        rows = []
        for s, idx in tp.groups(tp.is_opt):
            e, k, cp = self._info(s)
            mid, bid, ask = _last(tp.mid, idx), _last(tp.bid, idx), _last(tp.ask, idx)
            prev = self.prev_fvg.get(s)
            self.prev_fvg[s] = (tp.b, mid, fmp)
            if prev is None or prev[0] != tp.b - self.td or not greek:
                continue
            d_ = greek.get(s, (math.nan,))[0]
            fair = prev[1] + d_ * (fmp - prev[2])
            if fair != fair:
                continue
            rows.append({"ts": ts, "known_at": ka, "symbol": s, "expiry": e, "strike": k, "cp": "C" if cp == "CE" else "P", "fair": fair,
                         "buy_edge": _div(fair - ask, mid), "sell_edge": _div(bid - fair, mid)})
        return pd.DataFrame(rows)
