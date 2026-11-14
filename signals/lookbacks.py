"""
How far back into today's option chain and option ticks each live signal reads (owner decision 14 Nov 2026: a few minutes at most,
anything longer needs a strong reason).  The live runner gives each signal exactly this much of today's options data; the parity test
(tests/unit/test_live_runner.py) replays a day with these windows against the whole day at once, so a declared lookback that is too
short fails there.

Not counted here, because they are cheap and loaded once:
  - index, India VIX and futures minutes (about 375 rows a day each): read whole, for session anchors (open, VWAP, realised variance
    since the open) and the noise band's 14-session history;
  - anchors from earlier sessions, computed at the open: HAR forecast and daily realised measures, time-of-day variance profile,
    previous session's values for day-over-day changes, the 20-session spread and volume baselines, participant OI;
  - the option chain itself carries a contract's last trade for up to STALE_MAX (5) minutes; IV is only solved on prints up to
    IV_AGE (2) minutes old.  That is input validity, not signal memory.

Each entry: (lookback beyond the current bucket, reason).  "1 bucket" means the previous bucket only (a difference or a previous mid).
"""

from __future__ import annotations

import pandas as pd

BUCKET = "1 bucket"
LONGER_THAN = pd.Timedelta(minutes=5)          # longer lookbacks need a written justification (tested)

LOOKBACK: dict[str, tuple[str, str]] = {
    # cross-sections of the chain at the bucket
    "pin": ("0min", "OI and gamma by strike at the bucket"),
    "pin_full": ("0min", "OI by strike at the bucket"),
    "curvature": ("0min", "smile at the bucket"),
    "term_slope": ("0min", "ATM IV of the two front expiries at the bucket"),
    "event_variance": ("0min", "forward variance between the two front expiries at the bucket"),
    "cp_iv_gap": ("0min", "call and put IV at the same strikes at the bucket"),
    "rr25": ("0min", "25-delta IVs at the bucket"),
    "gex": ("0min", "gamma x OI at the bucket; participant OI is yesterday's file"),
    "gex_full": ("0min", "gamma x OI at the bucket"),
    "pcr_near": ("0min", "OI and the day's volume counter at the bucket"),
    "pcr_full": ("0min", "OI and the day's volume counter at the bucket"),
    "implied_move": ("0min", "straddle at the bucket against realised moves of earlier sessions"),
    "vrp": ("0min", "7-day IV at the bucket against yesterday's HAR forecast"),
    "rv_over_iv": ("0min", "from vrp"),
    "vol_size": ("0min", "from vrp"),
    "vrp_intraday": ("0min", "7-day IV at the bucket; realised variance since the open comes from index minutes"),
    "fair_straddle": ("0min", "straddle at the bucket against the forecast and the time-of-day profile"),
    # the book and the tape in the bucket, compared with the previous update
    "book_pressure": ("0min", "last quote in the bucket"),
    "illiquidity": ("0min", "quotes in the bucket"),
    "spread_gate": ("0min", "spreads in the bucket against the 20-session baseline of the same slot (loaded at the open)"),
    "fut_ofi": (BUCKET, "each book update against the previous one"),
    "mlofi_future": (BUCKET, "each book update against the previous one, five levels"),
    "signed_flow": (BUCKET, "each trade against the quote before it"),
    "trade_size_flow": (BUCKET, "each trade against the quote before it"),
    "block_prints": (BUCKET, "OI change against the previous bucket"),
    "opening_flow": (BUCKET, "OI change against the previous bucket"),
    "fair_value_gap": (BUCKET, "option mid and future microprice against the previous bucket"),
    # short windows
    "volume_burst": ("5min", "volume of the last 5 minutes against the same slot of earlier sessions (loaded at the open)"),
    "residual_iv": ("5min", "ATM IV change over the last 5 minutes net of beta x the index return over the same 5 minutes"),
    "depth_features": ("5min", "liquidity withdrawal: depth against its median of the previous 5 minutes"),
    "skew": ("10min", "smoothed skew: exponential average with a 2-minute half-life, truncated at 10 minutes (weights older than that "
                      "are under 4 %).  It bridges wing strikes whose last print is stale (IV is only solved on prints up to 2 minutes "
                      "old); it is a memory of the average, not a decision horizon.  The raw skew at the bucket is in the same row."),
    # anchored to the open (index and futures minutes only; no options data beyond the bucket)
    "noise_band": ("0min", "options not read; the band uses today's open and VWAP and 14 sessions of futures minutes"),
}


def lookback(sid: str, bucket: pd.Timedelta = pd.Timedelta(minutes=1)) -> pd.Timedelta:
    """The window of today's options data `sid` needs beyond its current bucket."""
    v = LOOKBACK[sid][0]
    return bucket if v == BUCKET else pd.Timedelta(v)
