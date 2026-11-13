"""
Every signal documented in Notion, mapped to the code that computes it (audit of 13 Nov 2026).  The test suite checks both directions: each
documented ID names registered outputs (or is listed as not feasible with the reason and what would unlock it), and every registered output
is documented here.

Sources: "Options signals: literature review and revised build plan (12 Nov 2026)" (catalogues A, B, C), "Options signals v1: catalogue and
build plan" (E and the IV gap), and the hedge-fund-style additions of 13 Nov 2026 (H).
"""

DOCUMENTED: dict[str, tuple[str, ...]] = {
    # A. implied volatility
    "A1 volatility risk premium": ("vrp",),
    "A2 RV above IV trigger": ("rv_over_iv",),
    "A3 implied vs realised move": ("implied_move", "implied_move_outcome"),
    "A4 overnight vs intraday holding rule": ("day_night_returns",),
    "A5 fair straddle on volatility time": ("fair_straddle",),
    "A6 India VIX regime": ("vix_regime",),
    "A7 spot-vol beta and residual IV": ("spot_vol_beta", "residual_iv"),
    "A8 term slope": ("term_slope",),
    "A9 forward variance and event jump": ("event_variance",),
    "A10 vol-of-vol": ("vol_of_vol",),
    "A11 smile slope and skew change": ("skew",),
    "A12 25-delta risk reversal": ("rr25",),
    "A13 near-money curvature": ("curvature",),
    "A14 surface factor shocks": ("surface_shocks",),
    "A15 skew stickiness": ("skew_stickiness",),
    "A16 call-put IV gap and implied basis": ("cp_iv_gap",),
    "A17 VRP tilt": ("vrp_tilt",),
    "A18 inverse-vol sizing": ("vol_size",),
    # B. positioning, flow, gamma, expiry
    "B1 expiry pinning": ("pin", "pin_test", "pin_full"),
    "B2 put-call ratio": ("pcr_near", "pcr_full"),
    "B3 OI quadrant and max pain": ("oi_quadrant",),
    "B4 signed buying pressure, volatility demand": ("vol_demand_proxy", "signed_flow"),
    "B5 dealer gamma, walls, flip": ("gex", "gex_full"),
    "B6 last-30-minute momentum": ("last30_momentum", "last30_outcome"),
    "B7 noise-band breakout": ("noise_band",),
    "B8 expiry last hour": ("expiry_last_hour",),
    "B9 same-day-expiry gamma dampening": ("gamma_dampening",),
    "B10 India VIX expiry dip": ("vix_expiry_dip",),
    "B11 participant positioning": ("participant_positioning",),
    "B12 futures basis and roll": ("basis_roll",),
    "B13 expiry manipulation footprint (Jane Street pattern)": ("basket_fair_value", "constituent_flow", "option_positioning_hourly",
                                                                  "expiry_inverted_v", "manipulation_flag"),
    "B14 opening-buyer PCR (proxy)": ("opening_flow", "opening_flow_daily"),
    # C. microstructure and execution
    "C1 delta-implied fair value": ("fair_value_gap",),
    "C2 relative-spread gate": ("spread_gate",),
    "C3 microprice": ("book_pressure",),
    "C4 queue imbalance": ("book_pressure",),
    "C5 futures order flow imbalance": ("fut_ofi",),
    "C6 own fill and cost log": ("cost_log",),
    "C7 illiquidity": ("illiquidity", "amihud"),
    "C8 trade signing": ("signed_flow",),
    "C9 future leads index": ("future_lead",),
    "C10 time-of-day normalisation": ("fair_straddle", "noise_band"),          # the `tod` profile, consumed by these
    "C11 semivariance and jumps": ("vrp",),                                      # the `daily` measures, consumed by the gate
    "C12 multi-level order flow imbalance": ("mlofi_future", "depth_features"),
    # E. v1 catalogue
    "E1 relative option volume": ("option_volume",),
    "E2 premium turnover": ("premium_turnover",),
    "E3 volume burst": ("volume_burst",),
    "v1 IV gap": ("iv_gap",),
    # composite and hedge-fund-style additions
    "composite buy permission": ("buy_permission",),
    "H basket fair value (index arbitrage)": ("basket_fair_value",),
    "H trade-size flow": ("trade_size_flow",),
    "H block prints": ("block_prints",),
    "H implied correlation proxy": ("implied_corr_proxy",),
}

NOT_FEASIBLE: dict[str, str] = {
    "A19 risk-neutral skewness, kurtosis, tail": "needs far out-of-the-money wings: the NSE bhavcopy (SCRUM-184) gives them at daily frequency",
    "A20 implied correlation": "needs heavyweight stock options (SCRUM-194); implied_corr_proxy uses realised constituent vols meanwhile",
    "B14 investor-type flow": "NSE publishes no customer or open/close flags; opening_flow is the proxy",
    "C9 GIFT Nifty lead": "GIFT Nifty intraday needs a paid vendor (SCRUM-188 collects end-of-day cues only)",
    "SENSEX cross-exchange signals": "SCRUM-195: BSE contracts are not collected yet",
}
