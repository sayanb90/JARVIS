"""Write the synthetic market into a lake-shaped directory, so the lake reader and the runner can be tested end to end."""

from pathlib import Path

import pandas as pd

from signals.synthetic import make_raw


def write_fake_lake(lake: Path, **kw) -> dict:
    raw = make_raw(**kw)
    bars = pd.concat([raw["index_1m"], raw["vix_1m"], raw["fut_1m"], raw["opt_1m"], raw["stocks_1m"]], ignore_index=True)
    bars["market"] = "IN"
    for c in ("volume", "oi"):
        if c not in bars.columns:
            bars[c] = None
    bars["volume"] = bars["volume"].fillna(0).astype("int64")
    for d, g in bars.groupby(bars["ts"].dt.date):
        folder = lake / "bars" / "interval=1m" / f"date={d}"
        folder.mkdir(parents=True, exist_ok=True)
        g.to_parquet(folder / "bars.parquet", index=False)
    t = raw["ticks_fo"]
    for d, g in t.groupby(t["ts"].dt.date):
        folder = lake / "ticks" / "segment=fo" / f"date={d}"
        folder.mkdir(parents=True, exist_ok=True)
        g.to_parquet(folder / "ticks.parquet", index=False)
    (lake / "instruments").mkdir(parents=True, exist_ok=True)
    c = raw["contracts"].copy()
    c["expired_key"] = c["symbol"]
    c.to_parquet(lake / "instruments" / "fo_expired.parquet", index=False)
    (lake / "positioning").mkdir(parents=True, exist_ok=True)
    raw["participant"].drop(columns=["known_at"]).to_parquet(lake / "positioning" / "participant_oi.parquet", index=False)
    (lake / "reference").mkdir(parents=True, exist_ok=True)
    w = raw["weights"].drop(columns=["known_at"]).assign(index="NIFTY 50", weight_pct=lambda x: x["weight"] * 100).drop(columns=["weight"])
    w.to_parquet(lake / "reference" / "index_weights.parquet", index=False)
    return raw
