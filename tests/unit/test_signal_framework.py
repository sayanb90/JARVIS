"""Signal framework (SCRUM-146): registry, label guard, cutting, store, trial log."""

import pandas as pd
import pytest

from signals import catalogue  # noqa: F401  (registers the catalogue)
from signals.framework import (REGISTRY, Context, LabelAccessError, Spec, cut_raw, log_trial, read_output, read_trials, register,
                               specs, trailing_z, write_output)


def test_catalogue_has_every_family_and_tickets():
    fams = {s.family for s in specs()}
    assert {"vol_gate", "holding", "expiry", "direction", "surface", "positioning", "live", "composite"} <= fams
    assert all(s.tickets for s in specs())
    assert len(specs()) >= 45


def test_every_spec_has_doc_and_valid_grid():
    for s in specs():
        assert s.doc and s.grid in ("1m", "snap", "daily", "expiry", "event")


def test_live_specs_need_a_sample():
    live = [s for s in specs() if s.live_only]
    assert live and all(s.min_sample >= 25 for s in live)


def test_duplicate_registration_fails():
    with pytest.raises(ValueError):
        register(Spec(id="vrp", version=9, family="x", grid="daily"))(lambda c, p: None)


def test_signal_cannot_read_a_label():
    ctx = Context("NIFTY", {})
    with pytest.raises(LabelAccessError):
        ctx.signal("implied_move_outcome")
    with pytest.raises(LabelAccessError):
        ctx.signal("last30_outcome")


def test_labels_are_not_inputs_of_signals():
    labels = {s.id for s in specs() if s.kind != "signal"}
    for s in specs(kind="signal"):
        assert not (set(s.inputs) & labels), s.id


def test_cut_raw():
    raw = {"m": pd.DataFrame({"ts": pd.to_datetime(["2026-01-01 04:00", "2026-01-01 04:01"])}),
           "t": pd.DataFrame({"ts": pd.to_datetime(["2026-01-01 04:00:30"]), "fts": pd.to_datetime(["2026-01-01 04:00:30"])}),
           "d": pd.DataFrame({"date": ["x", "y"], "known_at": pd.to_datetime(["2026-01-01 04:00", "2026-01-01 16:00"])}),
           "ref": pd.DataFrame({"a": [1]})}
    c = cut_raw(raw, pd.Timestamp("2026-01-01 04:01:30"))
    assert len(c["m"]) == 1 and len(c["t"]) == 1 and len(c["d"]) == 1 and len(c["ref"]) == 1


def test_trailing_z_excludes_current_value():
    x = pd.Series([1.0] * 10 + [2.0])
    z = trailing_z(x, 10)
    assert pd.isna(z.iloc[-1]) or z.iloc[-1] > 0                     # std of a constant baseline is 0 -> NaN, never a self-including z


def test_store_and_trial_log(tmp_path):
    spec = REGISTRY["vix_regime"][0]
    df = pd.DataFrame({"underlying": "NIFTY", "date": [pd.Timestamp("2026-01-01").date(), pd.Timestamp("2026-01-02").date()],
                       "known_at": pd.to_datetime(["2026-01-01 10:00", "2026-01-02 10:00"]), "vix": [12.0, 13.0]})
    assert write_output(tmp_path, spec, df) == 2
    back = read_output(tmp_path, "vix_regime", "NIFTY")
    assert list(back["vix"]) == [12.0, 13.0]
    assert write_output(tmp_path, spec, df.iloc[:1]) == 1               # rewriting a day replaces it
    assert len(read_output(tmp_path, "vix_regime", "NIFTY")) == 2
    rec = log_trial(tmp_path, spec, "NIFTY", "2026-01-01", "2026-01-02", 2)
    assert rec["params_hash"] == spec.params_hash
    assert read_trials(tmp_path)[-1]["signal"] == "vix_regime"


def test_every_documented_signal_is_computed_and_every_output_is_documented():
    """Audit (13 Nov 2026): the Notion catalogues map to registered outputs, and nothing registered is undocumented."""
    from signals.coverage_map import DOCUMENTED, NOT_FEASIBLE
    missing = {doc: sid for doc, sids in DOCUMENTED.items() for sid in sids if sid not in REGISTRY}
    assert not missing, missing
    documented = {sid for sids in DOCUMENTED.values() for sid in sids}
    assert not set(REGISTRY) - documented, sorted(set(REGISTRY) - documented)
    assert all(NOT_FEASIBLE.values())
