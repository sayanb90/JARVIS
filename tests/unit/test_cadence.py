"""Per-signal cadence config (owner decision 14 Nov 2026): 1s, 10s or 1min; minimums enforced; checked at start-up."""

import pytest

from signals import cadence
from signals.live_runner import live_signal_ids

IDS = live_signal_ids()


def test_tracked_config_is_valid():
    c = cadence.load(IDS, env={})
    assert set(c) == set(IDS) and set(c.values()) <= {"1s", "10s", "1min"}


def test_default_override_and_per_signal_lines(tmp_path):
    p = tmp_path / "c.toml"
    p.write_text('default = "10s"\n[signal]\nfut_ofi = "1s"\nskew = "1min"\n')
    c = cadence.load(IDS, path=p, env={})
    assert c["fut_ofi"] == "1s" and c["skew"] == "1min" and c["pcr_near"] == "10s"
    assert c["noise_band"] == "1min"                         # the default is faster than its minimum: it runs at its minimum
    c = cadence.load(IDS, path=p, env={"JARVIS_SIGNAL_CADENCE": "1m"})
    assert c["pcr_near"] == "1min" and c["fut_ofi"] == "1s"  # the env overrides the default only


@pytest.mark.parametrize("text, err", [('[signal]\nnoise_band = "1s"\n', "faster than its minimum"),
                                       ('[signal]\nnot_a_signal = "1s"\n', "not live signals"),
                                       ('default = "5s"\n', "use 1s, 10s or 1min")])
def test_invalid_configs_are_refused(tmp_path, text, err):
    p = tmp_path / "c.toml"
    p.write_text(text)
    with pytest.raises(ValueError, match=err):
        cadence.load(IDS, path=p, env={})


def test_groups_finest_first():
    g = cadence.groups({"a": "1min", "b": "1s", "c": "10s", "d": "1s"})
    assert list(g) == ["1s", "10s", "1min"] and g["1s"] == ["b", "d"]
