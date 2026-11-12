"""The nightly Yahoo jobs raise their own open-file limit, so cron's default of 1,024 cannot fail every market again."""
import resource

import pytest

from data_layer import limits


@pytest.fixture
def restore_limit():
    before = resource.getrlimit(resource.RLIMIT_NOFILE)
    yield
    resource.setrlimit(resource.RLIMIT_NOFILE, before)


def test_the_soft_limit_is_raised_to_the_target_or_the_hard_limit(restore_limit):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    low = min(soft, 512)
    resource.setrlimit(resource.RLIMIT_NOFILE, (low, hard))
    got = limits.raise_open_files()
    want = limits.TARGET if hard == resource.RLIM_INFINITY else min(limits.TARGET, hard)
    assert got == (want, hard) and resource.getrlimit(resource.RLIMIT_NOFILE)[0] == want and want > low


def test_a_limit_that_is_already_high_is_left_alone(restore_limit):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    assert limits.raise_open_files(target=64) == (soft, hard)                          # already above the target: nothing changes
    assert resource.getrlimit(resource.RLIMIT_NOFILE) == (soft, hard)


def test_a_refusal_never_stops_the_job(monkeypatch):
    def refuse(which, value):
        raise OSError("not permitted")
    monkeypatch.setattr(resource, "getrlimit", lambda which: (256, 100_000))
    monkeypatch.setattr(resource, "setrlimit", refuse)
    assert limits.raise_open_files() is None                                            # logged, not raised
