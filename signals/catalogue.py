"""Importing this module registers every signal and provider (SCRUM-146 onwards)."""

from signals.options import core, vol_gate, holding, expiry, direction, surface, positioning, live, composite  # noqa: F401
from signals.framework import REGISTRY, specs  # noqa: F401
