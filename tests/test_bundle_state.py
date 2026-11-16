"""Unit tests for bundle_state fingerprints (no Spark)."""


# mypy: ignore-errors

from __future__ import annotations

from dsa_interview_packet.bundle_state import seed_fingerprint, simulate_fingerprint


def test_seed_fingerprint_stable() -> None:
    assert seed_fingerprint(0.003, 42) == "seed:0.003:42"
    assert seed_fingerprint(1.0, 7) == "seed:1:7"


def test_simulate_fingerprint_stable() -> None:
    assert simulate_fingerprint(1) == "simulate:1"
    assert simulate_fingerprint(30) == "simulate:30"
