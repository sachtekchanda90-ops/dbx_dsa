"""Invariants for Spark ML tree maxBins vs categorical hash buckets."""


# mypy: ignore-errors

from __future__ import annotations

from dsa_interview_packet.categorical_hash import CAT_HASH_BUCKETS
from dsa_interview_packet.ml_tree_config import SPARK_TREE_MAX_BINS


def test_max_bins_covers_hash_buckets() -> None:
    """Spark GBT/RF require maxBins >= distinct values per feature; _hb cols are < CAT_HASH_BUCKETS."""
    assert SPARK_TREE_MAX_BINS >= CAT_HASH_BUCKETS
    assert SPARK_TREE_MAX_BINS == CAT_HASH_BUCKETS


def test_cat_hash_buckets_documented_range() -> None:
    """pmod(xxhash64(x), n) in Spark maps to [0, n) when n >= 2."""
    assert CAT_HASH_BUCKETS >= 2


def test_modulo_bucket_range_pure_math() -> None:
    """Mirror pmod(hash, n): remainder is in [0, n-1] for non-negative modulus."""
    n = CAT_HASH_BUCKETS
    for h in (0, 1, 42, 10**9, n - 1, n, n + 1, -1, -n):
        r = h % n
        assert 0 <= r < n
