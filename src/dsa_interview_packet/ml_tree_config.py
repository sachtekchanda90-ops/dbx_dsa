"""Spark ML tree hyperparameters shared by training jobs.

Hashed categoricals use ``pmod(xxhash64(...), CAT_HASH_BUCKETS)``; each ``_hb``
column has at most ``CAT_HASH_BUCKETS`` distinct values. Spark GBT/RF require
``maxBins`` to be at least that many per feature — see ``test_ml_tree_config``.
"""

from __future__ import annotations

from dsa_interview_packet.categorical_hash import CAT_HASH_BUCKETS

# Single source of truth: must be >= CAT_HASH_BUCKETS for hashed categorical columns.
SPARK_TREE_MAX_BINS = CAT_HASH_BUCKETS
