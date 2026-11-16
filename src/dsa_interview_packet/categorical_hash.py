"""Map high-cardinality strings to fixed buckets for Spark ML on Connect.

StringIndexer models embed full vocabularies and can exceed Spark Connect ML's
~256 MiB serialized model cap; ``pmod(xxhash64(...), n)`` stays small.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Bucket count for hashed categoricals (distinct values per column ≤ this value).
CAT_HASH_BUCKETS = 64

DEMAND_FORECAST_HASH_COLS = [
    "p_brand",
    "p_type",
    "p_container",
    "c_mktsegment",
]

SUPPLIER_SCORING_HASH_COLS = [
    "p_brand",
    "p_type",
    "o_orderpriority",
    "l_shipmode",
    "l_shipinstruct",
]


def with_categorical_hash_buckets(df: DataFrame, cols: list[str]) -> DataFrame:
    """Append ``<name>_hb`` double columns in ``[0, CAT_HASH_BUCKETS)`` for each string col."""
    out = df
    for c in cols:
        out = out.withColumn(
            f"{c}_hb",
            F.pmod(F.xxhash64(F.col(c)), F.lit(CAT_HASH_BUCKETS)).cast("double"),
        )
    return out
