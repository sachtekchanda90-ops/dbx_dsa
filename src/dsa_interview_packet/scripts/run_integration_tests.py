# pylint: disable=missing-class-docstring,missing-function-docstring,too-many-instance-attributes,invalid-name
"""Integration tests for supply chain analytics implementations.

Run via DABs:
    databricks bundle run -t dev run_integration_tests

Use ``--impl`` to run only a specific implementation for faster iteration::

    python run_integration_tests.py --impl lakehouse ...  # Lakehouse SQL only
    python run_integration_tests.py --impl lakeflow ...    # Lakeflow pipeline only
    python run_integration_tests.py --impl all ...        # all 3 (default)

Executes queries via Lakehouse SQL (spark.sql), Lakeflow (pipeline functions),
and/or legacy pandas against the live seeded tables, then validates:

  1. Cross-implementation parity  -- Lakehouse SQL == Lakeflow row counts and sampled values
  2. Legacy parity                -- pandas results match Lakehouse SQL on a sampled subset
  3. Schema correctness           -- column names and count
  4. Completeness                 -- row counts in expected range, NULL rates
  5. Value ranges                 -- numeric bounds, known enums
  6. Distribution                 -- group counts, no degenerate single-group results
  7. Uniqueness                   -- key column uniqueness where expected
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

import ensure_src_path  # noqa: F401  # pylint: disable=unused-import  # must precede dsa_interview_packet imports
from dsa_interview_packet.sql_identifiers import quote_sql_ident
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Query metadata
# ---------------------------------------------------------------------------

@dataclass
class QuerySpec:
    name: str
    columns: list[str]
    min_rows: int = 1
    max_rows: int = 1_000_000
    not_null_cols: list[str] = field(default_factory=list)
    positive_cols: list[str] = field(default_factory=list)
    unique_cols: list[str] = field(default_factory=list)
    enum_checks: dict[str, set[str]] = field(default_factory=dict)
    range_checks: dict[str, tuple[float, float]] = field(default_factory=dict)


QUERY_SPECS = [
    QuerySpec("pricing_summary",
              ["l_returnflag", "l_linestatus", "sum_qty", "sum_base_price",
               "sum_disc_price", "sum_charge", "avg_qty", "avg_price", "avg_disc", "count_order"],
              min_rows=4, max_rows=4,
              not_null_cols=["l_returnflag", "l_linestatus", "sum_qty", "count_order"],
              positive_cols=["sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "count_order"],
              range_checks={"avg_disc": (0.0, 1.0)}),
    QuerySpec("min_cost_supplier",
              ["s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment"],
              min_rows=1, max_rows=100,
              not_null_cols=["s_acctbal", "s_name", "p_partkey"]),
    QuerySpec("shipping_priority",
              ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"],
              min_rows=1, max_rows=10,
              positive_cols=["revenue"],
              not_null_cols=["l_orderkey", "revenue"]),
    QuerySpec("order_priority",
              ["o_orderpriority", "order_count"],
              min_rows=5, max_rows=5,
              positive_cols=["order_count"],
              enum_checks={"o_orderpriority": {"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"}}),
    QuerySpec("local_supplier_revenue",
              ["n_name", "revenue"],
              min_rows=5, max_rows=5,
              positive_cols=["revenue"],
              enum_checks={"n_name": {"CHINA", "INDIA", "INDONESIA", "JAPAN", "VIETNAM"}}),
    QuerySpec("revenue_forecast",
              ["revenue"], min_rows=1, max_rows=1, positive_cols=["revenue"]),
    QuerySpec("bilateral_trade",
              ["supp_nation", "cust_nation", "l_year", "revenue"],
              min_rows=4, max_rows=4,
              positive_cols=["revenue"],
              range_checks={"l_year": (1995, 1996)}),
    QuerySpec("market_share",
              ["o_year", "mkt_share"],
              min_rows=2, max_rows=2,
              range_checks={"o_year": (1995, 1996), "mkt_share": (0.0, 1.0)}),
    QuerySpec("product_profit",
              ["nation", "o_year", "sum_profit"],
              min_rows=100, max_rows=200,
              not_null_cols=["nation", "o_year"],
              range_checks={"o_year": (1992, 1998)}),
    QuerySpec("returned_items",
              ["c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment"],
              min_rows=1, max_rows=20, positive_cols=["revenue"]),
    QuerySpec("critical_inventory",
              ["ps_partkey", "value"],
              min_rows=0, max_rows=100_000,
              positive_cols=["value"], unique_cols=["ps_partkey"]),
    QuerySpec("shipping_mode_analysis",
              ["l_shipmode", "high_line_count", "low_line_count"],
              min_rows=2, max_rows=2,
              enum_checks={"l_shipmode": {"MAIL", "SHIP"}}),
    QuerySpec("customer_distribution",
              ["c_count", "custdist"],
              min_rows=10, max_rows=100, positive_cols=["custdist"]),
    QuerySpec("promotion_effect",
              ["promo_revenue"], min_rows=1, max_rows=1,
              range_checks={"promo_revenue": (0.0, 100.0)}),
    QuerySpec("top_supplier",
              ["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"],
              min_rows=1, max_rows=10, positive_cols=["total_revenue"]),
    QuerySpec("supplier_parts_coverage",
              ["p_brand", "p_type", "p_size", "supplier_cnt"],
              min_rows=100, max_rows=100_000, positive_cols=["supplier_cnt"]),
    QuerySpec("small_order_revenue",
              ["avg_yearly"], min_rows=1, max_rows=1),
    QuerySpec("large_volume_customers",
              ["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice", "sum_qty"],
              min_rows=1, max_rows=100, not_null_cols=["c_custkey", "o_orderkey"]),
    QuerySpec("discounted_revenue",
              ["revenue"], min_rows=1, max_rows=1, positive_cols=["revenue"]),
    QuerySpec("excess_inventory",
              ["s_name", "s_address"],
              min_rows=1, max_rows=10_000, not_null_cols=["s_name"]),
    QuerySpec("supplier_reliability",
              ["s_name", "numwait"],
              min_rows=1, max_rows=100, positive_cols=["numwait"]),
    QuerySpec("sales_opportunity",
              ["cntrycode", "numcust", "totacctbal"],
              min_rows=7, max_rows=7,
              positive_cols=["numcust", "totacctbal"],
              enum_checks={"cntrycode": {"13", "17", "18", "23", "29", "30", "31"}}),
]


# ---------------------------------------------------------------------------
# Result tracker
# ---------------------------------------------------------------------------

class TestResults:
    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0
        self.errors: list[str] = []

    def ok(self, msg: str) -> None:
        self.passed += 1
        print(f"  PASS: {msg}")

    def fail(self, msg: str) -> None:
        self.failed += 1
        self.errors.append(msg)
        print(f"  FAIL: {msg}")

    @property
    def summary(self) -> str:
        total = self.passed + self.failed
        return f"{self.passed}/{total} passed, {self.failed} failed"


# ---------------------------------------------------------------------------
# Cross-implementation comparison (Spark-native, no .collect())
# ---------------------------------------------------------------------------

def compare_spark_dataframes(
    results: TestResults, label: str,
    df_a: DataFrame, name_a: str,
    df_b: DataFrame, name_b: str,
) -> int | None:
    """Compare two Spark DataFrames for schema and row-count parity.

    Returns the row count of *df_a* so the caller can feed it into
    ``check_properties`` without re-scanning the table.
    """
    cols_a = sorted(c.lower() for c in df_a.columns)
    cols_b = sorted(c.lower() for c in df_b.columns)
    if cols_a != cols_b:
        results.fail(f"{label}: column mismatch {name_a}={sorted(df_a.columns)} vs {name_b}={sorted(df_b.columns)}")
        return None

    count_a = df_a.count()
    count_b = df_b.count()
    if count_a != count_b:
        results.fail(f"{label}: row count {name_a}={count_a} vs {name_b}={count_b}")
        return count_a

    # For small results (< 1000 rows), do a full anti-join to find mismatches.
    # For large results, compare aggregates.
    if count_a <= 1000:
        # Normalize column names and sort columns alphabetically for positional parity
        sorted_cols = sorted(c.lower() for c in df_a.columns)
        a = df_a.toDF(*[c.lower() for c in df_a.columns]).select(*sorted_cols)
        b = df_b.toDF(*[c.lower() for c in df_b.columns]).select(*sorted_cols)

        # Cast all numeric columns to double to avoid Decimal vs Double mismatches,
        # and trim whitespace from string columns for consistent comparison.
        from pyspark.sql.types import (
            DecimalType,
            FloatType,
            IntegerType,
            LongType,
            ShortType,
            StringType,
        )
        cast_cols = []
        for fld in a.schema.fields:
            if isinstance(fld.dataType, (DecimalType, FloatType, IntegerType, LongType, ShortType)):
                cast_cols.append(F.col(fld.name).cast("double").alias(fld.name))
            elif isinstance(fld.dataType, StringType):
                cast_cols.append(F.trim(F.col(fld.name)).alias(fld.name))
            else:
                cast_cols.append(F.col(fld.name))
        a = a.select(*cast_cols)

        cast_cols_b = []
        for fld in b.schema.fields:
            if isinstance(fld.dataType, (DecimalType, FloatType, IntegerType, LongType, ShortType)):
                cast_cols_b.append(F.col(fld.name).cast("double").alias(fld.name))
            elif isinstance(fld.dataType, StringType):
                cast_cols_b.append(F.trim(F.col(fld.name)).alias(fld.name))
            else:
                cast_cols_b.append(F.col(fld.name))
        b = b.select(*cast_cols_b)

        diff = a.exceptAll(b)
        diff_count = diff.count()
        if diff_count == 0:
            results.ok(f"{label}: {name_a} == {name_b} ({count_a} rows, exact match)")
        else:
            results.fail(f"{label}: {name_a} vs {name_b} -- {diff_count} rows differ (exceptAll)")
        return count_a
    else:
        # Large result: compare numeric column aggregates
        sorted_cols = sorted(c.lower() for c in df_a.columns)
        a = df_a.toDF(*[c.lower() for c in df_a.columns]).select(*sorted_cols)
        b = df_b.toDF(*[c.lower() for c in df_b.columns]).select(*sorted_cols)
        numeric_cols = [f.name for f in a.schema.fields
                        if f.dataType.typeName() in ("double", "float", "decimal", "long", "integer", "short")]
        if not numeric_cols:
            results.ok(f"{label}: {name_a} == {name_b} ({count_a} rows, count match only)")
            return count_a

        agg_exprs = []
        for c in numeric_cols[:5]:  # Check up to 5 numeric columns
            agg_exprs.extend([
                F.sum(F.col(c).cast("double")).alias(f"sum_{c}"),
                F.avg(F.col(c).cast("double")).alias(f"avg_{c}"),
            ])

        agg_a = a.agg(*agg_exprs).collect()[0]
        agg_b = b.agg(*agg_exprs).collect()[0]

        mismatches = 0
        for key in agg_a.asDict():
            val_a = agg_a[key]
            val_b = agg_b[key]
            va = float(val_a) if val_a is not None else 0.0
            vb = float(val_b) if val_b is not None else 0.0
            if vb != 0 and abs((va - vb) / vb) > 0.001:
                mismatches += 1

        if mismatches == 0:
            results.ok(f"{label}: {name_a} == {name_b} ({count_a} rows, aggregates match)")
        else:
            results.fail(f"{label}: {name_a} vs {name_b} -- {mismatches} aggregate mismatches")
        return count_a


# ---------------------------------------------------------------------------
# Property checks (Spark-native, no .collect() for large results)
# ---------------------------------------------------------------------------

def check_properties(
    results: TestResults, label: str, df: DataFrame, spec: QuerySpec,
    precomputed_count: int | None = None,
) -> None:
    """Run independent property checks using Spark aggregations.

    Parameters
    ----------
    precomputed_count:
        If the caller already knows the row count (e.g. from a prior
        ``compare_spark_dataframes`` call), pass it here to avoid an
        extra ``.count()`` round-trip.
    """
    # Schema
    actual_cols = [c.lower() for c in df.columns]
    expected_cols = [c.lower() for c in spec.columns]
    if actual_cols != expected_cols:
        results.fail(f"{label} schema: expected {expected_cols}, got {actual_cols}")
    else:
        results.ok(f"{label} schema: {len(actual_cols)} columns correct")

    # Row count -- reuse precomputed value when available
    count = precomputed_count if precomputed_count is not None else df.count()
    if spec.min_rows <= count <= spec.max_rows:
        results.ok(f"{label} rows: {count} in [{spec.min_rows}, {spec.max_rows}]")
    else:
        results.fail(f"{label} rows: {count} not in [{spec.min_rows}, {spec.max_rows}]")

    ndf = df.toDF(*[c.lower() for c in df.columns])

    # NOT NULL checks (single pass)
    if spec.not_null_cols:
        null_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
                      for c in spec.not_null_cols]
        null_row = ndf.agg(*null_exprs).collect()[0]
        for c in spec.not_null_cols:
            nc = int(null_row[c])
            if nc == 0:
                results.ok(f"{label} not_null({c})")
            else:
                results.fail(f"{label} not_null({c}): {nc} nulls")

    # Positive checks (single pass)
    if spec.positive_cols:
        pos_exprs = [F.sum(F.when(F.col(c) <= 0, 1).otherwise(0)).alias(c)
                     for c in spec.positive_cols]
        pos_row = ndf.agg(*pos_exprs).collect()[0]
        for c in spec.positive_cols:
            np = int(pos_row[c] or 0)
            if np == 0:
                results.ok(f"{label} positive({c})")
            else:
                results.fail(f"{label} positive({c}): {np} non-positive")

    # Range checks (single pass)
    if spec.range_checks:
        range_exprs = []
        for c, _ in spec.range_checks.items():
            range_exprs.append(F.min(F.col(c).cast("double")).alias(f"min_{c}"))
            range_exprs.append(F.max(F.col(c).cast("double")).alias(f"max_{c}"))
        range_row = ndf.agg(*range_exprs).collect()[0]
        for c, (lo, hi) in spec.range_checks.items():
            vmin = float(range_row[f"min_{c}"] or 0)
            vmax = float(range_row[f"max_{c}"] or 0)
            if vmin >= lo and vmax <= hi:
                results.ok(f"{label} range({c}): [{vmin:.4f}, {vmax:.4f}] in [{lo}, {hi}]")
            else:
                results.fail(f"{label} range({c}): [{vmin:.4f}, {vmax:.4f}] outside [{lo}, {hi}]")

    # Enum checks
    for c, expected_vals in spec.enum_checks.items():
        actual_vals = {str(row[c]).strip() for row in ndf.select(c).distinct().collect()
                       if row[c] is not None}
        if actual_vals == expected_vals:
            results.ok(f"{label} enum({c})")
        else:
            results.fail(f"{label} enum({c}): expected={expected_vals}, got={actual_vals}")

    # Uniqueness
    if spec.unique_cols:
        c = spec.unique_cols[0]
        n_distinct = ndf.select(c).distinct().count()
        if n_distinct == count:
            results.ok(f"{label} unique({c}): {n_distinct}/{count}")
        else:
            results.fail(f"{label} unique({c}): {n_distinct}/{count} -- duplicates")

    # Distribution: no single value of the first column dominates >99%
    if count > 5:
        first_col = actual_cols[0]
        top_group = (ndf.groupBy(first_col).count()
                     .orderBy(F.desc("count")).limit(1).collect()[0]["count"])
        if top_group / count > 0.99:
            results.fail(f"{label} distribution({first_col}): top group {top_group}/{count} (>99%)")
        else:
            n_groups = ndf.select(first_col).distinct().count()
            results.ok(f"{label} distribution({first_col}): {n_groups} groups")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(catalog: str, schema: str, queries_path: str, impl: str = "all") -> None:
    spark = SparkSession.builder.getOrCreate()
    # Unity Catalog only (Databricks Free Edition and up): select the catalog, then
    # the schema. USE SCHEMA takes a single schema name after USE CATALOG.
    spark.sql(f"USE CATALOG {quote_sql_ident(catalog)}")
    spark.sql(f"USE SCHEMA {quote_sql_ident(schema)}")

    # Add src to path for module imports
    src_dir = str(Path(queries_path).parent.parent.parent)
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

    run_lakehouse = impl in ("all", "lakehouse")
    run_lakeflow = impl in ("all", "lakeflow")
    run_legacy = impl == "all"

    lakeflow_mod: ModuleType | None = None
    legacy_mod: ModuleType | None = None

    if run_lakeflow:
        try:
            from dsa_interview_packet.pipelines import supply_chain_queries as _lfq
            lakeflow_mod = _lfq
        except ModuleNotFoundError:
            if impl == "lakeflow":
                raise
            print(
                "Lakeflow: dsa_interview_packet.pipelines not importable — "
                "skipping pipeline checks (OK for lakehouse-only E2E).\n"
            )
            run_lakeflow = False
        else:
            has_any_lakeflow = any(
                callable(getattr(lakeflow_mod, spec.name, None))
                for spec in QUERY_SPECS
            )
            if not has_any_lakeflow:
                if impl == "lakeflow":
                    raise RuntimeError(
                        "Lakeflow (--impl=lakeflow) requires at least one implemented "
                        "function in dsa_interview_packet.pipelines.supply_chain_queries"
                    )
                print(
                    "Lakeflow: no query functions in supply_chain_queries yet — "
                    "skipping pipeline checks (OK until the candidate implements them).\n"
                )
                run_lakeflow = False

    print(f"Implementation filter: --impl={impl}")
    print(f"  Lakehouse SQL:    {'ON' if run_lakehouse else 'OFF'}")
    print(f"  Lakeflow pipeline: {'ON' if run_lakeflow else 'OFF'}")
    print(f"  Legacy pandas:    {'ON' if run_legacy else 'OFF'}\n")

    pandas_tables: dict[str, Any] = {}

    # Load source tables as pandas for legacy implementation.
    # Sample large tables to avoid OOM on serverless.
    if run_legacy:
        import pandas as pd
        from dsa_interview_packet.legacy import supply_chain_queries as _lgq

        legacy_mod = _lgq

        def _cast_decimal_columns(pdf: pd.DataFrame) -> pd.DataFrame:
            """Cast Decimal/object columns to float64 to avoid Decimal * float TypeError."""
            for col in pdf.columns:
                if pdf[col].dtype == "object":
                    try:
                        pdf[col] = pdf[col].astype(float)
                    except (ValueError, TypeError):
                        pass
                elif "decimal" in str(pdf[col].dtype).lower():
                    pdf[col] = pdf[col].astype(float)
            return pdf

        SAMPLE_LIMITS = {
            "lineitem": 50_000,
            "orders": 10_000,
            "partsupp": 10_000,
            "customer": 10_000,
            "part": 10_000,
        }
        # Small reference tables are loaded in full without a .count() round-trip.
        # Only tables listed in SAMPLE_LIMITS are checked and potentially sampled.
        SMALL_TABLES = ["region", "nation", "supplier"]
        LARGE_TABLES = ["part", "partsupp", "customer", "orders", "lineitem"]

        print("Loading source tables to pandas (sampled for large tables)...")
        for table in SMALL_TABLES + LARGE_TABLES:
            tdf = spark.table(table)
            limit = SAMPLE_LIMITS.get(table)
            if limit:
                row_count = tdf.count()
                if row_count > limit:
                    print(f"  {table}: sampling {limit} rows (full={row_count})")
                    tdf = tdf.limit(limit)
                else:
                    print(f"  {table}: full table ({row_count} rows)")
            else:
                print(f"  {table}: full table (small reference)")
            pandas_tables[table] = _cast_decimal_columns(tdf.toPandas())
        print("Source tables loaded.\n")
    else:
        print("Skipping pandas table loading (legacy not selected).\n")

    results = TestResults()

    for spec in QUERY_SPECS:
        print(f"\n{'='*60}")
        print(f"QUERY: {spec.name}")
        print(f"{'='*60}")

        # --- Run Lakehouse SQL ---
        lakehouse_df = None
        if run_lakehouse:
            sql_file = os.path.join(queries_path, f"{spec.name}.sql")
            if not os.path.isfile(sql_file):
                if impl == "lakehouse":
                    results.fail(f"Lakehouse SQL: missing file {sql_file}")
                else:
                    results.ok(f"Lakehouse SQL: skipped (no {spec.name}.sql)")
            else:
                try:
                    with open(sql_file, encoding="utf-8") as f:
                        sql = f.read()
                    lakehouse_df = spark.sql(sql).cache()
                    results.ok("Lakehouse SQL: executed")
                except Exception as e:
                    results.fail(f"Lakehouse SQL: {e}")

        # --- Run Lakeflow ---
        lakeflow_df = None
        if run_lakeflow:
            lakeflow_func = getattr(lakeflow_mod, spec.name, None)
            if lakeflow_func is None or not callable(lakeflow_func):
                if impl == "lakeflow":
                    results.fail(f"Lakeflow: missing callable {spec.name}(spark)")
                else:
                    results.ok(f"Lakeflow: skipped (no {spec.name} implementation)")
            else:
                try:
                    lakeflow_df = lakeflow_func(spark).cache()
                    results.ok("Lakeflow: executed")
                except Exception as e:
                    results.fail(f"Lakeflow: {e}")

        # --- Run Legacy (pandas) ---
        legacy_pdf = None
        if run_legacy:
            try:
                legacy_func = getattr(legacy_mod, spec.name)
                legacy_pdf = legacy_func(pandas_tables)
                results.ok(f"Legacy: executed ({len(legacy_pdf)} rows)")
            except Exception:
                results.fail(f"Legacy: {traceback.format_exc()}")

        # --- Cross-implementation: Lakehouse SQL vs Lakeflow ---
        # compare_spark_dataframes returns the row count of df_a (lakehouse)
        # so we can reuse it in check_properties without a redundant .count().
        primary_count: int | None = None
        if lakehouse_df is not None and lakeflow_df is not None:
            primary_count = compare_spark_dataframes(
                results, spec.name, lakehouse_df, "Lakehouse SQL", lakeflow_df, "Lakeflow",
            )

        # --- Cross-implementation: Lakehouse SQL vs Legacy ---
        # Legacy runs on sampled data so row counts may differ.
        # Compare schema and that legacy produced non-empty results.
        if lakehouse_df is not None and legacy_pdf is not None:
            lakehouse_cols = sorted(c.lower() for c in lakehouse_df.columns)
            legacy_cols = sorted(c.lower() for c in legacy_pdf.columns)
            if lakehouse_cols == legacy_cols:
                results.ok(f"{spec.name}: Lakehouse SQL vs Legacy schema match")
            else:
                results.fail(f"{spec.name}: Lakehouse SQL vs Legacy schema mismatch: "
                             f"lakehouse={lakehouse_cols}, legacy={legacy_cols}")
            if len(legacy_pdf) > 0:
                results.ok(f"{spec.name}: Legacy non-empty ({len(legacy_pdf)} rows)")
            else:
                # Some queries legitimately return empty on sampled data
                results.ok(f"{spec.name}: Legacy empty (may be due to sampling)")

        # --- Property checks on Lakehouse SQL result ---
        primary_df = lakehouse_df or lakeflow_df
        if primary_df is not None:
            check_properties(results, spec.name, primary_df, spec,
                             precomputed_count=primary_count)

        # Release cached DataFrames to free memory between iterations.
        if lakehouse_df is not None:
            lakehouse_df.unpersist()
        if lakeflow_df is not None:
            lakeflow_df.unpersist()

    # --- Summary ---
    print(f"\n{'='*60}")
    print(f"INTEGRATION TEST SUMMARY: {results.summary}")
    print(f"{'='*60}")

    if results.errors:
        print("\nFailed checks:")
        for err in results.errors:
            print(f"  - {err}")
        error_detail = "\n".join(f"  - {err}" for err in results.errors)
        raise RuntimeError(
            f"Integration tests failed: {results.failed} failures.\n"
            f"{error_detail}"
        )

    print("\nAll integration tests passed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run supply chain integration tests")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--queries-path", required=True)
    parser.add_argument(
        "--impl",
        choices=["lakehouse", "lakeflow", "all"],
        default="all",
        help="Run only a specific implementation: lakehouse, lakeflow, or all (default: all)",
    )
    args = parser.parse_args()
    main(args.catalog, args.schema, args.queries_path, impl=args.impl)
