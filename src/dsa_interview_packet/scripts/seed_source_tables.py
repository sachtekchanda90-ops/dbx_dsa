"""Seed supply chain source tables from Databricks pre-loaded samples.

Run via DABs:
    databricks bundle run -t dev seed_source_tables

Copies the 8 source tables from ``samples.tpch`` (pre-loaded on every
Databricks workspace) into the configured catalog/schema as managed
Delta tables.

With ``orders_sample_fraction`` below ``1.0``, copies a **referential
subset**: sample ``orders``, then keep only ``lineitem`` rows for those
orders, and restrict dimensions (``customer``, ``part``, ``supplier``,
``partsupp``) to keys still present. ``region`` and ``nation`` stay
full (tiny). This is the recommended default for faster E2E; use
``1.0`` for full TPC-H sample volume.
"""

from __future__ import annotations

import argparse

import ensure_src_path  # noqa: F401  # pylint: disable=unused-import  # must precede dsa_interview_packet imports
from dsa_interview_packet.bundle_state import (
    COMPONENT_SEED,
    get_stored_version,
    lineitem_row_count,
    merge_stored_version,
    seed_fingerprint,
)
from dsa_interview_packet.sql_identifiers import catalog_schema_sql, table_fqn
from pyspark.sql import SparkSession
from pyspark.sql import functions as sql_fn

# Mapping: target table name -> samples.tpch source table
SOURCE_TABLES = {
    "region": "samples.tpch.region",
    "nation": "samples.tpch.nation",
    "supplier": "samples.tpch.supplier",
    "part": "samples.tpch.part",
    "partsupp": "samples.tpch.partsupp",
    "customer": "samples.tpch.customer",
    "orders": "samples.tpch.orders",
    "lineitem": "samples.tpch.lineitem",
}


def _copy_full_table(
    spark: SparkSession,
    source_fqn: str,
    full_name: str,
) -> None:
    print(f"Copying {source_fqn} -> {full_name}")
    df = spark.table(source_fqn)
    df.write.mode("overwrite").saveAsTable(full_name)
    count = spark.table(full_name).count()
    print(f"  {full_name}: {count} rows")


def _seed_subset_by_orders(
    spark: SparkSession,
    catalog: str,
    schema: str,
    orders_sample_fraction: float,
    seed: int,
) -> None:
    """Copy a random subset of orders and all dependent TPC-H rows (FK-safe)."""
    if orders_sample_fraction <= 0.0 or orders_sample_fraction >= 1.0:
        msg = "orders_sample_fraction must be in (0, 1) for subset seeding"
        raise ValueError(msg)

    # Tiny dimension tables: full copy
    for name in ("region", "nation"):
        src = SOURCE_TABLES[name]
        full_name = table_fqn(catalog, schema, name)
        _copy_full_table(spark, src, full_name)

    orders_src = spark.table(SOURCE_TABLES["orders"])
    orders_df = orders_src.sample(False, orders_sample_fraction, seed)
    order_keys = orders_df.select(sql_fn.col("o_orderkey").alias("_ok"))

    lineitem_src = spark.table(SOURCE_TABLES["lineitem"])
    lineitem_df = lineitem_src.join(
        order_keys,
        lineitem_src.l_orderkey == order_keys._ok,
        "inner",
    ).drop("_ok")

    cust_keys = orders_df.select(sql_fn.col("o_custkey").alias("_ck")).distinct()
    customer_src = spark.table(SOURCE_TABLES["customer"])
    customer_df = customer_src.join(
        cust_keys,
        customer_src.c_custkey == cust_keys._ck,
        "inner",
    ).drop("_ck")

    part_keys = lineitem_df.select(sql_fn.col("l_partkey").alias("_pk")).distinct()
    part_df = spark.table(SOURCE_TABLES["part"]).join(
        part_keys,
        sql_fn.col("p_partkey") == part_keys._pk,
        "inner",
    ).drop("_pk")

    supp_keys = lineitem_df.select(sql_fn.col("l_suppkey").alias("_sk")).distinct()
    supplier_df = spark.table(SOURCE_TABLES["supplier"]).join(
        supp_keys,
        sql_fn.col("s_suppkey") == supp_keys._sk,
        "inner",
    ).drop("_sk")

    ps_keys = lineitem_df.select("l_partkey", "l_suppkey").distinct().select(
        sql_fn.col("l_partkey").alias("_psk"),
        sql_fn.col("l_suppkey").alias("_pssk"),
    )
    partsupp_src = spark.table(SOURCE_TABLES["partsupp"])
    partsupp_df = partsupp_src.join(
        ps_keys,
        (sql_fn.col("ps_partkey") == sql_fn.col("_psk"))
        & (sql_fn.col("ps_suppkey") == sql_fn.col("_pssk")),
        "inner",
    ).drop("_psk", "_pssk")

    write_order = [
        ("orders", orders_df),
        ("lineitem", lineitem_df),
        ("customer", customer_df),
        ("part", part_df),
        ("supplier", supplier_df),
        ("partsupp", partsupp_df),
    ]
    for tname, df in write_order:
        full_name = table_fqn(catalog, schema, tname)
        print(f"Writing subset {SOURCE_TABLES[tname]} -> {full_name}")
        df.write.mode("overwrite").saveAsTable(full_name)
        count = spark.table(full_name).count()
        print(f"  {full_name}: {count} rows")


def _parse_bool_flag(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def main(
    catalog: str,
    schema: str,
    *,
    orders_sample_fraction: float = 1.0,
    seed: int = 42,
    skip_if_populated: bool = False,
    min_lineitem_rows: int = 100,
    data_version: str | None = None,
    require_version: str | None = None,
) -> None:
    """Copy samples.tpch tables into the target schema."""
    spark = SparkSession.builder.getOrCreate()

    if skip_if_populated:
        n = lineitem_row_count(spark, catalog, schema)
        populated = n is not None and n >= min_lineitem_rows
        if populated:
            if require_version:
                stored = get_stored_version(spark, catalog, schema, COMPONENT_SEED)
                if stored == require_version:
                    print(
                        f"Skip seed: source tables present (lineitem rows={n}) and "
                        f"version matches ({require_version!r})."
                    )
                    return
            else:
                print(
                    f"Skip seed: source tables already populated "
                    f"(lineitem rows={n} >= {min_lineitem_rows})."
                )
                return

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {catalog_schema_sql(catalog, schema)}"
    )

    if orders_sample_fraction >= 1.0:
        for target_name, source_fqn in SOURCE_TABLES.items():
            full_name = table_fqn(catalog, schema, target_name)
            _copy_full_table(spark, source_fqn, full_name)
    else:
        _seed_subset_by_orders(
            spark,
            catalog,
            schema,
            orders_sample_fraction,
            seed,
        )

    print("Done seeding source tables.")
    ver = (data_version or "").strip() or seed_fingerprint(orders_sample_fraction, seed)
    merge_stored_version(
        spark,
        catalog,
        schema,
        component=COMPONENT_SEED,
        version=ver,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed supply chain source tables")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument(
        "--orders-sample-fraction",
        type=float,
        default=0.003,
        help=(
            "Fraction of TPC-H orders to copy with dependent rows (0,1); "
            "1.0 copies full samples.tpch tables (slow, large)."
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for subset sampling (subset mode only).",
    )
    parser.add_argument(
        "--skip-if-populated",
        default="false",
        help='If "true", skip when lineitem exists with >= --min-lineitem-rows (default: false).',
    )
    parser.add_argument(
        "--min-lineitem-rows",
        type=int,
        default=100,
        help="Minimum lineitem rows to treat seeded data as populated (with --skip-if-populated).",
    )
    parser.add_argument(
        "--data-version",
        default="",
        help="Stored in bundle state after success; empty uses auto fingerprint from sampling params.",
    )
    parser.add_argument(
        "--require-version",
        default="",
        help="With --skip-if-populated, skip only when bundle state matches this exact string.",
    )
    args = parser.parse_args()
    dv = args.data_version.strip() or None
    rv = args.require_version.strip() or None
    main(
        args.catalog,
        args.schema,
        orders_sample_fraction=args.orders_sample_fraction,
        seed=args.seed,
        skip_if_populated=_parse_bool_flag(args.skip_if_populated),
        min_lineitem_rows=args.min_lineitem_rows,
        data_version=dv,
        require_version=rv,
    )
