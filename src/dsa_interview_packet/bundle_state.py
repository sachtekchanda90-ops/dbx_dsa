"""Internal UC table for idempotent bundle job runs (seed / simulate skip metadata)."""

from __future__ import annotations

from typing import cast

from dsa_interview_packet.sql_identifiers import table_fqn
from pyspark.sql import SparkSession

BUNDLE_STATE_TABLE = "_dsa_interview_bundle_state"

COMPONENT_SEED = "seed"
COMPONENT_SIMULATE = "simulate"


def seed_fingerprint(orders_sample_fraction: float, seed: int) -> str:
    """Stable token for seed params (invalidates skip when sampling changes)."""
    return f"seed:{orders_sample_fraction:g}:{seed}"


def simulate_fingerprint(simulation_days: int) -> str:
    """Stable token for simulate params."""
    return f"simulate:{simulation_days}"


def _state_table_fqn(catalog: str, schema: str) -> str:
    return cast(str, table_fqn(catalog, schema, BUNDLE_STATE_TABLE))


def _state_table_plain(catalog: str, schema: str) -> str:
    return f"{catalog}.{schema}.{BUNDLE_STATE_TABLE}"


def ensure_state_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create Delta metadata table if missing."""
    full = _state_table_fqn(catalog, schema)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full} (
            component STRING,
            version STRING,
            updated_at TIMESTAMP
        )
        USING DELTA
        """
    )


def get_stored_version(
    spark: SparkSession, catalog: str, schema: str, component: str
) -> str | None:
    """Return stored version string or None if table/row missing."""
    plain = _state_table_plain(catalog, schema)
    if not spark.catalog.tableExists(plain):
        return None
    full = _state_table_fqn(catalog, schema)
    esc = component.replace("'", "''")
    rows = spark.sql(
        f"SELECT version FROM {full} WHERE component = '{esc}' LIMIT 1"
    ).collect()
    if not rows:
        return None
    return str(rows[0]["version"])


def merge_stored_version(
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    component: str,
    version: str,
) -> None:
    """Upsert component version after a successful run."""
    ensure_state_table(spark, catalog, schema)
    full = _state_table_fqn(catalog, schema)
    ver_sql = version.replace("'", "''")
    comp_sql = component.replace("'", "''")
    merge_sql = f"""
        MERGE INTO {full} AS t
        USING (
          SELECT
            '{comp_sql}' AS component,
            '{ver_sql}' AS version,
            current_timestamp() AS updated_at
        ) AS s
        ON t.component = s.component
        WHEN MATCHED THEN UPDATE SET
          version = s.version,
          updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT (component, version, updated_at)
          VALUES (s.component, s.version, s.updated_at)
        """
    spark.sql(merge_sql)


def table_exists_three_part(spark: SparkSession, catalog: str, schema: str, table: str) -> bool:
    """Return True if ``catalog.schema.table`` exists."""
    name = f"{catalog}.{schema}.{table}"
    try:
        return bool(spark.catalog.tableExists(name))
    except Exception:
        return False


def lineitem_row_count(spark: SparkSession, catalog: str, schema: str) -> int | None:
    """Return lineitem row count or None if table does not exist."""
    if not table_exists_three_part(spark, catalog, schema, "lineitem"):
        return None
    full = table_fqn(catalog, schema, "lineitem")
    return int(spark.table(full).count())


def sim_tables_populated(spark: SparkSession, catalog: str, schema: str) -> bool:
    """True if sim_billing_usage exists and has at least one row."""
    if not table_exists_three_part(spark, catalog, schema, "sim_billing_usage"):
        return False
    full = table_fqn(catalog, schema, "sim_billing_usage")
    return spark.table(full).count() > 0
