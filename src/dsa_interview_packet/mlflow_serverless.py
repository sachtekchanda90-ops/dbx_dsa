"""MLflow Spark ML on Databricks serverless compute.

``mlflow.spark`` log/load requires a Unity Catalog volume path for temporary
DFS I/O. See MLflow's ``MLFLOW_DFS_TMP`` / ``dfs_tmpdir`` requirement.

Databricks Free Edition is Unity-Catalog-only and serverless-only, so the
managed volume lives under a UC catalog/schema (for example
``CREATE VOLUME IF NOT EXISTS workspace.dsa_interview_supply_chain.mlflow_scratch``,
used as ``/Volumes/workspace/dsa_interview_supply_chain/mlflow_scratch``).
The bundle variables ``mlflow_volume_catalog`` / ``mlflow_volume_schema``
default to the data ``catalog`` / ``schema`` in ``databricks.yml``, so the
scratch volume sits alongside the tables and no override is needed on Free
Edition or any single-catalog UC workspace. Override them only to place the
scratch volume in a different UC catalog/schema.
"""

from __future__ import annotations

import os
import re

from pyspark.sql import SparkSession

_DEFAULT_VOLUME = "mlflow_scratch"


def _validate_sql_ident(segment: str, *, kind: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", segment):
        msg = f"Invalid {kind} identifier for SQL: {segment!r}"
        raise ValueError(msg)
    return segment


def ensure_mlflow_dfs_tmp(
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    volume: str = _DEFAULT_VOLUME,
) -> str:
    """Create a managed UC volume if needed and set ``MLFLOW_DFS_TMP``.

    Returns the ``/Volumes/...`` path used for ``dfs_tmpdir``.
    """
    cat = _validate_sql_ident(catalog, kind="catalog")
    sch = _validate_sql_ident(schema, kind="schema")
    vol = _validate_sql_ident(volume, kind="volume")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {cat}.{sch}.{vol}")
    path = f"/Volumes/{cat}/{sch}/{vol}"
    os.environ["MLFLOW_DFS_TMP"] = path
    return path
