# pylint: disable=too-many-lines
"""Simulate Databricks system table data for cost-analysis exercises.

Run via DABs:
    databricks bundle run -t dev simulate_system_tables

Creates seven Delta tables that mirror real system tables:
  - ``sim_billing_usage``    (mirrors ``system.billing.usage``)
  - ``sim_cluster_info``     (mirrors ``system.compute.clusters``)
  - ``sim_job_run_timeline`` (mirrors ``system.lakeflow.job_run_timeline``)
  - ``sim_query_history``    (mirrors ``system.query.history``)
  - ``sim_pipeline_events``  (mirrors ``system.lakeflow.pipeline_events``)
  - ``sim_audit_logs``       (mirrors ``system.access.audit``)
  - ``sim_table_lineage``    (mirrors ``system.access.table_lineage``)

The generated data tells a clear story: a legacy pandas job running on an
expensive all-purpose cluster consumes about 42x more DBUs than the equivalent
Lakehouse SQL job, making it an obvious migration target.

Referential integrity: all tables share the same master pools of job IDs,
cluster IDs, warehouse IDs, pipeline IDs, and user emails. Timestamps are
correlated so that billing, audit, and query history entries align with the
job runs and pipeline events that produced them.
"""

from __future__ import annotations

import argparse
import hashlib
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import ensure_src_path  # noqa: F401  # pylint: disable=unused-import  # must precede dsa_interview_packet imports
from dsa_interview_packet.bundle_state import (
    COMPONENT_SIMULATE,
    get_stored_version,
    merge_stored_version,
    sim_tables_populated,
    simulate_fingerprint,
)
from dsa_interview_packet.sql_identifiers import (
    catalog_schema_sql,
    quote_sql_ident,
    table_fqn,
)
from pyspark.sql import SparkSession
from pyspark.sql import types as T

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ACCOUNT_ID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
WORKSPACE_ID = "1234567890123456"

# Default for generator functions (unit tests rely on ~30 days of history).
DEFAULT_SIMULATION_DAYS = 30
# Backwards-compatible name for tests importing ``DAYS``.
DAYS = DEFAULT_SIMULATION_DAYS

# The three supply chain analytics jobs
SUPPLY_CHAIN_JOBS: list[dict[str, Any]] = [
    {
        "job_id": "100001",
        "job_name": "legacy_pandas_daily",
        "sku_name": "ALL_PURPOSE_COMPUTE",
        "cluster_name": "legacy-pandas-cluster",
        "cluster_source": "UI",
        "driver_node_type": "i3.2xlarge",
        "worker_node_type": "i3.2xlarge",
        "num_workers": 4,
        "creator": "dataeng-legacy@company.com",
        "dbu_rate_per_hour": 3.0,  # expensive all-purpose
        "duration_mean_min": 37,
        "duration_std_min": 8,
        "duration_min_min": 22,
        "duration_max_min": 55,
    },
    {
        "job_id": "100002",
        "job_name": "lakehouse_queries_daily",
        "sku_name": "SQL_SERVERLESS",
        "cluster_name": None,  # serverless, no visible cluster
        "cluster_source": "SQL_WAREHOUSE",
        "driver_node_type": None,
        "worker_node_type": None,
        "num_workers": 0,
        "creator": "analytics-team@company.com",
        "dbu_rate_per_hour": 0.7,
        "duration_mean_min": 3.5,
        "duration_std_min": 1.0,
        "duration_min_min": 1.5,
        "duration_max_min": 6.0,
    },
    {
        "job_id": "100003",
        "job_name": "sdp_pipeline_daily",
        "sku_name": "DLT_SERVERLESS",
        "cluster_name": None,
        "cluster_source": "PIPELINE",
        "driver_node_type": None,
        "worker_node_type": None,
        "num_workers": 0,
        "creator": "analytics-team@company.com",
        "dbu_rate_per_hour": 0.9,
        "duration_mean_min": 2.5,
        "duration_std_min": 0.8,
        "duration_min_min": 1.0,
        "duration_max_min": 5.0,
        "pipeline_id": "p-300001",  # links to supply_chain_pipeline
    },
]

# Background noise jobs from other users
NOISE_USERS = [
    "alice@company.com",
    "bob@company.com",
    "carol@company.com",
    "dave@company.com",
    "eve@company.com",
    "frank@company.com",
    "grace@company.com",
    "heidi@company.com",
]

# Canonical list of ALL users (supply chain creators + noise users)
ALL_USERS = [
    "dataeng-legacy@company.com",
    "analytics-team@company.com",
    *NOISE_USERS,
]

NOISE_JOBS: list[dict[str, Any]] = [
    # --- JOBS_COMPUTE ---
    {"job_id": "200001", "job_name": "etl_customer_ingest", "sku_name": "JOBS_COMPUTE", "creator": "alice@company.com", "dbu_rate": 1.5, "dur_mean": 12, "dur_std": 3, "freq": 1.0},
    {"job_id": "200002", "job_name": "etl_orders_transform", "sku_name": "JOBS_COMPUTE", "creator": "alice@company.com", "dbu_rate": 1.8, "dur_mean": 18, "dur_std": 4, "freq": 1.0},
    {"job_id": "200003", "job_name": "etl_inventory_sync", "sku_name": "JOBS_COMPUTE", "creator": "bob@company.com", "dbu_rate": 1.2, "dur_mean": 8, "dur_std": 2, "freq": 1.0},
    {"job_id": "200004", "job_name": "etl_product_catalog", "sku_name": "JOBS_COMPUTE", "creator": "carol@company.com", "dbu_rate": 1.0, "dur_mean": 5, "dur_std": 1, "freq": 1.0},
    # --- JOBS_SERVERLESS ---
    {"job_id": "200005", "job_name": "feature_store_refresh", "sku_name": "JOBS_SERVERLESS", "creator": "dave@company.com", "dbu_rate": 1.1, "dur_mean": 10, "dur_std": 3, "freq": 1.0},
    {"job_id": "200006", "job_name": "ml_training_nightly", "sku_name": "JOBS_SERVERLESS", "creator": "eve@company.com", "dbu_rate": 2.0, "dur_mean": 45, "dur_std": 15, "freq": 1.0},
    {"job_id": "200007", "job_name": "anomaly_detection_hourly", "sku_name": "JOBS_SERVERLESS", "creator": "dave@company.com", "dbu_rate": 0.8, "dur_mean": 3, "dur_std": 1, "freq": 1.0},
    # --- SQL_SERVERLESS ---
    {"job_id": "200008", "job_name": "dashboard_refresh_sales", "sku_name": "SQL_SERVERLESS", "creator": "frank@company.com", "dbu_rate": 0.5, "dur_mean": 2, "dur_std": 0.5, "freq": 1.0},
    {"job_id": "200009", "job_name": "dashboard_refresh_ops", "sku_name": "SQL_SERVERLESS", "creator": "frank@company.com", "dbu_rate": 0.6, "dur_mean": 3, "dur_std": 1, "freq": 1.0},
    {"job_id": "200010", "job_name": "exec_kpi_report", "sku_name": "SQL_SERVERLESS", "creator": "grace@company.com", "dbu_rate": 0.4, "dur_mean": 1.5, "dur_std": 0.4, "freq": 1.0},
    {"job_id": "200011", "job_name": "finance_reconciliation", "sku_name": "SQL_SERVERLESS", "creator": "grace@company.com", "dbu_rate": 0.7, "dur_mean": 4, "dur_std": 1, "freq": 1.0},
    # --- DLT_SERVERLESS ---
    {"job_id": "200012", "job_name": "dlt_clickstream_bronze", "sku_name": "DLT_SERVERLESS", "creator": "heidi@company.com", "dbu_rate": 1.0, "dur_mean": 8, "dur_std": 2, "freq": 1.0, "pipeline_id": "p-300002"},
    {"job_id": "200013", "job_name": "dlt_clickstream_silver", "sku_name": "DLT_SERVERLESS", "creator": "heidi@company.com", "dbu_rate": 0.9, "dur_mean": 6, "dur_std": 2, "freq": 1.0, "pipeline_id": "p-300002"},
    {"job_id": "200014", "job_name": "dlt_iot_telemetry", "sku_name": "DLT_SERVERLESS", "creator": "bob@company.com", "dbu_rate": 1.2, "dur_mean": 15, "dur_std": 5, "freq": 1.0, "pipeline_id": "p-300003"},
    # --- ALL_PURPOSE_COMPUTE (interactive / exploration) ---
    {"job_id": "200015", "job_name": "data_exploration_adhoc", "sku_name": "ALL_PURPOSE_COMPUTE", "creator": "carol@company.com", "dbu_rate": 2.5, "dur_mean": 20, "dur_std": 10, "freq": 0.6},
    {"job_id": "200016", "job_name": "ml_experiment_notebook", "sku_name": "ALL_PURPOSE_COMPUTE", "creator": "eve@company.com", "dbu_rate": 2.8, "dur_mean": 30, "dur_std": 12, "freq": 0.4},
    # --- INTERACTIVE_SERVERLESS ---
    {"job_id": "200017", "job_name": "notebook_quick_analysis", "sku_name": "INTERACTIVE_SERVERLESS", "creator": "frank@company.com", "dbu_rate": 0.6, "dur_mean": 5, "dur_std": 2, "freq": 0.7},
    {"job_id": "200018", "job_name": "notebook_data_profiling", "sku_name": "INTERACTIVE_SERVERLESS", "creator": "grace@company.com", "dbu_rate": 0.5, "dur_mean": 4, "dur_std": 1.5, "freq": 0.5},
]

# TPC-H source tables used by the supply chain pipeline
TPCH_SOURCE_TABLES = [
    "region", "nation", "part", "supplier", "partsupp",
    "customer", "orders", "lineitem",
]

# The 22 TPC-H result tables produced by the supply chain pipeline
SUPPLY_CHAIN_RESULT_TABLES = [
    f"q{i:02d}_result" for i in range(1, 23)
]

# Dashboard views built on top of result tables
DASHBOARD_VIEWS = [
    "v_pricing_summary",
    "v_supplier_analysis",
    "v_shipping_priority",
    "v_order_priority",
    "v_revenue_by_region",
]

# SQL warehouse used for queries
SQL_WAREHOUSE_ID = "abcd1234ef567890"
SQL_WAREHOUSE_NAME = "supply_chain_warehouse"

# Pipeline definitions
PIPELINES: list[dict[str, Any]] = [
    {
        "pipeline_id": "p-300001",
        "pipeline_name": "supply_chain_pipeline",
        "creator": "analytics-team@company.com",
        "daily": True,
        "failure_rate": 0.05,
    },
    {
        "pipeline_id": "p-300002",
        "pipeline_name": "ingestion_pipeline",
        "creator": "alice@company.com",
        "daily": True,
        "failure_rate": 0.03,
    },
    {
        "pipeline_id": "p-300003",
        "pipeline_name": "ml_feature_pipeline",
        "creator": "eve@company.com",
        "daily": True,
        "failure_rate": 0.08,
    },
]


# ---------------------------------------------------------------------------
# Shared master data container
# ---------------------------------------------------------------------------

@dataclass
class MasterData:
    """Shared identifiers and generated records used across all table generators.

    This is populated once in ``main()`` and passed to every ``_generate_*``
    function so that IDs, timestamps, and user emails are referentially
    consistent across all seven simulated tables.
    """

    # Maps job_id (int) -> cluster_id (str)
    job_cluster_map: dict[int, str] = field(default_factory=dict)

    # Maps job_id (int) -> job metadata dict (sku, creator, name, etc.)
    job_meta: dict[int, dict[str, Any]] = field(default_factory=dict)

    # Maps pipeline_id -> pipeline metadata
    pipeline_meta: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Materialised job run rows (list of dicts); shared between billing,
    # audit-log, and query-history generators.
    job_runs: list[dict[str, Any]] = field(default_factory=list)

    # Materialised pipeline event rows; shared with audit-log generator.
    pipeline_events: list[dict[str, Any]] = field(default_factory=list)

    # Materialised query history rows; shared with billing generator for
    # warehouse timestamp alignment.
    query_runs: list[dict[str, Any]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _deterministic_id(seed_str: str) -> str:
    """Generate a deterministic UUID-like ID from a seed string."""
    return hashlib.md5(seed_str.encode(), usedforsecurity=False).hexdigest()


def _cluster_id(job_id: str) -> str:
    return f"0401-{job_id[-6:]}-abcde123"


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _random_duration_min(mean: float, std: float, lo: float, hi: float, rng: random.Random) -> float:
    """Sample a clamped Gaussian duration in minutes."""
    return _clamp(rng.gauss(mean, std), lo, hi)


def _build_master_data() -> MasterData:
    """Build the shared master data structures from the constant definitions."""
    master = MasterData()

    # Build job metadata and cluster mappings for supply chain jobs
    for job in SUPPLY_CHAIN_JOBS:
        jid = int(job["job_id"])
        cid = _cluster_id(job["job_id"])
        master.job_cluster_map[jid] = cid
        master.job_meta[jid] = {
            "job_id": job["job_id"],
            "job_name": job["job_name"],
            "sku_name": job["sku_name"],
            "dbu_rate_per_hour": job["dbu_rate_per_hour"],
            "creator": job["creator"],
            "cluster_name": job["cluster_name"],
            "cluster_source": job["cluster_source"],
            "driver_node_type": job["driver_node_type"],
            "worker_node_type": job["worker_node_type"],
            "num_workers": job["num_workers"],
            "pipeline_id": job.get("pipeline_id"),
        }

    # Build job metadata and cluster mappings for noise jobs
    for njob in NOISE_JOBS:
        jid = int(njob["job_id"])
        cid = _cluster_id(njob["job_id"])
        master.job_cluster_map[jid] = cid
        master.job_meta[jid] = {
            "job_id": njob["job_id"],
            "job_name": njob["job_name"],
            "sku_name": njob["sku_name"],
            "dbu_rate_per_hour": njob["dbu_rate"],
            "creator": njob["creator"],
            "pipeline_id": njob.get("pipeline_id"),
        }

    # Pipeline metadata
    for pipe in PIPELINES:
        master.pipeline_meta[pipe["pipeline_id"]] = pipe

    return master


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

def _generate_job_runs(
    rng: random.Random,
    now: datetime,
    master: MasterData,
    days: int = DEFAULT_SIMULATION_DAYS,
) -> list[dict[str, Any]]:
    """Generate job run timeline rows for all jobs over ``days`` days.

    Populates ``master.job_runs`` as a side-effect so downstream generators
    (billing, audit logs) can reference the exact same run records.
    """
    rows: list[dict[str, Any]] = []

    for day_offset in range(days, 0, -1):
        day_start = now - timedelta(days=day_offset)

        # Supply chain jobs: once per day, scheduled around 06:00 UTC
        for job in SUPPLY_CHAIN_JOBS:
            schedule_hour = 6 + rng.uniform(-0.5, 0.5)
            start = day_start.replace(
                hour=int(schedule_hour),
                minute=rng.randint(0, 59),
                second=rng.randint(0, 59),
                microsecond=0,
            )
            dur_min = _random_duration_min(
                job["duration_mean_min"],
                job["duration_std_min"],
                job["duration_min_min"],
                job["duration_max_min"],
                rng,
            )
            end = start + timedelta(minutes=dur_min)

            # ~5% failure rate for legacy, ~1% for modern
            fail_chance = 0.05 if "legacy" in job["job_name"] else 0.01
            result_state = "FAILED" if rng.random() < fail_chance else "SUCCESS"

            run_id = _deterministic_id(f"{job['job_id']}-{day_offset}")
            cluster_id = master.job_cluster_map[int(job["job_id"])]
            rows.append({
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "job_id": int(job["job_id"]),
                "run_id": int(run_id[:8], 16) % 10_000_000,
                "run_name": job["job_name"],
                "result_state": result_state,
                "start_time": start,
                "end_time": end,
                "job_name": job["job_name"],
                "creator": job["creator"],
                "duration_minutes": round(dur_min, 2),
                "cluster_id": cluster_id,
            })

        # Noise jobs
        for njob in NOISE_JOBS:
            if rng.random() > njob["freq"]:
                continue

            schedule_hour = rng.randint(0, 23)
            start = day_start.replace(
                hour=schedule_hour,
                minute=rng.randint(0, 59),
                second=rng.randint(0, 59),
                microsecond=0,
            )
            dur_min = _clamp(
                rng.gauss(njob["dur_mean"], njob["dur_std"]),
                max(0.5, njob["dur_mean"] - 3 * njob["dur_std"]),
                njob["dur_mean"] + 3 * njob["dur_std"],
            )
            end = start + timedelta(minutes=dur_min)
            result_state = "FAILED" if rng.random() < 0.03 else "SUCCESS"

            run_id = _deterministic_id(f"{njob['job_id']}-{day_offset}")
            cluster_id = master.job_cluster_map[int(njob["job_id"])]
            rows.append({
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "job_id": int(njob["job_id"]),
                "run_id": int(run_id[:8], 16) % 10_000_000,
                "run_name": njob["job_name"],
                "result_state": result_state,
                "start_time": start,
                "end_time": end,
                "job_name": njob["job_name"],
                "creator": njob["creator"],
                "duration_minutes": round(dur_min, 2),
                "cluster_id": cluster_id,
            })

    master.job_runs = rows
    return rows


def _generate_billing_rows(
    master: MasterData, rng: random.Random,
) -> list[dict[str, Any]]:
    """Generate billing usage rows from the shared job runs.

    For SQL_SERVERLESS SKUs the billing row references the SQL warehouse.
    For DLT_SERVERLESS SKUs the billing row tags the pipeline_id.
    All cluster_ids come from the shared ``master.job_cluster_map``.
    """
    rows: list[dict[str, Any]] = []
    for run in master.job_runs:
        meta = master.job_meta.get(run["job_id"])
        if meta is None:
            continue

        hours = run["duration_minutes"] / 60.0
        # Add +/-10% noise to DBU quantity
        base_dbu = hours * meta["dbu_rate_per_hour"]
        usage_quantity = base_dbu * rng.uniform(0.9, 1.1)

        cluster_id = master.job_cluster_map.get(run["job_id"])
        record_id = str(uuid.UUID(hex=_deterministic_id(f"bill-{run['run_id']}")))

        sku = meta["sku_name"]

        # Warehouse ID: set for SQL_SERVERLESS workloads
        warehouse_id = SQL_WAREHOUSE_ID if sku == "SQL_SERVERLESS" else None
        # For serverless SKUs there is no user-visible cluster
        if "SERVERLESS" in sku:
            cluster_id = None

        rows.append({
            "record_id": record_id,
            "account_id": ACCOUNT_ID,
            "workspace_id": WORKSPACE_ID,
            "sku_name": sku,
            "cloud": "AWS",
            "usage_start_time": run["start_time"],
            "usage_end_time": run["end_time"],
            "usage_date": run["start_time"].date(),
            "custom_tags": f'{{"job_id": "{run["job_id"]}", "job_name": "{run["job_name"]}"}}',
            "usage_quantity": round(usage_quantity, 4),
            "usage_metadata_cluster_id": cluster_id,
            "usage_metadata_job_id": str(run["job_id"]),
            "usage_metadata_notebook_id": None,
            "usage_metadata_warehouse_id": warehouse_id,
        })

    # Add warehouse-level billing entries aligned with query history runs.
    # These cover ad-hoc / dashboard queries that are NOT tied to a job.
    for qrun in master.query_runs:
        if qrun.get("_is_warehouse_only", False):
            hours = qrun["duration_ms"] / 3_600_000.0
            usage_quantity = hours * 0.7 * rng.uniform(0.9, 1.1)
            record_id = str(uuid.UUID(hex=_deterministic_id(f"bill-wh-{qrun['statement_id']}")))
            rows.append({
                "record_id": record_id,
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "sku_name": "SQL_SERVERLESS",
                "cloud": "AWS",
                "usage_start_time": qrun["execution_start_time"],
                "usage_end_time": qrun["execution_end_time"],
                "usage_date": qrun["execution_start_time"].date(),
                "custom_tags": "{}",
                "usage_quantity": round(usage_quantity, 6),
                "usage_metadata_cluster_id": None,
                "usage_metadata_job_id": None,
                "usage_metadata_notebook_id": None,
                "usage_metadata_warehouse_id": SQL_WAREHOUSE_ID,
            })

    return rows


def _generate_cluster_rows(master: MasterData) -> list[dict[str, Any]]:
    """Generate cluster info rows.

    Every cluster_id that appears in ``master.job_cluster_map`` gets a row
    so that billing and job-run references always resolve.
    """
    rows: list[dict[str, Any]] = []
    node_types = ["i3.xlarge", "i3.2xlarge", "m5.xlarge", "m5.2xlarge", "r5.xlarge"]

    # Supply chain job clusters
    for job in SUPPLY_CHAIN_JOBS:
        cluster_id = master.job_cluster_map[int(job["job_id"])]
        rows.append({
            "account_id": ACCOUNT_ID,
            "workspace_id": WORKSPACE_ID,
            "cluster_id": cluster_id,
            "cluster_name": job["cluster_name"] or f"job-{job['job_id']}-serverless",
            "cluster_source": job["cluster_source"],
            "creator": job["creator"],
            "driver_node_type": job["driver_node_type"] or "serverless",
            "worker_node_type": job["worker_node_type"] or "serverless",
            "num_workers": job["num_workers"],
            "autoscale_min_workers": None if job["num_workers"] == 0 else 2,
            "autoscale_max_workers": None if job["num_workers"] == 0 else job["num_workers"],
        })

    # Noise job clusters
    for njob in NOISE_JOBS:
        cluster_id = master.job_cluster_map[int(njob["job_id"])]
        is_serverless = "SERVERLESS" in njob["sku_name"]
        rows.append({
            "account_id": ACCOUNT_ID,
            "workspace_id": WORKSPACE_ID,
            "cluster_id": cluster_id,
            "cluster_name": f"job-cluster-{njob['job_name']}" if not is_serverless else f"job-{njob['job_id']}-serverless",
            "cluster_source": "JOB" if not is_serverless else "SERVERLESS",
            "creator": njob["creator"],
            "driver_node_type": node_types[hash(njob["job_id"]) % len(node_types)] if not is_serverless else "serverless",
            "worker_node_type": node_types[hash(njob["job_id"]) % len(node_types)] if not is_serverless else "serverless",
            "num_workers": 0 if is_serverless else (hash(njob["job_id"]) % 4 + 1),
            "autoscale_min_workers": None if is_serverless else 1,
            "autoscale_max_workers": None if is_serverless else 4,
        })

    return rows


def _generate_query_history(
    rng: random.Random,
    now: datetime,
    master: MasterData,
    catalog: str,
    schema: str,
    days: int = DEFAULT_SIMULATION_DAYS,
) -> list[dict[str, Any]]:
    """Generate SQL warehouse query history rows (volume scales with ``days``).

    All queries reference the shared ``SQL_WAREHOUSE_ID``.  Users are drawn
    from the canonical ``ALL_USERS`` list.  Rows are stored in
    ``master.query_runs`` for downstream billing alignment.
    """
    rows: list[dict[str, Any]] = []
    cs = f"{quote_sql_ident(catalog)}.{quote_sql_ident(schema)}"

    # Dashboard refresh queries: SELECT from result tables, every 15-30 min
    dashboard_queries = [
        f"SELECT * FROM {cs}.{tbl} LIMIT 10000"
        for tbl in SUPPLY_CHAIN_RESULT_TABLES[:10]
    ]
    # Ad-hoc exploration queries
    adhoc_queries = [
        f"SELECT count(*) FROM {cs}.q01_result",
        f"SELECT * FROM {cs}.q05_result WHERE revenue > 100000",
        f"DESCRIBE TABLE {cs}.q10_result",
        f"SELECT nation, sum(revenue) FROM {cs}.q05_result GROUP BY nation ORDER BY 2 DESC",
        f"SELECT * FROM {cs}.lineitem LIMIT 100",
        f"SHOW TABLES IN {cs}",
        f"SELECT l_shipmode, count(*) FROM {cs}.lineitem GROUP BY l_shipmode",
        f"SELECT * FROM {cs}.q12_result ORDER BY high_line_count DESC LIMIT 20",
        f"SELECT avg(total_revenue) FROM {cs}.q15_result",
        f"SELECT * FROM {cs}.q07_result WHERE supp_nation = 'UNITED STATES'",
    ]
    # Expensive pandas-like CTAS queries (the legacy pattern)
    expensive_queries = [
        "CREATE TABLE {cs}.q{qnum:02d}_result AS SELECT /*+ BROADCAST(n) */ * FROM {cs}.lineitem l JOIN {cs}.nation n ON l.l_suppkey = n.n_nationkey",
        "CREATE TABLE {cs}.q{qnum:02d}_result AS SELECT * FROM {cs}.orders o JOIN {cs}.customer c ON o.o_custkey = c.c_custkey WHERE o.o_orderdate >= '1995-01-01'",
        "CREATE TABLE {cs}.q{qnum:02d}_result AS SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price FROM {cs}.lineitem GROUP BY l_returnflag, l_linestatus",
    ]
    # Duplicate/redundant queries (same text from different sources)
    redundant_query = f"SELECT * FROM {cs}.q01_result"
    redundant_sources = ["DASHBOARD", "NOTEBOOK", "DBSQL_EDITOR"]

    statuses = ["FINISHED", "FAILED", "CANCELED"]

    for day_offset in range(days, 0, -1):
        day_start = now - timedelta(days=day_offset)

        # Dashboard refresh queries: ~48 per day (every 30 min for 10 queries, staggered)
        for q_idx, q_text in enumerate(dashboard_queries):
            intervals_per_day = rng.randint(32, 48)  # 15-30 min intervals
            for interval in range(intervals_per_day // len(dashboard_queries)):
                minutes_offset = q_idx * 3 + interval * rng.randint(15, 30)
                if minutes_offset >= 1440:
                    continue
                start = day_start + timedelta(minutes=minutes_offset)
                dur_ms = rng.randint(200, 3000)
                end = start + timedelta(milliseconds=dur_ms)
                row = {
                    "account_id": ACCOUNT_ID,
                    "workspace_id": WORKSPACE_ID,
                    "statement_id": _deterministic_id(f"qh-dash-{day_offset}-{q_idx}-{interval}"),
                    "executed_by": "dashboard_refresh@system",
                    "statement_text": q_text,
                    "status": "FINISHED" if rng.random() < 0.98 else "FAILED",
                    "execution_start_time": start,
                    "execution_end_time": end,
                    "duration_ms": dur_ms,
                    "rows_produced": rng.randint(100, 50000),
                    "warehouse_id": SQL_WAREHOUSE_ID,
                    "warehouse_name": SQL_WAREHOUSE_NAME,
                    "source": "DASHBOARD",
                    "_is_warehouse_only": True,  # no job backing; for billing alignment
                }
                rows.append(row)

        # Ad-hoc queries from canonical users: ~20-40 per day
        num_adhoc = rng.randint(20, 40)
        for i in range(num_adhoc):
            user = rng.choice(ALL_USERS)
            q_text = rng.choice(adhoc_queries)
            hour = rng.randint(8, 18)
            start = day_start.replace(
                hour=hour, minute=rng.randint(0, 59), second=rng.randint(0, 59),
            )
            dur_ms = rng.randint(100, 15000)
            end = start + timedelta(milliseconds=dur_ms)
            status = rng.choices(statuses, weights=[0.90, 0.07, 0.03], k=1)[0]
            row = {
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "statement_id": _deterministic_id(f"qh-adhoc-{day_offset}-{i}"),
                "executed_by": user,
                "statement_text": q_text,
                "status": status,
                "execution_start_time": start,
                "execution_end_time": end,
                "duration_ms": dur_ms,
                "rows_produced": rng.randint(0, 100000) if status == "FINISHED" else 0,
                "warehouse_id": SQL_WAREHOUSE_ID,
                "warehouse_name": SQL_WAREHOUSE_NAME,
                "source": rng.choice(["NOTEBOOK", "DBSQL_EDITOR", "API"]),
                "_is_warehouse_only": True,
            }
            rows.append(row)

        # Expensive CTAS queries (legacy pandas pattern): 1-3 per day
        num_expensive = rng.randint(1, 3)
        for i in range(num_expensive):
            q_template = rng.choice(expensive_queries)
            qnum = rng.randint(1, 22)
            q_text = q_template.format(cs=cs, qnum=qnum)
            start = day_start.replace(
                hour=6, minute=rng.randint(0, 30), second=rng.randint(0, 59),
            )
            dur_ms = rng.randint(120000, 600000)  # 2-10 minutes
            end = start + timedelta(milliseconds=dur_ms)
            row = {
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "statement_id": _deterministic_id(f"qh-ctas-{day_offset}-{i}"),
                "executed_by": "dataeng-legacy@company.com",
                "statement_text": q_text,
                "status": "FINISHED" if rng.random() < 0.92 else "FAILED",
                "execution_start_time": start,
                "execution_end_time": end,
                "duration_ms": dur_ms,
                "rows_produced": rng.randint(50000, 5000000),
                "warehouse_id": SQL_WAREHOUSE_ID,
                "warehouse_name": SQL_WAREHOUSE_NAME,
                "source": "NOTEBOOK",
                "_is_warehouse_only": True,
            }
            rows.append(row)

        # Redundant/duplicate queries from different sources: ~5-10 per day
        num_redundant = rng.randint(5, 10)
        for i in range(num_redundant):
            source = rng.choice(redundant_sources)
            user = "dashboard_refresh@system" if source == "DASHBOARD" else rng.choice(ALL_USERS)
            start = day_start.replace(
                hour=rng.randint(8, 20), minute=rng.randint(0, 59), second=rng.randint(0, 59),
            )
            dur_ms = rng.randint(200, 5000)
            end = start + timedelta(milliseconds=dur_ms)
            row = {
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "statement_id": _deterministic_id(f"qh-dup-{day_offset}-{i}"),
                "executed_by": user,
                "statement_text": redundant_query,
                "status": "FINISHED",
                "execution_start_time": start,
                "execution_end_time": end,
                "duration_ms": dur_ms,
                "rows_produced": rng.randint(1000, 50000),
                "warehouse_id": SQL_WAREHOUSE_ID,
                "warehouse_name": SQL_WAREHOUSE_NAME,
                "source": source,
                "_is_warehouse_only": True,
            }
            rows.append(row)

    master.query_runs = rows
    return rows


def _generate_pipeline_events(
    rng: random.Random,
    now: datetime,
    master: MasterData,
    days: int = DEFAULT_SIMULATION_DAYS,
) -> list[dict[str, Any]]:
    """Generate Lakeflow pipeline event rows (volume scales with ``days``).

    Uses pipeline definitions from the shared ``PIPELINES`` constant and
    stores results in ``master.pipeline_events`` for audit-log alignment.
    """
    rows: list[dict[str, Any]] = []

    failure_messages = [
        "FAILED: java.lang.OutOfMemoryError: Java heap space",
        "FAILED: com.databricks.sql.transaction.tahoe.DeltaAnalysisException: Table not found",
        "FAILED: org.apache.spark.SparkException: Job aborted due to stage failure",
        "FAILED: io.netty.channel.ConnectTimeoutException: connection timed out",
        "FAILED: Exceeded maximum number of retries (3) for update",
    ]

    for day_offset in range(days, 0, -1):
        day_start = now - timedelta(days=day_offset)

        for pipe in PIPELINES:
            if not pipe["daily"]:
                continue

            # Schedule around 05:00-07:00 UTC
            schedule_hour = 5 + rng.randint(0, 2)
            base_time = day_start.replace(
                hour=schedule_hour, minute=rng.randint(0, 59), second=rng.randint(0, 59),
            )
            update_id = _deterministic_id(f"upd-{pipe['pipeline_id']}-{day_offset}")

            is_failure = rng.random() < pipe["failure_rate"]

            # STARTING event
            rows.append({
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "pipeline_id": pipe["pipeline_id"],
                "pipeline_name": pipe["pipeline_name"],
                "update_id": update_id,
                "event_type": "STARTING",
                "timestamp": base_time,
                "message": f"Starting update {update_id[:8]} for {pipe['pipeline_name']}",
            })

            # RUNNING event (10-30 seconds later)
            running_time = base_time + timedelta(seconds=rng.randint(10, 30))
            rows.append({
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "pipeline_id": pipe["pipeline_id"],
                "pipeline_name": pipe["pipeline_name"],
                "update_id": update_id,
                "event_type": "RUNNING",
                "timestamp": running_time,
                "message": f"Update {update_id[:8]} is running",
            })

            # Intermediate progress events (flow_progress, data quality, etc.)
            num_progress = rng.randint(2, 5)
            progress_messages = [
                f"Flow '{pipe['pipeline_name']}_bronze' has processed batch",
                f"Flow '{pipe['pipeline_name']}_silver' has processed batch",
                f"Data quality check passed for {pipe['pipeline_name']}",
                f"Cluster resized to {rng.randint(2, 8)} executors",
                f"Checkpoint saved for {pipe['pipeline_name']}",
                f"Schema validation passed for update {update_id[:8]}",
            ]
            for prog_i in range(num_progress):
                prog_time = running_time + timedelta(
                    seconds=rng.randint(30, 300) * (prog_i + 1),
                )
                rows.append({
                    "account_id": ACCOUNT_ID,
                    "workspace_id": WORKSPACE_ID,
                    "pipeline_id": pipe["pipeline_id"],
                    "pipeline_name": pipe["pipeline_name"],
                    "update_id": update_id,
                    "event_type": "RUNNING",
                    "timestamp": prog_time,
                    "message": rng.choice(progress_messages),
                })

            if is_failure:
                # FAILED event
                fail_time = running_time + timedelta(minutes=rng.randint(1, 10))
                fail_msg = rng.choice(failure_messages)
                rows.append({
                    "account_id": ACCOUNT_ID,
                    "workspace_id": WORKSPACE_ID,
                    "pipeline_id": pipe["pipeline_id"],
                    "pipeline_name": pipe["pipeline_name"],
                    "update_id": update_id,
                    "event_type": "FAILED",
                    "timestamp": fail_time,
                    "message": fail_msg,
                })

                # Retry: STARTING, RUNNING, COMPLETED (usually succeeds on retry)
                retry_id = _deterministic_id(f"retry-{pipe['pipeline_id']}-{day_offset}")
                retry_start = fail_time + timedelta(minutes=rng.randint(2, 10))
                rows.append({
                    "account_id": ACCOUNT_ID,
                    "workspace_id": WORKSPACE_ID,
                    "pipeline_id": pipe["pipeline_id"],
                    "pipeline_name": pipe["pipeline_name"],
                    "update_id": retry_id,
                    "event_type": "STARTING",
                    "timestamp": retry_start,
                    "message": f"Retrying update {retry_id[:8]} for {pipe['pipeline_name']}",
                })
                retry_running = retry_start + timedelta(seconds=rng.randint(10, 30))
                rows.append({
                    "account_id": ACCOUNT_ID,
                    "workspace_id": WORKSPACE_ID,
                    "pipeline_id": pipe["pipeline_id"],
                    "pipeline_name": pipe["pipeline_name"],
                    "update_id": retry_id,
                    "event_type": "RUNNING",
                    "timestamp": retry_running,
                    "message": f"Update {retry_id[:8]} is running",
                })
                retry_dur = rng.randint(2, 15)
                retry_end = retry_running + timedelta(minutes=retry_dur)
                rows.append({
                    "account_id": ACCOUNT_ID,
                    "workspace_id": WORKSPACE_ID,
                    "pipeline_id": pipe["pipeline_id"],
                    "pipeline_name": pipe["pipeline_name"],
                    "update_id": retry_id,
                    "event_type": "COMPLETED",
                    "timestamp": retry_end,
                    "message": f"Update {retry_id[:8]} completed successfully in {retry_dur} minutes",
                })
            else:
                # COMPLETED event
                dur_min = rng.randint(2, 15)
                end_time = running_time + timedelta(minutes=dur_min)
                rows.append({
                    "account_id": ACCOUNT_ID,
                    "workspace_id": WORKSPACE_ID,
                    "pipeline_id": pipe["pipeline_id"],
                    "pipeline_name": pipe["pipeline_name"],
                    "update_id": update_id,
                    "event_type": "COMPLETED",
                    "timestamp": end_time,
                    "message": f"Update {update_id[:8]} completed successfully in {dur_min} minutes",
                })

    master.pipeline_events = rows
    return rows


def _generate_audit_logs(
    rng: random.Random,
    now: datetime,
    master: MasterData,
    days: int = DEFAULT_SIMULATION_DAYS,
) -> list[dict[str, Any]]:
    """Generate audit log rows (volume scales with ``days``).

    Includes three categories of events:
    1. **Correlated events** -- directly derived from ``master.job_runs`` and
       ``master.pipeline_events`` so that job/cluster/warehouse/pipeline IDs
       and timestamps match the other tables exactly.
    2. **Background noise events** -- random service actions that give the
       audit log realistic volume and diversity, using the canonical
       ``ALL_USERS`` list and referencing known cluster/warehouse IDs.
    """
    rows: list[dict[str, Any]] = []

    source_ips = [
        "10.0.1.100", "10.0.1.101", "10.0.1.102", "10.0.1.103",
        "10.0.2.50", "10.0.2.51", "10.0.3.10", "10.0.3.11",
        "172.16.0.20", "172.16.0.21",
    ]

    # ------------------------------------------------------------------
    # 1. Correlated audit events from job runs
    # ------------------------------------------------------------------
    for run in master.job_runs:
        meta = master.job_meta.get(run["job_id"], {})
        user = run["creator"]
        ip = rng.choice(source_ips)

        # "jobs/runNow" at job start
        rows.append({
            "account_id": ACCOUNT_ID,
            "workspace_id": WORKSPACE_ID,
            "event_time": run["start_time"],
            "service_name": "jobs",
            "action_name": "runNow",
            "request_id": _deterministic_id(f"audit-run-{run['run_id']}"),
            "user_identity_email": user,
            "source_ip_address": ip,
            "response_status_code": 200,
        })

        # For non-serverless: "clusters/start" a few seconds before job start
        sku = meta.get("sku_name", "")
        if "SERVERLESS" not in sku:
            cluster_start = run["start_time"] - timedelta(seconds=rng.randint(5, 30))
            rows.append({
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "event_time": cluster_start,
                "service_name": "clusters",
                "action_name": "start",
                "request_id": _deterministic_id(f"audit-cls-{run['run_id']}"),
                "user_identity_email": user,
                "source_ip_address": ip,
                "response_status_code": 200,
            })

        # For SQL_SERVERLESS: "databrickssql/executeStatement" at start
        if sku == "SQL_SERVERLESS":
            rows.append({
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "event_time": run["start_time"] + timedelta(seconds=rng.randint(1, 5)),
                "service_name": "databrickssql",
                "action_name": "executeStatement",
                "request_id": _deterministic_id(f"audit-sql-{run['run_id']}"),
                "user_identity_email": user,
                "source_ip_address": ip,
                "response_status_code": 200,
            })

    # ------------------------------------------------------------------
    # 2. Correlated audit events from pipeline events
    # ------------------------------------------------------------------
    seen_pipeline_starts: set[str] = set()
    for evt in master.pipeline_events:
        if evt["event_type"] != "STARTING":
            continue
        key = f"{evt['pipeline_id']}-{evt['update_id']}"
        if key in seen_pipeline_starts:
            continue
        seen_pipeline_starts.add(key)

        pipe_meta = master.pipeline_meta.get(evt["pipeline_id"], {})
        user = pipe_meta.get("creator", rng.choice(ALL_USERS))
        rows.append({
            "account_id": ACCOUNT_ID,
            "workspace_id": WORKSPACE_ID,
            "event_time": evt["timestamp"],
            "service_name": "jobs",
            "action_name": "runNow",
            "request_id": _deterministic_id(f"audit-pipe-{evt['update_id']}"),
            "user_identity_email": user,
            "source_ip_address": rng.choice(source_ips),
            "response_status_code": 200,
        })

    # ------------------------------------------------------------------
    # 3. Correlated audit events from warehouse queries
    # ------------------------------------------------------------------
    # Sample a subset of query history entries to avoid bloating the audit log
    wh_queries = [q for q in master.query_runs if q.get("_is_warehouse_only")]
    sample_size = min(len(wh_queries), days * 15)  # ~15 per day
    sampled_queries = rng.sample(wh_queries, sample_size) if wh_queries else []
    for qrun in sampled_queries:
        rows.append({
            "account_id": ACCOUNT_ID,
            "workspace_id": WORKSPACE_ID,
            "event_time": qrun["execution_start_time"],
            "service_name": "databrickssql",
            "action_name": "executeStatement",
            "request_id": _deterministic_id(f"audit-qh-{qrun['statement_id']}"),
            "user_identity_email": qrun["executed_by"],
            "source_ip_address": rng.choice(source_ips),
            "response_status_code": 200,
        })

    # ------------------------------------------------------------------
    # 4. Background noise audit events
    # ------------------------------------------------------------------
    # Service/action pairs with relative weights (background noise only)
    noise_service_actions: list[tuple[str, str, float]] = [
        ("clusters", "create", 0.05),
        ("clusters", "permanentDelete", 0.02),
        ("clusters", "resize", 0.03),
        ("clusters", "edit", 0.04),
        ("notebook", "runCommand", 0.15),
        ("notebook", "attachNotebook", 0.05),
        ("notebook", "detachNotebook", 0.03),
        ("notebook", "createNotebook", 0.02),
        ("sqlPermissions", "executeStatement", 0.10),
        ("sqlPermissions", "getWarehouse", 0.05),
        ("sqlPermissions", "createWarehouse", 0.01),
        ("databrickssql", "getQueryHistory", 0.04),
        ("unityCatalog", "getTable", 0.08),
        ("unityCatalog", "listTables", 0.05),
        ("unityCatalog", "createTable", 0.02),
        ("jobs", "create", 0.03),
        ("jobs", "reset", 0.02),
        ("jobs", "delete", 0.01),
    ]

    total_weight = sum(w for _, _, w in noise_service_actions)
    action_weights = [w / total_weight for _, _, w in noise_service_actions]

    for day_offset in range(days, 0, -1):
        day_start = now - timedelta(days=day_offset)

        # ~40-70 noise events per day (correlated events add the rest)
        num_events = rng.randint(40, 70)
        for i in range(num_events):
            sa_idx = rng.choices(range(len(noise_service_actions)), weights=action_weights, k=1)[0]
            service_name, action_name, _ = noise_service_actions[sa_idx]
            user = rng.choice(ALL_USERS)
            hour = rng.randint(0, 23)
            event_time = day_start.replace(
                hour=hour, minute=rng.randint(0, 59), second=rng.randint(0, 59),
            )
            # Mostly 200, some 403/404
            status_code = rng.choices([200, 403, 404], weights=[0.94, 0.03, 0.03], k=1)[0]

            rows.append({
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "event_time": event_time,
                "service_name": service_name,
                "action_name": action_name,
                "request_id": _deterministic_id(f"audit-noise-{day_offset}-{i}"),
                "user_identity_email": user,
                "source_ip_address": rng.choice(source_ips),
                "response_status_code": status_code,
            })

    return rows


def _generate_table_lineage(
    rng: random.Random,
    now: datetime,
    catalog: str,
    schema: str,
    days: int = DEFAULT_SIMULATION_DAYS,
) -> list[dict[str, Any]]:
    """Generate table lineage rows (~100-200 rows).

    Uses the canonical ``ALL_USERS`` list for ``created_by``.
    """
    rows: list[dict[str, Any]] = []

    # Lineage from source tables to result tables
    for result_table in SUPPLY_CHAIN_RESULT_TABLES:
        # Each result table depends on 2-5 source tables
        num_sources = rng.randint(2, min(5, len(TPCH_SOURCE_TABLES)))
        sources = rng.sample(TPCH_SOURCE_TABLES, num_sources)
        for src in sources:
            event_time = now - timedelta(days=rng.randint(1, days))
            rows.append({
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "source_table_full_name": table_fqn(catalog, schema, src),
                "target_table_full_name": table_fqn(catalog, schema, result_table),
                "source_type": "TABLE",
                "target_type": "TABLE",
                "created_by": "dataeng-legacy@company.com",
                "event_time": event_time,
            })

    # Lineage from result tables to dashboard views
    for view in DASHBOARD_VIEWS:
        # Each view depends on 2-6 result tables
        num_sources = rng.randint(2, 6)
        sources = rng.sample(SUPPLY_CHAIN_RESULT_TABLES, num_sources)
        for src in sources:
            event_time = now - timedelta(days=rng.randint(1, days))
            rows.append({
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "source_table_full_name": table_fqn(catalog, schema, src),
                "target_table_full_name": table_fqn(catalog, schema, view),
                "source_type": "TABLE",
                "target_type": "VIEW",
                "created_by": "analytics-team@company.com",
                "event_time": event_time,
            })

    return rows


# ---------------------------------------------------------------------------
# Spark write helpers
# ---------------------------------------------------------------------------

BILLING_SCHEMA = T.StructType([
    T.StructField("record_id", T.StringType()),
    T.StructField("account_id", T.StringType()),
    T.StructField("workspace_id", T.StringType()),
    T.StructField("sku_name", T.StringType()),
    T.StructField("cloud", T.StringType()),
    T.StructField("usage_start_time", T.TimestampType()),
    T.StructField("usage_end_time", T.TimestampType()),
    T.StructField("usage_date", T.DateType()),
    T.StructField("custom_tags", T.StringType()),
    T.StructField("usage_quantity", T.DoubleType()),
    T.StructField("usage_metadata_cluster_id", T.StringType()),
    T.StructField("usage_metadata_job_id", T.StringType()),
    T.StructField("usage_metadata_notebook_id", T.StringType()),
    T.StructField("usage_metadata_warehouse_id", T.StringType()),
])

CLUSTER_SCHEMA = T.StructType([
    T.StructField("account_id", T.StringType()),
    T.StructField("workspace_id", T.StringType()),
    T.StructField("cluster_id", T.StringType()),
    T.StructField("cluster_name", T.StringType()),
    T.StructField("cluster_source", T.StringType()),
    T.StructField("creator", T.StringType()),
    T.StructField("driver_node_type", T.StringType()),
    T.StructField("worker_node_type", T.StringType()),
    T.StructField("num_workers", T.IntegerType()),
    T.StructField("autoscale_min_workers", T.IntegerType()),
    T.StructField("autoscale_max_workers", T.IntegerType()),
])

JOB_RUN_SCHEMA = T.StructType([
    T.StructField("account_id", T.StringType()),
    T.StructField("workspace_id", T.StringType()),
    T.StructField("job_id", T.LongType()),
    T.StructField("run_id", T.LongType()),
    T.StructField("run_name", T.StringType()),
    T.StructField("result_state", T.StringType()),
    T.StructField("start_time", T.TimestampType()),
    T.StructField("end_time", T.TimestampType()),
    T.StructField("job_name", T.StringType()),
    T.StructField("creator", T.StringType()),
    T.StructField("duration_minutes", T.DoubleType()),
])

QUERY_HISTORY_SCHEMA = T.StructType([
    T.StructField("account_id", T.StringType()),
    T.StructField("workspace_id", T.StringType()),
    T.StructField("statement_id", T.StringType()),
    T.StructField("executed_by", T.StringType()),
    T.StructField("statement_text", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("execution_start_time", T.TimestampType()),
    T.StructField("execution_end_time", T.TimestampType()),
    T.StructField("duration_ms", T.LongType()),
    T.StructField("rows_produced", T.LongType()),
    T.StructField("warehouse_id", T.StringType()),
    T.StructField("warehouse_name", T.StringType()),
    T.StructField("source", T.StringType()),
])

PIPELINE_EVENTS_SCHEMA = T.StructType([
    T.StructField("account_id", T.StringType()),
    T.StructField("workspace_id", T.StringType()),
    T.StructField("pipeline_id", T.StringType()),
    T.StructField("pipeline_name", T.StringType()),
    T.StructField("update_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("timestamp", T.TimestampType()),
    T.StructField("message", T.StringType()),
])

AUDIT_LOG_SCHEMA = T.StructType([
    T.StructField("account_id", T.StringType()),
    T.StructField("workspace_id", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("service_name", T.StringType()),
    T.StructField("action_name", T.StringType()),
    T.StructField("request_id", T.StringType()),
    T.StructField("user_identity_email", T.StringType()),
    T.StructField("source_ip_address", T.StringType()),
    T.StructField("response_status_code", T.IntegerType()),
])

TABLE_LINEAGE_SCHEMA = T.StructType([
    T.StructField("account_id", T.StringType()),
    T.StructField("workspace_id", T.StringType()),
    T.StructField("source_table_full_name", T.StringType()),
    T.StructField("target_table_full_name", T.StringType()),
    T.StructField("source_type", T.StringType()),
    T.StructField("target_type", T.StringType()),
    T.StructField("created_by", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
])

# Keys present in query run dicts but NOT in the Spark schema -- stripped
# before creating DataFrames.
_QUERY_INTERNAL_KEYS = frozenset({"_is_warehouse_only"})


def _parse_bool_flag(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def main(
    catalog: str,
    schema: str,
    *,
    simulation_days: int = 1,
    skip_if_populated: bool = False,
    data_version: str | None = None,
    require_version: str | None = None,
) -> None:
    """Generate simulated system tables and write as Delta."""
    if simulation_days < 1:
        msg = "simulation_days must be >= 1"
        raise ValueError(msg)
    spark = SparkSession.builder.getOrCreate()

    if skip_if_populated and sim_tables_populated(spark, catalog, schema):
        if require_version:
            stored = get_stored_version(spark, catalog, schema, COMPONENT_SIMULATE)
            if stored == require_version:
                print(
                    f"Skip simulate: sim tables present and version matches ({require_version!r})."
                )
                return
        else:
            print("Skip simulate: sim_* tables already populated.")
            return

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {catalog_schema_sql(catalog, schema)}"
    )

    rng = random.Random(42)
    now = datetime.now(tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # Build shared master data (IDs, metadata) used by all generators
    master = _build_master_data()

    # --- Job run timeline (generates master.job_runs) ---
    print("Generating job run timeline...")
    job_runs = _generate_job_runs(rng, now, master, days=simulation_days)
    job_run_rows = [
        (
            r["account_id"], r["workspace_id"], r["job_id"], r["run_id"],
            r["run_name"], r["result_state"], r["start_time"], r["end_time"],
            r["job_name"], r["creator"], r["duration_minutes"],
        )
        for r in job_runs
    ]
    job_run_df = spark.createDataFrame(job_run_rows, schema=JOB_RUN_SCHEMA)
    table_name = table_fqn(catalog, schema, "sim_job_run_timeline")
    job_run_df.write.mode("overwrite").saveAsTable(table_name)
    print(f"  {table_name}: {spark.table(table_name).count()} rows")

    # --- Query history (generates master.query_runs, needed by billing) ---
    print("Generating query history...")
    query_history_data = _generate_query_history(
        rng, now, master, catalog, schema, days=simulation_days,
    )
    query_history_rows = [
        (
            r["account_id"], r["workspace_id"], r["statement_id"],
            r["executed_by"], r["statement_text"], r["status"],
            r["execution_start_time"], r["execution_end_time"],
            r["duration_ms"], r["rows_produced"],
            r["warehouse_id"], r["warehouse_name"], r["source"],
        )
        for r in query_history_data
    ]
    query_history_df = spark.createDataFrame(query_history_rows, schema=QUERY_HISTORY_SCHEMA)
    table_name = table_fqn(catalog, schema, "sim_query_history")
    query_history_df.write.mode("overwrite").saveAsTable(table_name)
    print(f"  {table_name}: {spark.table(table_name).count()} rows")

    # --- Pipeline events (generates master.pipeline_events) ---
    print("Generating pipeline events...")
    pipeline_events_data = _generate_pipeline_events(
        rng, now, master, days=simulation_days,
    )
    pipeline_events_rows = [
        (
            r["account_id"], r["workspace_id"], r["pipeline_id"],
            r["pipeline_name"], r["update_id"], r["event_type"],
            r["timestamp"], r["message"],
        )
        for r in pipeline_events_data
    ]
    pipeline_events_df = spark.createDataFrame(pipeline_events_rows, schema=PIPELINE_EVENTS_SCHEMA)
    table_name = table_fqn(catalog, schema, "sim_pipeline_events")
    pipeline_events_df.write.mode("overwrite").saveAsTable(table_name)
    print(f"  {table_name}: {spark.table(table_name).count()} rows")

    # --- Billing usage (depends on master.job_runs and master.query_runs) ---
    print("Generating billing usage...")
    billing_rows_data = _generate_billing_rows(master, rng)
    billing_rows = [
        (
            r["record_id"], r["account_id"], r["workspace_id"], r["sku_name"],
            r["cloud"], r["usage_start_time"], r["usage_end_time"],
            r["usage_date"], r["custom_tags"], r["usage_quantity"],
            r["usage_metadata_cluster_id"], r["usage_metadata_job_id"],
            r["usage_metadata_notebook_id"], r["usage_metadata_warehouse_id"],
        )
        for r in billing_rows_data
    ]
    billing_df = spark.createDataFrame(billing_rows, schema=BILLING_SCHEMA)
    table_name = table_fqn(catalog, schema, "sim_billing_usage")
    billing_df.write.mode("overwrite").saveAsTable(table_name)
    print(f"  {table_name}: {spark.table(table_name).count()} rows")

    # --- Cluster info ---
    print("Generating cluster info...")
    cluster_rows_data = _generate_cluster_rows(master)
    cluster_rows = [
        (
            r["account_id"], r["workspace_id"], r["cluster_id"],
            r["cluster_name"], r["cluster_source"], r["creator"],
            r["driver_node_type"], r["worker_node_type"], r["num_workers"],
            r["autoscale_min_workers"], r["autoscale_max_workers"],
        )
        for r in cluster_rows_data
    ]
    cluster_df = spark.createDataFrame(cluster_rows, schema=CLUSTER_SCHEMA)
    table_name = table_fqn(catalog, schema, "sim_cluster_info")
    cluster_df.write.mode("overwrite").saveAsTable(table_name)
    print(f"  {table_name}: {spark.table(table_name).count()} rows")

    # --- Audit logs (depends on master.job_runs and master.pipeline_events) ---
    print("Generating audit logs...")
    audit_logs_data = _generate_audit_logs(
        rng, now, master, days=simulation_days,
    )
    audit_log_rows = [
        (
            r["account_id"], r["workspace_id"], r["event_time"],
            r["service_name"], r["action_name"], r["request_id"],
            r["user_identity_email"], r["source_ip_address"],
            r["response_status_code"],
        )
        for r in audit_logs_data
    ]
    audit_log_df = spark.createDataFrame(audit_log_rows, schema=AUDIT_LOG_SCHEMA)
    table_name = table_fqn(catalog, schema, "sim_audit_logs")
    audit_log_df.write.mode("overwrite").saveAsTable(table_name)
    print(f"  {table_name}: {spark.table(table_name).count()} rows")

    # --- Table lineage ---
    print("Generating table lineage...")
    table_lineage_data = _generate_table_lineage(
        rng, now, catalog, schema, days=simulation_days,
    )
    table_lineage_rows = [
        (
            r["account_id"], r["workspace_id"],
            r["source_table_full_name"], r["target_table_full_name"],
            r["source_type"], r["target_type"],
            r["created_by"], r["event_time"],
        )
        for r in table_lineage_data
    ]
    table_lineage_df = spark.createDataFrame(table_lineage_rows, schema=TABLE_LINEAGE_SCHEMA)
    table_name = table_fqn(catalog, schema, "sim_table_lineage")
    table_lineage_df.write.mode("overwrite").saveAsTable(table_name)
    print(f"  {table_name}: {spark.table(table_name).count()} rows")

    print("Done simulating system tables.")
    ver = (data_version or "").strip() or simulate_fingerprint(simulation_days)
    merge_stored_version(
        spark,
        catalog,
        schema,
        component=COMPONENT_SIMULATE,
        version=ver,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simulate Databricks system tables for cost analysis"
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument(
        "--simulation-days",
        type=int,
        default=1,
        help=(
            "Number of days of synthetic billing/query/audit history "
            "(lower is faster; default tuned for E2E)."
        ),
    )
    parser.add_argument(
        "--skip-if-populated",
        default="false",
        help='If "true", skip when sim_billing_usage exists with rows (default: false).',
    )
    parser.add_argument(
        "--data-version",
        default="",
        help="Stored in bundle state after success; empty uses auto fingerprint from simulation_days.",
    )
    parser.add_argument(
        "--require-version",
        default="",
        help="With --skip-if-populated, skip only when bundle state matches this string.",
    )
    args = parser.parse_args()
    dv = args.data_version.strip() or None
    rv = args.require_version.strip() or None
    main(
        args.catalog,
        args.schema,
        simulation_days=args.simulation_days,
        skip_if_populated=_parse_bool_flag(args.skip_if_populated),
        data_version=dv,
        require_version=rv,
    )
