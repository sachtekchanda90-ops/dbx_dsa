# pylint: disable=redefined-outer-name
"""Unit tests for the simulated system table data generators.

These tests import the generation functions directly from the simulation
script and validate referential integrity, row counts, schema correctness,
data quality, and the intended cost story without requiring Spark or Delta.
"""


# mypy: ignore-errors

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from dsa_interview_packet.scripts.simulate_system_tables import (
    ACCOUNT_ID,
    ALL_USERS,
    AUDIT_LOG_SCHEMA,
    BILLING_SCHEMA,
    CLUSTER_SCHEMA,
    DAYS,
    DEFAULT_SIMULATION_DAYS,
    JOB_RUN_SCHEMA,
    NOISE_JOBS,
    PIPELINE_EVENTS_SCHEMA,
    PIPELINES,
    QUERY_HISTORY_SCHEMA,
    SQL_WAREHOUSE_ID,
    SQL_WAREHOUSE_NAME,
    SUPPLY_CHAIN_JOBS,
    TABLE_LINEAGE_SCHEMA,
    WORKSPACE_ID,
    _build_master_data,
    _generate_audit_logs,
    _generate_billing_rows,
    _generate_cluster_rows,
    _generate_job_runs,
    _generate_pipeline_events,
    _generate_query_history,
    _generate_table_lineage,
)

# ---------------------------------------------------------------------------
# Shared fixture: run all generators once with a fixed seed
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def generated_data() -> dict[str, Any]:
    """Run all generation functions once and return results in a dict.

    The generators are executed in the same order as ``main()`` so that
    cross-table references (master.job_runs, master.query_runs,
    master.pipeline_events) are populated correctly.
    """
    rng = random.Random(42)
    now = datetime.now(tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    master = _build_master_data()

    job_runs = _generate_job_runs(rng, now, master)
    query_history = _generate_query_history(rng, now, master, "main", "default")
    pipeline_events = _generate_pipeline_events(rng, now, master)
    billing = _generate_billing_rows(master, rng)
    clusters = _generate_cluster_rows(master)
    audit_logs = _generate_audit_logs(rng, now, master)
    table_lineage = _generate_table_lineage(rng, now, "main", "default")

    return {
        "master": master,
        "now": now,
        "job_runs": job_runs,
        "query_history": query_history,
        "pipeline_events": pipeline_events,
        "billing": billing,
        "clusters": clusters,
        "audit_logs": audit_logs,
        "table_lineage": table_lineage,
    }


# ===================================================================
# 1. Referential integrity
# ===================================================================


class TestReferentialIntegrity:
    """All cross-table ID references must resolve."""

    def test_billing_job_ids_exist_in_job_run_timeline(
        self, generated_data: dict[str, Any],
    ) -> None:
        """Every job_id in billing must appear in job_run_timeline."""
        job_ids_in_runs = {r["job_id"] for r in generated_data["job_runs"]}
        billing_job_ids = {
            int(r["usage_metadata_job_id"])
            for r in generated_data["billing"]
            if r["usage_metadata_job_id"] is not None
        }
        assert billing_job_ids.issubset(job_ids_in_runs), (
            f"Billing job_ids not in job_run_timeline: "
            f"{billing_job_ids - job_ids_in_runs}"
        )

    def test_billing_cluster_ids_exist_in_cluster_info(
        self, generated_data: dict[str, Any],
    ) -> None:
        """Every cluster_id in billing must appear in cluster_info."""
        cluster_ids_in_info = {
            r["cluster_id"] for r in generated_data["clusters"]
        }
        billing_cluster_ids = {
            r["usage_metadata_cluster_id"]
            for r in generated_data["billing"]
            if r["usage_metadata_cluster_id"] is not None
        }
        assert billing_cluster_ids.issubset(cluster_ids_in_info), (
            f"Billing cluster_ids not in cluster_info: "
            f"{billing_cluster_ids - cluster_ids_in_info}"
        )

    def test_query_history_warehouse_ids(
        self, generated_data: dict[str, Any],
    ) -> None:
        """All warehouse_ids in query_history match the defined warehouse."""
        warehouse_ids = {
            r["warehouse_id"] for r in generated_data["query_history"]
        }
        assert warehouse_ids == {SQL_WAREHOUSE_ID}, (
            f"Unexpected warehouse_ids: {warehouse_ids}"
        )

    def test_pipeline_event_ids_match_defined_pipelines(
        self, generated_data: dict[str, Any],
    ) -> None:
        """All pipeline_ids in pipeline_events match defined PIPELINES."""
        defined_ids = {p["pipeline_id"] for p in PIPELINES}
        event_ids = {
            r["pipeline_id"] for r in generated_data["pipeline_events"]
        }
        assert event_ids.issubset(defined_ids), (
            f"Pipeline event IDs not in PIPELINES: {event_ids - defined_ids}"
        )

    def test_all_user_emails_from_canonical_list(
        self, generated_data: dict[str, Any],
    ) -> None:
        """User emails across all tables come from the canonical ALL_USERS list.

        Note: query_history also uses 'dashboard_refresh@system' as a
        system user, so we add that to the allowed set.
        """
        allowed_users = set(ALL_USERS) | {"dashboard_refresh@system"}

        # job_run_timeline
        job_users = {r["creator"] for r in generated_data["job_runs"]}
        assert job_users.issubset(allowed_users), (
            f"Unknown users in job_runs: {job_users - allowed_users}"
        )

        # cluster_info
        cluster_users = {r["creator"] for r in generated_data["clusters"]}
        assert cluster_users.issubset(allowed_users), (
            f"Unknown users in clusters: {cluster_users - allowed_users}"
        )

        # query_history
        qh_users = {r["executed_by"] for r in generated_data["query_history"]}
        assert qh_users.issubset(allowed_users), (
            f"Unknown users in query_history: {qh_users - allowed_users}"
        )

        # audit_logs
        audit_users = {
            r["user_identity_email"] for r in generated_data["audit_logs"]
        }
        assert audit_users.issubset(allowed_users), (
            f"Unknown users in audit_logs: {audit_users - allowed_users}"
        )

        # table_lineage
        lineage_users = {
            r["created_by"] for r in generated_data["table_lineage"]
        }
        assert lineage_users.issubset(allowed_users), (
            f"Unknown users in table_lineage: {lineage_users - allowed_users}"
        )

    def test_account_and_workspace_ids_consistent(
        self, generated_data: dict[str, Any],
    ) -> None:
        """account_id and workspace_id are consistent across all tables."""
        table_names = [
            "job_runs", "billing", "clusters", "query_history",
            "pipeline_events", "audit_logs", "table_lineage",
        ]
        for table_name in table_names:
            rows = generated_data[table_name]
            for row in rows:
                assert row["account_id"] == ACCOUNT_ID, (
                    f"{table_name}: unexpected account_id {row['account_id']}"
                )
                assert row["workspace_id"] == WORKSPACE_ID, (
                    f"{table_name}: unexpected workspace_id "
                    f"{row['workspace_id']}"
                )


# ===================================================================
# 2. Row count bounds
# ===================================================================


class TestRowCountBounds:
    """Generated row counts fall within expected ranges."""

    def test_job_run_timeline_row_count(
        self, generated_data: dict[str, Any],
    ) -> None:
        count = len(generated_data["job_runs"])
        assert 400 <= count <= 800, (
            f"sim_job_run_timeline: {count} rows, expected 400-800"
        )

    def test_billing_usage_row_count(
        self, generated_data: dict[str, Any],
    ) -> None:
        count = len(generated_data["billing"])
        assert 400 <= count <= 4000, (
            f"sim_billing_usage: {count} rows, expected 400-4000"
        )

    def test_cluster_info_row_count(
        self, generated_data: dict[str, Any],
    ) -> None:
        count = len(generated_data["clusters"])
        assert 15 <= count <= 30, (
            f"sim_cluster_info: {count} rows, expected 15-30"
        )

    def test_query_history_row_count(
        self, generated_data: dict[str, Any],
    ) -> None:
        count = len(generated_data["query_history"])
        assert 1500 <= count <= 4000, (
            f"sim_query_history: {count} rows, expected 1500-4000"
        )

    def test_pipeline_events_row_count(
        self, generated_data: dict[str, Any],
    ) -> None:
        count = len(generated_data["pipeline_events"])
        assert 300 <= count <= 1000, (
            f"sim_pipeline_events: {count} rows, expected 300-1000"
        )

    def test_audit_logs_row_count(
        self, generated_data: dict[str, Any],
    ) -> None:
        count = len(generated_data["audit_logs"])
        assert 2000 <= count <= 6000, (
            f"sim_audit_logs: {count} rows, expected 2000-6000"
        )

    def test_table_lineage_row_count(
        self, generated_data: dict[str, Any],
    ) -> None:
        count = len(generated_data["table_lineage"])
        assert 50 <= count <= 300, (
            f"sim_table_lineage: {count} rows, expected 50-300"
        )


# ===================================================================
# 3. Schema correctness
# ===================================================================


def _spark_schema_field_names(spark_schema) -> list[str]:
    """Extract field names from a PySpark StructType."""
    return [f.name for f in spark_schema.fields]


class TestSchemaCorrectness:
    """Each generated table has the expected column names."""

    def test_job_run_timeline_columns(
        self, generated_data: dict[str, Any],
    ) -> None:
        expected_cols = _spark_schema_field_names(JOB_RUN_SCHEMA)
        row = generated_data["job_runs"][0]
        # cluster_id is in the raw dict but stripped before Spark write
        for col in expected_cols:
            assert col in row, f"Missing column '{col}' in job_run_timeline"

    def test_billing_usage_columns(
        self, generated_data: dict[str, Any],
    ) -> None:
        expected_cols = _spark_schema_field_names(BILLING_SCHEMA)
        row = generated_data["billing"][0]
        for col in expected_cols:
            assert col in row, f"Missing column '{col}' in billing_usage"

    def test_cluster_info_columns(
        self, generated_data: dict[str, Any],
    ) -> None:
        expected_cols = _spark_schema_field_names(CLUSTER_SCHEMA)
        row = generated_data["clusters"][0]
        for col in expected_cols:
            assert col in row, f"Missing column '{col}' in cluster_info"

    def test_query_history_columns(
        self, generated_data: dict[str, Any],
    ) -> None:
        expected_cols = _spark_schema_field_names(QUERY_HISTORY_SCHEMA)
        row = generated_data["query_history"][0]
        for col in expected_cols:
            assert col in row, f"Missing column '{col}' in query_history"

    def test_pipeline_events_columns(
        self, generated_data: dict[str, Any],
    ) -> None:
        expected_cols = _spark_schema_field_names(PIPELINE_EVENTS_SCHEMA)
        row = generated_data["pipeline_events"][0]
        for col in expected_cols:
            assert col in row, f"Missing column '{col}' in pipeline_events"

    def test_audit_logs_columns(
        self, generated_data: dict[str, Any],
    ) -> None:
        expected_cols = _spark_schema_field_names(AUDIT_LOG_SCHEMA)
        row = generated_data["audit_logs"][0]
        for col in expected_cols:
            assert col in row, f"Missing column '{col}' in audit_logs"

    def test_table_lineage_columns(
        self, generated_data: dict[str, Any],
    ) -> None:
        expected_cols = _spark_schema_field_names(TABLE_LINEAGE_SCHEMA)
        row = generated_data["table_lineage"][0]
        for col in expected_cols:
            assert col in row, f"Missing column '{col}' in table_lineage"


# ===================================================================
# 4. Data quality
# ===================================================================


class TestDataQuality:
    """Field values are well-formed and within expected domains."""

    def test_no_null_job_ids(
        self, generated_data: dict[str, Any],
    ) -> None:
        """job_id and run_id must never be NULL in job_run_timeline."""
        for row in generated_data["job_runs"]:
            assert row["job_id"] is not None, "NULL job_id in job_run_timeline"
            assert row["run_id"] is not None, "NULL run_id in job_run_timeline"

    def test_no_null_cluster_ids(
        self, generated_data: dict[str, Any],
    ) -> None:
        """cluster_id must never be NULL in cluster_info."""
        for row in generated_data["clusters"]:
            assert row["cluster_id"] is not None, (
                "NULL cluster_id in cluster_info"
            )

    def test_no_null_statement_ids(
        self, generated_data: dict[str, Any],
    ) -> None:
        """statement_id must never be NULL in query_history."""
        for row in generated_data["query_history"]:
            assert row["statement_id"] is not None, (
                "NULL statement_id in query_history"
            )

    def test_no_null_pipeline_ids(
        self, generated_data: dict[str, Any],
    ) -> None:
        """pipeline_id must never be NULL in pipeline_events."""
        for row in generated_data["pipeline_events"]:
            assert row["pipeline_id"] is not None, (
                "NULL pipeline_id in pipeline_events"
            )

    def test_no_null_record_ids(
        self, generated_data: dict[str, Any],
    ) -> None:
        """record_id must never be NULL in billing_usage."""
        for row in generated_data["billing"]:
            assert row["record_id"] is not None, (
                "NULL record_id in billing_usage"
            )

    def test_timestamps_within_30_day_window(
        self, generated_data: dict[str, Any],
    ) -> None:
        """All timestamps must fall within the 30-day generation window."""
        now = generated_data["now"]
        window_start = now - timedelta(days=DAYS + 1)  # small buffer
        window_end = now + timedelta(days=1)  # small buffer

        # job_run_timeline
        for row in generated_data["job_runs"]:
            assert window_start <= row["start_time"] <= window_end, (
                f"job_run start_time out of range: {row['start_time']}"
            )
            assert window_start <= row["end_time"] <= window_end, (
                f"job_run end_time out of range: {row['end_time']}"
            )

        # query_history
        for row in generated_data["query_history"]:
            assert window_start <= row["execution_start_time"] <= window_end, (
                f"query execution_start_time out of range: "
                f"{row['execution_start_time']}"
            )

        # pipeline_events
        for row in generated_data["pipeline_events"]:
            assert window_start <= row["timestamp"] <= window_end, (
                f"pipeline event timestamp out of range: {row['timestamp']}"
            )

        # audit_logs
        for row in generated_data["audit_logs"]:
            assert window_start <= row["event_time"] <= window_end, (
                f"audit log event_time out of range: {row['event_time']}"
            )

        # table_lineage
        for row in generated_data["table_lineage"]:
            assert window_start <= row["event_time"] <= window_end, (
                f"lineage event_time out of range: {row['event_time']}"
            )

    def test_valid_sku_names(
        self, generated_data: dict[str, Any],
    ) -> None:
        """All SKU names in billing must be valid Databricks SKUs."""
        valid_skus = {
            "ALL_PURPOSE_COMPUTE",
            "JOBS_COMPUTE",
            "JOBS_SERVERLESS",
            "SQL_SERVERLESS",
            "DLT_SERVERLESS",
            "INTERACTIVE_SERVERLESS",
        }
        billing_skus = {r["sku_name"] for r in generated_data["billing"]}
        assert billing_skus.issubset(valid_skus), (
            f"Invalid SKUs in billing: {billing_skus - valid_skus}"
        )

    def test_valid_result_states(
        self, generated_data: dict[str, Any],
    ) -> None:
        """Job result_state values must be valid."""
        valid_states = {"SUCCESS", "FAILED"}
        states = {r["result_state"] for r in generated_data["job_runs"]}
        assert states.issubset(valid_states), (
            f"Invalid result_states: {states - valid_states}"
        )

    def test_valid_query_statuses(
        self, generated_data: dict[str, Any],
    ) -> None:
        """Query status values must be valid."""
        valid_statuses = {"FINISHED", "FAILED", "CANCELED"}
        statuses = {r["status"] for r in generated_data["query_history"]}
        assert statuses.issubset(valid_statuses), (
            f"Invalid query statuses: {statuses - valid_statuses}"
        )

    def test_positive_usage_quantities(
        self, generated_data: dict[str, Any],
    ) -> None:
        """All billing usage_quantity values must be positive."""
        for row in generated_data["billing"]:
            assert row["usage_quantity"] > 0, (
                f"Non-positive usage_quantity: {row['usage_quantity']}"
            )

    def test_positive_durations(
        self, generated_data: dict[str, Any],
    ) -> None:
        """All job run durations must be positive."""
        for row in generated_data["job_runs"]:
            assert row["duration_minutes"] > 0, (
                f"Non-positive duration_minutes: {row['duration_minutes']}"
            )

    def test_query_history_warehouse_names(
        self, generated_data: dict[str, Any],
    ) -> None:
        """All warehouse_name values must match the defined warehouse."""
        names = {r["warehouse_name"] for r in generated_data["query_history"]}
        assert names == {SQL_WAREHOUSE_NAME}, (
            f"Unexpected warehouse_names: {names}"
        )

    def test_pipeline_event_types_valid(
        self, generated_data: dict[str, Any],
    ) -> None:
        """Pipeline event types must be from the known set."""
        valid_types = {"STARTING", "RUNNING", "COMPLETED", "FAILED"}
        event_types = {
            r["event_type"] for r in generated_data["pipeline_events"]
        }
        assert event_types.issubset(valid_types), (
            f"Invalid pipeline event types: {event_types - valid_types}"
        )


# ===================================================================
# 5. Cost story validation
# ===================================================================


class TestCostStory:
    """The generated data tells the intended cost optimisation story."""

    def test_legacy_pandas_higher_total_usage_than_lakehouse(
        self, generated_data: dict[str, Any],
    ) -> None:
        """legacy_pandas_daily has higher total usage_quantity than lakehouse_queries_daily."""
        legacy_job_id = "100001"
        lakehouse_job_id = "100002"

        legacy_total = sum(
            r["usage_quantity"]
            for r in generated_data["billing"]
            if r["usage_metadata_job_id"] == legacy_job_id
        )
        lakehouse_total = sum(
            r["usage_quantity"]
            for r in generated_data["billing"]
            if r["usage_metadata_job_id"] == lakehouse_job_id
        )

        assert legacy_total > lakehouse_total, (
            f"legacy_pandas_daily total ({legacy_total:.2f}) should exceed "
            f"lakehouse_queries_daily total ({lakehouse_total:.2f})"
        )

    def test_legacy_pandas_longer_avg_duration(
        self, generated_data: dict[str, Any],
    ) -> None:
        """legacy_pandas_daily has longer average duration than other supply chain jobs."""
        supply_chain_ids = {
            int(j["job_id"]) for j in SUPPLY_CHAIN_JOBS
        }
        legacy_id = int(SUPPLY_CHAIN_JOBS[0]["job_id"])  # 100001

        legacy_runs = [
            r for r in generated_data["job_runs"]
            if r["job_id"] == legacy_id
        ]
        other_sc_runs = [
            r for r in generated_data["job_runs"]
            if r["job_id"] in supply_chain_ids and r["job_id"] != legacy_id
        ]

        assert legacy_runs, "No legacy_pandas_daily runs found"
        assert other_sc_runs, "No other supply chain runs found"

        legacy_avg = sum(
            r["duration_minutes"] for r in legacy_runs
        ) / len(legacy_runs)
        other_avg = sum(
            r["duration_minutes"] for r in other_sc_runs
        ) / len(other_sc_runs)

        assert legacy_avg > other_avg, (
            f"legacy_pandas avg duration ({legacy_avg:.2f} min) should exceed "
            f"other supply chain avg ({other_avg:.2f} min)"
        )

    def test_legacy_pandas_uses_all_purpose_compute(
        self, generated_data: dict[str, Any],
    ) -> None:
        """legacy_pandas_daily billing rows use ALL_PURPOSE_COMPUTE SKU."""
        legacy_job_id = "100001"
        legacy_skus = {
            r["sku_name"]
            for r in generated_data["billing"]
            if r["usage_metadata_job_id"] == legacy_job_id
        }
        assert legacy_skus == {"ALL_PURPOSE_COMPUTE"}, (
            f"Expected ALL_PURPOSE_COMPUTE for legacy job, got {legacy_skus}"
        )


# ===================================================================
# 6. Module constants validation
# ===================================================================


class TestModuleConstants:
    """Module-level constants are correctly defined."""

    def test_all_users_includes_supply_chain_creators(self) -> None:
        """ALL_USERS includes creators from SUPPLY_CHAIN_JOBS."""
        creators = {j["creator"] for j in SUPPLY_CHAIN_JOBS}
        assert creators.issubset(set(ALL_USERS)), (
            f"Supply chain creators missing from ALL_USERS: "
            f"{creators - set(ALL_USERS)}"
        )

    def test_all_users_includes_noise_users(self) -> None:
        """ALL_USERS includes creators from NOISE_JOBS."""
        creators = {j["creator"] for j in NOISE_JOBS}
        assert creators.issubset(set(ALL_USERS)), (
            f"Noise job creators missing from ALL_USERS: "
            f"{creators - set(ALL_USERS)}"
        )

    def test_pipeline_ids_match_job_definitions(self) -> None:
        """Pipeline IDs in PIPELINES match those referenced in job defs."""
        pipeline_ids_in_jobs = {
            j.get("pipeline_id")
            for j in [*SUPPLY_CHAIN_JOBS, *NOISE_JOBS]
            if j.get("pipeline_id")
        }
        pipeline_ids_defined = {p["pipeline_id"] for p in PIPELINES}
        assert pipeline_ids_in_jobs.issubset(pipeline_ids_defined), (
            f"Job pipeline_ids not in PIPELINES: "
            f"{pipeline_ids_in_jobs - pipeline_ids_defined}"
        )

    def test_supply_chain_job_ids_unique(self) -> None:
        """All supply chain and noise job IDs are unique."""
        all_ids = [j["job_id"] for j in SUPPLY_CHAIN_JOBS] + [
            j["job_id"] for j in NOISE_JOBS
        ]
        assert len(all_ids) == len(set(all_ids)), "Duplicate job_ids found"

    def test_days_constant(self) -> None:
        """DEFAULT_SIMULATION_DAYS keeps unit-test fixtures on a 30-day timeline."""
        assert DEFAULT_SIMULATION_DAYS == 30
        assert DAYS == DEFAULT_SIMULATION_DAYS


# ===================================================================
# 7. Determinism
# ===================================================================


class TestDeterminism:
    """Generation with the same seed produces identical output."""

    def test_same_seed_same_job_run_count(self) -> None:
        """Running generation twice with seed=42 yields the same row count."""
        rng1 = random.Random(42)
        rng2 = random.Random(42)
        now = datetime.now(tz=timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0,
        )

        master1 = _build_master_data()
        master2 = _build_master_data()

        runs1 = _generate_job_runs(rng1, now, master1)
        runs2 = _generate_job_runs(rng2, now, master2)

        assert len(runs1) == len(runs2)

    def test_same_seed_same_job_ids(self) -> None:
        """Running generation twice with seed=42 yields identical job_ids."""
        rng1 = random.Random(42)
        rng2 = random.Random(42)
        now = datetime.now(tz=timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0,
        )

        master1 = _build_master_data()
        master2 = _build_master_data()

        runs1 = _generate_job_runs(rng1, now, master1)
        runs2 = _generate_job_runs(rng2, now, master2)

        ids1 = [(r["job_id"], r["run_id"]) for r in runs1]
        ids2 = [(r["job_id"], r["run_id"]) for r in runs2]
        assert ids1 == ids2
