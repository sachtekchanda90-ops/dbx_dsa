"""Unit tests for the seed_source_tables script.

Verifies that main() creates the target schema, copies all 8 TPC-H source
tables from samples.tpch into the target catalog/schema, and reports row
counts.  All Spark interactions are mocked.
"""


# mypy: ignore-errors

from __future__ import annotations

from unittest.mock import MagicMock, patch

from dsa_interview_packet.scripts.seed_source_tables import SOURCE_TABLES, main
from dsa_interview_packet.sql_identifiers import catalog_schema_sql, table_fqn


class TestSeedSourceTables:
    """Validate seed_source_tables.main() orchestration logic."""

    def _build_mock_spark(self, row_counts: dict[str, int] | None = None):
        """Build a mock SparkSession with chained write/count support.

        Args:
            row_counts: Optional mapping of target table name -> row count
                returned by ``spark.table(full_name).count()``.  Defaults to
                100 for every table.
        """
        if row_counts is None:
            row_counts = {name: 100 for name in SOURCE_TABLES}

        spark = MagicMock(name="SparkSession")

        # spark.table() is called twice per table:
        #   1. spark.table(source_fqn) -> df to read
        #   2. spark.table(full_name)  -> df whose .count() we check
        source_dfs = {}
        count_dfs = {}

        for target_name, source_fqn in SOURCE_TABLES.items():
            # Source read DF
            src_df = MagicMock(name=f"df_{source_fqn}")
            src_df.write.mode.return_value.saveAsTable = MagicMock()
            source_dfs[source_fqn] = src_df

            # Count DF (returned when spark.table is called with the full target name)
            cnt_df = MagicMock(name=f"cnt_{target_name}")
            cnt_df.count.return_value = row_counts.get(target_name, 100)
            count_dfs[target_name] = cnt_df

        def table_side_effect(name):
            if name in source_dfs:
                return source_dfs[name]
            # Full target name: `catalog`.`schema`.`table`
            short = name.rsplit(".", 1)[-1].strip("`")
            return count_dfs[short]

        spark.table.side_effect = table_side_effect
        return spark, source_dfs, count_dfs

    @patch("dsa_interview_packet.scripts.seed_source_tables.merge_stored_version")
    @patch(
        "dsa_interview_packet.scripts.seed_source_tables.SparkSession"
    )
    def test_creates_schema(self, mock_session_cls, _merge):
        """main() should issue CREATE DATABASE IF NOT EXISTS (Spark schema namespace)."""
        spark, _, _ = self._build_mock_spark()
        mock_session_cls.builder.getOrCreate.return_value = spark

        main("my_catalog", "my_schema", orders_sample_fraction=1.0)

        spark.sql.assert_any_call(
            f"CREATE DATABASE IF NOT EXISTS {catalog_schema_sql('my_catalog', 'my_schema')}"
        )

    @patch("dsa_interview_packet.scripts.seed_source_tables.merge_stored_version")
    @patch(
        "dsa_interview_packet.scripts.seed_source_tables.SparkSession"
    )
    def test_copies_all_eight_tables(self, mock_session_cls, _merge):
        """main() should read all 8 source tables and write them to target."""
        spark, source_dfs, _ = self._build_mock_spark()
        mock_session_cls.builder.getOrCreate.return_value = spark

        main("cat", "sch", orders_sample_fraction=1.0)

        # Verify spark.table was called for each source
        for source_fqn in SOURCE_TABLES.values():
            spark.table.assert_any_call(source_fqn)

        # Verify write.mode("overwrite").saveAsTable(full_name) for each target
        for target_name, source_fqn in SOURCE_TABLES.items():
            expected_full = table_fqn("cat", "sch", target_name)
            src_df = source_dfs[source_fqn]
            src_df.write.mode.assert_called_with("overwrite")
            src_df.write.mode("overwrite").saveAsTable.assert_any_call(
                expected_full
            )

    @patch("dsa_interview_packet.scripts.seed_source_tables.merge_stored_version")
    @patch(
        "dsa_interview_packet.scripts.seed_source_tables.SparkSession"
    )
    def test_reads_row_count_after_write(self, mock_session_cls, _merge):
        """main() should call spark.table(full_name).count() after each write."""
        custom_counts = {name: i * 10 for i, name in enumerate(SOURCE_TABLES, start=1)}
        spark, _, count_dfs = self._build_mock_spark(row_counts=custom_counts)
        mock_session_cls.builder.getOrCreate.return_value = spark

        main("cat", "sch", orders_sample_fraction=1.0)

        for target_name in SOURCE_TABLES:
            full_name = table_fqn("cat", "sch", target_name)
            spark.table.assert_any_call(full_name)
            count_dfs[target_name].count.assert_called_once()

    @patch("dsa_interview_packet.scripts.seed_source_tables.merge_stored_version")
    @patch(
        "dsa_interview_packet.scripts.seed_source_tables.SparkSession"
    )
    def test_source_tables_mapping_completeness(self, mock_session_cls, _merge):
        """SOURCE_TABLES should contain exactly the 8 TPC-H tables."""
        expected = {
            "region", "nation", "supplier", "part",
            "partsupp", "customer", "orders", "lineitem",
        }
        assert set(SOURCE_TABLES.keys()) == expected

        # Every source should point to samples.tpch.<name>
        for target_name, source_fqn in SOURCE_TABLES.items():
            assert source_fqn == f"samples.tpch.{target_name}"

    @patch("dsa_interview_packet.scripts.seed_source_tables.merge_stored_version")
    @patch(
        "dsa_interview_packet.scripts.seed_source_tables.SparkSession"
    )
    def test_uses_correct_catalog_and_schema(self, mock_session_cls, _merge):
        """Target table names should use the provided catalog and schema."""
        spark, source_dfs, _ = self._build_mock_spark()
        mock_session_cls.builder.getOrCreate.return_value = spark

        main("prod_catalog", "bronze_schema", orders_sample_fraction=1.0)

        spark.sql.assert_any_call(
            f"CREATE DATABASE IF NOT EXISTS {catalog_schema_sql('prod_catalog', 'bronze_schema')}"
        )

        # Check that at least one saveAsTable used the right prefix
        first_target = next(iter(SOURCE_TABLES))
        first_source = SOURCE_TABLES[first_target]
        expected_full = table_fqn("prod_catalog", "bronze_schema", first_target)
        source_dfs[first_source].write.mode("overwrite").saveAsTable.assert_any_call(
            expected_full
        )
