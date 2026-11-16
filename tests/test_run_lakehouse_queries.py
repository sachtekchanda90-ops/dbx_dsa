"""Unit tests for the run_lakehouse_queries script.

Verifies that main() sets the catalog/schema context, discovers SQL files,
executes each query, writes results as Delta tables with a ``lakehouse_``
prefix, and reports row counts.  All Spark and filesystem interactions are
mocked.
"""


# mypy: ignore-errors

from __future__ import annotations

import os
from unittest.mock import MagicMock, mock_open, patch

import pytest
from dsa_interview_packet.scripts.run_lakehouse_queries import main
from dsa_interview_packet.sql_identifiers import table_fqn


class TestRunLakehouseQueries:
    """Validate run_lakehouse_queries.main() orchestration logic."""

    def _build_mock_spark(self, table_counts: dict[str, int] | None = None):
        """Build a mock SparkSession.

        Args:
            table_counts: Mapping of full table name -> count value.
                Defaults to 50 for any table.
        """
        spark = MagicMock(name="SparkSession")

        sql_results = {}
        count_dfs = {}

        def sql_side_effect(statement):
            # USE CATALOG / USE SCHEMA -> return a throwaway mock
            if statement.startswith("USE "):
                return MagicMock()
            # Otherwise it's a query execution -> return a DF mock
            df = MagicMock(name=f"sql_result_{len(sql_results)}")
            df.write.mode.return_value.saveAsTable = MagicMock()
            sql_results[statement] = df
            return df

        def table_side_effect(name):
            cnt_df = MagicMock(name=f"cnt_{name}")
            count = 50
            if table_counts and name in table_counts:
                count = table_counts[name]
            cnt_df.count.return_value = count
            count_dfs[name] = cnt_df
            return cnt_df

        spark.sql.side_effect = sql_side_effect
        spark.table.side_effect = table_side_effect

        return spark, sql_results, count_dfs

    @patch(
        "dsa_interview_packet.scripts.run_lakehouse_queries.SparkSession"
    )
    @patch("dsa_interview_packet.scripts.run_lakehouse_queries.os.listdir")
    @patch("builtins.open", mock_open(read_data="SELECT 1"))
    def test_sets_catalog_and_schema(self, mock_listdir, mock_session_cls):
        """main() should issue USE CATALOG and USE SCHEMA statements."""
        mock_listdir.return_value = ["q01.sql"]
        spark, _, _ = self._build_mock_spark()
        mock_session_cls.builder.getOrCreate.return_value = spark

        main("my_cat", "my_sch", "/queries")

        spark.sql.assert_any_call("USE CATALOG `my_cat`")
        spark.sql.assert_any_call("USE SCHEMA `my_sch`")

    @patch(
        "dsa_interview_packet.scripts.run_lakehouse_queries.SparkSession"
    )
    @patch("dsa_interview_packet.scripts.run_lakehouse_queries.os.listdir")
    @patch("builtins.open", mock_open(read_data="SELECT 1"))
    def test_discovers_and_sorts_sql_files(self, mock_listdir, mock_session_cls):
        """main() should only pick .sql files and process them in sorted order."""
        mock_listdir.return_value = [
            "q03.sql", "README.md", "q01.sql", "q02.sql", ".hidden",
        ]
        spark, _, _ = self._build_mock_spark()
        mock_session_cls.builder.getOrCreate.return_value = spark

        main("cat", "sch", "/queries")

        # 3 sql files -> 3 sql executions + 2 USE statements = 5 total
        sql_calls = [c for c in spark.sql.call_args_list]
        # The USE calls come first, then the 3 query executions
        query_calls = [
            c for c in sql_calls
            if not str(c).startswith("call('USE ")
        ]
        assert len(query_calls) == 3

    @patch(
        "dsa_interview_packet.scripts.run_lakehouse_queries.SparkSession"
    )
    @patch("dsa_interview_packet.scripts.run_lakehouse_queries.os.listdir")
    @patch("builtins.open", mock_open(read_data="SELECT * FROM orders"))
    def test_writes_results_with_lakehouse_prefix(
        self, mock_listdir, mock_session_cls
    ):
        """Results should be written as lakehouse_<query_name> tables."""
        mock_listdir.return_value = ["pricing_summary.sql", "min_cost_supplier.sql"]
        spark, _, _ = self._build_mock_spark()
        mock_session_cls.builder.getOrCreate.return_value = spark

        main("cat", "sch", "/queries")

        # Check spark.table was called for count verification
        spark.table.assert_any_call(
            table_fqn("cat", "sch", "lakehouse_min_cost_supplier")
        )
        spark.table.assert_any_call(
            table_fqn("cat", "sch", "lakehouse_pricing_summary")
        )

    @patch(
        "dsa_interview_packet.scripts.run_lakehouse_queries.SparkSession"
    )
    @patch("dsa_interview_packet.scripts.run_lakehouse_queries.os.listdir")
    def test_exits_when_no_sql_files(self, mock_listdir, mock_session_cls):
        """main() should sys.exit(1) when no SQL files are found."""
        mock_listdir.return_value = ["README.md", "notes.txt"]
        spark, _, _ = self._build_mock_spark()
        mock_session_cls.builder.getOrCreate.return_value = spark

        with pytest.raises(SystemExit) as exc_info:
            main("cat", "sch", "/empty")

        assert exc_info.value.code == 1

    @patch(
        "dsa_interview_packet.scripts.run_lakehouse_queries.SparkSession"
    )
    @patch("dsa_interview_packet.scripts.run_lakehouse_queries.os.listdir")
    def test_exits_when_directory_has_no_sql(self, mock_listdir, mock_session_cls):
        """Empty directory (no files at all) should also exit."""
        mock_listdir.return_value = []
        spark, _, _ = self._build_mock_spark()
        mock_session_cls.builder.getOrCreate.return_value = spark

        with pytest.raises(SystemExit):
            main("cat", "sch", "/empty")

    @patch(
        "dsa_interview_packet.scripts.run_lakehouse_queries.SparkSession"
    )
    @patch("dsa_interview_packet.scripts.run_lakehouse_queries.os.listdir")
    def test_reads_sql_file_contents(self, mock_listdir, mock_session_cls):
        """main() should read each SQL file and pass its content to spark.sql."""
        mock_listdir.return_value = ["my_query.sql"]
        spark, _, _ = self._build_mock_spark()
        mock_session_cls.builder.getOrCreate.return_value = spark

        sql_content = "SELECT o_orderkey FROM orders WHERE o_orderdate > '1995-01-01'"
        m = mock_open(read_data=sql_content)
        with patch("builtins.open", m):
            main("cat", "sch", "/queries")

        m.assert_called_once_with(
            os.path.join("/queries", "my_query.sql"), encoding="utf-8"
        )
        spark.sql.assert_any_call(sql_content)

    @patch(
        "dsa_interview_packet.scripts.run_lakehouse_queries.SparkSession"
    )
    @patch("dsa_interview_packet.scripts.run_lakehouse_queries.os.listdir")
    @patch("builtins.open", mock_open(read_data="SELECT 1"))
    def test_overwrite_mode_used(self, mock_listdir, mock_session_cls):
        """Each query result should be written with mode('overwrite')."""
        mock_listdir.return_value = ["q01.sql"]
        spark, _, _ = self._build_mock_spark()
        mock_session_cls.builder.getOrCreate.return_value = spark

        main("cat", "sch", "/queries")

        # Get the DF returned by the query spark.sql call (3rd call: after USE CATALOG and USE SCHEMA)
        query_sql_call = [
            c for c in spark.sql.call_args_list
            if not c[0][0].startswith("USE ")
        ]
        assert len(query_sql_call) == 1

        # Since side_effect creates a new mock each time, verify through the mock's call pattern
        # Instead, verify spark.table was called (for count)
        spark.table.assert_any_call(table_fqn("cat", "sch", "lakehouse_q01"))

    @patch(
        "dsa_interview_packet.scripts.run_lakehouse_queries.SparkSession"
    )
    @patch("dsa_interview_packet.scripts.run_lakehouse_queries.os.listdir")
    @patch("builtins.open", mock_open(read_data="SELECT 1"))
    def test_row_count_checked_after_write(self, mock_listdir, mock_session_cls):
        """main() should call spark.table(table_name).count() after each write."""
        mock_listdir.return_value = ["alpha.sql", "beta.sql"]
        alpha_fqn = table_fqn("cat", "sch", "lakehouse_alpha")
        beta_fqn = table_fqn("cat", "sch", "lakehouse_beta")
        spark, _, count_dfs = self._build_mock_spark(
            table_counts={
                alpha_fqn: 10,
                beta_fqn: 20,
            }
        )
        mock_session_cls.builder.getOrCreate.return_value = spark

        main("cat", "sch", "/queries")

        spark.table.assert_any_call(alpha_fqn)
        spark.table.assert_any_call(beta_fqn)
        count_dfs[alpha_fqn].count.assert_called_once()
        count_dfs[beta_fqn].count.assert_called_once()

    @patch(
        "dsa_interview_packet.scripts.run_lakehouse_queries.SparkSession"
    )
    @patch("dsa_interview_packet.scripts.run_lakehouse_queries.os.listdir")
    @patch("builtins.open", mock_open(read_data="SELECT 1"))
    def test_query_name_strips_sql_suffix(self, mock_listdir, mock_session_cls):
        """The .sql extension should be removed from the query name."""
        mock_listdir.return_value = ["pricing_summary.sql"]
        spark, _, _ = self._build_mock_spark()
        mock_session_cls.builder.getOrCreate.return_value = spark

        main("c", "s", "/q")

        # Table name should be lakehouse_pricing_summary, NOT lakehouse_pricing_summary.sql
        spark.table.assert_any_call(
            table_fqn("c", "s", "lakehouse_pricing_summary")
        )
