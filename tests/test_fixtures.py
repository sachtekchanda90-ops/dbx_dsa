"""Sanity tests for supply chain analytics fixture data and expected results.

Validates that the parquet fixture files are present, loadable, and contain
the expected row counts and columns.
"""


# mypy: ignore-errors

from __future__ import annotations

import pytest

from conftest import DATA_DIR, EXPECTED_DIR, load_expected

# Expected row counts for source tables
EXPECTED_ROW_COUNTS = {
    "region": 5,
    "nation": 25,
    "supplier": 100,
    "part": 2000,
    "partsupp": 8000,
    "customer": 1500,
    "orders": 15000,
    "lineitem": 60175,
}

# Expected columns per source table (lowercase)
EXPECTED_COLUMNS = {
    "region": ["r_regionkey", "r_name", "r_comment"],
    "nation": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    "supplier": [
        "s_suppkey", "s_name", "s_address", "s_nationkey",
        "s_phone", "s_acctbal", "s_comment",
    ],
    "part": [
        "p_partkey", "p_name", "p_mfgr", "p_brand", "p_type",
        "p_size", "p_container", "p_retailprice", "p_comment",
    ],
    "partsupp": [
        "ps_partkey", "ps_suppkey", "ps_availqty",
        "ps_supplycost", "ps_comment",
    ],
    "customer": [
        "c_custkey", "c_name", "c_address", "c_nationkey",
        "c_phone", "c_acctbal", "c_mktsegment", "c_comment",
    ],
    "orders": [
        "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice",
        "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority",
        "o_comment",
    ],
    "lineitem": [
        "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
        "l_quantity", "l_extendedprice", "l_discount", "l_tax",
        "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
        "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment",
    ],
}

# Expected output columns per query (lowercase)
EXPECTED_QUERY_COLUMNS = {
    "pricing_summary": [
        "l_returnflag", "l_linestatus", "sum_qty", "sum_base_price",
        "sum_disc_price", "sum_charge", "avg_qty", "avg_price",
        "avg_disc", "count_order",
    ],
    "min_cost_supplier": [
        "s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr",
        "s_address", "s_phone", "s_comment",
    ],
    "shipping_priority": ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"],
    "order_priority": ["o_orderpriority", "order_count"],
    "local_supplier_revenue": ["n_name", "revenue"],
    "revenue_forecast": ["revenue"],
    "bilateral_trade": ["supp_nation", "cust_nation", "l_year", "revenue"],
    "market_share": ["o_year", "mkt_share"],
    "product_profit": ["nation", "o_year", "sum_profit"],
    "returned_items": [
        "c_custkey", "c_name", "revenue", "c_acctbal", "n_name",
        "c_address", "c_phone", "c_comment",
    ],
    "critical_inventory": ["ps_partkey", "value"],
    "shipping_mode_analysis": ["l_shipmode", "high_line_count", "low_line_count"],
    "customer_distribution": ["c_count", "custdist"],
    "promotion_effect": ["promo_revenue"],
    "top_supplier": ["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"],
    "supplier_parts_coverage": ["p_brand", "p_type", "p_size", "supplier_cnt"],
    "small_order_revenue": ["avg_yearly"],
    "large_volume_customers": [
        "c_name", "c_custkey", "o_orderkey", "o_orderdate",
        "o_totalprice", "sum_qty",
    ],
    "discounted_revenue": ["revenue"],
    "excess_inventory": ["s_name", "s_address"],
    "supplier_reliability": ["s_name", "numwait"],
    "sales_opportunity": ["cntrycode", "numcust", "totacctbal"],
}

# Expected row counts per query result
EXPECTED_QUERY_ROW_COUNTS = {
    "pricing_summary": 4,
    "min_cost_supplier": 4,
    "shipping_priority": 10,
    "order_priority": 5,
    "local_supplier_revenue": 5,
    "revenue_forecast": 1,
    "bilateral_trade": 4,
    "market_share": 2,
    "product_profit": 173,
    "returned_items": 20,
    "critical_inventory": 359,
    "shipping_mode_analysis": 2,
    "customer_distribution": 32,
    "promotion_effect": 1,
    "top_supplier": 1,
    "supplier_parts_coverage": 296,
    "small_order_revenue": 1,
    "large_volume_customers": 2,
    "discounted_revenue": 1,
    "excess_inventory": 1,
    "supplier_reliability": 1,
    "sales_opportunity": 7,
}

ALL_QUERY_NAMES = list(EXPECTED_QUERY_COLUMNS.keys())


@pytest.fixture(autouse=True)
def _setup_views(data_views):
    """Ensure source data temp views are registered."""


class TestFixtureData:
    """Validate the source data fixtures are complete and correct."""

    def test_all_table_parquet_files_exist(self):
        """Every source table has a parquet fixture file."""
        for table in EXPECTED_ROW_COUNTS:
            path = DATA_DIR / table / "data.parquet"
            assert path.exists(), f"Missing fixture: {path}"

    def test_table_row_counts(self, data_views):
        """Each table has the expected row count."""
        for table, expected_count in EXPECTED_ROW_COUNTS.items():
            assert data_views[table] == expected_count, (
                f"{table}: expected {expected_count} rows, got {data_views[table]}"
            )

    def test_table_columns(self, spark):
        """Each table has the expected column names."""
        for table, expected_cols in EXPECTED_COLUMNS.items():
            df = spark.sql(f"SELECT * FROM {table} LIMIT 0")
            actual_cols = [c.lower() for c in df.columns]
            assert actual_cols == expected_cols, (
                f"{table} columns mismatch:\n"
                f"  actual:   {actual_cols}\n"
                f"  expected: {expected_cols}"
            )


class TestExpectedResults:
    """Validate the expected result fixtures are complete and correct."""

    def test_all_expected_parquet_files_exist(self):
        """Every query has an expected result fixture."""
        for query_name in ALL_QUERY_NAMES:
            path = EXPECTED_DIR / query_name / "data.parquet"
            assert path.exists(), f"Missing expected result: {path}"

    @pytest.mark.parametrize("query_name", ALL_QUERY_NAMES)
    def test_expected_result_columns(self, spark, query_name):
        """Each expected result has the correct output columns."""
        expected_cols = EXPECTED_QUERY_COLUMNS[query_name]
        df = load_expected(spark, query_name)
        actual_cols = [c.lower() for c in df.columns]
        assert actual_cols == expected_cols, (
            f"{query_name} columns mismatch:\n"
            f"  actual:   {actual_cols}\n"
            f"  expected: {expected_cols}"
        )

    @pytest.mark.parametrize("query_name", ALL_QUERY_NAMES)
    def test_expected_result_row_counts(self, spark, query_name):
        """Each expected result has the correct row count."""
        expected_count = EXPECTED_QUERY_ROW_COUNTS[query_name]
        df = load_expected(spark, query_name)
        actual_count = df.count()
        assert actual_count == expected_count, (
            f"{query_name}: expected {expected_count} rows, got {actual_count}"
        )

    @pytest.mark.parametrize("query_name", ALL_QUERY_NAMES)
    def test_expected_results_not_empty(self, spark, query_name):
        """No expected result should be completely empty."""
        df = load_expected(spark, query_name)
        assert df.count() > 0, f"{query_name}: expected results are empty"
