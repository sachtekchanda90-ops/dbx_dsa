"""Unit tests for supply chain analytics Lakeflow (Spark Declarative Pipeline) implementations.

Each test imports a pipeline function from
src/dsa_interview_packet/pipelines/supply_chain_queries,
calls it with a SparkSession that has fixture data registered as
temp views, and validates the result against pre-computed expected output.

Expected module structure:

    src/dsa_interview_packet/pipelines/
    ├── __init__.py
    ├── supply_chain_queries.py   # Pure query logic (tested here)
    └── supply_chain_pipeline.py  # Lakeflow entry point with @dlt.table decorators (SDP)

The supply_chain_queries module should contain functions that accept a
SparkSession and return a DataFrame. These functions must NOT import
the SDP/DLT SDK -- they contain only the query logic. The @dlt.table decorators
live in supply_chain_pipeline.py, which wraps these functions for
production use.

These tests follow TDD: they are written BEFORE the pipeline implementations
and will fail until the corresponding pipeline modules are created.

See test_lakehouse.py module docstring for ordering notes on
shipping_priority/local_supplier_revenue/returned_items/critical_inventory/
top_supplier/large_volume_customers.
"""


# mypy: ignore-errors

from __future__ import annotations

import importlib
from typing import Any

import pytest

from conftest import assert_results_equal, load_expected

PIPELINE_MODULE = "dsa_interview_packet.pipelines.supply_chain_queries"


def _import_pipeline_func(func_name: str) -> Any:
    """Import a pipeline function by name from the pipeline module."""
    try:
        module = importlib.import_module(PIPELINE_MODULE)
    except ModuleNotFoundError:
        pytest.skip(f"Pipeline module not yet implemented: {PIPELINE_MODULE}")
    func = getattr(module, func_name, None)
    if func is None:
        pytest.skip(
            f"Pipeline function '{func_name}' not yet implemented in {PIPELINE_MODULE}"
        )
    return func


@pytest.fixture(autouse=True)
def _setup_views(data_views):
    """Ensure source data temp views are registered before each test."""


class TestPipelineQueries:
    """Validate all 22 supply chain analytics Lakeflow implementations.

    Each pipeline function should accept a SparkSession and return a DataFrame
    with the query results. The function naming convention is:

        def pricing_summary(spark) -> DataFrame
        def min_cost_supplier(spark) -> DataFrame
        ...

    In the actual Lakeflow pipeline, these would be decorated with @dlt.table.
    For testing, we call them directly.
    """

    def test_pricing_summary(self, spark):
        """Pricing Summary Report."""
        func = _import_pipeline_func("pricing_summary")
        actual = func(spark)
        expected = load_expected(spark, "pricing_summary")
        assert_results_equal(actual, expected)

    def test_min_cost_supplier(self, spark):
        """Minimum Cost Supplier."""
        func = _import_pipeline_func("min_cost_supplier")
        actual = func(spark)
        expected = load_expected(spark, "min_cost_supplier")
        assert_results_equal(actual, expected)

    def test_shipping_priority(self, spark):
        """Shipping Priority (l_orderkey tiebreaker)."""
        func = _import_pipeline_func("shipping_priority")
        actual = func(spark)
        expected = load_expected(spark, "shipping_priority")
        assert_results_equal(actual, expected, order_by=["l_orderkey"])

    def test_order_priority(self, spark):
        """Order Priority Checking."""
        func = _import_pipeline_func("order_priority")
        actual = func(spark)
        expected = load_expected(spark, "order_priority")
        assert_results_equal(actual, expected)

    def test_local_supplier_revenue(self, spark):
        """Local Supplier Revenue (n_name tiebreaker)."""
        func = _import_pipeline_func("local_supplier_revenue")
        actual = func(spark)
        expected = load_expected(spark, "local_supplier_revenue")
        assert_results_equal(actual, expected, order_by=["n_name"])

    def test_revenue_forecast(self, spark):
        """Revenue Forecast."""
        func = _import_pipeline_func("revenue_forecast")
        actual = func(spark)
        expected = load_expected(spark, "revenue_forecast")
        assert_results_equal(actual, expected)

    def test_bilateral_trade(self, spark):
        """Bilateral Trade."""
        func = _import_pipeline_func("bilateral_trade")
        actual = func(spark)
        expected = load_expected(spark, "bilateral_trade")
        assert_results_equal(actual, expected)

    def test_market_share(self, spark):
        """Market Share."""
        func = _import_pipeline_func("market_share")
        actual = func(spark)
        expected = load_expected(spark, "market_share")
        assert_results_equal(actual, expected)

    def test_product_profit(self, spark):
        """Product Profit."""
        func = _import_pipeline_func("product_profit")
        actual = func(spark)
        expected = load_expected(spark, "product_profit")
        assert_results_equal(actual, expected)

    def test_returned_items(self, spark):
        """Returned Item Reporting (c_custkey tiebreaker)."""
        func = _import_pipeline_func("returned_items")
        actual = func(spark)
        expected = load_expected(spark, "returned_items")
        assert_results_equal(actual, expected, order_by=["c_custkey"])

    def test_critical_inventory(self, spark):
        """Critical Inventory (ps_partkey tiebreaker)."""
        func = _import_pipeline_func("critical_inventory")
        actual = func(spark)
        expected = load_expected(spark, "critical_inventory")
        assert_results_equal(actual, expected, order_by=["ps_partkey"])

    def test_shipping_mode_analysis(self, spark):
        """Shipping Mode Analysis."""
        func = _import_pipeline_func("shipping_mode_analysis")
        actual = func(spark)
        expected = load_expected(spark, "shipping_mode_analysis")
        assert_results_equal(actual, expected)

    def test_customer_distribution(self, spark):
        """Customer Distribution."""
        func = _import_pipeline_func("customer_distribution")
        actual = func(spark)
        expected = load_expected(spark, "customer_distribution")
        assert_results_equal(actual, expected)

    def test_promotion_effect(self, spark):
        """Promotion Effect."""
        func = _import_pipeline_func("promotion_effect")
        actual = func(spark)
        expected = load_expected(spark, "promotion_effect")
        assert_results_equal(actual, expected)

    def test_top_supplier(self, spark):
        """Top Supplier (s_suppkey tiebreaker)."""
        func = _import_pipeline_func("top_supplier")
        actual = func(spark)
        expected = load_expected(spark, "top_supplier")
        assert_results_equal(actual, expected, order_by=["s_suppkey"])

    def test_supplier_parts_coverage(self, spark):
        """Supplier Parts Coverage."""
        func = _import_pipeline_func("supplier_parts_coverage")
        actual = func(spark)
        expected = load_expected(spark, "supplier_parts_coverage")
        assert_results_equal(actual, expected)

    def test_small_order_revenue(self, spark):
        """Small Order Revenue."""
        func = _import_pipeline_func("small_order_revenue")
        actual = func(spark)
        expected = load_expected(spark, "small_order_revenue")
        assert_results_equal(actual, expected)

    def test_large_volume_customers(self, spark):
        """Large Volume Customers (o_orderkey tiebreaker)."""
        func = _import_pipeline_func("large_volume_customers")
        actual = func(spark)
        expected = load_expected(spark, "large_volume_customers")
        assert_results_equal(actual, expected, order_by=["o_orderkey"])

    def test_discounted_revenue(self, spark):
        """Discounted Revenue."""
        func = _import_pipeline_func("discounted_revenue")
        actual = func(spark)
        expected = load_expected(spark, "discounted_revenue")
        assert_results_equal(actual, expected)

    def test_excess_inventory(self, spark):
        """Excess Inventory."""
        func = _import_pipeline_func("excess_inventory")
        actual = func(spark)
        expected = load_expected(spark, "excess_inventory")
        assert_results_equal(actual, expected)

    def test_supplier_reliability(self, spark):
        """Supplier Reliability."""
        func = _import_pipeline_func("supplier_reliability")
        actual = func(spark)
        expected = load_expected(spark, "supplier_reliability")
        assert_results_equal(actual, expected)

    def test_sales_opportunity(self, spark):
        """Sales Opportunity."""
        func = _import_pipeline_func("sales_opportunity")
        actual = func(spark)
        expected = load_expected(spark, "sales_opportunity")
        assert_results_equal(actual, expected)
