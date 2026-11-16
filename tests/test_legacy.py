"""Unit tests for the legacy pandas supply chain analytics implementation.

Each test dynamically imports a function from
src/dsa_interview_packet/legacy/supply_chain_queries,
calls it with a dict of pandas DataFrames, and validates the pandas DataFrame
result against pre-computed expected output.

These tests mirror test_pipeline.py but target the legacy pandas code
path instead of the Lakeflow Spark implementation.

See test_lakehouse.py module docstring for ordering notes on
shipping_priority/local_supplier_revenue/returned_items/critical_inventory/
top_supplier/large_volume_customers.
"""


# mypy: ignore-errors

from __future__ import annotations

import importlib
from typing import Any

import pytest

from conftest import assert_pandas_results_equal, load_expected_pandas

LEGACY_MODULE = "dsa_interview_packet.legacy.supply_chain_queries"


def _import_legacy_func(func_name: str) -> Any:
    """Import a legacy function by name from the legacy module."""
    try:
        module = importlib.import_module(LEGACY_MODULE)
    except ModuleNotFoundError:
        pytest.skip(f"Legacy module not yet implemented: {LEGACY_MODULE}")
    func = getattr(module, func_name, None)
    if func is None:
        pytest.skip(
            f"Legacy function '{func_name}' not yet implemented in {LEGACY_MODULE}"
        )
    return func


@pytest.fixture(autouse=True)
def _setup_tables(pandas_tables):
    """Ensure dask tables are loaded before each test."""


class TestLegacyQueries:
    """Validate all 22 supply chain analytics legacy pandas implementations.

    Each legacy function should accept a dict of pandas DataFrames and return a
    pandas DataFrame with the query results. The function naming convention is:

        def pricing_summary(tables) -> pd.DataFrame
        def min_cost_supplier(tables) -> pd.DataFrame
        ...
    """

    def test_pricing_summary(self, pandas_tables):
        """Pricing Summary Report."""
        func = _import_legacy_func("pricing_summary")
        actual = func(pandas_tables)
        expected = load_expected_pandas("pricing_summary")
        assert_pandas_results_equal(actual, expected)

    def test_min_cost_supplier(self, pandas_tables):
        """Minimum Cost Supplier."""
        func = _import_legacy_func("min_cost_supplier")
        actual = func(pandas_tables)
        expected = load_expected_pandas("min_cost_supplier")
        assert_pandas_results_equal(actual, expected)

    def test_shipping_priority(self, pandas_tables):
        """Shipping Priority (l_orderkey tiebreaker)."""
        func = _import_legacy_func("shipping_priority")
        actual = func(pandas_tables)
        expected = load_expected_pandas("shipping_priority")
        assert_pandas_results_equal(actual, expected, order_by=["l_orderkey"])

    def test_order_priority(self, pandas_tables):
        """Order Priority Checking."""
        func = _import_legacy_func("order_priority")
        actual = func(pandas_tables)
        expected = load_expected_pandas("order_priority")
        assert_pandas_results_equal(actual, expected)

    def test_local_supplier_revenue(self, pandas_tables):
        """Local Supplier Revenue (n_name tiebreaker)."""
        func = _import_legacy_func("local_supplier_revenue")
        actual = func(pandas_tables)
        expected = load_expected_pandas("local_supplier_revenue")
        assert_pandas_results_equal(actual, expected, order_by=["n_name"])

    def test_revenue_forecast(self, pandas_tables):
        """Revenue Forecast."""
        func = _import_legacy_func("revenue_forecast")
        actual = func(pandas_tables)
        expected = load_expected_pandas("revenue_forecast")
        assert_pandas_results_equal(actual, expected)

    def test_bilateral_trade(self, pandas_tables):
        """Bilateral Trade."""
        func = _import_legacy_func("bilateral_trade")
        actual = func(pandas_tables)
        expected = load_expected_pandas("bilateral_trade")
        assert_pandas_results_equal(actual, expected)

    def test_market_share(self, pandas_tables):
        """Market Share."""
        func = _import_legacy_func("market_share")
        actual = func(pandas_tables)
        expected = load_expected_pandas("market_share")
        assert_pandas_results_equal(actual, expected)

    def test_product_profit(self, pandas_tables):
        """Product Profit."""
        func = _import_legacy_func("product_profit")
        actual = func(pandas_tables)
        expected = load_expected_pandas("product_profit")
        assert_pandas_results_equal(actual, expected)

    def test_returned_items(self, pandas_tables):
        """Returned Item Reporting (c_custkey tiebreaker)."""
        func = _import_legacy_func("returned_items")
        actual = func(pandas_tables)
        expected = load_expected_pandas("returned_items")
        assert_pandas_results_equal(actual, expected, order_by=["c_custkey"])

    def test_critical_inventory(self, pandas_tables):
        """Critical Inventory (ps_partkey tiebreaker)."""
        func = _import_legacy_func("critical_inventory")
        actual = func(pandas_tables)
        expected = load_expected_pandas("critical_inventory")
        assert_pandas_results_equal(actual, expected, order_by=["ps_partkey"])

    def test_shipping_mode_analysis(self, pandas_tables):
        """Shipping Mode Analysis."""
        func = _import_legacy_func("shipping_mode_analysis")
        actual = func(pandas_tables)
        expected = load_expected_pandas("shipping_mode_analysis")
        assert_pandas_results_equal(actual, expected)

    def test_customer_distribution(self, pandas_tables):
        """Customer Distribution."""
        func = _import_legacy_func("customer_distribution")
        actual = func(pandas_tables)
        expected = load_expected_pandas("customer_distribution")
        assert_pandas_results_equal(actual, expected)

    def test_promotion_effect(self, pandas_tables):
        """Promotion Effect."""
        func = _import_legacy_func("promotion_effect")
        actual = func(pandas_tables)
        expected = load_expected_pandas("promotion_effect")
        assert_pandas_results_equal(actual, expected)

    def test_top_supplier(self, pandas_tables):
        """Top Supplier (s_suppkey tiebreaker)."""
        func = _import_legacy_func("top_supplier")
        actual = func(pandas_tables)
        expected = load_expected_pandas("top_supplier")
        assert_pandas_results_equal(actual, expected, order_by=["s_suppkey"])

    def test_supplier_parts_coverage(self, pandas_tables):
        """Supplier Parts Coverage."""
        func = _import_legacy_func("supplier_parts_coverage")
        actual = func(pandas_tables)
        expected = load_expected_pandas("supplier_parts_coverage")
        assert_pandas_results_equal(actual, expected)

    def test_small_order_revenue(self, pandas_tables):
        """Small Order Revenue."""
        func = _import_legacy_func("small_order_revenue")
        actual = func(pandas_tables)
        expected = load_expected_pandas("small_order_revenue")
        assert_pandas_results_equal(actual, expected)

    def test_large_volume_customers(self, pandas_tables):
        """Large Volume Customers (o_orderkey tiebreaker)."""
        func = _import_legacy_func("large_volume_customers")
        actual = func(pandas_tables)
        expected = load_expected_pandas("large_volume_customers")
        assert_pandas_results_equal(actual, expected, order_by=["o_orderkey"])

    def test_discounted_revenue(self, pandas_tables):
        """Discounted Revenue."""
        func = _import_legacy_func("discounted_revenue")
        actual = func(pandas_tables)
        expected = load_expected_pandas("discounted_revenue")
        assert_pandas_results_equal(actual, expected)

    def test_excess_inventory(self, pandas_tables):
        """Excess Inventory."""
        func = _import_legacy_func("excess_inventory")
        actual = func(pandas_tables)
        expected = load_expected_pandas("excess_inventory")
        assert_pandas_results_equal(actual, expected)

    def test_supplier_reliability(self, pandas_tables):
        """Supplier Reliability."""
        func = _import_legacy_func("supplier_reliability")
        actual = func(pandas_tables)
        expected = load_expected_pandas("supplier_reliability")
        assert_pandas_results_equal(actual, expected)

    def test_sales_opportunity(self, pandas_tables):
        """Sales Opportunity."""
        func = _import_legacy_func("sales_opportunity")
        actual = func(pandas_tables)
        expected = load_expected_pandas("sales_opportunity")
        assert_pandas_results_equal(actual, expected)
