"""Unit tests for supply chain analytics Lakehouse SQL query implementations.

Each test loads a SQL file from src/dsa_interview_packet/queries/lakehouse/,
executes it against fixture data in PySpark, and validates the result
against pre-computed expected output.

These tests follow TDD: they are written BEFORE the SQL implementations
and will fail until the corresponding SQL files are created.

Ordering note
-------------
Several queries have ORDER BY clauses that are not fully deterministic
(ties are possible). For those queries we pass ``order_by`` tiebreaker
columns to ``assert_results_equal`` so the comparison is stable regardless
of which engine produced the expected results (DuckDB) vs the implementation
(Spark SQL).

Queries affected:
  shipping_priority  -- LIMIT 10, ORDER BY revenue DESC, o_orderdate (add l_orderkey)
  local_supplier_revenue -- no LIMIT, ORDER BY revenue DESC (add n_name)
  returned_items -- LIMIT 20, ORDER BY revenue DESC (add c_custkey)
  critical_inventory -- no LIMIT, ORDER BY value DESC (add ps_partkey)
  top_supplier -- result set may vary on max-revenue ties (add s_suppkey)
  large_volume_customers -- LIMIT 100, ORDER BY o_totalprice DESC, o_orderdate (add o_orderkey)
"""

# mypy: ignore-errors

from __future__ import annotations

from pathlib import Path

import pytest

from conftest import assert_results_equal, load_expected

QUERIES_DIR = (
    Path(__file__).parent.parent / "src" / "dsa_interview_packet" / "queries" / "lakehouse"
)


def _load_sql(query_name: str) -> str:
    """Load a SQL file by query name (e.g., 'pricing_summary')."""
    sql_path = QUERIES_DIR / f"{query_name}.sql"
    if not sql_path.exists():
        pytest.skip(f"SQL file not yet implemented: {sql_path}")
    return sql_path.read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _setup_views(data_views):
    """Ensure source data temp views are registered before each test."""


class TestLakehouseQueries:
    """Validate all 22 supply chain analytics Lakehouse SQL query implementations."""

    def test_pricing_summary(self, spark):
        """Pricing Summary Report.

        Summarizes pricing/shipping for lineitems shipped before a cutoff date.
        Groups by return flag and line status with quantity, price, discount,
        and tax aggregations.
        """
        sql = _load_sql("pricing_summary")
        actual = spark.sql(sql)
        expected = load_expected(spark, "pricing_summary")
        assert_results_equal(actual, expected)

    def test_min_cost_supplier(self, spark):
        """Minimum Cost Supplier.

        Finds the cheapest supplier for parts of a given size/type in a region.
        Uses a correlated subquery to find minimum supply cost.
        """
        sql = _load_sql("min_cost_supplier")
        actual = spark.sql(sql)
        expected = load_expected(spark, "min_cost_supplier")
        assert_results_equal(actual, expected)

    def test_shipping_priority(self, spark):
        """Shipping Priority.

        Top 10 unshipped orders with highest revenue for a market segment.
        Joins customer, orders, and lineitem with date filters.

        ORDER BY (revenue DESC, o_orderdate) is not fully deterministic;
        l_orderkey breaks ties.
        """
        sql = _load_sql("shipping_priority")
        actual = spark.sql(sql)
        expected = load_expected(spark, "shipping_priority")
        assert_results_equal(
            actual, expected, order_by=["l_orderkey"],
        )

    def test_order_priority(self, spark):
        """Order Priority Checking.

        Counts orders per priority where at least one lineitem was received
        late (after commit date) within a 3-month window.
        """
        sql = _load_sql("order_priority")
        actual = spark.sql(sql)
        expected = load_expected(spark, "order_priority")
        assert_results_equal(actual, expected)

    def test_local_supplier_revenue(self, spark):
        """Local Supplier Revenue.

        Revenue from local suppliers (customer and supplier in same nation)
        for a given region and year. Six-way join.

        ORDER BY revenue DESC may have ties; n_name breaks them.
        """
        sql = _load_sql("local_supplier_revenue")
        actual = spark.sql(sql)
        expected = load_expected(spark, "local_supplier_revenue")
        assert_results_equal(actual, expected, order_by=["n_name"])

    def test_revenue_forecast(self, spark):
        """Revenue Forecast.

        Single aggregate over lineitem: potential revenue gain from
        eliminating certain discounts in a given year.
        """
        sql = _load_sql("revenue_forecast")
        actual = spark.sql(sql)
        expected = load_expected(spark, "revenue_forecast")
        assert_results_equal(actual, expected)

    def test_bilateral_trade(self, spark):
        """Bilateral Trade.

        Revenue from bilateral trade between FRANCE and GERMANY over
        1995-1996, broken down by supplier/customer nation and year.
        """
        sql = _load_sql("bilateral_trade")
        actual = spark.sql(sql)
        expected = load_expected(spark, "bilateral_trade")
        assert_results_equal(actual, expected)

    def test_market_share(self, spark):
        """Market Share.

        Market share of BRAZIL's suppliers for ECONOMY ANODIZED STEEL
        parts in the AMERICA region over 1995-1996. Eight-way join.
        """
        sql = _load_sql("market_share")
        actual = spark.sql(sql)
        expected = load_expected(spark, "market_share")
        assert_results_equal(actual, expected)

    def test_product_profit(self, spark):
        """Product Profit.

        Profit by nation and year for parts matching a name pattern.
        Revenue minus supply cost, grouped by nation and order year.
        """
        sql = _load_sql("product_profit")
        actual = spark.sql(sql)
        expected = load_expected(spark, "product_profit")
        assert_results_equal(actual, expected)

    def test_returned_items(self, spark):
        """Returned Item Reporting.

        Top 20 customers with the most revenue from returned items
        in a 3-month period.

        ORDER BY revenue DESC is not fully deterministic; c_custkey breaks ties.
        """
        sql = _load_sql("returned_items")
        actual = spark.sql(sql)
        expected = load_expected(spark, "returned_items")
        assert_results_equal(
            actual, expected, order_by=["c_custkey"],
        )

    def test_critical_inventory(self, spark):
        """Critical Inventory.

        Parts representing a significant percentage of total inventory
        value for GERMANY's suppliers. Uses HAVING with subquery threshold.

        ORDER BY value DESC may have ties; ps_partkey breaks them.
        """
        sql = _load_sql("critical_inventory")
        actual = spark.sql(sql)
        expected = load_expected(spark, "critical_inventory")
        assert_results_equal(
            actual, expected, order_by=["ps_partkey"],
        )

    def test_shipping_mode_analysis(self, spark):
        """Shipping Mode Analysis.

        Checks whether cheaper shipping modes (MAIL, SHIP) cause late
        deliveries for high-priority orders.
        """
        sql = _load_sql("shipping_mode_analysis")
        actual = spark.sql(sql)
        expected = load_expected(spark, "shipping_mode_analysis")
        assert_results_equal(actual, expected)

    def test_customer_distribution(self, spark):
        """Customer Distribution.

        Distribution of customers by number of orders, excluding orders
        with specific comment patterns. Uses LEFT OUTER JOIN.
        """
        sql = _load_sql("customer_distribution")
        actual = spark.sql(sql)
        expected = load_expected(spark, "customer_distribution")
        assert_results_equal(actual, expected)

    def test_promotion_effect(self, spark):
        """Promotion Effect.

        Percentage of revenue from promotional parts in a given month.
        CASE WHEN on part type prefix.
        """
        sql = _load_sql("promotion_effect")
        actual = spark.sql(sql)
        expected = load_expected(spark, "promotion_effect")
        assert_results_equal(actual, expected)

    def test_top_supplier(self, spark):
        """Top Supplier.

        Supplier(s) with highest total revenue in a 3-month period.
        Uses a CTE (WITH clause) for the revenue aggregation.

        If multiple suppliers tie for max revenue the result set size
        varies; s_suppkey stabilises ordering.
        """
        sql = _load_sql("top_supplier")
        actual = spark.sql(sql)
        expected = load_expected(spark, "top_supplier")
        assert_results_equal(
            actual, expected, order_by=["s_suppkey"],
        )

    def test_supplier_parts_coverage(self, spark):
        """Supplier Parts Coverage.

        Count of distinct suppliers per brand/type/size combination,
        excluding suppliers with complaints.
        """
        sql = _load_sql("supplier_parts_coverage")
        actual = spark.sql(sql)
        expected = load_expected(spark, "supplier_parts_coverage")
        assert_results_equal(actual, expected)

    def test_small_order_revenue(self, spark):
        """Small Order Revenue.

        Average yearly revenue for small orders of a specific brand/container.
        Uses correlated subquery for quantity threshold.
        """
        sql = _load_sql("small_order_revenue")
        actual = spark.sql(sql)
        expected = load_expected(spark, "small_order_revenue")
        assert_results_equal(actual, expected)

    def test_large_volume_customers(self, spark):
        """Large Volume Customers.

        Customers who placed large-quantity orders (sum > 300), ranked
        by total price.

        ORDER BY (o_totalprice DESC, o_orderdate) is not fully deterministic;
        o_orderkey breaks ties.
        """
        sql = _load_sql("large_volume_customers")
        actual = spark.sql(sql)
        expected = load_expected(spark, "large_volume_customers")
        assert_results_equal(
            actual, expected, order_by=["o_orderkey"],
        )

    def test_discounted_revenue(self, spark):
        """Discounted Revenue.

        Revenue from lineitems matching specific brand/container/quantity/size
        combinations across three disjunctive filter blocks.
        """
        sql = _load_sql("discounted_revenue")
        actual = spark.sql(sql)
        expected = load_expected(spark, "discounted_revenue")
        assert_results_equal(actual, expected)

    def test_excess_inventory(self, spark):
        """Excess Inventory.

        Suppliers in CANADA with excess inventory for 'forest%' parts.
        Three levels of nested subqueries.
        """
        sql = _load_sql("excess_inventory")
        actual = spark.sql(sql)
        expected = load_expected(spark, "excess_inventory")
        assert_results_equal(actual, expected)

    def test_supplier_reliability(self, spark):
        """Supplier Reliability.

        Suppliers in SAUDI ARABIA who were the sole late deliverer on
        multi-supplier orders. Uses EXISTS / NOT EXISTS.
        """
        sql = _load_sql("supplier_reliability")
        actual = spark.sql(sql)
        expected = load_expected(spark, "supplier_reliability")
        assert_results_equal(actual, expected)

    def test_sales_opportunity(self, spark):
        """Sales Opportunity.

        Customers with above-average account balance in specific country
        codes who have never placed an order.
        """
        sql = _load_sql("sales_opportunity")
        actual = spark.sql(sql)
        expected = load_expected(spark, "sales_opportunity")
        assert_results_equal(actual, expected)
