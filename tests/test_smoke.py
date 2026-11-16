"""Smoke tests for quick feedback during supply chain analytics implementation.

These tests check basic properties -- schema, row counts, domain invariants,
and ordering -- without doing full row-by-row value comparison against expected
results.  They catch common mistakes early (wrong table name, missing column
alias, broken join, flipped sort direction) while the full correctness tests
in test_lakehouse.py / test_pipeline.py validate exact values.

Each query has one smoke test that verifies:
  1. Output schema (column names and count)
  2. Row count (exact or bounded, depending on LIMIT)
  3. Domain-specific invariants (value ranges, known enum values, ordering)

Smoke tests try Lakehouse SQL first, then the pipeline module, and skip if neither
is implemented yet. Use ``--impl lakehouse`` or ``--impl lakeflow`` to restrict which
implementation is tested.
"""


# mypy: ignore-errors

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

LAKEHOUSE_DIR = (
    Path(__file__).parent.parent / "src" / "dsa_interview_packet" / "queries" / "lakehouse"
)
PIPELINE_MODULE = "dsa_interview_packet.pipelines.supply_chain_queries"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_sql(query_name: str) -> str | None:
    sql_path = LAKEHOUSE_DIR / f"{query_name}.sql"
    if not sql_path.exists():
        return None
    return sql_path.read_text(encoding="utf-8")


def _load_pipeline_func(func_name: str):
    try:
        module = importlib.import_module(PIPELINE_MODULE)
        return getattr(module, func_name, None)
    except ModuleNotFoundError:
        return None


def _get_actual(spark, query_name: str, pipeline_func_name: str, impl_filter: str = "all"):
    """Try Lakehouse SQL and/or pipeline based on --impl filter; skip if not available."""
    if impl_filter in ("all", "lakehouse"):
        sql = _load_sql(query_name)
        if sql is not None:
            return spark.sql(sql)

    if impl_filter in ("all", "lakeflow"):
        func = _load_pipeline_func(pipeline_func_name)
        if func is not None:
            return func(spark)

    if impl_filter == "all":
        pytest.skip(f"Neither SQL file nor pipeline function implemented for {query_name}")
    elif impl_filter == "lakehouse":
        pytest.skip(f"Lakehouse SQL file not yet implemented for {query_name}")
    else:
        pytest.skip(f"Lakeflow pipeline function not yet implemented for {query_name}")


def _cols(df):
    """Return lowercased column names."""
    return [c.lower() for c in df.columns]


def _collect_col(df, col: str):
    """Collect a single column as a Python list."""
    return [r[col] for r in df.collect()]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _setup_views(data_views):
    """Ensure source data temp views are registered before each test."""


# ---------------------------------------------------------------------------
# Smoke tests -- one per query
# ---------------------------------------------------------------------------

class TestSmoke:
    """Fast, property-based smoke checks for all 22 supply chain analytics queries."""

    @pytest.fixture(autouse=True)
    def _inject_impl_filter(self, request):
        """Make --impl filter available to all test methods."""
        self._impl = request.config.getoption("--impl")

    def test_pricing_summary(self, spark):
        """Single-table aggregation over lineitem.

        - 10 columns, exactly 4 rows (one per returnflag/linestatus combo)
        - Known flag combos present: (A,F), (N,F), (N,O), (R,F)
        - All quantity/price sums positive
        - avg_disc between 0 and 1 (discount is a fraction)
        """
        df = _get_actual(spark, "pricing_summary", "pricing_summary", self._impl)
        assert len(df.columns) == 10
        assert df.count() == 4

        rows = df.collect()
        flags = {(r["l_returnflag"].strip(), r["l_linestatus"].strip()) for r in rows}
        assert flags == {("A", "F"), ("N", "F"), ("N", "O"), ("R", "F")}

        for r in rows:
            assert float(r["sum_qty"]) > 0
            assert float(r["sum_base_price"]) > 0
            assert float(r["sum_disc_price"]) > 0
            assert float(r["sum_charge"]) > 0
            assert int(r["count_order"]) > 0
            assert 0 <= float(r["avg_disc"]) <= 1

    def test_min_cost_supplier(self, spark):
        """Correlated subquery -- cheapest supplier in EUROPE for BRASS parts.

        - 8 columns, <=100 rows (LIMIT 100)
        - s_acctbal in descending order (primary sort)
        """
        df = _get_actual(spark, "min_cost_supplier", "min_cost_supplier", self._impl)
        assert len(df.columns) == 8
        assert 0 < df.count() <= 100

        acctbals = [float(r["s_acctbal"]) for r in df.collect()]
        assert acctbals == sorted(acctbals, reverse=True)

    def test_shipping_priority(self, spark):
        """3-way join (customer/orders/lineitem) with LIMIT.

        - 4 columns, <=10 rows
        - Positive revenue, ordered descending
        - o_orderdate before 1995-03-15
        """
        df = _get_actual(spark, "shipping_priority", "shipping_priority", self._impl)
        assert len(df.columns) == 4
        assert 0 < df.count() <= 10

        rows = df.collect()
        revenues = [float(r["revenue"]) for r in rows]
        assert all(v > 0 for v in revenues)
        assert revenues == sorted(revenues, reverse=True)

        from datetime import date  # pylint: disable=import-outside-toplevel

        for r in rows:
            assert r["o_orderdate"] < date(1995, 3, 15)

    def test_order_priority(self, spark):
        """EXISTS subquery -- orders with late lineitems.

        - 2 columns, exactly 5 rows (one per priority level)
        - All 5 priority levels present
        - Positive order counts
        """
        df = _get_actual(spark, "order_priority", "order_priority", self._impl)
        assert _cols(df) == ["o_orderpriority", "order_count"]
        assert df.count() == 5

        rows = df.collect()
        priorities = {r["o_orderpriority"].strip() for r in rows}
        assert priorities == {
            "1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW",
        }
        assert all(int(r["order_count"]) > 0 for r in rows)

    def test_local_supplier_revenue(self, spark):
        """6-way join -- local supplier revenue for ASIA region.

        - 2 columns, exactly 5 rows (one per ASIA nation)
        - Known ASIA nations: CHINA, INDIA, INDONESIA, JAPAN, VIETNAM
        - Positive revenue
        """
        df = _get_actual(spark, "local_supplier_revenue", "local_supplier_revenue", self._impl)
        assert _cols(df) == ["n_name", "revenue"]
        assert df.count() == 5

        rows = df.collect()
        nations = {r["n_name"].strip() for r in rows}
        assert nations == {"CHINA", "INDIA", "INDONESIA", "JAPAN", "VIETNAM"}
        assert all(float(r["revenue"]) > 0 for r in rows)

    def test_revenue_forecast(self, spark):
        """Single scalar -- potential revenue from discount elimination.

        - 1 column, 1 row, positive value
        """
        df = _get_actual(spark, "revenue_forecast", "revenue_forecast", self._impl)
        assert _cols(df) == ["revenue"]
        assert df.count() == 1
        assert float(df.collect()[0]["revenue"]) > 0

    def test_bilateral_trade(self, spark):
        """Bilateral trade between FRANCE and GERMANY, 1995-1996.

        - 4 columns, exactly 4 rows (2 nation-pairs x 2 years)
        - Only FRANCE/GERMANY combos
        - Years 1995 and 1996 only
        - Positive revenue
        """
        df = _get_actual(spark, "bilateral_trade", "bilateral_trade", self._impl)
        assert _cols(df) == ["supp_nation", "cust_nation", "l_year", "revenue"]
        assert df.count() == 4

        rows = df.collect()
        pairs = {(r["supp_nation"].strip(), r["cust_nation"].strip()) for r in rows}
        assert pairs == {("FRANCE", "GERMANY"), ("GERMANY", "FRANCE")}
        years = {int(r["l_year"]) for r in rows}
        assert years == {1995, 1996}
        assert all(float(r["revenue"]) > 0 for r in rows)

    def test_market_share(self, spark):
        """Market share of BRAZIL in AMERICA region, 1995-1996.

        - 2 columns, exactly 2 rows (one per year)
        - Years 1995 and 1996
        - mkt_share between 0 and 1
        """
        df = _get_actual(spark, "market_share", "market_share", self._impl)
        assert _cols(df) == ["o_year", "mkt_share"]
        assert df.count() == 2

        rows = df.collect()
        years = {int(r["o_year"]) for r in rows}
        assert years == {1995, 1996}
        for r in rows:
            share = float(r["mkt_share"])
            assert 0 <= share <= 1, f"mkt_share={share} not in [0,1]"

    def test_product_profit(self, spark):
        """6-way join -- profit by nation/year for 'green' parts.

        - 3 columns, many rows (173 expected)
        - Years within 1992-1998
        - Ordered by nation ASC, o_year DESC
        """
        df = _get_actual(spark, "product_profit", "product_profit", self._impl)
        assert _cols(df) == ["nation", "o_year", "sum_profit"]
        count = df.count()
        assert count > 100

        rows = df.collect()
        years = {int(r["o_year"]) for r in rows}
        assert years.issubset(set(range(1992, 1999)))

        # Verify ordering: nation ASC, then o_year DESC within each nation
        prev_nation, prev_year = "", 9999
        for r in rows:
            nation = r["nation"].strip()
            year = int(r["o_year"])
            if nation == prev_nation:
                assert year <= prev_year, "o_year should be DESC within nation"
            else:
                assert nation >= prev_nation, "nation should be ASC"
            prev_nation, prev_year = nation, year

    def test_returned_items(self, spark):
        """Top 20 customers by returned-item revenue.

        - 8 columns, <=20 rows (LIMIT 20)
        - Positive revenue, descending order
        """
        df = _get_actual(spark, "returned_items", "returned_items", self._impl)
        assert len(df.columns) == 8
        assert 0 < df.count() <= 20

        revenues = [float(r["revenue"]) for r in df.collect()]
        assert all(v > 0 for v in revenues)
        assert revenues == sorted(revenues, reverse=True)

    def test_critical_inventory(self, spark):
        """High-value parts for GERMANY suppliers.

        - 2 columns, many rows (359 expected)
        - All values positive (above threshold)
        - Value in descending order
        """
        df = _get_actual(spark, "critical_inventory", "critical_inventory", self._impl)
        assert _cols(df) == ["ps_partkey", "value"]
        assert df.count() > 200

        values = [float(r["value"]) for r in df.collect()]
        assert all(v > 0 for v in values)
        assert values == sorted(values, reverse=True)

    def test_shipping_mode_analysis(self, spark):
        """Late deliveries by ship mode (MAIL, SHIP).

        - 3 columns, exactly 2 rows
        - Ship modes MAIL and SHIP
        - Non-negative high/low counts, at least one positive per mode
        """
        df = _get_actual(spark, "shipping_mode_analysis", "shipping_mode_analysis", self._impl)
        assert _cols(df) == ["l_shipmode", "high_line_count", "low_line_count"]
        assert df.count() == 2

        rows = df.collect()
        modes = {r["l_shipmode"].strip() for r in rows}
        assert modes == {"MAIL", "SHIP"}
        for r in rows:
            high = int(r["high_line_count"])
            low = int(r["low_line_count"])
            assert high >= 0 and low >= 0
            assert high + low > 0

    def test_customer_distribution(self, spark):
        """LEFT JOIN -- distribution of customers by order count.

        - 2 columns
        - custdist sums to 1500 (total customers)
        - Includes c_count=0 (customers with no qualifying orders)
        - Ordered by custdist DESC, c_count DESC
        """
        df = _get_actual(spark, "customer_distribution", "customer_distribution", self._impl)
        assert _cols(df) == ["c_count", "custdist"]

        rows = df.collect()
        total = sum(int(r["custdist"]) for r in rows)
        assert total == 1500, f"custdist total {total} != 1500"

        c_counts = {int(r["c_count"]) for r in rows}
        assert 0 in c_counts, "Must include c_count=0"

        # Verify ordering: custdist DESC, c_count DESC
        prev_custdist, prev_c_count = float("inf"), float("inf")
        for r in rows:
            cd = int(r["custdist"])
            cc = int(r["c_count"])
            assert (cd, cc) <= (prev_custdist, prev_c_count), (
                "Must be ordered by custdist DESC, c_count DESC"
            )
            prev_custdist, prev_c_count = cd, cc

    def test_promotion_effect(self, spark):
        """Single scalar -- percentage of revenue from promo parts.

        - 1 column, 1 row
        - Value between 0 and 100 (it's a percentage)
        """
        df = _get_actual(spark, "promotion_effect", "promotion_effect", self._impl)
        assert _cols(df) == ["promo_revenue"]
        assert df.count() == 1
        pct = float(df.collect()[0]["promo_revenue"])
        assert 0 < pct < 100, f"promo_revenue={pct} not in (0,100)"

    def test_top_supplier(self, spark):
        """CTE -- supplier(s) with highest revenue in a quarter.

        - 5 columns, >=1 row
        - Positive revenue
        - All returned rows have the same total_revenue (they tie for max)
        """
        df = _get_actual(spark, "top_supplier", "top_supplier", self._impl)
        assert len(df.columns) == 5
        assert df.count() >= 1

        revenues = [float(r["total_revenue"]) for r in df.collect()]
        assert all(v > 0 for v in revenues)
        assert len(set(revenues)) == 1, "All top suppliers must share the same revenue"

    def test_supplier_parts_coverage(self, spark):
        """NOT IN subquery -- supplier count per brand/type/size.

        - 4 columns, many rows (296 expected)
        - supplier_cnt > 0
        - No Brand#45 in results (excluded by query)
        - Ordered by supplier_cnt DESC
        """
        df = _get_actual(spark, "supplier_parts_coverage", "supplier_parts_coverage", self._impl)
        assert _cols(df) == ["p_brand", "p_type", "p_size", "supplier_cnt"]
        assert df.count() > 200

        rows = df.collect()
        for r in rows:
            assert int(r["supplier_cnt"]) > 0
            assert r["p_brand"].strip() != "Brand#45"

        counts = [int(r["supplier_cnt"]) for r in rows]
        assert counts == sorted(counts, reverse=True)

    def test_small_order_revenue(self, spark):
        """Correlated subquery -- avg yearly revenue for small orders.

        - 1 column, 1 row
        - Value is positive or NULL (NULL is valid at small scale factors
          when no parts match Brand#23 / MED BOX with quantity below threshold)
        """
        df = _get_actual(spark, "small_order_revenue", "small_order_revenue", self._impl)
        assert _cols(df) == ["avg_yearly"]
        assert df.count() == 1
        val = df.collect()[0]["avg_yearly"]
        assert val is None or float(val) > 0

    def test_large_volume_customers(self, spark):
        """Large-quantity orders (sum > 300).

        - 6 columns, <=100 rows (LIMIT 100)
        - sum_qty > 300 for every row
        - Ordered by o_totalprice DESC
        """
        df = _get_actual(spark, "large_volume_customers", "large_volume_customers", self._impl)
        assert len(df.columns) == 6
        assert 0 < df.count() <= 100

        rows = df.collect()
        for r in rows:
            assert float(r["sum_qty"]) > 300

        prices = [float(r["o_totalprice"]) for r in rows]
        assert prices == sorted(prices, reverse=True)

    def test_discounted_revenue(self, spark):
        """Three-way OR filter -- discounted revenue for specific combos.

        - 1 column, 1 row, positive value
        """
        df = _get_actual(spark, "discounted_revenue", "discounted_revenue", self._impl)
        assert _cols(df) == ["revenue"]
        assert df.count() == 1
        assert float(df.collect()[0]["revenue"]) > 0

    def test_excess_inventory(self, spark):
        """Nested subqueries -- CANADA suppliers with excess 'forest' parts.

        - 2 columns, >=1 row
        - Ordered by s_name ASC
        """
        df = _get_actual(spark, "excess_inventory", "excess_inventory", self._impl)
        assert _cols(df) == ["s_name", "s_address"]
        assert df.count() >= 1

        names = [r["s_name"].strip() for r in df.collect()]
        assert names == sorted(names)

    def test_supplier_reliability(self, spark):
        """EXISTS + NOT EXISTS -- unreliable SAUDI ARABIA suppliers.

        - 2 columns, <=100 rows (LIMIT 100)
        - numwait >= 1
        - Ordered by numwait DESC, s_name ASC
        """
        df = _get_actual(spark, "supplier_reliability", "supplier_reliability", self._impl)
        assert _cols(df) == ["s_name", "numwait"]
        assert 0 < df.count() <= 100

        rows = df.collect()
        for r in rows:
            assert int(r["numwait"]) >= 1

        # Verify ordering: numwait DESC, s_name ASC
        prev_numwait = float("inf")
        prev_name = ""
        for r in rows:
            nw = int(r["numwait"])
            nm = r["s_name"].strip()
            if nw == prev_numwait:
                assert nm >= prev_name, "s_name should be ASC within same numwait"
            else:
                assert nw <= prev_numwait, "numwait should be DESC"
            prev_numwait, prev_name = nw, nm

    def test_sales_opportunity(self, spark):
        """NOT EXISTS -- high-balance customers with no orders.

        - 3 columns, exactly 7 rows (one per country code)
        - Known country codes: 13, 17, 18, 23, 29, 30, 31
        - numcust > 0, totacctbal > 0
        - Ordered by cntrycode ASC
        """
        df = _get_actual(spark, "sales_opportunity", "sales_opportunity", self._impl)
        assert _cols(df) == ["cntrycode", "numcust", "totacctbal"]
        assert df.count() == 7

        rows = df.collect()
        codes = [r["cntrycode"].strip() for r in rows]
        assert set(codes) == {"13", "17", "18", "23", "29", "30", "31"}
        assert codes == sorted(codes)

        for r in rows:
            assert int(r["numcust"]) > 0
            assert float(r["totacctbal"]) > 0
