"""Smoke tests for the legacy pandas supply chain analytics implementation.

These tests mirror test_smoke.py but target the legacy implementation which uses
pandas DataFrames as input and returns pandas DataFrames.  Each query function
takes a dict of pandas DataFrames (keyed by table name) and returns a
pd.DataFrame with the query results.

Each query has one smoke test that verifies:
  1. Output schema (column names and count)
  2. Row count (exact or bounded, depending on LIMIT)
  3. Domain-specific invariants (value ranges, known enum values, ordering)
"""


# mypy: ignore-errors

from __future__ import annotations

import importlib

import pytest

LEGACY_MODULE = "dsa_interview_packet.legacy.supply_chain_queries"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_legacy_func(func_name: str):
    """Try to import a function from the legacy module; return None if not found."""
    try:
        module = importlib.import_module(LEGACY_MODULE)
        return getattr(module, func_name, None)
    except ModuleNotFoundError:
        return None


def _get_actual(pandas_tables, query_name: str):
    """Import and call the legacy query function; skip if not found."""
    func = _load_legacy_func(query_name)
    if func is None:
        pytest.skip(f"Legacy function not implemented for {query_name}")
    return func(pandas_tables)


def _cols(df):
    """Return lowercased column names."""
    return [c.lower() for c in df.columns.tolist()]


def _col_values(df, col: str):
    """Return a column as a Python list."""
    return df[col].tolist()


# ---------------------------------------------------------------------------
# Smoke tests -- one per query
# ---------------------------------------------------------------------------

class TestLegacySmoke:
    """Fast, property-based smoke checks for all 22 legacy supply chain queries."""

    def test_pricing_summary(self, pandas_tables):
        """Single-table aggregation over lineitem.

        - 10 columns, exactly 4 rows (one per returnflag/linestatus combo)
        - Known flag combos present: (A,F), (N,F), (N,O), (R,F)
        - All quantity/price sums positive
        - avg_disc between 0 and 1 (discount is a fraction)
        """
        df = _get_actual(pandas_tables, "pricing_summary")
        assert len(df.columns) == 10
        assert len(df) == 4

        flags = {
            (r.l_returnflag.strip(), r.l_linestatus.strip())
            for r in df.itertuples()
        }
        assert flags == {("A", "F"), ("N", "F"), ("N", "O"), ("R", "F")}

        for r in df.itertuples():
            assert float(r.sum_qty) > 0
            assert float(r.sum_base_price) > 0
            assert float(r.sum_disc_price) > 0
            assert float(r.sum_charge) > 0
            assert int(r.count_order) > 0
            assert 0 <= float(r.avg_disc) <= 1

    def test_min_cost_supplier(self, pandas_tables):
        """Correlated subquery -- cheapest supplier in EUROPE for BRASS parts.

        - 8 columns, <=100 rows (LIMIT 100)
        - s_acctbal in descending order (primary sort)
        """
        df = _get_actual(pandas_tables, "min_cost_supplier")
        assert len(df.columns) == 8
        assert 0 < len(df) <= 100

        acctbals = [float(v) for v in df["s_acctbal"].values]
        assert acctbals == sorted(acctbals, reverse=True)

    def test_shipping_priority(self, pandas_tables):
        """3-way join (customer/orders/lineitem) with LIMIT.

        - 4 columns, <=10 rows
        - Positive revenue, ordered descending
        - o_orderdate before 1995-03-15
        """
        df = _get_actual(pandas_tables, "shipping_priority")
        assert len(df.columns) == 4
        assert 0 < len(df) <= 10

        revenues = [float(v) for v in df["revenue"].values]
        assert all(v > 0 for v in revenues)
        assert revenues == sorted(revenues, reverse=True)

        import pandas as pd  # pylint: disable=import-outside-toplevel

        cutoff = pd.Timestamp("1995-03-15")
        for val in df["o_orderdate"].values:
            assert pd.Timestamp(val) < cutoff

    def test_order_priority(self, pandas_tables):
        """EXISTS subquery -- orders with late lineitems.

        - 2 columns, exactly 5 rows (one per priority level)
        - All 5 priority levels present
        - Positive order counts
        """
        df = _get_actual(pandas_tables, "order_priority")
        assert _cols(df) == ["o_orderpriority", "order_count"]
        assert len(df) == 5

        priorities = {str(v).strip() for v in df["o_orderpriority"].values}
        assert priorities == {
            "1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW",
        }
        assert all(int(v) > 0 for v in df["order_count"].values)

    def test_local_supplier_revenue(self, pandas_tables):
        """6-way join -- local supplier revenue for ASIA region.

        - 2 columns, exactly 5 rows (one per ASIA nation)
        - Known ASIA nations: CHINA, INDIA, INDONESIA, JAPAN, VIETNAM
        - Positive revenue
        """
        df = _get_actual(pandas_tables, "local_supplier_revenue")
        assert _cols(df) == ["n_name", "revenue"]
        assert len(df) == 5

        nations = {str(v).strip() for v in df["n_name"].values}
        assert nations == {"CHINA", "INDIA", "INDONESIA", "JAPAN", "VIETNAM"}
        assert all(float(v) > 0 for v in df["revenue"].values)

    def test_revenue_forecast(self, pandas_tables):
        """Single scalar -- potential revenue from discount elimination.

        - 1 column, 1 row, positive value
        """
        df = _get_actual(pandas_tables, "revenue_forecast")
        assert _cols(df) == ["revenue"]
        assert len(df) == 1
        assert float(df["revenue"].iloc[0]) > 0

    def test_bilateral_trade(self, pandas_tables):
        """Bilateral trade between FRANCE and GERMANY, 1995-1996.

        - 4 columns, exactly 4 rows (2 nation-pairs x 2 years)
        - Only FRANCE/GERMANY combos
        - Years 1995 and 1996 only
        - Positive revenue
        """
        df = _get_actual(pandas_tables, "bilateral_trade")
        assert _cols(df) == ["supp_nation", "cust_nation", "l_year", "revenue"]
        assert len(df) == 4

        pairs = {
            (str(r.supp_nation).strip(), str(r.cust_nation).strip())
            for r in df.itertuples()
        }
        assert pairs == {("FRANCE", "GERMANY"), ("GERMANY", "FRANCE")}
        years = {int(r.l_year) for r in df.itertuples()}
        assert years == {1995, 1996}
        assert all(float(v) > 0 for v in df["revenue"].values)

    def test_market_share(self, pandas_tables):
        """Market share of BRAZIL in AMERICA region, 1995-1996.

        - 2 columns, exactly 2 rows (one per year)
        - Years 1995 and 1996
        - mkt_share between 0 and 1
        """
        df = _get_actual(pandas_tables, "market_share")
        assert _cols(df) == ["o_year", "mkt_share"]
        assert len(df) == 2

        years = {int(v) for v in df["o_year"].values}
        assert years == {1995, 1996}
        for val in df["mkt_share"].values:
            share = float(val)
            assert 0 <= share <= 1, f"mkt_share={share} not in [0,1]"

    def test_product_profit(self, pandas_tables):
        """6-way join -- profit by nation/year for 'green' parts.

        - 3 columns, many rows (173 expected)
        - Years within 1992-1998
        - Ordered by nation ASC, o_year DESC
        """
        df = _get_actual(pandas_tables, "product_profit")
        assert _cols(df) == ["nation", "o_year", "sum_profit"]
        assert len(df) > 100

        years = {int(v) for v in df["o_year"].values}
        assert years.issubset(set(range(1992, 1999)))

        # Verify ordering: nation ASC, then o_year DESC within each nation
        prev_nation, prev_year = "", 9999
        for r in df.itertuples():
            nation = str(r.nation).strip()
            year = int(r.o_year)
            if nation == prev_nation:
                assert year <= prev_year, "o_year should be DESC within nation"
            else:
                assert nation >= prev_nation, "nation should be ASC"
            prev_nation, prev_year = nation, year

    def test_returned_items(self, pandas_tables):
        """Top 20 customers by returned-item revenue.

        - 8 columns, <=20 rows (LIMIT 20)
        - Positive revenue, descending order
        """
        df = _get_actual(pandas_tables, "returned_items")
        assert len(df.columns) == 8
        assert 0 < len(df) <= 20

        revenues = [float(v) for v in df["revenue"].values]
        assert all(v > 0 for v in revenues)
        assert revenues == sorted(revenues, reverse=True)

    def test_critical_inventory(self, pandas_tables):
        """High-value parts for GERMANY suppliers.

        - 2 columns, many rows (359 expected)
        - All values positive (above threshold)
        - Value in descending order
        """
        df = _get_actual(pandas_tables, "critical_inventory")
        assert _cols(df) == ["ps_partkey", "value"]
        assert len(df) > 200

        values = [float(v) for v in df["value"].values]
        assert all(v > 0 for v in values)
        assert values == sorted(values, reverse=True)

    def test_shipping_mode_analysis(self, pandas_tables):
        """Late deliveries by ship mode (MAIL, SHIP).

        - 3 columns, exactly 2 rows
        - Ship modes MAIL and SHIP
        - Non-negative high/low counts, at least one positive per mode
        """
        df = _get_actual(pandas_tables, "shipping_mode_analysis")
        assert _cols(df) == ["l_shipmode", "high_line_count", "low_line_count"]
        assert len(df) == 2

        modes = {str(v).strip() for v in df["l_shipmode"].values}
        assert modes == {"MAIL", "SHIP"}
        for r in df.itertuples():
            high = int(r.high_line_count)
            low = int(r.low_line_count)
            assert high >= 0 and low >= 0
            assert high + low > 0

    def test_customer_distribution(self, pandas_tables):
        """LEFT JOIN -- distribution of customers by order count.

        - 2 columns
        - custdist sums to 1500 (total customers)
        - Includes c_count=0 (customers with no qualifying orders)
        - Ordered by custdist DESC, c_count DESC
        """
        df = _get_actual(pandas_tables, "customer_distribution")
        assert _cols(df) == ["c_count", "custdist"]

        total = int(df["custdist"].sum())
        assert total == 1500, f"custdist total {total} != 1500"

        c_counts = set(int(v) for v in df["c_count"].values)
        assert 0 in c_counts, "Must include c_count=0"

        # Verify ordering: custdist DESC, c_count DESC
        prev_custdist, prev_c_count = float("inf"), float("inf")
        for r in df.itertuples():
            cd = int(r.custdist)
            cc = int(r.c_count)
            assert (cd, cc) <= (prev_custdist, prev_c_count), (
                "Must be ordered by custdist DESC, c_count DESC"
            )
            prev_custdist, prev_c_count = cd, cc

    def test_promotion_effect(self, pandas_tables):
        """Single scalar -- percentage of revenue from promo parts.

        - 1 column, 1 row
        - Value between 0 and 100 (it's a percentage)
        """
        df = _get_actual(pandas_tables, "promotion_effect")
        assert _cols(df) == ["promo_revenue"]
        assert len(df) == 1
        pct = float(df["promo_revenue"].iloc[0])
        assert 0 < pct < 100, f"promo_revenue={pct} not in (0,100)"

    def test_top_supplier(self, pandas_tables):
        """CTE -- supplier(s) with highest revenue in a quarter.

        - 5 columns, >=1 row
        - Positive revenue
        - All returned rows have the same total_revenue (they tie for max)
        """
        df = _get_actual(pandas_tables, "top_supplier")
        assert len(df.columns) == 5
        assert len(df) >= 1

        revenues = [float(v) for v in df["total_revenue"].values]
        assert all(v > 0 for v in revenues)
        assert len(set(revenues)) == 1, "All top suppliers must share the same revenue"

    def test_supplier_parts_coverage(self, pandas_tables):
        """NOT IN subquery -- supplier count per brand/type/size.

        - 4 columns, many rows (296 expected)
        - supplier_cnt > 0
        - No Brand#45 in results (excluded by query)
        - Ordered by supplier_cnt DESC
        """
        df = _get_actual(pandas_tables, "supplier_parts_coverage")
        assert _cols(df) == ["p_brand", "p_type", "p_size", "supplier_cnt"]
        assert len(df) > 200

        for r in df.itertuples():
            assert int(r.supplier_cnt) > 0
            assert str(r.p_brand).strip() != "Brand#45"

        counts = [int(v) for v in df["supplier_cnt"].values]
        assert counts == sorted(counts, reverse=True)

    def test_small_order_revenue(self, pandas_tables):
        """Correlated subquery -- avg yearly revenue for small orders.

        - 1 column, 1 row
        - Value is positive or None/NaN (valid at small scale factors
          when no parts match Brand#23 / MED BOX with quantity below threshold)
        """
        df = _get_actual(pandas_tables, "small_order_revenue")
        assert _cols(df) == ["avg_yearly"]
        assert len(df) == 1
        val = df["avg_yearly"].iloc[0]
        import math  # pylint: disable=import-outside-toplevel

        is_missing = val is None or (isinstance(val, float) and math.isnan(val))
        assert is_missing or float(val) > 0

    def test_large_volume_customers(self, pandas_tables):
        """Large-quantity orders (sum > 300).

        - 6 columns, <=100 rows (LIMIT 100)
        - sum_qty > 300 for every row
        - Ordered by o_totalprice DESC
        """
        df = _get_actual(pandas_tables, "large_volume_customers")
        assert len(df.columns) == 6
        assert 0 < len(df) <= 100

        for val in df["sum_qty"].values:
            assert float(val) > 300

        prices = [float(v) for v in df["o_totalprice"].values]
        assert prices == sorted(prices, reverse=True)

    def test_discounted_revenue(self, pandas_tables):
        """Three-way OR filter -- discounted revenue for specific combos.

        - 1 column, 1 row, positive value
        """
        df = _get_actual(pandas_tables, "discounted_revenue")
        assert _cols(df) == ["revenue"]
        assert len(df) == 1
        assert float(df["revenue"].iloc[0]) > 0

    def test_excess_inventory(self, pandas_tables):
        """Nested subqueries -- CANADA suppliers with excess 'forest' parts.

        - 2 columns, >=1 row
        - Ordered by s_name ASC
        """
        df = _get_actual(pandas_tables, "excess_inventory")
        assert _cols(df) == ["s_name", "s_address"]
        assert len(df) >= 1

        names = [str(v).strip() for v in df["s_name"].values]
        assert names == sorted(names)

    def test_supplier_reliability(self, pandas_tables):
        """EXISTS + NOT EXISTS -- unreliable SAUDI ARABIA suppliers.

        - 2 columns, <=100 rows (LIMIT 100)
        - numwait >= 1
        - Ordered by numwait DESC, s_name ASC
        """
        df = _get_actual(pandas_tables, "supplier_reliability")
        assert _cols(df) == ["s_name", "numwait"]
        assert 0 < len(df) <= 100

        for val in df["numwait"].values:
            assert int(val) >= 1

        # Verify ordering: numwait DESC, s_name ASC
        prev_numwait = float("inf")
        prev_name = ""
        for r in df.itertuples():
            nw = int(r.numwait)
            nm = str(r.s_name).strip()
            if nw == prev_numwait:
                assert nm >= prev_name, "s_name should be ASC within same numwait"
            else:
                assert nw <= prev_numwait, "numwait should be DESC"
            prev_numwait, prev_name = nw, nm

    def test_sales_opportunity(self, pandas_tables):
        """NOT EXISTS -- high-balance customers with no orders.

        - 3 columns, exactly 7 rows (one per country code)
        - Known country codes: 13, 17, 18, 23, 29, 30, 31
        - numcust > 0, totacctbal > 0
        - Ordered by cntrycode ASC
        """
        df = _get_actual(pandas_tables, "sales_opportunity")
        assert _cols(df) == ["cntrycode", "numcust", "totacctbal"]
        assert len(df) == 7

        codes = [str(v).strip() for v in df["cntrycode"].values]
        assert set(codes) == {"13", "17", "18", "23", "29", "30", "31"}
        assert codes == sorted(codes)

        for r in df.itertuples():
            assert int(r.numcust) > 0
            assert float(r.totacctbal) > 0
