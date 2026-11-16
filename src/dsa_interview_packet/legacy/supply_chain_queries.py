"""Legacy supply chain analytics queries using pandas.

This module provides the same 22 analytical queries as the Lakehouse SQL and Lakeflow
implementations, written using the pandas DataFrame API for migration exercises.

Each function accepts a dict of pandas DataFrames (keyed by table name)
and returns a pandas DataFrame with the query result.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

Tables = dict[str, pd.DataFrame]


def _date(s: str) -> date:
    """Parse a YYYY-MM-DD string into a datetime.date for parquet comparison."""
    return date.fromisoformat(s)


def pricing_summary(tables: Tables) -> pd.DataFrame:
    """Summarize pricing metrics by return flag and line status."""
    li = tables["lineitem"].copy()
    li = li[li["l_shipdate"] <= _date("1998-09-02")]
    li["disc_price"] = li["l_extendedprice"] * (1 - li["l_discount"])
    li["charge"] = li["disc_price"] * (1 + li["l_tax"])

    result = (
        li.groupby(["l_returnflag", "l_linestatus"])
        .agg(
            sum_qty=("l_quantity", "sum"),
            sum_base_price=("l_extendedprice", "sum"),
            sum_disc_price=("disc_price", "sum"),
            sum_charge=("charge", "sum"),
            avg_qty=("l_quantity", "mean"),
            avg_price=("l_extendedprice", "mean"),
            avg_disc=("l_discount", "mean"),
            count_order=("l_quantity", "count"),
        )
        .reset_index()
        .sort_values(["l_returnflag", "l_linestatus"])
        .reset_index(drop=True)
    )
    return result


def min_cost_supplier(tables: Tables) -> pd.DataFrame:
    """Find the lowest-cost supplier for brass parts in Europe."""
    part = tables["part"]
    partsupp = tables["partsupp"]
    supplier = tables["supplier"]
    nation = tables["nation"]
    region = tables["region"]

    merged = part.merge(partsupp, left_on="p_partkey", right_on="ps_partkey")
    merged = merged.merge(supplier, left_on="ps_suppkey", right_on="s_suppkey")
    merged = merged.merge(nation, left_on="s_nationkey", right_on="n_nationkey")
    merged = merged.merge(region, left_on="n_regionkey", right_on="r_regionkey")

    merged = merged[
        (merged["p_size"] == 15)
        & (merged["p_type"].str.strip().str.endswith("BRASS"))
        & (merged["r_name"].str.strip() == "EUROPE")
    ]

    min_cost = (
        merged.groupby("ps_partkey")
        .agg(min_cost=("ps_supplycost", "min"))
        .reset_index()
    )

    merged = merged.merge(min_cost, on="ps_partkey")
    merged = merged[merged["ps_supplycost"] == merged["min_cost"]]

    result = (
        merged[["s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr",
                "s_address", "s_phone", "s_comment"]]
        .sort_values(
            ["s_acctbal", "n_name", "s_name", "p_partkey"],
            ascending=[False, True, True, True],
        )
        .head(100)
        .reset_index(drop=True)
    )
    return result


def shipping_priority(tables: Tables) -> pd.DataFrame:
    """Rank unshipped orders by revenue for the BUILDING market segment."""
    customer = tables["customer"]
    orders = tables["orders"]
    lineitem = tables["lineitem"]

    merged = customer.merge(orders, left_on="c_custkey", right_on="o_custkey")
    merged = merged.merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")

    merged = merged[
        (merged["c_mktsegment"].str.strip() == "BUILDING")
        & (merged["o_orderdate"] < _date("1995-03-15"))
        & (merged["l_shipdate"] > _date("1995-03-15"))
    ]
    merged = merged.copy()
    merged["revenue"] = merged["l_extendedprice"] * (1 - merged["l_discount"])

    result = (
        merged.groupby(["l_orderkey", "o_orderdate", "o_shippriority"])
        .agg(revenue=("revenue", "sum"))
        .reset_index()
        .sort_values(["revenue", "o_orderdate"], ascending=[False, True])
        .head(10)
        .reset_index(drop=True)
    )[["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]]
    return result


def order_priority(tables: Tables) -> pd.DataFrame:
    """Count orders by priority level that had late-delivery line items."""
    orders = tables["orders"]
    lineitem = tables["lineitem"]

    filtered_orders = orders[
        (orders["o_orderdate"] >= _date("1993-07-01"))
        & (orders["o_orderdate"] < _date("1993-10-01"))
    ]

    late_lines = lineitem[lineitem["l_commitdate"] < lineitem["l_receiptdate"]]
    late_keys = late_lines["l_orderkey"].unique()
    filtered_orders = filtered_orders[filtered_orders["o_orderkey"].isin(late_keys)]

    result = (
        filtered_orders.groupby("o_orderpriority")
        .agg(order_count=("o_orderkey", "count"))
        .reset_index()
        .sort_values("o_orderpriority")
        .reset_index(drop=True)
    )
    return result


def local_supplier_revenue(tables: Tables) -> pd.DataFrame:
    """Calculate revenue from local suppliers by nation within Asia."""
    customer = tables["customer"]
    orders = tables["orders"]
    lineitem = tables["lineitem"]
    supplier = tables["supplier"]
    nation = tables["nation"]
    region = tables["region"]

    merged = customer.merge(orders, left_on="c_custkey", right_on="o_custkey")
    merged = merged.merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
    merged = merged.merge(
        supplier,
        left_on=["l_suppkey", "c_nationkey"],
        right_on=["s_suppkey", "s_nationkey"],
    )
    merged = merged.merge(nation, left_on="s_nationkey", right_on="n_nationkey")
    merged = merged.merge(region, left_on="n_regionkey", right_on="r_regionkey")

    merged = merged[
        (merged["r_name"].str.strip() == "ASIA")
        & (merged["o_orderdate"] >= _date("1994-01-01"))
        & (merged["o_orderdate"] < _date("1995-01-01"))
    ]
    merged = merged.copy()
    merged["revenue"] = merged["l_extendedprice"] * (1 - merged["l_discount"])

    result = (
        merged.groupby("n_name")
        .agg(revenue=("revenue", "sum"))
        .reset_index()
        .sort_values("revenue", ascending=False)
        .reset_index(drop=True)
    )
    return result


def revenue_forecast(tables: Tables) -> pd.DataFrame:
    """Estimate revenue impact of eliminating certain discounts."""
    li = tables["lineitem"]
    li = li[
        (li["l_shipdate"] >= _date("1994-01-01"))
        & (li["l_shipdate"] < _date("1995-01-01"))
        & (li["l_discount"] >= 0.05)
        & (li["l_discount"] <= 0.07)
        & (li["l_quantity"] < 24)
    ]
    revenue = float((li["l_extendedprice"] * li["l_discount"]).sum())
    return pd.DataFrame({"revenue": [revenue]})


def bilateral_trade(tables: Tables) -> pd.DataFrame:
    """Analyze shipping volume between France and Germany over time."""
    supplier = tables["supplier"]
    lineitem = tables["lineitem"]
    orders = tables["orders"]
    customer = tables["customer"]
    nation = tables["nation"]

    n1 = nation.rename(columns={"n_name": "supp_nation", "n_nationkey": "n1_nationkey"})
    n2 = nation.rename(columns={"n_name": "cust_nation", "n_nationkey": "n2_nationkey"})

    merged = supplier.merge(lineitem, left_on="s_suppkey", right_on="l_suppkey")
    merged = merged.merge(orders, left_on="l_orderkey", right_on="o_orderkey")
    merged = merged.merge(customer, left_on="o_custkey", right_on="c_custkey")
    merged = merged.merge(n1, left_on="s_nationkey", right_on="n1_nationkey")
    merged = merged.merge(n2, left_on="c_nationkey", right_on="n2_nationkey")

    sn = merged["supp_nation"].str.strip()
    cn = merged["cust_nation"].str.strip()
    merged = merged[
        ((sn == "FRANCE") & (cn == "GERMANY"))
        | ((sn == "GERMANY") & (cn == "FRANCE"))
    ]
    merged = merged[
        (merged["l_shipdate"] >= _date("1995-01-01"))
        & (merged["l_shipdate"] <= _date("1996-12-31"))
    ]
    merged = merged.copy()
    merged["l_year"] = pd.to_datetime(merged["l_shipdate"]).dt.year
    merged["volume"] = merged["l_extendedprice"] * (1 - merged["l_discount"])

    result = (
        merged.groupby(["supp_nation", "cust_nation", "l_year"])
        .agg(revenue=("volume", "sum"))
        .reset_index()
        .sort_values(["supp_nation", "cust_nation", "l_year"])
        .reset_index(drop=True)
    )
    return result


def market_share(tables: Tables) -> pd.DataFrame:
    """Compute Brazil's market share for a product type within the Americas."""
    part = tables["part"]
    supplier = tables["supplier"]
    lineitem = tables["lineitem"]
    orders = tables["orders"]
    customer = tables["customer"]
    nation = tables["nation"]
    region = tables["region"]

    n1 = nation.rename(columns={"n_name": "cust_nation", "n_nationkey": "n1_nationkey"})
    n2 = nation.rename(columns={"n_name": "supp_nation", "n_nationkey": "n2_nationkey"})

    merged = part.merge(lineitem, left_on="p_partkey", right_on="l_partkey")
    merged = merged.merge(supplier, left_on="l_suppkey", right_on="s_suppkey")
    merged = merged.merge(orders, left_on="l_orderkey", right_on="o_orderkey")
    merged = merged.merge(customer, left_on="o_custkey", right_on="c_custkey")
    merged = merged.merge(n1, left_on="c_nationkey", right_on="n1_nationkey")
    merged = merged.merge(region, left_on="n_regionkey", right_on="r_regionkey")
    merged = merged.merge(n2, left_on="s_nationkey", right_on="n2_nationkey")

    merged = merged[
        (merged["r_name"].str.strip() == "AMERICA")
        & (merged["o_orderdate"] >= _date("1995-01-01"))
        & (merged["o_orderdate"] <= _date("1996-12-31"))
        & (merged["p_type"].str.strip() == "ECONOMY ANODIZED STEEL")
    ]
    merged = merged.copy()
    merged["o_year"] = pd.to_datetime(merged["o_orderdate"]).dt.year
    merged["volume"] = merged["l_extendedprice"] * (1 - merged["l_discount"])
    merged["brazil_volume"] = merged["volume"].where(
        merged["supp_nation"].str.strip() == "BRAZIL", 0.0
    )

    agg = (
        merged.groupby("o_year")
        .agg(brazil_vol=("brazil_volume", "sum"), total_vol=("volume", "sum"))
        .reset_index()
    )
    agg["mkt_share"] = agg["brazil_vol"].astype(float) / agg["total_vol"].astype(float)
    result = agg[["o_year", "mkt_share"]].sort_values("o_year").reset_index(drop=True)
    return result


def product_profit(tables: Tables) -> pd.DataFrame:
    """Measure profit by nation and year for parts containing 'green'."""
    part = tables["part"]
    supplier = tables["supplier"]
    lineitem = tables["lineitem"]
    partsupp = tables["partsupp"]
    orders = tables["orders"]
    nation = tables["nation"]

    merged = supplier.merge(lineitem, left_on="s_suppkey", right_on="l_suppkey")
    merged = merged.merge(
        partsupp,
        left_on=["l_suppkey", "l_partkey"],
        right_on=["ps_suppkey", "ps_partkey"],
    )
    merged = merged.merge(part, left_on="l_partkey", right_on="p_partkey")
    merged = merged.merge(orders, left_on="l_orderkey", right_on="o_orderkey")
    merged = merged.merge(nation, left_on="s_nationkey", right_on="n_nationkey")

    merged = merged[merged["p_name"].str.contains("green", regex=False)]
    merged = merged.copy()
    merged["nation"] = merged["n_name"]
    merged["o_year"] = pd.to_datetime(merged["o_orderdate"]).dt.year
    merged["amount"] = (
        merged["l_extendedprice"] * (1 - merged["l_discount"])
        - merged["ps_supplycost"] * merged["l_quantity"]
    )

    result = (
        merged.groupby(["nation", "o_year"])
        .agg(sum_profit=("amount", "sum"))
        .reset_index()
        .sort_values(["nation", "o_year"], ascending=[True, False])
        .reset_index(drop=True)
    )
    return result


def returned_items(tables: Tables) -> pd.DataFrame:
    """Report top customers by revenue lost to returned items."""
    customer = tables["customer"]
    orders = tables["orders"]
    lineitem = tables["lineitem"]
    nation = tables["nation"]

    merged = customer.merge(orders, left_on="c_custkey", right_on="o_custkey")
    merged = merged.merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
    merged = merged.merge(nation, left_on="c_nationkey", right_on="n_nationkey")

    merged = merged[
        (merged["o_orderdate"] >= _date("1993-10-01"))
        & (merged["o_orderdate"] < _date("1994-01-01"))
        & (merged["l_returnflag"].str.strip() == "R")
    ]
    merged = merged.copy()
    merged["revenue"] = merged["l_extendedprice"] * (1 - merged["l_discount"])

    result = (
        merged.groupby(
            ["c_custkey", "c_name", "c_acctbal", "c_phone",
             "n_name", "c_address", "c_comment"]
        )
        .agg(revenue=("revenue", "sum"))
        .reset_index()
        .sort_values("revenue", ascending=False)
        .head(20)
        .reset_index(drop=True)
    )[["c_custkey", "c_name", "revenue", "c_acctbal", "n_name",
       "c_address", "c_phone", "c_comment"]]
    return result


def critical_inventory(tables: Tables) -> pd.DataFrame:
    """Identify high-value stock items exceeding a significance threshold."""
    partsupp = tables["partsupp"]
    supplier = tables["supplier"]
    nation = tables["nation"]

    merged = partsupp.merge(supplier, left_on="ps_suppkey", right_on="s_suppkey")
    merged = merged.merge(nation, left_on="s_nationkey", right_on="n_nationkey")
    merged = merged[merged["n_name"].str.strip() == "GERMANY"]

    merged = merged.copy()
    merged["value_col"] = merged["ps_supplycost"] * merged["ps_availqty"]

    grouped = (
        merged.groupby("ps_partkey")
        .agg(value=("value_col", "sum"))
        .reset_index()
    )

    threshold = float(grouped["value"].sum()) * 0.0001
    result = (
        grouped[grouped["value"] > threshold]
        .sort_values("value", ascending=False)
        .reset_index(drop=True)
    )
    return result


def shipping_mode_analysis(tables: Tables) -> pd.DataFrame:
    """Analyze order priority distribution across shipping modes."""
    orders = tables["orders"]
    lineitem = tables["lineitem"]

    merged = orders.merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
    merged = merged[
        (merged["l_shipmode"].str.strip().isin(["MAIL", "SHIP"]))
        & (merged["l_commitdate"] < merged["l_receiptdate"])
        & (merged["l_shipdate"] < merged["l_commitdate"])
        & (merged["l_receiptdate"] >= _date("1994-01-01"))
        & (merged["l_receiptdate"] < _date("1995-01-01"))
    ]
    merged = merged.copy()
    prio = merged["o_orderpriority"].str.strip()
    merged["high_line_count"] = prio.isin(["1-URGENT", "2-HIGH"]).astype(int)
    merged["low_line_count"] = (~prio.isin(["1-URGENT", "2-HIGH"])).astype(int)

    result = (
        merged.groupby("l_shipmode")
        .agg(
            high_line_count=("high_line_count", "sum"),
            low_line_count=("low_line_count", "sum"),
        )
        .reset_index()
        .sort_values("l_shipmode")
        .reset_index(drop=True)
    )
    return result


def customer_distribution(tables: Tables) -> pd.DataFrame:
    """Show the distribution of customers by their order count."""
    customer = tables["customer"]
    orders = tables["orders"]

    filtered_orders = orders[
        ~orders["o_comment"].str.contains("special.*requests", regex=True)
    ]

    merged = customer.merge(
        filtered_orders, left_on="c_custkey", right_on="o_custkey", how="left"
    )

    c_orders = (
        merged.groupby("c_custkey")
        .agg(c_count=("o_orderkey", "count"))
        .reset_index()
    )

    result = (
        c_orders.groupby("c_count")
        .agg(custdist=("c_custkey", "count"))
        .reset_index()
        .sort_values(["custdist", "c_count"], ascending=[False, False])
        .reset_index(drop=True)
    )
    return result


def promotion_effect(tables: Tables) -> pd.DataFrame:
    """Measure the revenue share attributable to promotional parts."""
    lineitem = tables["lineitem"]
    part = tables["part"]

    merged = lineitem.merge(part, left_on="l_partkey", right_on="p_partkey")
    merged = merged[
        (merged["l_shipdate"] >= _date("1995-09-01"))
        & (merged["l_shipdate"] < _date("1995-10-01"))
    ]
    merged = merged.copy()
    merged["disc_revenue"] = merged["l_extendedprice"] * (1 - merged["l_discount"])
    merged["promo_disc_revenue"] = merged["disc_revenue"].where(
        merged["p_type"].str.strip().str.startswith("PROMO"), 0.0
    )

    total = float(merged["disc_revenue"].sum())
    promo = float(merged["promo_disc_revenue"].sum())
    promo_revenue = 100.0 * promo / total
    return pd.DataFrame({"promo_revenue": [promo_revenue]})


def top_supplier(tables: Tables) -> pd.DataFrame:
    """Find the supplier with the highest total revenue in a quarter."""
    lineitem = tables["lineitem"]
    supplier = tables["supplier"]

    li = lineitem[
        (lineitem["l_shipdate"] >= _date("1996-01-01"))
        & (lineitem["l_shipdate"] < _date("1996-04-01"))
    ].copy()
    li["revenue"] = li["l_extendedprice"] * (1 - li["l_discount"])

    rev = (
        li.groupby("l_suppkey")
        .agg(total_revenue=("revenue", "sum"))
        .reset_index()
    )

    max_revenue = rev["total_revenue"].max()
    top = rev[rev["total_revenue"] == max_revenue]

    result = top.merge(supplier, left_on="l_suppkey", right_on="s_suppkey")
    result = (
        result[["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]]
        .sort_values("s_suppkey")
        .reset_index(drop=True)
    )
    return result


def supplier_parts_coverage(tables: Tables) -> pd.DataFrame:
    """Analyze supplier coverage excluding flagged suppliers and part types."""
    supplier = tables["supplier"]
    partsupp = tables["partsupp"]
    part = tables["part"]

    complaint_keys = set(
        supplier[
            supplier["s_comment"].str.contains("Customer.*Complaints", regex=True)
        ]["s_suppkey"]
    )

    merged = partsupp.merge(part, left_on="ps_partkey", right_on="p_partkey")
    merged = merged[
        (merged["p_brand"].str.strip() != "Brand#45")
        & (~merged["p_type"].str.strip().str.startswith("MEDIUM POLISHED"))
        & (merged["p_size"].isin([49, 14, 23, 45, 19, 3, 36, 9]))
        & (~merged["ps_suppkey"].isin(complaint_keys))
    ]

    result = (
        merged.groupby(["p_brand", "p_type", "p_size"])
        .agg(supplier_cnt=("ps_suppkey", "nunique"))
        .reset_index()
        .sort_values(
            ["supplier_cnt", "p_brand", "p_type", "p_size"],
            ascending=[False, True, True, True],
        )
        .reset_index(drop=True)
    )
    return result


def small_order_revenue(tables: Tables) -> pd.DataFrame:
    """Calculate average yearly revenue from small-quantity orders."""
    lineitem = tables["lineitem"]
    part = tables["part"]

    merged = lineitem.merge(part, left_on="l_partkey", right_on="p_partkey")
    merged = merged[
        (merged["p_brand"].str.strip() == "Brand#23")
        & (merged["p_container"].str.strip() == "MED BOX")
    ]

    thresholds = (
        merged.groupby("l_partkey")
        .agg(threshold=("l_quantity", "mean"))
        .reset_index()
    )
    thresholds["threshold"] = thresholds["threshold"] * 0.2

    merged = merged.merge(thresholds, on="l_partkey")
    merged = merged[merged["l_quantity"] < merged["threshold"]]

    if merged.empty:
        return pd.DataFrame({"avg_yearly": [None]})
    avg_yearly = float(merged["l_extendedprice"].sum()) / 7.0
    return pd.DataFrame({"avg_yearly": [avg_yearly]})


def large_volume_customers(tables: Tables) -> pd.DataFrame:
    """Identify customers placing unusually large orders."""
    lineitem = tables["lineitem"]
    customer = tables["customer"]
    orders = tables["orders"]

    order_qty = (
        lineitem.groupby("l_orderkey")
        .agg(total_qty=("l_quantity", "sum"))
        .reset_index()
    )
    qualifying_keys = set(order_qty[order_qty["total_qty"] > 300]["l_orderkey"])

    merged = customer.merge(orders, left_on="c_custkey", right_on="o_custkey")
    merged = merged.merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
    merged = merged[merged["o_orderkey"].isin(qualifying_keys)]

    result = (
        merged.groupby(
            ["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"]
        )
        .agg(sum_qty=("l_quantity", "sum"))
        .reset_index()
        .sort_values(["o_totalprice", "o_orderdate"], ascending=[False, True])
        .head(100)
        .reset_index(drop=True)
    )
    return result


def discounted_revenue(tables: Tables) -> pd.DataFrame:
    """Compute revenue for discounted parts across multiple brand/size tiers."""
    lineitem = tables["lineitem"]
    part = tables["part"]

    merged = lineitem.merge(part, left_on="l_partkey", right_on="p_partkey")

    ship_ok = (
        merged["l_shipmode"].str.strip().isin(["AIR", "AIR REG"])
    ) & (merged["l_shipinstruct"].str.strip() == "DELIVER IN PERSON")

    branch1 = (
        ship_ok
        & (merged["p_brand"].str.strip() == "Brand#12")
        & (merged["p_container"].str.strip().isin(["SM CASE", "SM BOX", "SM PACK", "SM PKG"]))
        & (merged["l_quantity"] >= 1) & (merged["l_quantity"] <= 11)
        & (merged["p_size"] >= 1) & (merged["p_size"] <= 5)
    )
    branch2 = (
        ship_ok
        & (merged["p_brand"].str.strip() == "Brand#23")
        & (merged["p_container"].str.strip().isin(["MED BAG", "MED BOX", "MED PKG", "MED PACK"]))
        & (merged["l_quantity"] >= 10) & (merged["l_quantity"] <= 20)
        & (merged["p_size"] >= 1) & (merged["p_size"] <= 10)
    )
    branch3 = (
        ship_ok
        & (merged["p_brand"].str.strip() == "Brand#34")
        & (merged["p_container"].str.strip().isin(["LG CASE", "LG BOX", "LG PACK", "LG PKG"]))
        & (merged["l_quantity"] >= 20) & (merged["l_quantity"] <= 30)
        & (merged["p_size"] >= 1) & (merged["p_size"] <= 15)
    )

    filtered = merged[branch1 | branch2 | branch3]
    revenue = float((filtered["l_extendedprice"] * (1 - filtered["l_discount"])).sum())
    return pd.DataFrame({"revenue": [revenue]})


def excess_inventory(tables: Tables) -> pd.DataFrame:
    """Identify Canadian suppliers with excess inventory for forest parts."""
    part = tables["part"]
    lineitem = tables["lineitem"]
    partsupp = tables["partsupp"]
    supplier = tables["supplier"]
    nation = tables["nation"]

    forest_keys = set(
        part[part["p_name"].str.startswith("forest")]["p_partkey"]
    )

    li = lineitem[
        (lineitem["l_shipdate"] >= _date("1994-01-01"))
        & (lineitem["l_shipdate"] < _date("1995-01-01"))
        & (lineitem["l_partkey"].isin(forest_keys))
    ]

    thresholds = (
        li.groupby(["l_partkey", "l_suppkey"])
        .agg(threshold=("l_quantity", "sum"))
        .reset_index()
    )
    thresholds["threshold"] = thresholds["threshold"] * 0.5

    ps_forest = partsupp[partsupp["ps_partkey"].isin(forest_keys)]
    ps_merged = ps_forest.merge(
        thresholds,
        left_on=["ps_partkey", "ps_suppkey"],
        right_on=["l_partkey", "l_suppkey"],
        how="inner",
    )
    qualifying_suppkeys = set(
        ps_merged[ps_merged["ps_availqty"] > ps_merged["threshold"]]["ps_suppkey"]
    )

    supp_nation = supplier.merge(nation, left_on="s_nationkey", right_on="n_nationkey")
    result = (
        supp_nation[
            (supp_nation["n_name"].str.strip() == "CANADA")
            & (supp_nation["s_suppkey"].isin(qualifying_suppkeys))
        ][["s_name", "s_address"]]
        .sort_values("s_name")
        .reset_index(drop=True)
    )
    return result


def supplier_reliability(tables: Tables) -> pd.DataFrame:
    """Find suppliers who were the sole cause of late order fulfillment."""
    supplier = tables["supplier"]
    nation = tables["nation"]
    lineitem = tables["lineitem"]
    orders = tables["orders"]

    supp_nation = supplier.merge(nation, left_on="s_nationkey", right_on="n_nationkey")
    sa_suppliers = supp_nation[supp_nation["n_name"].str.strip() == "SAUDI ARABIA"]

    base = sa_suppliers.merge(lineitem, left_on="s_suppkey", right_on="l_suppkey")
    base = base.merge(orders, left_on="l_orderkey", right_on="o_orderkey")
    base = base[
        (base["o_orderstatus"].str.strip() == "F")
        & (base["l_receiptdate"] > base["l_commitdate"])
    ]

    # EXISTS: orders with more than one distinct supplier
    order_supp_counts = (
        lineitem.groupby("l_orderkey")
        .agg(n_supp=("l_suppkey", "nunique"))
        .reset_index()
    )
    multi_supp_orders = set(
        order_supp_counts[order_supp_counts["n_supp"] > 1]["l_orderkey"]
    )
    base = base[base["l_orderkey"].isin(multi_supp_orders)]

    # NOT EXISTS: no other supplier was late on the same order
    late_pairs = lineitem[lineitem["l_receiptdate"] > lineitem["l_commitdate"]][
        ["l_orderkey", "l_suppkey"]
    ].drop_duplicates()
    late_supp_counts = (
        late_pairs.groupby("l_orderkey")
        .agg(n_late=("l_suppkey", "nunique"))
        .reset_index()
    )
    single_late_orders = set(
        late_supp_counts[late_supp_counts["n_late"] == 1]["l_orderkey"]
    )
    base = base[base["l_orderkey"].isin(single_late_orders)]

    result = (
        base.groupby("s_name")
        .agg(numwait=("l_orderkey", "count"))
        .reset_index()
        .sort_values(["numwait", "s_name"], ascending=[False, True])
        .head(100)
        .reset_index(drop=True)
    )
    return result


def sales_opportunity(tables: Tables) -> pd.DataFrame:
    """Identify high-balance customers with no orders as sales opportunities."""
    customer = tables["customer"]
    orders = tables["orders"]

    target_codes = ["13", "31", "23", "29", "30", "18", "17"]

    cust = customer.copy()
    cust["cntrycode"] = cust["c_phone"].str[:2]
    cust = cust[cust["cntrycode"].isin(target_codes)]

    avg_bal = float(cust[cust["c_acctbal"] > 0.00]["c_acctbal"].mean())
    cust = cust[cust["c_acctbal"] > avg_bal]

    cust_with_orders = set(orders["o_custkey"])
    cust = cust[~cust["c_custkey"].isin(cust_with_orders)]

    result = (
        cust.groupby("cntrycode")
        .agg(numcust=("c_custkey", "count"), totacctbal=("c_acctbal", "sum"))
        .reset_index()
        .sort_values("cntrycode")
        .reset_index(drop=True)
    )
    return result
