# pylint: disable=redefined-outer-name
"""Shared fixtures for supply chain analytics tests.

Loads parquet fixture data into PySpark temp views (for Lakehouse SQL/Lakeflow tests)
and dask DataFrames (for legacy pandas tests), and provides helpers for
comparing query results against expected output.

Use ``--impl`` to run only a specific implementation's tests::

    pytest --impl lakehouse   # only Lakehouse SQL tests
    pytest --impl lakeflow    # only Lakeflow pipeline tests
    pytest --impl all         # all implementations (default)
"""

# mypy: ignore-errors

from __future__ import annotations

import math
import sys
from decimal import Decimal
from pathlib import Path

# spark_python_task scripts use ``import ensure_src_path`` before ``dsa_interview_packet``;
# add the scripts dir so those imports resolve in local pytest (matches cluster behavior).
_scripts_dir = Path(__file__).resolve().parent.parent / "src" / "dsa_interview_packet" / "scripts"
_sp = str(_scripts_dir)
if _sp not in sys.path:
    sys.path.insert(0, _sp)

# Add the ``src`` root so ``import dsa_interview_packet.<module>`` resolves to the
# real package (not the namespace-package shadow of the bundle dir on cwd), without
# relying on an externally set PYTHONPATH.
_src_dir = Path(__file__).resolve().parent.parent / "src"
_srcp = str(_src_dir)
if _srcp not in sys.path:
    sys.path.insert(0, _srcp)

import pandas as pd
import pytest

try:
    from databricks.connect import DatabricksSession

    SparkSessionClass = DatabricksSession
    USE_DATABRICKS_CONNECT = True
except ImportError:
    from pyspark.sql import SparkSession

    SparkSessionClass = SparkSession  # type: ignore[misc,assignment]
    USE_DATABRICKS_CONNECT = False


# ---------------------------------------------------------------------------
# --impl CLI option: run only lakehouse, lakeflow, or all implementations
# ---------------------------------------------------------------------------

def pytest_addoption(parser):
    parser.addoption(
        "--impl",
        action="store",
        default="all",
        choices=["lakehouse", "lakeflow", "all"],
        help="Run tests for a specific implementation: lakehouse, lakeflow, or all (default: all)",
    )


@pytest.fixture(scope="session")
def impl_filter(request):
    """Return the selected implementation filter."""
    return request.config.getoption("--impl")


def pytest_collection_modifyitems(config, items):
    """Skip test classes that don't match the --impl filter."""
    impl = config.getoption("--impl")
    if impl == "all":
        return

    skip_lakehouse = pytest.mark.skip(reason="--impl=lakeflow excludes Lakehouse SQL tests")
    skip_lakeflow = pytest.mark.skip(reason="--impl=lakehouse excludes Lakeflow pipeline tests")

    for item in items:
        # Identify tests by their module and class names
        module_name = item.module.__name__ if item.module else ""
        cls_name = item.cls.__name__ if item.cls else ""

        if impl == "lakehouse":
            # Skip Lakeflow pipeline tests
            if module_name.endswith("test_pipeline") or cls_name == "TestPipelineQueries":
                item.add_marker(skip_lakeflow)
        elif impl == "lakeflow":
            # Skip Lakehouse SQL tests
            if module_name.endswith("test_lakehouse") or cls_name == "TestLakehouseQueries":
                item.add_marker(skip_lakehouse)


FIXTURES_DIR = Path(__file__).parent / "fixtures"
DATA_DIR = FIXTURES_DIR / "data"
EXPECTED_DIR = FIXTURES_DIR / "expected"

DATA_TABLES = [
    "region",
    "nation",
    "supplier",
    "part",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
]


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for the test session."""
    if USE_DATABRICKS_CONNECT:
        try:
            session = SparkSessionClass.builder.getOrCreate()
        except Exception:  # pylint: disable=broad-exception-caught
            pytest.skip(
                "Databricks Connect not configured; "
                "run with a local PySpark env (make setup; JDK 17 or 21)"
            )
    else:
        session = SparkSessionClass.builder.master("local[*]").appName("supply-chain-tests").getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def data_views(spark):
    """Load all source tables from parquet fixtures as temp views.

    Returns a dict mapping table name -> row count for basic sanity checks.
    """
    counts = {}
    for table in DATA_TABLES:
        parquet_path = str(DATA_DIR / table / "data.parquet")
        df = spark.read.parquet(parquet_path)
        df.createOrReplaceTempView(table)
        counts[table] = df.count()
    return counts


def load_expected(spark, query_name: str):
    """Load expected results for a query from parquet fixtures.

    Returns a PySpark DataFrame with the expected output.
    """
    path = str(EXPECTED_DIR / query_name / "data.parquet")
    return spark.read.parquet(path)


def _normalize_str(val):
    """Strip trailing whitespace from string values.

    Source data uses CHAR(N) columns which may have trailing padding.
    DuckDB and Spark may handle this padding differently.
    """
    if isinstance(val, str):
        return val.rstrip()
    return val


def assert_results_equal(
    actual_df,
    expected_df,
    rtol: float = 1e-4,
    order_by: list[str] | None = None,
):
    """Assert that two DataFrames have matching content.

    - Compares schemas (column names, case-insensitive)
    - Compares row counts
    - Compares values row-by-row with relative tolerance for floats/decimals
    - Strips trailing whitespace from strings (CHAR padding differences)

    Args:
        actual_df: The DataFrame produced by the implementation under test.
        expected_df: The DataFrame loaded from the expected-result fixture.
        rtol: Relative tolerance for float/Decimal comparisons.
        order_by: Optional list of column names to sort both DataFrames before
            comparison. Use this for queries whose ORDER BY is not fully
            deterministic so that row-by-row comparison is stable across
            engines.
    """
    from pyspark.sql import functions as F  # pylint: disable=import-outside-toplevel

    # Check column names match
    actual_cols = [c.lower() for c in actual_df.columns]
    expected_cols = [c.lower() for c in expected_df.columns]
    assert actual_cols == expected_cols, (
        f"Column mismatch:\n  actual:   {actual_cols}\n  expected: {expected_cols}"
    )

    # Check row counts
    actual_count = actual_df.count()
    expected_count = expected_df.count()
    assert actual_count == expected_count, (
        f"Row count mismatch: actual={actual_count}, expected={expected_count}"
    )

    # Re-sort both sides if caller specified tiebreaker columns
    if order_by:
        sort_exprs = [F.col(c) for c in order_by]
        actual_df = actual_df.orderBy(*sort_exprs)
        expected_df = expected_df.orderBy(*sort_exprs)

    # Compare values row-by-row
    actual_rows = actual_df.collect()
    expected_rows = expected_df.collect()

    for i, (actual_row, expected_row) in enumerate(zip(actual_rows, expected_rows)):
        for col in actual_cols:
            actual_val = actual_row[col]
            expected_val = expected_row[col]

            if actual_val is None and expected_val is None:
                continue

            if (actual_val is None) != (expected_val is None):
                raise AssertionError(
                    f"Row {i}, col '{col}': one value is None -- "
                    f"actual={actual_val!r}, expected={expected_val!r}"
                )

            if isinstance(expected_val, (float, Decimal)) and isinstance(
                actual_val, (float, Decimal)
            ):
                a = float(actual_val)
                e = float(expected_val)
                if e == 0.0:
                    assert abs(a) < 1e-6, (
                        f"Row {i}, col '{col}': actual={a}, expected={e}"
                    )
                else:
                    assert abs((a - e) / e) < rtol, (
                        f"Row {i}, col '{col}': actual={a}, expected={e}, "
                        f"rel_diff={abs((a - e) / e):.6f}"
                    )
            else:
                a = _normalize_str(actual_val)
                e = _normalize_str(expected_val)
                assert a == e, (
                    f"Row {i}, col '{col}': actual={a!r}, expected={e!r}"
                )


# ---------------------------------------------------------------------------
# Pandas fixtures and helpers (for legacy implementation tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pandas_tables():
    """Load all source tables from parquet fixtures as pandas DataFrames.

    Returns a dict mapping table name -> pandas DataFrame.
    """
    tables = {}
    for table in DATA_TABLES:
        parquet_path = str(DATA_DIR / table / "data.parquet")
        df = pd.read_parquet(parquet_path)
        # Convert Decimal/pyarrow columns to native float64 so arithmetic
        # with Python floats works without type errors.
        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]) or "decimal" in str(df[col].dtype).lower():
                try:
                    df[col] = df[col].astype(float)
                except (ValueError, TypeError):
                    pass  # leave non-numeric object columns as-is
        tables[table] = df
    return tables


def load_expected_pandas(query_name: str) -> pd.DataFrame:
    """Load expected results for a query as a pandas DataFrame."""
    path = str(EXPECTED_DIR / query_name / "data.parquet")
    return pd.read_parquet(path)


def assert_pandas_results_equal(
    actual_df: pd.DataFrame,
    expected_df: pd.DataFrame,
    rtol: float = 1e-4,
    order_by: list[str] | None = None,
) -> None:
    """Assert that two pandas DataFrames have matching content.

    Same semantics as ``assert_results_equal`` but for pandas DataFrames.
    """
    # Check column names match (case-insensitive)
    actual_cols = [c.lower() for c in actual_df.columns]
    expected_cols = [c.lower() for c in expected_df.columns]
    assert actual_cols == expected_cols, (
        f"Column mismatch:\n  actual:   {actual_cols}\n  expected: {expected_cols}"
    )

    # Normalize column names to lowercase
    actual_df = actual_df.copy()
    expected_df = expected_df.copy()
    actual_df.columns = [c.lower() for c in actual_df.columns]
    expected_df.columns = [c.lower() for c in expected_df.columns]

    # Check row counts
    assert len(actual_df) == len(expected_df), (
        f"Row count mismatch: actual={len(actual_df)}, expected={len(expected_df)}"
    )

    # Re-sort both sides if caller specified tiebreaker columns
    if order_by:
        actual_df = actual_df.sort_values(order_by).reset_index(drop=True)
        expected_df = expected_df.sort_values(order_by).reset_index(drop=True)

    # Compare values row-by-row
    for i in range(len(actual_df)):
        for col in actual_cols:
            actual_val = actual_df.iloc[i][col]
            expected_val = expected_df.iloc[i][col]

            # Handle pandas NA / None / NaN
            a_na = actual_val is None or (isinstance(actual_val, float) and math.isnan(actual_val))
            e_na = expected_val is None or (isinstance(expected_val, float) and math.isnan(expected_val))
            if hasattr(actual_val, "__class__") and actual_val.__class__.__name__ == "NAType":
                a_na = True
            if hasattr(expected_val, "__class__") and expected_val.__class__.__name__ == "NAType":
                e_na = True

            if a_na and e_na:
                continue

            if a_na != e_na:
                raise AssertionError(
                    f"Row {i}, col '{col}': one value is NA -- "
                    f"actual={actual_val!r}, expected={expected_val!r}"
                )

            # Coerce Decimal to float for comparison
            if isinstance(actual_val, Decimal):
                actual_val = float(actual_val)
            if isinstance(expected_val, Decimal):
                expected_val = float(expected_val)

            if isinstance(expected_val, float) and isinstance(actual_val, (int, float)):
                a = float(actual_val)
                e = float(expected_val)
                if e == 0.0:
                    assert abs(a) < 1e-6, (
                        f"Row {i}, col '{col}': actual={a}, expected={e}"
                    )
                else:
                    assert abs((a - e) / e) < rtol, (
                        f"Row {i}, col '{col}': actual={a}, expected={e}, "
                        f"rel_diff={abs((a - e) / e):.6f}"
                    )
            elif isinstance(expected_val, (int,)) and isinstance(actual_val, (int, float)):
                assert int(actual_val) == int(expected_val), (
                    f"Row {i}, col '{col}': actual={actual_val!r}, expected={expected_val!r}"
                )
            else:
                a = _normalize_str(actual_val)
                e = _normalize_str(expected_val)
                assert a == e, (
                    f"Row {i}, col '{col}': actual={a!r}, expected={e!r}"
                )
