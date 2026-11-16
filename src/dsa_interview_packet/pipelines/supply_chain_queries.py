"""Candidate Lakeflow query layer (pure Spark DataFrames; no DLT imports here).

Implement one function per analytical query — same names as in ``legacy/supply_chain_queries``
(e.g. ``def pricing_summary(spark) -> DataFrame``). Each function takes a ``SparkSession``
with source tables registered under their TPC-H names and returns the result
``DataFrame``.

See the bundle README and FAQ for layout, tests (``tests/test_pipeline.py``), and
the optional ``supply_chain_pipeline.py`` entry point with ``@dlt.table`` decorators.

This module is intentionally empty of implementations: the candidate adds them.
"""
