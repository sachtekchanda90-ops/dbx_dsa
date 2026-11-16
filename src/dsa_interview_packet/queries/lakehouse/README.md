# Lakehouse SQL query layer

Candidate Lakehouse SQL implementation of the 22 supply-chain analytical queries
(pure Spark SQL; no Python here).

Add one `.sql` file per query, named after the query — the same names as in
`legacy/supply_chain_queries.py` and the `pipelines/` functions
(e.g. `pricing_summary.sql`, `min_cost_supplier.sql`). Each file contains a
**single** Spark SQL query that uses **unqualified** TPC-H table names
(`lineitem`, `orders`, `customer`, ...), which the harness registers for you.

The 22 query names and descriptions are in the repo `README.md` (Challenge 1);
table/column reference is in `SCHEMA.md`.

Tests are gated on file presence and turn on automatically as you add files:
`tests/test_lakehouse.py` and `tests/test_smoke.py` (each `<query_name>.sql` is
loaded and run against the fixture data, then compared to the expected output).
Until a file exists, its test is skipped. Run the whole packet with the
Lakehouse-only slice via `make test-lakehouse` (or `pytest tests/ --impl lakehouse`).

This directory is intentionally empty of implementations: the candidate adds them.
