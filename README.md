# DSA Interview Packet: Supply Chain Analytics

A Declarative Automation Bundle that provisions a supply chain analytics environment for interview labs. The bundle deploys source data, simulated system tables, and ML models on a target workspace. Compatible with Databricks Free Edition (Unity Catalog and serverless compute, which is all Free Edition offers). The candidate receives the legacy pandas code as a starting point and the test suite as an answer key.

**The candidate prompt is the source of truth** for the scenario, priorities, deliverables, and scoring. This README describes what is in the workspace and the challenges the prompt asks the candidate to choose from; it does not add requirements.

**Development loop and tests:** [AI_WORKFLOW.md](AI_WORKFLOW.md)

> **Test coverage:** local tests covering fixtures, smoke, correctness, simulation, and bundle scripts. The query tests define the expected output for each of the 22 queries; the candidate writes the implementations and runs the tests to validate.

## What's Deployed

After `databricks bundle deploy` and running the default init jobs (`seed_source_tables` → `simulate_system_tables` → `ml_train_eval_pipeline`), the target workspace contains:

| Asset | Description |
|-------|-------------|
| **8 source tables** | Seeded from `samples.tpch` (default: FK-safe subset via bundle vars; `1.0` = full SF=5 copy) |
| **7 simulated system tables** | Realistic cost, billing, audit, and lineage data (see [System Tables Simulation](#system-tables-simulation)) |
| **Demand forecast model** | GBTRegressor predicting `l_quantity`, logged to MLflow |
| **Supplier scoring model** | RandomForestClassifier predicting late delivery, logged to MLflow |
| **Model evaluation jobs** | Jobs that run inference and log metrics for both models |
| **Integration test job** | Cross-implementation parity and property checks on live data |

### Fast iteration and E2E

Not every change requires a full cold start (`validate` → `deploy` → `seed` → `simulate` → `ml_train_eval_pipeline` → `run_integration_tests`).

| Tier | When to use | Typical commands |
|------|-------------|------------------|
| **Migration (pandas → Lakehouse / Lakeflow)** | You’re adding `queries/lakehouse/*.sql` or `pipelines/` and validating against the test suite | **Local:** `make test-lakehouse` or `make test-lakeflow` ([Testing](#testing)); uses the packet's Parquet fixtures only.<br>**Lakehouse (workspace):** `make integration`, or run `scripts/run_lakehouse_queries.py` as a script task in a job you create to materialize `lakehouse_*` Delta tables (no bundle job is defined for it; see [AI_WORKFLOW.md](AI_WORKFLOW.md)).<br>**Lakeflow (workspace):** this bundle has no separate “run all pipeline queries” job; use **`make integration`** to run Lakeflow functions against live UC data, or deploy a **Lakeflow/DLT pipeline** built from your `pipelines/` module ([Challenge 1](#challenge-1-migrate-legacy-queries-to-lakehouse-sql-andor-lakeflow)). |
| **Presentation layer (`app/`)** | You’re changing the Databricks App, Lakeview dashboard, or Genie space only | Build in the workspace (dashboard/Genie) or under `src/dsa_interview_packet/app/` (App; see its [README](src/dsa_interview_packet/app/README.md)). **Skip** seed → simulate → ML unless you also changed supply-chain data or models. |
| **ML only** | Source + sim tables already in UC; you changed ML code | `make deploy ml` |
| **ML + integration tests** | Above + you need legacy/SQL checks | `make deploy ml integration` |
| **Full cold start** | New catalog/schema or you need to re-copy TPC-H | `make init` (deploy → seed → simulate → ML; see [SETUP.md](SETUP.md)) |

**Faster sampling:** use bundle target `dev_fast` (`make init TARGET=dev_fast`, or `databricks bundle deploy -t dev_fast ...`). It overrides variables to smaller `seed_orders_sample_fraction` and `ml_lineitem_sample_fraction` (see `databricks.yml`). Tradeoff: less data, faster runs.

**Skip re-seed / re-simulate:** jobs write a small Delta metadata table `_dsa_interview_bundle_state` in the target schema. After a successful run, set bundle variables `seed_skip_if_populated` / `simulate_skip_if_populated` to `true` so the next run exits early when tables already look populated. Optional `seed_require_version` / `simulate_require_version` skip only when the stored version string matches (use `seed_data_version` / `simulate_data_version` to pin a custom label, or rely on the auto fingerprint from sampling params). To force a rewrite, omit the skip flags or clear the metadata table.

Examples of commented command blocks: `src/dsa_interview_packet/scripts/e2e_selective.sh`.

## Starting Point: Legacy pandas

The 22 analytical queries are implemented as Python functions in `src/dsa_interview_packet/legacy/supply_chain_queries.py`. Each function accepts a `dict[str, pd.DataFrame]` (keyed by table name) and returns a `pd.DataFrame`. This implementation uses standard pandas idioms (`merge()`, `groupby().agg()`, boolean indexing, `sort_values()`) and serves as the **migration source**: the candidate reads this code and rewrites it as Lakehouse SQL or Lakeflow pipelines.

Design constraints for the legacy code:
- No `.apply()` or lambdas in groupby, keeping operations translatable to SQL
- Standard join/filter/aggregate patterns throughout
- Each function is self-contained and readable without SQL knowledge

## Candidate Challenges

There is more work here than four hours allows. Decide what this customer needs from you right now, and be ready to explain what you deferred and why. At least one guardrail is expected (see [Guardrails](#guardrails)).

### Challenge 1: Migrate Legacy Queries to Lakehouse SQL and/or Lakeflow

Read the 22 pandas functions in `legacy/supply_chain_queries.py` and rewrite as many as you judge worthwhile (be ready to explain your choice), using one or both of these approaches:

**Lakehouse SQL**: Add standalone SQL files to `src/dsa_interview_packet/queries/lakehouse/` (the directory exists with a placeholder README). Each file contains a single query using Spark SQL syntax with unqualified table names, designed to run on Databricks SQL warehouses. Tests in `test_lakehouse.py` and `test_smoke.py` define the expected output.

**Lakeflow (Spark Declarative Pipelines)**: Implement the functions in `src/dsa_interview_packet/pipelines/supply_chain_queries.py` (a stub module is already there). Each function accepts a `SparkSession` and returns a `DataFrame`. Tests in `test_pipeline.py` and `test_smoke.py` define the expected output. In production, a separate entry point wraps them with `@dlt.table` decorators.

> **Naming:** Lakeflow is the Databricks managed service for data pipelines, jobs, and ingestion. Spark Declarative Pipelines (SDP) is the pipeline authoring framework within Lakeflow, formerly known as "Delta Live Tables" (DLT). The Python SDK still uses `import dlt` and `@dlt.table` decorators. Throughout this project, we use "Lakeflow" when referring to the managed pipeline feature and `dlt` when referring to the Python API.

### Challenge 2: Cost Attribution Analysis

Use the simulated system tables (see [System Tables Simulation](#system-tables-simulation)) to identify current spend drivers: which workloads, compute types, and query patterns drive spend, and what that costs. Assume **1 DBU = 1,000 (local currency)**. There is no team or project attribution today (see FAQ, tagging question). The output can be a notebook, a set of queries, or a dashboard, as long as it supports the remediation plan.

### Challenge 3: Diagnose ML Model Quality Issues

Two ML models are deployed with evaluation runs in MLflow, and stakeholders are complaining about prediction quality ("forecast accuracy feels worse," "the late-delivery alerts miss most late shipments"). Investigate the models, their training data, and evaluation metrics to find the root cause and propose fixes.

- **Demand Forecasting**: GBTRegressor predicting `l_quantity` (order line quantity). Features include part attributes, customer segment/nation, supplier attributes, time features, and rolling historical averages. Train on orders before 1997, test on 1997+.
- **Supplier Reliability Scoring**: RandomForestClassifier predicting late delivery (`l_receiptdate > l_commitdate`). Features include supplier attributes, part attributes, order priority, shipping mode, quantity/price, and rolling supplier late-rate. Train on orders before 1997, test on 1997+.

### Challenge 4: Executive Visibility Layer

The business-facing reporting layer was never finished, so stakeholders can't see what they are paying for. Build something an executive can open and understand, connected to the relevant supply chain data and, if selected, model outputs:

- **Option A: AI/BI Dashboard (optionally with a Genie space)**. Lighter lift.
- **Option B: Databricks App**. Interactive, more effort.

The `app/` directory contains a minimal Databricks App config (`app.yaml`) and a [README](src/dsa_interview_packet/app/README.md) describing what exists.

### Guardrails

Implement at least one preventative control you can demo (for example a SQL alert on spend, job tags with a tagging standard, or a scheduled cost check; Free Edition can't create budgets or compute policies, so propose those in your plan) and be ready to explain what it catches.

## Tests as the Answer Key

The test suite serves as the specification. When the candidate implements their Lakehouse SQL or Lakeflow queries, they run the tests to validate correctness:

- **Lakehouse SQL tests** (`test_lakehouse.py`): skip if `queries/lakehouse/*.sql` files do not exist
- **Lakeflow pipeline tests** (`test_pipeline.py`): skip for each query function not yet defined in `pipelines/supply_chain_queries.py`
- **Smoke tests** (`test_smoke.py`): skip for any implementation whose files are missing
- **Legacy tests** (`test_legacy.py`, `test_legacy_smoke.py`): pass immediately (the legacy code is provided)

As the candidate adds implementations, the corresponding tests "turn on" and provide pass/fail feedback.

## Queries

| Query | Description |
|-------|-------------|
| `pricing_summary` | Pricing and shipping summary by return flag and line status |
| `min_cost_supplier` | Cheapest supplier for parts of a given size/type in a region |
| `shipping_priority` | Top unshipped orders with highest revenue for a market segment |
| `order_priority` | Order counts by priority where lineitems were received late |
| `local_supplier_revenue` | Revenue from local suppliers (customer and supplier in same nation) |
| `revenue_forecast` | Potential revenue from eliminating certain discount ranges |
| `bilateral_trade` | Trade volume between two nations over a date range |
| `market_share` | National market share for a part type within a region |
| `product_profit` | Profit by nation and year for a product name pattern |
| `returned_items` | Top customers by revenue from returned items |
| `critical_inventory` | Parts representing significant inventory value for a nation |
| `shipping_mode_analysis` | Late delivery rates by shipping mode and order priority |
| `customer_distribution` | Distribution of customers by number of orders placed |
| `promotion_effect` | Percentage of revenue from promotional parts |
| `top_supplier` | Supplier(s) with highest total revenue in a period |
| `supplier_parts_coverage` | Distinct supplier count per brand/type/size combination |
| `small_order_revenue` | Average yearly revenue for below-threshold quantity orders |
| `large_volume_customers` | Customers with large-quantity orders ranked by total price |
| `discounted_revenue` | Revenue from specific brand/container/shipmode combinations |
| `excess_inventory` | Suppliers with excess inventory for parts matching a pattern |
| `supplier_reliability` | Suppliers who were the sole late deliverer on multi-supplier orders |
| `sales_opportunity` | High-balance customers with no orders, grouped by country code |

## Project Structure

```
dsa_interview_packet/
├── databricks.yml                        # Bundle configuration (variables, targets, includes)
├── Makefile                              # Local setup/tests and bundle shortcuts (`make help`)
├── requirements.txt                      # Pinned local test dependencies
├── README.md
├── SETUP.md                              # Workspace setup, verification, teardown
├── FAQ.md
├── AI_WORKFLOW.md                        # Local vs workspace feedback, devloop mechanics
├── SCHEMA.md                             # Table schema reference
├── pytest.ini
├── resources/
│   ├── infrastructure.yml                # (empty; database created by seed script)
│   ├── jobs.yml                          # Databricks jobs (seed, queries, integration tests, simulation)
│   └── ml_models.yml                     # MLflow experiments
├── src/dsa_interview_packet/
│   ├── legacy/
│   │   ├── __init__.py
│   │   └── supply_chain_queries.py       # 22 pandas functions (migration source, starting point)
│   ├── bundle_state.py                   # UC Delta metadata for idempotent seed/simulate skips
│   ├── sql_identifiers.py, lineitem_sample.py, categorical_hash.py,
│   │   ml_tree_config.py, mlflow_uc.py, mlflow_serverless.py   # Helpers used by the job scripts
│   ├── scripts/
│   │   ├── e2e_selective.sh              # Commented examples: selective bundle runs / dev_fast / skip flags
│   │   ├── seed_source_tables.py         # Copy samples.tpch (SF=5) into target schema
│   │   ├── simulate_system_tables.py     # Generate simulated system table data for cost analysis
│   │   ├── run_lakehouse_queries.py      # Execute all 22 Lakehouse SQL queries, write results to Delta
│   │   ├── run_integration_tests.py      # Cross-implementation parity and property checks
│   │   ├── run_ml_pipeline.py            # Default: train demand + train supplier + evaluate (one job)
│   │   ├── evaluate_models.py            # Evaluate only (optional; use for debugging)
│   │   ├── train_demand_forecast.py      # Demand training only (optional)
│   │   └── train_supplier_scoring.py     # Supplier training only (optional)
│   └── app/
│       ├── app.yaml                      # Databricks App config (skeleton)
│       └── README.md                     # Executive visibility layer status
│
│   # --- Candidate fills these in (placeholders already exist) ---
│   # queries/lakehouse/                  # README placeholder; add standalone SQL files (one per query)
│   # pipelines/
│   #   ├── __init__.py
│   #   ├── supply_chain_queries.py       # stub; add Python functions (SparkSession -> DataFrame)
│   #   └── supply_chain_pipeline.py      # (optional, candidate-created) Lakeflow entry point with @dlt.table decorators
│
└── tests/
    ├── conftest.py                       # Spark/pandas fixtures, comparison helpers
    ├── test_fixtures.py                  # Fixture data integrity (70 tests)
    ├── test_lakehouse.py                 # Lakehouse SQL correctness tests (22 tests, skip until implemented)
    ├── test_pipeline.py                  # Lakeflow pipeline correctness tests (22 tests, skip until implemented)
    ├── test_legacy.py                    # Legacy pandas correctness tests (22 tests)
    ├── test_smoke.py                     # Lakehouse SQL/Lakeflow smoke tests (22 tests, skip until implemented)
    ├── test_legacy_smoke.py              # Legacy pandas smoke tests (22 tests)
    ├── test_simulate_system_tables.py    # Simulation data quality tests (43 tests)
    ├── test_makefile.py                  # Makefile workspace targets against a fake CLI (32 tests)
    ├── test_*.py                         # Bundle/script unit tests (bundle_state, seed, ML config, MLflow UC, ...)
    └── fixtures/
        ├── data/                         # 8 source tables as Parquet (SF=0.01)
        │   ├── customer/
        │   ├── lineitem/
        │   ├── nation/
        │   ├── orders/
        │   ├── part/
        │   ├── partsupp/
        │   ├── region/
        │   └── supplier/
        └── expected/                     # 22 expected result sets as Parquet
            ├── pricing_summary/
            ├── min_cost_supplier/
            └── ...
```

## Deployment

For step-by-step setup instructions including verification queries, see **[SETUP.md](SETUP.md)**.

### Prerequisites

- Databricks CLI >= 0.260.0, authenticated to your workspace (`databricks auth login`; see [SETUP.md](SETUP.md))
- A Databricks workspace (Free Edition or higher)
- Serverless compute (the bundle's jobs run on serverless environments; this is the only option on Free Edition)

### Deploy and run

The packet `Makefile` wraps the Databricks CLI (`make help` lists every target). Add `PROFILE=<cli-profile>` if you don't use the CLI's `DEFAULT` profile, and `VARS='--var=catalog=... --var=schema=...'` to override bundle variables (a successful deploy saves `VARS`, so later commands reuse it).

```bash
make init          # deploy, then seed_source_tables -> simulate_system_tables -> ml_train_eval_pipeline
make integration   # run_integration_tests (after you implement Lakehouse SQL and/or Lakeflow)
make destroy       # tear down jobs and experiments (tables/models: see SETUP.md Teardown)
```

Each step is also its own target (`make deploy`, `make seed`, `make simulate`, `make ml`); the job targets deploy first, so the workspace always runs your current files. The equivalent CLI commands are `databricks bundle deploy -t dev` followed by `databricks bundle run -t dev <job>` (`bundle run` alone uses the last deployed files). To debug a single ML step, run the `train_demand_forecast`, `train_supplier_scoring`, or `evaluate_models` job on its own.

## Source Data

- **Local tests** use SF=0.01 Parquet fixtures checked into `tests/fixtures/data/`. These are small enough to run quickly with a local Spark session.
- **Databricks** uses `samples.tpch` via `seed_source_tables`. Bundle defaults use an FK-safe **subset** for fast setup; set `seed_orders_sample_fraction=1.0` for a full SF=5 copy of all 8 tables.

## Testing

### Test tiers

| Tier | File | Count | Runs on | What it validates |
|------|------|-------|---------|-------------------|
| **Fixtures** | `test_fixtures.py` | 70 | Local | Parquet files exist, correct row counts/columns, expected results non-empty |
| **Smoke (Lakehouse SQL/Lakeflow)** | `test_smoke.py` | 22 | Local | Schema, row counts, domain invariants; **skips until candidate creates implementations** |
| **Smoke (legacy)** | `test_legacy_smoke.py` | 22 | Local | Same invariants as above, targeting the pandas implementation |
| **Lakehouse SQL correctness** | `test_lakehouse.py` | 22 | Local | Exact row-by-row match of SQL output against expected results; **skips until candidate creates SQL files** |
| **Lakeflow correctness** | `test_pipeline.py` | 22 | Local | Exact row-by-row match of pipeline output against expected results; **skips until candidate creates pipeline module** |
| **Legacy correctness** | `test_legacy.py` | 22 | Local | Exact row-by-row match of pandas output against expected results |
| **Bundle/scripts** | `test_bundle_state.py`, `test_seed_source_tables.py`, `test_run_lakehouse_queries.py`, `test_ml_*.py`, `test_mlflow_uc.py`, `test_lineitem_sample.py` | varies | Local | Script and bundle helper behavior |
| **Simulation** | `test_simulate_system_tables.py` | 43 | Local | Referential integrity, schemas, data quality, cost story, determinism |
| **Makefile** | `test_makefile.py` | 32 | Local | Job targets deploy first, saved `VARS`, destroy confirmation, sign-in and setup messages; uses a fake `databricks` CLI, never contacts a workspace |
| **Integration** | `run_integration_tests` job | 22 x 3 | Databricks | Cross-implementation parity and property checks on live data |

### Running local tests

You need **Python 3.11+**. The Spark-backed tests also need a local **JDK 17 or 21** on `PATH` or via `JAVA_HOME`; newer JDKs are not supported by the pinned PySpark 3.5. `make setup` creates `.venv/` and installs the pinned versions from `requirements.txt`, then checks your JDK. Run everything from this directory:

```bash
make setup             # once: create .venv and install requirements.txt
make test              # everything
make test-legacy       # legacy pandas tests only (pass out of the box; no JDK needed)
make test-lakehouse    # Lakehouse SQL tests (skip until you add SQL files); or make test-lakeflow
make test TESTS=tests/test_lakehouse.py::TestLakehouseQueries::test_pricing_summary
make test-lakehouse ARGS="-k pricing_summary -x"   # extra pytest flags
```

Without `make` (e.g. on Windows), install `requirements.txt` into any Python 3.11+ environment and call pytest directly: `pytest tests/`, `pytest tests/ --impl lakehouse` (or `--impl lakeflow`).

### Correctness comparison

The correctness tests compare query output against pre-computed expected results stored as Parquet fixtures. The comparison helper (`assert_results_equal`) handles:

- **Case-insensitive** column name matching
- **Relative tolerance** (`1e-4`) for float/Decimal values
- **CHAR padding**: trailing whitespace stripped from string comparisons
- **Non-deterministic ordering**: queries with ambiguous `ORDER BY` (e.g., `shipping_priority`, `returned_items`, `large_volume_customers`) are sorted on extra tiebreaker columns by the test harness before the row-by-row comparison, so you don't need to add tiebreakers to your own `ORDER BY`

### Smoke test design

Each smoke test checks domain-specific properties without full value comparison:

- **Row count** bounds (exact for grouped queries, `LIMIT` for bounded ones)
- **Known enum values** (e.g., order priorities, ship modes, nation names)
- **Value ranges** (e.g., percentages between 0-100, market share between 0-1)
- **Sort order** verification (e.g., revenue descending, name ascending)
- **Relational invariants** (e.g., `sum(custdist) == total_customers`)

### Integration tests

The `run_integration_tests` Databricks job runs all 22 queries via all 3 implementations (Lakehouse SQL, Lakeflow, legacy pandas) against live data seeded from `samples.tpch` and validates:

- **Cross-implementation parity**: Lakehouse SQL results exactly match Lakeflow results (verified via `exceptAll` in both directions, ensuring zero mismatched rows)
- **Legacy schema parity**: pandas output columns match Lakehouse SQL output columns (column names and count)
- **Property checks** applied independently to each query result:
  - **Schema**: expected column names and count
  - **Row counts**: within defined min/max bounds
  - **NOT NULL**: critical columns contain no nulls
  - **Positive values**: numeric columns that must be > 0
  - **Enum values**: string columns restricted to known domain values
  - **Ranges**: numeric columns within expected bounds (e.g., discount between 0 and 1)
  - **Uniqueness**: key columns have no duplicates where expected
  - **Distribution**: group counts verify no degenerate single-group results

## System Tables Simulation

The `simulate_system_tables` job creates simulated system table data in the target schema, designed to demonstrate cost analysis patterns for the interview lab. Row counts scale with bundle variable `simulation_days` (default favors fast setup). The simulation generates 7 tables:

- **`sim_billing_usage`**: Mirrors `system.billing.usage`. Shows the legacy pandas job consuming about 42x more DBUs than the equivalent Lakehouse SQL job, with background noise from ~15-20 other users' jobs across SKUs (`sku_name` values `ALL_PURPOSE_COMPUTE`, `JOBS_COMPUTE`, `JOBS_SERVERLESS`, `SQL_SERVERLESS`, `DLT_SERVERLESS`, `INTERACTIVE_SERVERLESS`)
- **`sim_cluster_info`**: Mirrors `system.compute.clusters`. Cluster metadata for the simulated workloads.
- **`sim_job_run_timeline`**: Mirrors `system.lakeflow.job_run_timeline`. Job run timeline entries showing execution duration and status.
- **`sim_query_history`**: Mirrors `system.query.history`. SQL warehouse query patterns including dashboard refreshes, ad-hoc queries, and duplicate/unconsolidated queries.
- **`sim_pipeline_events`**: Mirrors `system.lakeflow.pipeline_events`. Lakeflow pipeline run events with occasional failures.
- **`sim_audit_logs`**: Mirrors `system.access.audit`. Workspace audit trail (cluster, job, notebook, warehouse events).
- **`sim_table_lineage`**: Mirrors `system.access.table_lineage`. Table-level lineage from source to result tables.

### Simulation-to-system-table mapping

| Simulated Table | Real System Table |
|-----------------|-------------------|
| `sim_billing_usage` | `system.billing.usage` |
| `sim_cluster_info` | `system.compute.clusters` |
| `sim_job_run_timeline` | `system.lakeflow.job_run_timeline` |
| `sim_query_history` | `system.query.history` |
| `sim_pipeline_events` | `system.lakeflow.pipeline_events` |
| `sim_audit_logs` | `system.access.audit` |
| `sim_table_lineage` | `system.access.table_lineage` |

These tables follow real system table schemas so that queries written against them are portable to production `system.*` tables with only a catalog/schema swap.
