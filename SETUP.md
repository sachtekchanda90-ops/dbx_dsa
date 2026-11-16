# Setup Instructions

This guide walks through deploying the DSA interview packet to a Databricks workspace and initializing all data, models, and infrastructure you will use. Run every command from this directory. `make help` lists the shortcuts; each step below also shows the plain Databricks CLI command.

## Prerequisites

- A Databricks workspace (Free Edition or higher)
- Serverless compute (the bundle's jobs run on serverless environments; required on Free Edition, which is serverless-only). Classic all-purpose compute is only an option if you retarget the jobs on a higher edition.
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) >= 0.260.0, signed in to your workspace:

  ```bash
  databricks auth login --host https://<your-workspace-url>
  ```

  The CLI saves a profile. If it is not your `DEFAULT` profile, add `PROFILE=<name>` to the `make` commands below (or `--profile <name>` to the CLI commands).
- For local tests only: Python 3.11+ and a JDK 17 or 21 (see [README.md — Running local tests](README.md#running-local-tests)), then `make setup`.

Unless you override them, tables land in the `workspace` catalog, schema `dsa_interview_supply_chain` (bundle variables `catalog` and `schema` in `databricks.yml`).

## Quick start

```bash
make validate   # databricks bundle validate -t dev
make init       # deploy, then steps 2-4 below in order
```

## 1. Deploy the bundle

This registers jobs and uploads source files. The schema is created by the seed job.

```bash
make deploy     # databricks bundle deploy -t dev
```

Override catalog/schema if needed:

```bash
make deploy VARS='--var=catalog=my_catalog --var=schema=my_schema'
```

You only pass `VARS` once. A successful deploy saves it to `.bundle-vars-<target>` (one file per `TARGET`), and later `make` commands reuse it and print `Using saved VARS ...`. Passing `VARS` again replaces the whole saved value, so include every override you want to keep. `make deploy VARS=` goes back to the defaults.

## 2. Seed source tables

**Default:** copies an FK-consistent **subset** of `samples.tpch` (bundle variable `seed_orders_sample_fraction`, default `0.003`, i.e. 0.3% of orders) so initialization stays fast.

```bash
make seed       # deploy, then databricks bundle run -t dev seed_source_tables
```

**Full TPC-H sample (large, slow):** job parameters are fixed at deploy time, so redeploy with the variable set, then run (add your catalog/schema overrides too if you set them, since this replaces the saved `VARS`):

```bash
make deploy seed VARS='--var=seed_orders_sample_fraction=1.0'
```

Verify:

```sql
SELECT COUNT(*) FROM workspace.dsa_interview_supply_chain.lineitem;
-- Subset default: on the order of 10^4–10^5 rows; full copy: ~30M rows
```

## 3. Generate simulated system tables

Creates 7 simulated system tables with realistic cost, billing, audit, and lineage data. Volume scales with bundle variable `simulation_days` (default is short for fast setup).

```bash
make simulate   # deploy, then databricks bundle run -t dev simulate_system_tables
```

Verify:

```sql
SHOW TABLES IN workspace.dsa_interview_supply_chain LIKE 'sim_*';
-- Expected: 7 tables
```

## 4. Train and evaluate ML models (default path)

**Default:** one job runs demand training, supplier training, and evaluation in a **single Spark session**.

```bash
make ml         # deploy, then databricks bundle run -t dev ml_train_eval_pipeline
```

Verify in **Experiments**: the bundle creates three experiments under `/Workspace/Users/<your-email>/`, whose names end in `dsa_interview_packet_demand_forecast`, `dsa_interview_packet_supplier_scoring`, and `dsa_interview_packet_model_evaluation`. The `dev` target runs in development mode, which prefixes deployed resource names (jobs and experiments) with `[dev <your-user>]`.

### Optional: separate jobs (debug one step)

`bundle run` uses the last deployed files, so run `make deploy` first if you changed anything.

```bash
databricks bundle run -t dev train_demand_forecast
databricks bundle run -t dev train_supplier_scoring
databricks bundle run -t dev evaluate_models
```

## 5. Verify the workspace

After all jobs complete, the workspace should have:

| Asset | Location | How to verify |
|-------|----------|---------------|
| 8 source tables | `workspace.dsa_interview_supply_chain` | `SHOW TABLES` in the schema |
| 7 simulated system tables | `workspace.dsa_interview_supply_chain.sim_*` | Query any `sim_` table |
| Demand forecast model | Experiments | Name ends in `dsa_interview_packet_demand_forecast` |
| Supplier scoring model | Experiments | Name ends in `dsa_interview_packet_supplier_scoring` |
| Evaluation metrics | Experiments | Name ends in `dsa_interview_packet_model_evaluation` |
| Jobs | Jobs & Pipelines | `[dev <your-user>] seed_source_tables`, …, `[dev <your-user>] run_integration_tests` |

## 6. Integration tests (after you implement queries)

```bash
make integration   # deploy your current files, then databricks bundle run -t dev run_integration_tests
```

## Teardown

To remove all deployed resources:

```bash
make destroy    # databricks bundle destroy -t dev
```

The CLI asks you to confirm. If you run it somewhere it can't ask (for example through an AI coding agent), it stops instead; run it in your own terminal, or add `AUTO_APPROVE=1` once you're sure.

Each target is a separate deployment: if you also used `dev_fast`, run `make destroy TARGET=dev_fast` too. Both targets write to the same schema by default.

This removes the jobs, experiments, and uploaded files. It does not remove the tables or the registered models (`demand_forecast`, `supplier_scoring`), which the jobs created in Unity Catalog. To remove those too, run this in a SQL editor or notebook:

```sql
DROP SCHEMA IF EXISTS workspace.dsa_interview_supply_chain CASCADE;
```
