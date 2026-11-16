#!/usr/bin/env sh
# Selective E2E examples for dsa_interview_packet (copy the block you need).
# Set PROFILE and VAR to match your workspace, e.g.:
#   PROFILE=my-workspace
#   VAR='--var=catalog=my_catalog'

set -eu

# --- Tier A: Migration (pandas -> Lakehouse SQL / Lakeflow) — mostly local pytest ---
# See README.md (Testing): from the packet directory, `make test-lakehouse`
#   (or make test-lakeflow).
# Lakehouse on workspace: make integration, or run scripts/run_lakehouse_queries.py as a script
#   task in a job you create (no bundle job is defined for it; see AI_WORKFLOW.md).
# Lakeflow on workspace: bundle run run_integration_tests, or deploy a Lakeflow/DLT pipeline from pipelines/.

# --- Tier B: Presentation layer (app/) — no data jobs unless models/tables changed ---
# Edit src/dsa_interview_packet/app/ and deploy the app; skip seed/simulate/ml unless needed.

# --- Tier C: ML only (assumes seed + simulate already ran) ---
# databricks bundle deploy -p "$PROFILE" -t dev $VAR
# databricks bundle run -p "$PROFILE" -t dev $VAR ml_train_eval_pipeline

# --- Tier D: ML + integration tests ---
# databricks bundle deploy -p "$PROFILE" -t dev $VAR
# databricks bundle run -p "$PROFILE" -t dev $VAR ml_train_eval_pipeline
# databricks bundle run -p "$PROFILE" -t dev $VAR run_integration_tests

# --- Tier E: Faster bundle target (smaller samples; see databricks.yml dev_fast) ---
# databricks bundle deploy -p "$PROFILE" -t dev_fast $VAR
# databricks bundle run -p "$PROFILE" -t dev_fast $VAR seed_source_tables
# databricks bundle run -p "$PROFILE" -t dev_fast $VAR simulate_system_tables
# databricks bundle run -p "$PROFILE" -t dev_fast $VAR ml_train_eval_pipeline

# --- Tier F: Skip re-seed / re-sim when UC data exists (set bundle vars or CLI flags) ---
# In databricks.yml / bundle deploy overrides:
#   --var=seed_skip_if_populated=true --var=simulate_skip_if_populated=true
# Or pass explicit --require-version after a full run (matches _dsa_interview_bundle_state).

echo "This file documents selective E2E commands; uncomment and run the tier you need."
