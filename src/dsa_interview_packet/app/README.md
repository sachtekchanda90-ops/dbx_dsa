# Executive Visibility Layer (Not Yet Built)

The business-facing reporting layer for the supply chain use case was never finished. Stakeholders can't see what the platform delivers (README, Challenge 4).

## What Exists

- 8 source tables with supply chain data (an FK-safe subset of `samples.tpch` by default; see SETUP.md)
- 22 analytical queries (currently only implemented as legacy pandas; migration to Lakehouse SQL or Lakeflow is pending)
- Simulated system tables for cost analysis (`sim_*`)
- Demand forecasting and supplier reliability models, logged in MLflow
- `app.yaml`: a minimal Databricks App config, if you choose the App option. There is no app code yet and the bundle does not deploy an app; if you choose Option B, add the source here and deploy it yourself.

No dashboards or Genie spaces are deployed; for Option A, build them in the workspace.

## What's Needed

Something an executive can open and understand, surfacing the relevant supply chain data and, if selected, model outputs. Either approach works:

- **Option A: AI/BI Dashboard (optionally with a Genie space)**: lightest lift.
- **Option B: Databricks App**: interactive, more effort.

Name who it's for and what decision it supports. It should look functional in the demo; back-end perfection is not required.

## Relevant Tables

Tables are in the bundle's `catalog`/`schema` (default schema `dsa_interview_supply_chain`). See SCHEMA.md at the bundle root for full column definitions and primary keys.
