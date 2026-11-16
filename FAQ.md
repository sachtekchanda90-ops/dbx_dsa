# DSA Interview Packet: Frequently Asked Questions

This file documents the **`dsa_interview_packet`** [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/index.html).

**Canonical docs for this bundle (read these first; this FAQ does not duplicate them):**

| Topic | Document |
|--------|----------|
| Scenario, challenges, tests, deployment commands, system-table simulation | [README.md](README.md) |
| Workspace setup, verify queries, teardown | [SETUP.md](SETUP.md) |
| Local vs workspace feedback loops, `dev_fast`, skip flags | [AI_WORKFLOW.md](AI_WORKFLOW.md) |
| Table columns and relationships | [SCHEMA.md](SCHEMA.md) |
| Executive visibility layer status | [src/dsa_interview_packet/app/README.md](src/dsa_interview_packet/app/README.md) |

**Tooling:** Use the packet **`Makefile`** from this directory (`make help`): `make setup` / `make test` for local tests, and `make deploy` / `make init` / `make destroy`, which wrap the **Databricks CLI** (`databricks bundle deploy`, `databricks bundle run`, `databricks bundle destroy`). Prefer these over one-off scripts unless you are debugging. See [README.md — Deployment](README.md#deployment) and [README.md — Testing](README.md#testing). Commented examples live in `src/dsa_interview_packet/scripts/e2e_selective.sh`.

Questions below are ones a **candidate** might ask after reading [README.md](README.md) and [AI_WORKFLOW.md](AI_WORKFLOW.md). **Your candidate prompt is the source of truth** for the scenario, priorities, deliverables, and scoring; these answers are written to match it. For workspace initialization, see **[SETUP.md](SETUP.md)**.

---

## Workspace Access & Setup

**Q1: How do I log into the Databricks workspace?**  
Use your own Databricks Free Edition workspace, signed up with the email on your application (see the Setup & Environment Guide you received before the interview). Authenticate the Databricks CLI against it (for example `databricks auth login --host <workspace-url>`) and deploy with that profile (`--profile=<name>`).

**Q2: What catalog and schema are my tables in?**  
By default, catalog `workspace` and schema `dsa_interview_supply_chain`. Both are bundle variables (`catalog`, `schema`) you can override at deploy time; see [SETUP.md](SETUP.md).

**Q3: Is a SQL warehouse or cluster available?**  
Jobs use the bundle’s configured compute (see `resources/jobs.yml`), which is serverless. Databricks Free Edition is serverless-only. For ad hoc queries, use the workspace's serverless SQL warehouse (Free Edition includes one, 2X-Small) or a serverless notebook.

**Q4: How do I find the MLflow experiments and models?**  
Use the MLflow UI in the workspace. Experiment names and locations are listed in [SETUP.md — Verify the workspace](SETUP.md#5-verify-the-workspace). Models are logged to the experiment runs and registered in the Unity Catalog model registry as `<catalog>.<schema>.<model>` by the training jobs.

---

## Tests and implementations

**Q5: How do I run the tests?**  
From this packet directory: `make setup` once, then `make test` (or `make test-lakehouse` / `make test-lakeflow` to focus). You need Python 3.11+ and, for the Spark-backed tests, a local JDK 17 or 21. See [README.md — Testing](README.md#testing).

**Q6: Can I run only one implementation at a time for faster feedback?**  
Locally, yes—use `--impl lakehouse`, `--impl lakeflow`, or `--impl all` with the test invocation described in [README.md — Testing](README.md#testing). On Databricks, run integration checks via `databricks bundle run … run_integration_tests` (see [SETUP.md](SETUP.md)); the script also supports `--impl` when run manually for advanced debugging—see `src/dsa_interview_packet/scripts/run_integration_tests.py`.

**Q7: Am I running tests locally or on Databricks?**  
See [AI_WORKFLOW.md](AI_WORKFLOW.md): local tests use small Parquet fixtures; the `run_integration_tests` bundle job validates against live seeded data in the workspace.

**Q8: Where do I put my implementation files?**  
See [README.md — Challenge 1](README.md#challenge-1-migrate-legacy-queries-to-lakehouse-sql-andor-lakeflow) and the **Project Structure** tree in [README.md](README.md). The directories already exist with placeholder files (`queries/lakehouse/README.md`, `pipelines/supply_chain_queries.py`); add your implementations there.

---

## Scope & Prioritization

**Q9: Four challenges in four hours. What's realistically expected?**  
Not everything. Prioritize as your candidate prompt describes.

**Q10: Must I pass all 22 tests per implementation?**  
No. Rewrite as many as you judge worthwhile and be ready to explain your choice. The tests exist to give you fast feedback, not as a checklist to exhaust.

**Q11: Is the executive visibility layer (Challenge 4) expected?**  
It is intentionally unbuilt. Whether it belongs in your four hours is a prioritization call, like every challenge here. Either way, your remediation plan should say how and when the business will see value.

---

## The "Partially Built" Artifacts

**Q12: How much cost analysis already exists?**  
The simulated system tables are populated with realistic data; no analysis, dashboard, or summary exists on top of them.

**Q13: What is the "partially built pipeline"?**  
The legacy pandas implementation is complete (22 functions). The Lakehouse SQL and Lakeflow pipeline implementations are empty. The test suite defines the expected output for each query.

---

## System Tables: Real vs. Simulated

**Q14–Q15: Which table names do I use in SQL? How do simulated tables map to `system.*`?**  
See [README.md — System Tables Simulation](README.md#system-tables-simulation) and [SCHEMA.md](SCHEMA.md). Query `{catalog}.{schema}.sim_*` in the lab; the README maps each `sim_*` table to its production `system.*` counterpart.

---

## The Steering Committee Presentation

**Q16: Is there a preferred tool for the 3-slide deck?**  
No. Use whatever you're comfortable with: Google Slides, PowerPoint, a Databricks notebook in presentation mode, or even a Lakeview Dashboard. The format is less important than the content.

**Q17: Will panelists role-play their personas?**  
Yes. Expect pointed questions from each persona's perspective.

**Q18: Do I have to follow the suggested time allocation?**  
No. The schedule in the prompt is a suggestion. Do protect the rehearsal time; candidates who skip it tend to present less clearly.

---

## Cost Analysis

**Q19: Is there a specific "original projection" figure?**  
No. The ≈2.8× is relative to the original projection for this phase, which is not in the data. You don't need it: use the system tables to show what is driving spend today and what it would cost after your remediation.

**Q20: Are cluster tags empty or inconsistently populated?**  
Tags are inconsistently populated. The `sim_cluster_info` table has no `custom_tags` column at all; cluster metadata is limited to name, source, creator, node types, and worker counts. In the `sim_billing_usage` table, the `custom_tags` field contains `job_id` and `job_name` for job compute rows (for example `{"job_id": "100001", "job_name": "legacy_pandas_daily"}`) but is empty (`{}`) for SQL warehouse query usage. There is no cost attribution by team, project, or business unit. Identifying this tagging gap and proposing a tagging standard is part of the exercise.

---

## ML Models

**Q21: Is there enough signal in MLflow to diagnose the quality issues?**  
Yes. The experiment logs, evaluation metrics, and model metadata contain enough information to identify root causes; the clues are in the data. You are not expected to retrain the models.

**Q22: Are the evaluation jobs running on a schedule, or do I trigger them?**  
None of the ML jobs are scheduled by default. After seeding and simulation, run the bundled ML pipeline once—for example `databricks bundle run -t <target> ml_train_eval_pipeline` (see [SETUP.md](SETUP.md)). That trains both models and populates the MLflow experiments. For debugging a single step, use the separate `train_demand_forecast`, `train_supplier_scoring`, or `evaluate_models` jobs as documented in [SETUP.md](SETUP.md).

---

## Deliverables

**Q23: Is the remediation plan a standalone document or part of the slides?**  
Either approach works. Some candidates make it a slide in the deck; others create a separate artifact and reference it during the presentation. The plan must be concrete and prioritized regardless of format.

**Q24: For "implement guardrails," do I create real Databricks resources or describe them?**  
Nothing is configured today: no budgets, policies, alerts, or tagging standards. Build at least one as a working artifact you can demo (for example a SQL alert on spend, job tags with a tagging standard, or a scheduled cost check). Free Edition has no account console or custom compute configuration, so budgets and compute/cluster policies can't be built there; describe them with specific configurations in your plan instead.

---

## Logistics

**Q25: Can I use the internet while I prepare?**  
Yes. You may use the internet freely (Databricks documentation, Stack Overflow, blog posts, etc.). There are no restrictions on reference material. The interview is open-book.

**Q26: Are there restrictions on which AI tools I can use?**  
No restrictions. Use Claude, ChatGPT, Copilot, or any tool you prefer. Be prepared to explain what they produced and the decisions you made on top of their output.

**Q27: Do I work locally or only in the workspace UI?**  
Either. You use your own machine: unzip the packet, install the Databricks CLI to deploy the bundle, and optionally run the local tests against small fixtures (Q5). The workspace holds the seeded data, MLflow, and jobs. Many candidates iterate locally and validate in the workspace; see [AI_WORKFLOW.md](AI_WORKFLOW.md).
