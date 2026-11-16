# DSA Interview Packet: Schema Reference

Complete schema documentation for all tables in the bundle.

---

## Source Tables

TPC-H source data. In the workspace, `seed_source_tables` copies these tables from `samples.tpch`; local tests load the same schemas from the Parquet fixtures under `tests/fixtures/data/`.

### region

| Column | Type |
|--------|------|
| r_regionkey | integer |
| r_name | string |
| r_comment | string |

- **Primary key**: `r_regionkey`
- **Foreign keys**: none
- **Row count**: 5 (SF=0.01), 5 (SF=5)

### nation

| Column | Type |
|--------|------|
| n_nationkey | integer |
| n_name | string |
| n_regionkey | integer |
| n_comment | string |

- **Primary key**: `n_nationkey`
- **Foreign keys**: `n_regionkey` -> `region.r_regionkey`
- **Row count**: 25 (SF=0.01), 25 (SF=5)

### supplier

| Column | Type |
|--------|------|
| s_suppkey | integer |
| s_name | string |
| s_address | string |
| s_nationkey | integer |
| s_phone | string |
| s_acctbal | decimal |
| s_comment | string |

- **Primary key**: `s_suppkey`
- **Foreign keys**: `s_nationkey` -> `nation.n_nationkey`
- **Row count**: 100 (SF=0.01), 50,000 (SF=5)

### part

| Column | Type |
|--------|------|
| p_partkey | integer |
| p_name | string |
| p_mfgr | string |
| p_brand | string |
| p_type | string |
| p_size | integer |
| p_container | string |
| p_retailprice | decimal |
| p_comment | string |

- **Primary key**: `p_partkey`
- **Foreign keys**: none
- **Row count**: 2,000 (SF=0.01), 1,000,000 (SF=5)

### partsupp

| Column | Type |
|--------|------|
| ps_partkey | integer |
| ps_suppkey | integer |
| ps_availqty | integer |
| ps_supplycost | decimal |
| ps_comment | string |

- **Primary key**: (`ps_partkey`, `ps_suppkey`)
- **Foreign keys**: `ps_partkey` -> `part.p_partkey`, `ps_suppkey` -> `supplier.s_suppkey`
- **Row count**: 8,000 (SF=0.01), 4,000,000 (SF=5)

### customer

| Column | Type |
|--------|------|
| c_custkey | integer |
| c_name | string |
| c_address | string |
| c_nationkey | integer |
| c_phone | string |
| c_acctbal | decimal |
| c_mktsegment | string |
| c_comment | string |

- **Primary key**: `c_custkey`
- **Foreign keys**: `c_nationkey` -> `nation.n_nationkey`
- **Row count**: 1,500 (SF=0.01), 750,000 (SF=5)

### orders

| Column | Type |
|--------|------|
| o_orderkey | integer |
| o_custkey | integer |
| o_orderstatus | string |
| o_totalprice | decimal |
| o_orderdate | date |
| o_orderpriority | string |
| o_clerk | string |
| o_shippriority | integer |
| o_comment | string |

- **Primary key**: `o_orderkey`
- **Foreign keys**: `o_custkey` -> `customer.c_custkey`
- **Row count**: 15,000 (SF=0.01), 7,500,000 (SF=5)

### lineitem

| Column | Type |
|--------|------|
| l_orderkey | integer |
| l_partkey | integer |
| l_suppkey | integer |
| l_linenumber | integer |
| l_quantity | decimal |
| l_extendedprice | decimal |
| l_discount | decimal |
| l_tax | decimal |
| l_returnflag | string |
| l_linestatus | string |
| l_shipdate | date |
| l_commitdate | date |
| l_receiptdate | date |
| l_shipinstruct | string |
| l_shipmode | string |
| l_comment | string |

- **Primary key**: (`l_orderkey`, `l_linenumber`)
- **Foreign keys**: `l_orderkey` -> `orders.o_orderkey`, (`l_partkey`, `l_suppkey`) -> `partsupp.(ps_partkey, ps_suppkey)`
- **Row count**: 60,175 (SF=0.01), ~30,000,000 (SF=5)

---

## Query Result Tables

Output of the 22 TPC-H analytic queries. Row counts are at SF=0.01.

### Q01: pricing_summary

| Column | Type |
|--------|------|
| l_returnflag | string |
| l_linestatus | string |
| sum_qty | decimal |
| sum_base_price | decimal |
| sum_disc_price | decimal |
| sum_charge | decimal |
| avg_qty | decimal |
| avg_price | decimal |
| avg_disc | decimal |
| count_order | long |

- **Primary key**: (`l_returnflag`, `l_linestatus`)
- **Row count**: 4

### Q02: min_cost_supplier

| Column | Type |
|--------|------|
| s_acctbal | decimal |
| s_name | string |
| n_name | string |
| p_partkey | integer |
| p_mfgr | string |
| s_address | string |
| s_phone | string |
| s_comment | string |

- **Primary key**: `p_partkey` (one row per cheapest-supplier-per-part)
- **Row count**: 4

### Q03: shipping_priority

| Column | Type |
|--------|------|
| l_orderkey | integer |
| revenue | decimal |
| o_orderdate | date |
| o_shippriority | integer |

- **Primary key**: `l_orderkey`
- **Row count**: 10

### Q04: order_priority

| Column | Type |
|--------|------|
| o_orderpriority | string |
| order_count | long |

- **Primary key**: `o_orderpriority`
- **Row count**: 5

### Q05: local_supplier_revenue

| Column | Type |
|--------|------|
| n_name | string |
| revenue | decimal |

- **Primary key**: `n_name`
- **Row count**: 5

### Q06: revenue_forecast

| Column | Type |
|--------|------|
| revenue | decimal |

- **Primary key**: none (single-row aggregate)
- **Row count**: 1

### Q07: bilateral_trade

| Column | Type |
|--------|------|
| supp_nation | string |
| cust_nation | string |
| l_year | integer |
| revenue | decimal |

- **Primary key**: (`supp_nation`, `cust_nation`, `l_year`)
- **Row count**: 4

### Q08: market_share

| Column | Type |
|--------|------|
| o_year | integer |
| mkt_share | decimal |

- **Primary key**: `o_year`
- **Row count**: 2

### Q09: product_profit

| Column | Type |
|--------|------|
| nation | string |
| o_year | integer |
| sum_profit | decimal |

- **Primary key**: (`nation`, `o_year`)
- **Row count**: 173

### Q10: returned_items

| Column | Type |
|--------|------|
| c_custkey | integer |
| c_name | string |
| revenue | decimal |
| c_acctbal | decimal |
| n_name | string |
| c_address | string |
| c_phone | string |
| c_comment | string |

- **Primary key**: `c_custkey`
- **Row count**: 20

### Q11: critical_inventory

| Column | Type |
|--------|------|
| ps_partkey | integer |
| value | decimal |

- **Primary key**: `ps_partkey`
- **Row count**: 359

### Q12: shipping_mode_analysis

| Column | Type |
|--------|------|
| l_shipmode | string |
| high_line_count | long |
| low_line_count | long |

- **Primary key**: `l_shipmode`
- **Row count**: 2

### Q13: customer_distribution

| Column | Type |
|--------|------|
| c_count | long |
| custdist | long |

- **Primary key**: `c_count`
- **Row count**: 32

### Q14: promotion_effect

| Column | Type |
|--------|------|
| promo_revenue | decimal |

- **Primary key**: none (single-row aggregate)
- **Row count**: 1

### Q15: top_supplier

| Column | Type |
|--------|------|
| s_suppkey | integer |
| s_name | string |
| s_address | string |
| s_phone | string |
| total_revenue | decimal |

- **Primary key**: `s_suppkey`
- **Row count**: 1

### Q16: supplier_parts_coverage

| Column | Type |
|--------|------|
| p_brand | string |
| p_type | string |
| p_size | integer |
| supplier_cnt | long |

- **Primary key**: (`p_brand`, `p_type`, `p_size`)
- **Row count**: 296

### Q17: small_order_revenue

| Column | Type |
|--------|------|
| avg_yearly | decimal |

- **Primary key**: none (single-row aggregate)
- **Row count**: 1

### Q18: large_volume_customers

| Column | Type |
|--------|------|
| c_name | string |
| c_custkey | integer |
| o_orderkey | integer |
| o_orderdate | date |
| o_totalprice | decimal |
| sum_qty | decimal |

- **Primary key**: (`c_custkey`, `o_orderkey`)
- **Row count**: 2

### Q19: discounted_revenue

| Column | Type |
|--------|------|
| revenue | decimal |

- **Primary key**: none (single-row aggregate)
- **Row count**: 1

### Q20: excess_inventory

| Column | Type |
|--------|------|
| s_name | string |
| s_address | string |

- **Primary key**: `s_name`
- **Row count**: 1

### Q21: supplier_reliability

| Column | Type |
|--------|------|
| s_name | string |
| numwait | long |

- **Primary key**: `s_name`
- **Row count**: 1

### Q22: sales_opportunity

| Column | Type |
|--------|------|
| cntrycode | string |
| numcust | long |
| totacctbal | decimal |

- **Primary key**: `cntrycode`
- **Row count**: 7

---

## Simulated System Tables

Seven Delta tables generated by `simulate_system_tables.py`. The time span is controlled by bundle variable `simulation_days` (unit tests use 30 days; default bundle values favor fast setup) with a deterministic seed.

### sim_billing_usage

Mirrors `system.billing.usage`.

| Column | Type | Description |
|--------|------|-------------|
| record_id | string | Unique billing record ID |
| account_id | string | Databricks account ID |
| workspace_id | string | Workspace ID |
| sku_name | string | SKU (ALL_PURPOSE_COMPUTE, SQL_SERVERLESS, etc.) |
| cloud | string | Cloud provider (AWS) |
| usage_start_time | timestamp | Usage period start |
| usage_end_time | timestamp | Usage period end |
| usage_date | date | Usage calendar date |
| custom_tags | string | JSON tags containing job_id and job_name |
| usage_quantity | double | DBU quantity consumed |
| usage_metadata_cluster_id | string | Cluster ID (null for serverless) |
| usage_metadata_job_id | string | Job ID |
| usage_metadata_notebook_id | string | Notebook ID (always null) |
| usage_metadata_warehouse_id | string | Warehouse ID (set for SQL_SERVERLESS) |

- **Primary key**: `record_id`

### sim_cluster_info

Mirrors `system.compute.clusters`.

| Column | Type | Description |
|--------|------|-------------|
| account_id | string | Databricks account ID |
| workspace_id | string | Workspace ID |
| cluster_id | string | Unique cluster identifier |
| cluster_name | string | Human-readable cluster name |
| cluster_source | string | Creation source (UI, JOB, SQL_WAREHOUSE, etc.) |
| creator | string | Email of cluster creator |
| driver_node_type | string | Driver instance type |
| worker_node_type | string | Worker instance type |
| num_workers | integer | Fixed worker count |
| autoscale_min_workers | integer | Autoscale minimum (null for serverless) |
| autoscale_max_workers | integer | Autoscale maximum (null for serverless) |

- **Primary key**: `cluster_id`

### sim_job_run_timeline

Mirrors `system.lakeflow.job_run_timeline`.

| Column | Type | Description |
|--------|------|-------------|
| account_id | string | Databricks account ID |
| workspace_id | string | Workspace ID |
| job_id | long | Job identifier |
| run_id | long | Run identifier |
| run_name | string | Run name |
| result_state | string | Outcome (SUCCESS, FAILED) |
| start_time | timestamp | Run start |
| end_time | timestamp | Run end |
| job_name | string | Job name |
| creator | string | Email of job creator |
| duration_minutes | double | Run duration in minutes |

- **Primary key**: `run_id`

### sim_query_history

Mirrors `system.query.history`.

| Column | Type | Description |
|--------|------|-------------|
| account_id | string | Databricks account ID |
| workspace_id | string | Workspace ID |
| statement_id | string | Unique statement identifier |
| executed_by | string | Email of query executor |
| statement_text | string | SQL text |
| status | string | Status (FINISHED, FAILED, CANCELED) |
| execution_start_time | timestamp | Query start |
| execution_end_time | timestamp | Query end |
| duration_ms | long | Duration in milliseconds |
| rows_produced | long | Number of result rows |
| warehouse_id | string | SQL warehouse ID |
| warehouse_name | string | SQL warehouse name |
| source | string | Query source (DASHBOARD, NOTEBOOK, DBSQL_EDITOR, API) |

- **Primary key**: `statement_id`

### sim_pipeline_events

Mirrors `system.lakeflow.pipeline_events`.

| Column | Type | Description |
|--------|------|-------------|
| account_id | string | Databricks account ID |
| workspace_id | string | Workspace ID |
| pipeline_id | string | Pipeline identifier |
| pipeline_name | string | Pipeline name |
| update_id | string | Update identifier |
| event_type | string | Event type (STARTING, RUNNING, COMPLETED, FAILED) |
| timestamp | timestamp | Event timestamp |
| message | string | Event message |

- **Primary key**: (`pipeline_id`, `update_id`, `event_type`, `timestamp`)

### sim_audit_logs

Mirrors `system.access.audit`.

| Column | Type | Description |
|--------|------|-------------|
| account_id | string | Databricks account ID |
| workspace_id | string | Workspace ID |
| event_time | timestamp | When the event occurred |
| service_name | string | Service (jobs, clusters, databrickssql, etc.) |
| action_name | string | Action (runNow, start, executeStatement, etc.) |
| request_id | string | Unique request identifier |
| user_identity_email | string | Email of acting user |
| source_ip_address | string | Client IP address |
| response_status_code | integer | HTTP status code |

- **Primary key**: `request_id`

### sim_table_lineage

Mirrors `system.access.table_lineage`.

| Column | Type | Description |
|--------|------|-------------|
| account_id | string | Databricks account ID |
| workspace_id | string | Workspace ID |
| source_table_full_name | string | Fully qualified source table name |
| target_table_full_name | string | Fully qualified target table name |
| source_type | string | Source entity type (TABLE) |
| target_type | string | Target entity type (TABLE, VIEW) |
| created_by | string | Email of lineage creator |
| event_time | timestamp | When the lineage was recorded |

- **Primary key**: (`source_table_full_name`, `target_table_full_name`)

---

## Cross-Table Relationships

Referential integrity constraints between simulated system tables.

| Source Column | Target Column | Relationship |
|--------------|---------------|--------------|
| sim_billing_usage.usage_metadata_job_id | sim_job_run_timeline.job_id | Billing rows reference the job that incurred cost |
| sim_billing_usage.usage_metadata_cluster_id | sim_cluster_info.cluster_id | Billing rows reference the cluster used (null for serverless) |
| sim_billing_usage.usage_metadata_warehouse_id | sim_query_history.warehouse_id | Warehouse billing aligns with query history |
| sim_job_run_timeline.job_id | sim_cluster_info.cluster_id | Every job maps to a cluster via the internal job-cluster map |
| sim_audit_logs.user_identity_email | sim_job_run_timeline.creator | Audit events use the same canonical user pool |
| sim_audit_logs.event_time | sim_job_run_timeline.start_time | Job audit events (runNow) are timestamped at job start |
| sim_audit_logs.event_time | sim_pipeline_events.timestamp | Pipeline audit events (runNow) match pipeline STARTING timestamps |
| sim_audit_logs.event_time | sim_query_history.execution_start_time | Warehouse audit events match query start times |
| sim_pipeline_events.pipeline_id | sim_billing_usage (DLT_SERVERLESS rows) | DLT billing rows correspond to pipeline runs |
| sim_table_lineage.created_by | sim_job_run_timeline.creator | Lineage entries reference the same canonical users |

---

## ML Model Artifacts

Two ML models are trained on the TPC-H source data and logged to MLflow experiments.
Both use a temporal train/test split (train on orders before 1997-01-01, test on 1997+).

### demand_forecast

GBT regression model predicting order line quantity (`l_quantity`).

- **MLflow artifact path**: `demand_forecast_model` (logged to the run; also registered in the UC Model Registry as `<catalog>.<schema>.demand_forecast` unless disabled with `--no-uc-model-registry`)
- **Algorithm**: `GBTRegressor` (maxIter=50, maxDepth=5, seed=42)
- **MLflow experiment**: `/Users/{user}/dsa_interview_packet_demand_forecast`
- **Label**: `l_quantity` (double)
- **Training metrics**: RMSE, MAE, R2
- **Evaluation metrics**: `demand_forecast_train_rmse`, `demand_forecast_eval_rmse`, `demand_forecast_eval_mae`, `demand_forecast_eval_r2`, `demand_forecast_rmse_degradation_pct`

| Feature | Type | Source |
|---------|------|--------|
| p_brand | categorical (StringIndexer) | part.p_brand |
| p_type | categorical (StringIndexer) | part.p_type |
| p_container | categorical (StringIndexer) | part.p_container |
| c_mktsegment | categorical (StringIndexer) | customer.c_mktsegment |
| p_size | double | part.p_size |
| p_retailprice | double | part.p_retailprice |
| c_nationkey | double | customer.c_nationkey |
| s_nationkey | double | supplier.s_nationkey |
| s_acctbal | double | supplier.s_acctbal |
| order_month | double | month(orders.o_orderdate) |
| order_quarter | double | quarter(orders.o_orderdate) |
| order_year | double | year(orders.o_orderdate) |
| order_day_of_week | double | dayofweek(orders.o_orderdate) |
| avg_qty_part_90d | double | rolling 90-day avg quantity per part |
| avg_qty_cust_90d | double | rolling 90-day avg quantity per customer |

### supplier_reliability_score

Random forest classifier predicting whether a lineitem will be delivered late (`l_receiptdate > l_commitdate`).

- **MLflow artifact path**: `supplier_scoring_model` (logged to the run; also registered in the UC Model Registry as `<catalog>.<schema>.supplier_scoring` unless disabled with `--no-uc-model-registry`)
- **Algorithm**: `RandomForestClassifier` (numTrees=100, maxDepth=8, seed=42)
- **MLflow experiment**: `/Users/{user}/dsa_interview_packet_supplier_scoring`
- **Label**: 1.0 if late delivery, 0.0 otherwise
- **Training metrics**: AUC, accuracy, weightedPrecision, weightedRecall, F1
- **Evaluation metrics**: `supplier_scoring_accuracy`, `supplier_scoring_f1`, `supplier_scoring_auc`, `supplier_scoring_recall_late`, `supplier_scoring_recall_ontime`
- **Known issue**: class imbalance (~30% late) causes high aggregate accuracy but poor recall on the minority "late" class

| Feature | Type | Source |
|---------|------|--------|
| p_brand | categorical (StringIndexer) | part.p_brand |
| p_type | categorical (StringIndexer) | part.p_type |
| o_orderpriority | categorical (StringIndexer) | orders.o_orderpriority |
| l_shipmode | categorical (StringIndexer) | lineitem.l_shipmode |
| l_shipinstruct | categorical (StringIndexer) | lineitem.l_shipinstruct |
| s_nationkey | double | supplier.s_nationkey |
| s_acctbal | double | supplier.s_acctbal |
| p_size | double | part.p_size |
| l_quantity | double | lineitem.l_quantity |
| l_extendedprice | double | lineitem.l_extendedprice |
| l_discount | double | lineitem.l_discount |
| supp_late_rate_180d | double | rolling 180-day late delivery rate per supplier |

### model_evaluation

The `ml_train_eval_pipeline` job (default) or the on-demand `evaluate_models` job runs both models on the 1997-1998 evaluation period and logs metrics to a shared MLflow experiment.

- **MLflow experiment**: `/Users/{user}/dsa_interview_packet_model_evaluation`
- **Logged params**: catalog, schema, eval_period, training_cutoff
- **Logged metrics**: all metrics from both models above (10 total)
