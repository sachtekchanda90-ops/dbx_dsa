"""Evaluate trained models on recent data and log metrics to MLflow.

Default path: use ``ml_train_eval_pipeline`` (runs both trainings + this).
Run this job alone when debugging only evaluation:

    databricks bundle run -t dev evaluate_models

Loads the demand forecast and supplier scoring models from MLflow
experiment runs, runs inference on the most recent data (1997-1998
orders), and logs evaluation metrics back to MLflow.

If the models have not been trained yet, the script computes approximate
metrics directly from the data instead of loading a model.
"""

from __future__ import annotations

import argparse
from typing import Any

import ensure_src_path  # noqa: F401  # pylint: disable=unused-import  # must precede dsa_interview_packet imports
import mlflow
from dsa_interview_packet.categorical_hash import (
    DEMAND_FORECAST_HASH_COLS,
    SUPPLIER_SCORING_HASH_COLS,
    with_categorical_hash_buckets,
)
from dsa_interview_packet.lineitem_sample import sample_lineitem
from dsa_interview_packet.mlflow_serverless import ensure_mlflow_dfs_tmp
from dsa_interview_packet.sql_identifiers import table_fqn
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_spark() -> SparkSession:
    """Return the active Spark session."""
    return SparkSession.builder.getOrCreate()


def _join_base_lineitem_orders_part_supplier(
    spark: SparkSession,
    catalog: str,
    schema: str,
    lineitem: DataFrame,
) -> DataFrame:
    """Shared join for demand and supplier eval (computed once, then persisted)."""
    orders = spark.table(table_fqn(catalog, schema, "orders"))
    part = spark.table(table_fqn(catalog, schema, "part"))
    supplier = spark.table(table_fqn(catalog, schema, "supplier"))
    return (
        lineitem.alias("l")
        .join(orders.alias("o"), F.col("l.l_orderkey") == F.col("o.o_orderkey"))
        .join(part.alias("p"), F.col("l.l_partkey") == F.col("p.p_partkey"))
        .join(supplier.alias("s"), F.col("l.l_suppkey") == F.col("s.s_suppkey"))
    )


def _find_latest_model_run(experiment_name: str, _artifact_path: str) -> str | None:
    """Find the latest MLflow run that logged a model artifact.

    Returns the run ID if found, None otherwise.
    """
    try:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment is None:
            return None
        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"],
            max_results=1,
        )
        if runs.empty:
            return None
        rid = runs.iloc[0]["run_id"]
        return None if rid is None else str(rid)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Demand-forecast evaluation
# ---------------------------------------------------------------------------

def _evaluate_demand_forecast(
    spark: SparkSession,
    catalog: str,
    schema: str,
    base_lops: DataFrame,
    *,
    dfs_tmpdir: str,
    experiment_name: str | None = None,
) -> dict[str, Any]:
    """Evaluate demand forecast model on 1997-1998 data.

    If the model is registered, load it and run inference. Otherwise,
    compute simulated metrics directly from the data to approximate
    what a degraded model would produce.

    ``base_lops`` is lineitem ⋈ orders ⋈ part ⋈ supplier (shared with supplier eval).
    """
    print("--- Demand Forecast Evaluation ---")

    customer = spark.table(table_fqn(catalog, schema, "customer"))
    partsupp = spark.table(table_fqn(catalog, schema, "partsupp"))

    # Build feature set (mirrors training pipeline)
    df = (
        base_lops.join(
            customer.alias("c"),
            F.col("o.o_custkey") == F.col("c.c_custkey"),
        ).join(
            partsupp.alias("ps"),
            (F.col("l.l_partkey") == F.col("ps.ps_partkey"))
            & (F.col("l.l_suppkey") == F.col("ps.ps_suppkey")),
        )
    )

    # Time features
    df = (
        df.withColumn("order_month", F.month("o.o_orderdate"))
        .withColumn("order_quarter", F.quarter("o.o_orderdate"))
        .withColumn("order_year", F.year("o.o_orderdate"))
        .withColumn("order_day_of_week", F.dayofweek("o.o_orderdate"))
    )

    # Historical aggregates
    part_window = (
        Window.partitionBy("l.l_partkey")
        .orderBy(F.unix_timestamp(F.col("o.o_orderdate")))
        .rangeBetween(-45 * 86400, -1)
    )
    df = df.withColumn(
        "avg_qty_part_90d", F.avg("l.l_quantity").over(part_window)
    ).fillna(0.0, subset=["avg_qty_part_90d"])

    cust_window = (
        Window.partitionBy("o.o_custkey")
        .orderBy(F.unix_timestamp(F.col("o.o_orderdate")))
        .rangeBetween(-45 * 86400, -1)
    )
    df = df.withColumn(
        "avg_qty_cust_90d", F.avg("l.l_quantity").over(cust_window)
    ).fillna(0.0, subset=["avg_qty_cust_90d"])

    df = df.select(
        F.col("l.l_quantity").alias("label"),
        F.col("o.o_orderdate").alias("order_date"),
        F.col("p.p_brand").alias("p_brand"),
        F.col("p.p_type").alias("p_type"),
        F.col("p.p_container").alias("p_container"),
        F.col("c.c_mktsegment").alias("c_mktsegment"),
        F.col("p.p_size").cast("double").alias("p_size"),
        F.col("p.p_retailprice").cast("double").alias("p_retailprice"),
        F.col("c.c_nationkey").cast("double").alias("c_nationkey"),
        F.col("s.s_nationkey").cast("double").alias("s_nationkey"),
        F.col("s.s_acctbal").cast("double").alias("s_acctbal"),
        F.col("order_month").cast("double"),
        F.col("order_quarter").cast("double"),
        F.col("order_year").cast("double"),
        F.col("order_day_of_week").cast("double"),
        F.col("avg_qty_part_90d").cast("double"),
        F.col("avg_qty_cust_90d").cast("double"),
    )
    df = with_categorical_hash_buckets(df, DEMAND_FORECAST_HASH_COLS)

    # Split into the same training period and the newer evaluation period
    train_period = df.filter(F.col("order_date") < "1997-01-01")
    eval_period = df.filter(F.col("order_date") >= "1997-01-01")

    train_count = train_period.count()
    eval_count = eval_period.count()
    print(f"  Training period rows: {train_count}")
    print(f"  Evaluation period rows (1997-1998): {eval_count}")

    if experiment_name is None:
        user_name = spark.sql("SELECT current_user()").collect()[0][0]
        experiment_name = f"/Users/{user_name}/dsa_interview_packet_demand_forecast"
    demand_experiment = experiment_name
    model_run_id = _find_latest_model_run(demand_experiment, "demand_forecast_model")

    if model_run_id:
        print(f"  Loading model from MLflow run: {model_run_id}")
        from pyspark.ml.evaluation import RegressionEvaluator

        model_uri = f"runs:/{model_run_id}/demand_forecast_model"
        model = mlflow.spark.load_model(model_uri, dfs_tmpdir=dfs_tmpdir)

        train_preds = model.transform(train_period.drop("order_date"))
        eval_preds = model.transform(eval_period.drop("order_date"))

        rmse_eval_obj = RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName="rmse"
        )
        mae_eval_obj = RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName="mae"
        )
        r2_eval_obj = RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName="r2"
        )

        train_rmse = rmse_eval_obj.evaluate(train_preds)
        eval_rmse = rmse_eval_obj.evaluate(eval_preds)
        eval_mae = mae_eval_obj.evaluate(eval_preds)
        eval_r2 = r2_eval_obj.evaluate(eval_preds)
    else:
        print("  No trained model found -- simulating metrics from data.")
        # Simulate: use the training-period mean as a naive predictor, then
        # compute actual error on both periods. The eval period will naturally
        # show degradation due to distribution shift in the 1997-1998 data.
        train_stats = train_period.select(
            F.avg("label").alias("mean_qty"),
            F.stddev("label").alias("std_qty"),
        ).collect()[0]
        train_mean = float(train_stats["mean_qty"])
        train_std = float(train_stats["std_qty"])

        # On training data a reasonable model achieves roughly the std-dev as
        # RMSE (baseline). We simulate a model that's decent on training data.
        train_rmse = train_std * 0.55

        # On the evaluation period, we compute actual variance from the mean
        # to simulate drift. The 1997-1998 data has shifted ordering patterns
        # so the RMSE is roughly 20% worse.
        eval_stats = eval_period.select(
            F.sqrt(F.avg((F.col("label") - F.lit(train_mean)) ** 2)).alias(
                "rmse_from_train_mean"
            ),
            F.avg(F.abs(F.col("label") - F.lit(train_mean))).alias(
                "mae_from_train_mean"
            ),
        ).collect()[0]

        # Simulated model error: the model is better than raw mean but still
        # degrades ~20% relative to its training-period performance.
        eval_rmse = train_rmse * 1.20
        eval_mae = float(eval_stats["mae_from_train_mean"]) * 0.65
        eval_r2 = 1 - (eval_rmse**2) / (train_std**2)

    metrics = {
        "demand_forecast_train_rmse": round(train_rmse, 4),
        "demand_forecast_eval_rmse": round(eval_rmse, 4),
        "demand_forecast_eval_mae": round(eval_mae, 4),
        "demand_forecast_eval_r2": round(eval_r2, 4),
        "demand_forecast_rmse_degradation_pct": round(
            100 * (eval_rmse - train_rmse) / train_rmse, 2
        ),
    }

    for key, val in metrics.items():
        print(f"  {key}: {val}")

    return metrics


# ---------------------------------------------------------------------------
# Supplier-scoring evaluation
# ---------------------------------------------------------------------------

def _evaluate_supplier_scoring(
    base_lops: DataFrame,
    *,
    dfs_tmpdir: str,
    experiment_name: str | None = None,
) -> dict[str, Any]:
    """Evaluate supplier scoring model on 1997-1998 data.

    Logs aggregate accuracy (which looks acceptable) but hides the
    poor per-class recall on the minority "late" class.

    ``base_lops`` is lineitem ⋈ orders ⋈ part ⋈ supplier (shared with demand eval).
    """
    print("\n--- Supplier Scoring Evaluation ---")

    df = base_lops

    df = df.withColumn(
        "label",
        F.when(
            F.col("l.l_receiptdate") > F.col("l.l_commitdate"), 1.0
        ).otherwise(0.0),
    )

    supp_window = (
        Window.partitionBy("l.l_suppkey")
        .orderBy(F.unix_timestamp(F.col("o.o_orderdate")))
        .rangeBetween(-90 * 86400, -1)
    )
    df = df.withColumn(
        "supp_late_rate_180d", F.avg("label").over(supp_window)
    ).fillna(0.0, subset=["supp_late_rate_180d"])

    df = df.select(
        F.col("label"),
        F.col("o.o_orderdate").alias("order_date"),
        F.col("p.p_brand").alias("p_brand"),
        F.col("p.p_type").alias("p_type"),
        F.col("o.o_orderpriority").alias("o_orderpriority"),
        F.col("l.l_shipmode").alias("l_shipmode"),
        F.col("l.l_shipinstruct").alias("l_shipinstruct"),
        F.col("s.s_nationkey").cast("double").alias("s_nationkey"),
        F.col("s.s_acctbal").cast("double").alias("s_acctbal"),
        F.col("p.p_size").cast("double").alias("p_size"),
        F.col("l.l_quantity").cast("double").alias("l_quantity"),
        F.col("l.l_extendedprice").cast("double").alias("l_extendedprice"),
        F.col("l.l_discount").cast("double").alias("l_discount"),
        F.col("supp_late_rate_180d").cast("double"),
    )
    df = with_categorical_hash_buckets(df, SUPPLIER_SCORING_HASH_COLS)

    eval_df = df.filter(F.col("order_date") >= "1997-01-01")
    eval_count = eval_df.count()
    late_count = eval_df.filter(F.col("label") == 1.0).count()
    late_pct = 100 * late_count / eval_count if eval_count > 0 else 0
    print(f"  Evaluation rows: {eval_count}")
    print(f"  Late deliveries: {late_count} ({late_pct:.1f}%)")

    spark = base_lops.sparkSession
    if experiment_name is None:
        user_name = spark.sql("SELECT current_user()").collect()[0][0]
        experiment_name = f"/Users/{user_name}/dsa_interview_packet_supplier_scoring"
    scoring_experiment = experiment_name
    model_run_id = _find_latest_model_run(scoring_experiment, "supplier_scoring_model")

    if model_run_id:
        print(f"  Loading model from MLflow run: {model_run_id}")
        from pyspark.ml.evaluation import (
            BinaryClassificationEvaluator,
            MulticlassClassificationEvaluator,
        )

        model_uri = f"runs:/{model_run_id}/supplier_scoring_model"
        model = mlflow.spark.load_model(model_uri, dfs_tmpdir=dfs_tmpdir)
        predictions = model.transform(eval_df.drop("order_date"))

        acc_eval = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="accuracy"
        )
        f1_eval = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="f1"
        )
        auc_eval = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        )

        accuracy = acc_eval.evaluate(predictions)
        f1 = f1_eval.evaluate(predictions)
        auc = auc_eval.evaluate(predictions)

        # Per-class recall (the hidden problem)
        late_preds = predictions.filter(F.col("label") == 1.0)
        late_correct = late_preds.filter(F.col("prediction") == 1.0).count()
        late_total = late_preds.count()
        recall_late = late_correct / late_total if late_total > 0 else 0.0

        ontime_preds = predictions.filter(F.col("label") == 0.0)
        ontime_correct = ontime_preds.filter(
            F.col("prediction") == 0.0
        ).count()
        ontime_total = ontime_preds.count()
        recall_ontime = (
            ontime_correct / ontime_total if ontime_total > 0 else 0.0
        )
    else:
        print("  No trained model found -- simulating metrics from data.")
        # Simulate a model that predicts the majority class ("on-time") most
        # of the time. This gives decent overall accuracy but terrible recall
        # on the late class -- a classic class-imbalance problem.
        late_rate = late_count / eval_count if eval_count > 0 else 0.3

        # The model learns to mostly predict "on-time" because that's ~70%
        # of the data. Only a few late items get correctly flagged.
        recall_ontime = 0.92
        recall_late = 0.15

        # Overall accuracy is weighted by class prevalence, so it looks OK
        accuracy = recall_ontime * (1 - late_rate) + recall_late * late_rate
        # F1 is also dominated by the majority class
        precision_ontime = (1 - late_rate) * recall_ontime / (
            (1 - late_rate) * recall_ontime + late_rate * (1 - recall_late)
        )
        precision_late = late_rate * recall_late / (
            late_rate * recall_late + (1 - late_rate) * (1 - recall_ontime)
        )
        f1_ontime = (
            2 * precision_ontime * recall_ontime
            / (precision_ontime + recall_ontime)
            if (precision_ontime + recall_ontime) > 0
            else 0.0
        )
        f1_late = (
            2 * precision_late * recall_late
            / (precision_late + recall_late)
            if (precision_late + recall_late) > 0
            else 0.0
        )
        f1 = (1 - late_rate) * f1_ontime + late_rate * f1_late
        auc = 0.62

    # Intentionally log only aggregate metrics -- no per-class breakdown
    # is surfaced in the MLflow summary. The candidate should notice that
    # recall_late is buried and not flagged.
    metrics = {
        "supplier_scoring_accuracy": round(accuracy, 4),
        "supplier_scoring_f1": round(f1, 4),
        "supplier_scoring_auc": round(auc, 4),
        # These are logged but not highlighted or alerted on
        "supplier_scoring_recall_late": round(recall_late, 4),
        "supplier_scoring_recall_ontime": round(recall_ontime, 4),
    }

    for key, val in metrics.items():
        print(f"  {key}: {val}")

    return metrics


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(
    catalog: str,
    schema: str,
    *,
    mlflow_volume_catalog: str | None = None,
    mlflow_volume_schema: str | None = None,
    mlflow_volume_name: str = "mlflow_scratch",
    lineitem_sample_fraction: float = 0.008,
    demand_experiment_name: str | None = None,
    scoring_experiment_name: str | None = None,
    evaluation_experiment_name: str | None = None,
) -> None:
    """Run model evaluation and log all metrics to MLflow.

    The three experiment names are passed by the bundle job (resolved managed
    experiments); each falls back to its flat /Users/<user>/... name for local runs.
    """
    vol_cat = mlflow_volume_catalog if mlflow_volume_catalog is not None else catalog
    vol_sch = mlflow_volume_schema if mlflow_volume_schema is not None else schema
    spark = _get_spark()
    _dfs_tmp = ensure_mlflow_dfs_tmp(
        spark,
        vol_cat,
        vol_sch,
        volume=mlflow_volume_name,
    )

    _lineitem = spark.table(table_fqn(catalog, schema, "lineitem"))
    _lineitem = sample_lineitem(_lineitem, lineitem_sample_fraction)
    # Serverless Spark Connect does not support DataFrame.persist() (NOT_SUPPORTED_WITH_SERVERLESS).
    base_lops = _join_base_lineitem_orders_part_supplier(
        spark, catalog, schema, _lineitem
    )

    if evaluation_experiment_name is None:
        user_name = spark.sql("SELECT current_user()").collect()[0][0]
        evaluation_experiment_name = f"/Users/{user_name}/dsa_interview_packet_model_evaluation"
    mlflow.set_experiment(evaluation_experiment_name)
    print(f"MLflow experiment: {evaluation_experiment_name}")

    with mlflow.start_run(run_name="scheduled_model_evaluation"):
        mlflow.log_param("catalog", catalog)
        mlflow.log_param("schema", schema)
        mlflow.log_param("eval_period", "1997-01-01 to 1998-12-31")
        mlflow.log_param("training_cutoff", "1997-01-01")
        mlflow.log_param("lineitem_sample_fraction", lineitem_sample_fraction)

        # --- Demand forecast ----------------------------------------------
        demand_metrics = _evaluate_demand_forecast(
            spark,
            catalog,
            schema,
            base_lops,
            dfs_tmpdir=_dfs_tmp,
            experiment_name=demand_experiment_name,
        )
        for key, val in demand_metrics.items():
            mlflow.log_metric(key, val)

        # --- Supplier scoring ---------------------------------------------
        supplier_metrics = _evaluate_supplier_scoring(
            base_lops,
            dfs_tmpdir=_dfs_tmp,
            experiment_name=scoring_experiment_name,
        )
        for key, val in supplier_metrics.items():
            mlflow.log_metric(key, val)

    # Intentionally bland summary -- does NOT flag the RMSE degradation or
    # the poor late-class recall. No alerting, no thresholds, no Lakehouse
    # Monitor. The candidate should notice this observability gap.
    print("\nEvaluation complete. Metrics logged to MLflow.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Evaluate trained models on recent supply chain data"
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument(
        "--mlflow-volume-catalog",
        default=None,
        help="UC catalog for the MLflow dfs_tmpdir volume (default: same as --catalog).",
    )
    parser.add_argument(
        "--mlflow-volume-schema",
        default=None,
        help="UC schema for the MLflow dfs_tmpdir volume (default: same as --schema).",
    )
    parser.add_argument(
        "--mlflow-volume-name",
        default="mlflow_scratch",
        help="Managed volume name for mlflow.spark load_model dfs_tmpdir.",
    )
    parser.add_argument(
        "--lineitem-sample-fraction",
        type=float,
        default=0.008,
        help="Random fraction of lineitem rows before joins (1.0 = full data).",
    )
    parser.add_argument(
        "--demand-experiment-name",
        default=None,
        help="Demand-forecast experiment to read (bundle passes the managed experiment).",
    )
    parser.add_argument(
        "--scoring-experiment-name",
        default=None,
        help="Supplier-scoring experiment to read (bundle passes the managed experiment).",
    )
    parser.add_argument(
        "--evaluation-experiment-name",
        default=None,
        help="Experiment to write evaluation metrics to (bundle passes the managed experiment).",
    )
    args = parser.parse_args()
    main(
        args.catalog,
        args.schema,
        mlflow_volume_catalog=args.mlflow_volume_catalog,
        mlflow_volume_schema=args.mlflow_volume_schema,
        mlflow_volume_name=args.mlflow_volume_name,
        lineitem_sample_fraction=args.lineitem_sample_fraction,
        demand_experiment_name=args.demand_experiment_name,
        scoring_experiment_name=args.scoring_experiment_name,
        evaluation_experiment_name=args.evaluation_experiment_name,
    )
