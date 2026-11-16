"""Train a demand forecasting model on supply chain data.

Default path: use ``ml_train_eval_pipeline`` (runs this plus supplier train + evaluate).
Run this job alone when debugging only demand training:

    databricks bundle run -t dev train_demand_forecast

Predicts ``l_quantity`` (order line quantity) for future orders using
features engineered from parts, customers, suppliers, and historical
order patterns. Uses SparkML GBTRegressor and logs metrics/model
to MLflow.
"""

from __future__ import annotations

import argparse
import os
from typing import cast

import ensure_src_path  # noqa: F401  # pylint: disable=unused-import  # must precede dsa_interview_packet imports
import mlflow
from dsa_interview_packet.categorical_hash import (
    DEMAND_FORECAST_HASH_COLS,
    with_categorical_hash_buckets,
)
from dsa_interview_packet.lineitem_sample import sample_lineitem
from dsa_interview_packet.ml_tree_config import SPARK_TREE_MAX_BINS
from dsa_interview_packet.mlflow_serverless import ensure_mlflow_dfs_tmp
from dsa_interview_packet.mlflow_uc import (
    UC_REGISTRY_URI,
    resolve_registered_model_name,
)
from dsa_interview_packet.sql_identifiers import table_fqn
from mlflow.models import infer_signature
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _build_features(
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    lineitem_sample_fraction: float = 1.0,
    log_row_counts: bool = False,
) -> DataFrame:
    """Join source tables and engineer features for demand forecasting."""
    print("Loading source tables ...")
    lineitem = spark.table(table_fqn(catalog, schema, "lineitem"))
    lineitem = sample_lineitem(lineitem, lineitem_sample_fraction)
    orders = spark.table(table_fqn(catalog, schema, "orders"))
    part = spark.table(table_fqn(catalog, schema, "part"))
    customer = spark.table(table_fqn(catalog, schema, "customer"))
    supplier = spark.table(table_fqn(catalog, schema, "supplier"))
    partsupp = spark.table(table_fqn(catalog, schema, "partsupp"))

    # --- Base join ----------------------------------------------------------
    print("Joining tables ...")
    df = (
        lineitem.alias("l")
        .join(orders.alias("o"), F.col("l.l_orderkey") == F.col("o.o_orderkey"))
        .join(part.alias("p"), F.col("l.l_partkey") == F.col("p.p_partkey"))
        .join(customer.alias("c"), F.col("o.o_custkey") == F.col("c.c_custkey"))
        .join(supplier.alias("s"), F.col("l.l_suppkey") == F.col("s.s_suppkey"))
        .join(
            partsupp.alias("ps"),
            (F.col("l.l_partkey") == F.col("ps.ps_partkey"))
            & (F.col("l.l_suppkey") == F.col("ps.ps_suppkey")),
        )
    )

    # --- Time features ------------------------------------------------------
    print("Engineering time features ...")
    df = (
        df.withColumn("order_month", F.month("o.o_orderdate"))
        .withColumn("order_quarter", F.quarter("o.o_orderdate"))
        .withColumn("order_year", F.year("o.o_orderdate"))
        .withColumn("order_day_of_week", F.dayofweek("o.o_orderdate"))
    )

    # --- Historical aggregates ----------------------------------------------
    print("Computing historical aggregates ...")
    # Average quantity for the same part in the rolling window before this order
    # (45-day window keeps E2E fast; widen for production-quality features).
    part_window = (
        Window.partitionBy("l.l_partkey")
        .orderBy(F.unix_timestamp(F.col("o.o_orderdate")))
        .rangeBetween(-45 * 86400, -1)
    )
    df = df.withColumn(
        "avg_qty_part_90d", F.avg("l.l_quantity").over(part_window)
    ).fillna(0.0, subset=["avg_qty_part_90d"])

    # Average quantity for the same customer
    cust_window = (
        Window.partitionBy("o.o_custkey")
        .orderBy(F.unix_timestamp(F.col("o.o_orderdate")))
        .rangeBetween(-45 * 86400, -1)
    )
    df = df.withColumn(
        "avg_qty_cust_90d", F.avg("l.l_quantity").over(cust_window)
    ).fillna(0.0, subset=["avg_qty_cust_90d"])

    # --- Select final feature columns --------------------------------------
    df = df.select(
        F.col("l.l_quantity").alias("label"),
        F.col("o.o_orderdate").alias("order_date"),
        # Categorical
        F.col("p.p_brand").alias("p_brand"),
        F.col("p.p_type").alias("p_type"),
        F.col("p.p_container").alias("p_container"),
        F.col("c.c_mktsegment").alias("c_mktsegment"),
        # Numeric
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

    if log_row_counts:
        _n = df.count()
        print(f"Feature dataframe count: {_n}")
    else:
        print(
            "Feature dataframe ready (row count skipped; set "
            "DSA_INTERVIEW_LOG_ROW_COUNTS=1 or --log-row-counts to count)."
        )
    return cast(DataFrame, with_categorical_hash_buckets(df, DEMAND_FORECAST_HASH_COLS))


def main(
    catalog: str,
    schema: str,
    *,
    mlflow_volume_catalog: str | None = None,
    mlflow_volume_schema: str | None = None,
    mlflow_volume_name: str = "mlflow_scratch",
    lineitem_sample_fraction: float = 0.008,
    log_row_counts: bool = False,
    register_to_uc: bool = True,
    experiment_name: str | None = None,
) -> None:
    """Train demand forecast model and log to MLflow.

    When ``register_to_uc`` is set, the model is also registered in the UC Model
    Registry as ``catalog.schema.demand_forecast``; otherwise it is logged as a
    run artifact only.
    """
    vol_cat = mlflow_volume_catalog if mlflow_volume_catalog is not None else catalog
    vol_sch = mlflow_volume_schema if mlflow_volume_schema is not None else schema
    spark = SparkSession.builder.getOrCreate()
    _dfs_tmp = ensure_mlflow_dfs_tmp(
        spark,
        vol_cat,
        vol_sch,
        volume=mlflow_volume_name,
    )

    df = _build_features(
        spark,
        catalog,
        schema,
        lineitem_sample_fraction=lineitem_sample_fraction,
        log_row_counts=log_row_counts,
    )

    # --- Train / test split by date ----------------------------------------
    print("Splitting data by date (train < 1997, test >= 1997) ...")
    train_df = df.filter(F.col("order_date") < "1997-01-01").drop("order_date")
    test_df = df.filter(F.col("order_date") >= "1997-01-01").drop("order_date")
    if log_row_counts:
        tr_n = train_df.count()
        te_n = test_df.count()
        print(f"  Train rows: {tr_n}, Test rows: {te_n}")
    else:
        print("  Train/test split ready (row counts skipped).")

    # --- Pipeline -----------------------------------------------------------
    hashed_cat_cols = [f"{c}_hb" for c in DEMAND_FORECAST_HASH_COLS]

    numeric_cols = [
        "p_size",
        "p_retailprice",
        "c_nationkey",
        "s_nationkey",
        "s_acctbal",
        "order_month",
        "order_quarter",
        "order_year",
        "order_day_of_week",
        "avg_qty_part_90d",
        "avg_qty_cust_90d",
    ]
    feature_cols = numeric_cols + hashed_cat_cols

    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features", handleInvalid="skip"
    )

    # maxBins must be >= distinct values per feature; hashed categoricals are bounded by
    # CAT_HASH_BUCKETS (see ml_tree_config, test_ml_tree_config).
    gbt_iter, gbt_depth = 2, 2
    gbt_bins = SPARK_TREE_MAX_BINS

    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="label",
        maxIter=gbt_iter,
        maxDepth=gbt_depth,
        maxBins=gbt_bins,
        seed=42,
    )

    pipeline = Pipeline(stages=[assembler, gbt])

    # --- Train with MLflow --------------------------------------------------
    # Prefer the bundle-managed experiment name passed by the job (resolves to the
    # tagged resource, including the [dev <user>] prefix in development mode). Fall
    # back to the flat name under the user home for standalone/local runs.
    if experiment_name is None:
        user_name = spark.sql("SELECT current_user()").collect()[0][0]
        experiment_name = f"/Users/{user_name}/dsa_interview_packet_demand_forecast"
    mlflow.set_experiment(experiment_name)
    print(f"MLflow experiment: {experiment_name}")

    with mlflow.start_run(run_name="gbt_demand_forecast") as run:
        mlflow.log_param("model_type", "GBTRegressor")
        mlflow.log_param("max_iter", gbt_iter)
        mlflow.log_param("max_depth", gbt_depth)
        mlflow.log_param("max_bins", gbt_bins)
        mlflow.log_param("feature_cols", feature_cols)
        mlflow.log_param("train_cutoff", "1997-01-01")
        mlflow.log_param("lineitem_sample_fraction", lineitem_sample_fraction)

        print("Training GBTRegressor ...")
        model = pipeline.fit(train_df)

        # --- Evaluate -------------------------------------------------------
        print("Evaluating on test set ...")
        predictions = model.transform(test_df)

        evaluators = {
            "rmse": RegressionEvaluator(
                labelCol="label", predictionCol="prediction", metricName="rmse"
            ),
            "mae": RegressionEvaluator(
                labelCol="label", predictionCol="prediction", metricName="mae"
            ),
            "r2": RegressionEvaluator(
                labelCol="label", predictionCol="prediction", metricName="r2"
            ),
        }

        metrics = {}
        for name, evaluator in evaluators.items():
            value = evaluator.evaluate(predictions)
            metrics[name] = value
            mlflow.log_metric(name, value)
            print(f"  {name.upper()}: {value:.4f}")

        # --- Log & register model -------------------------------------------
        # Always log the model to the run; additionally register it in the Unity
        # Catalog Model Registry unless registration was disabled via
        # --no-uc-model-registry, in which case log the run artifact only.
        registered_model_name = resolve_registered_model_name(
            catalog, schema, "demand_forecast", register_to_uc=register_to_uc
        )
        if registered_model_name:
            mlflow.set_registry_uri(UC_REGISTRY_URI)
            print(f"Registering model in Unity Catalog: {registered_model_name}")
        else:
            print("UC model registry disabled; logging run artifact only.")

        # UC requires a model signature; infer it from a sample of the scored
        # test set (feature inputs -> prediction).
        _sig_sample = predictions.select(*feature_cols, "prediction").limit(100).toPandas()
        signature = infer_signature(_sig_sample[feature_cols], _sig_sample["prediction"])
        mlflow.spark.log_model(
            model,
            artifact_path="demand_forecast_model",
            signature=signature,
            registered_model_name=registered_model_name,
            dfs_tmpdir=_dfs_tmp,
        )
        print(f"Model logged to MLflow run {run.info.run_id}")

    print("Demand forecast training complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Train demand forecasting model on supply chain data"
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
        help="Managed volume name for mlflow.spark log_model dfs_tmpdir.",
    )
    parser.add_argument(
        "--lineitem-sample-fraction",
        type=float,
        default=0.008,
        help="Random fraction of lineitem rows before joins (1.0 = full data).",
    )
    parser.add_argument(
        "--log-row-counts",
        action="store_true",
        help="Run expensive df.count() for diagnostics (slow on large data).",
    )
    parser.add_argument(
        "--no-uc-model-registry",
        action="store_true",
        help="Disable Unity Catalog model registration (log the model as a run artifact only).",
    )
    parser.add_argument(
        "--experiment-name",
        default=None,
        help="MLflow experiment name (bundle passes the managed experiment; "
        "defaults to /Users/<user>/dsa_interview_packet_demand_forecast for local runs).",
    )
    args = parser.parse_args()
    log_row_counts = args.log_row_counts or (
        os.environ.get("DSA_INTERVIEW_LOG_ROW_COUNTS") == "1"
    )
    main(
        args.catalog,
        args.schema,
        mlflow_volume_catalog=args.mlflow_volume_catalog,
        mlflow_volume_schema=args.mlflow_volume_schema,
        mlflow_volume_name=args.mlflow_volume_name,
        lineitem_sample_fraction=args.lineitem_sample_fraction,
        log_row_counts=log_row_counts,
        register_to_uc=not args.no_uc_model_registry,
        experiment_name=args.experiment_name,
    )
