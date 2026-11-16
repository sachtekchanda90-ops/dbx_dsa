"""Train a supplier reliability scoring model on supply chain data.

Default path: use ``ml_train_eval_pipeline`` (runs demand train + this + evaluate).
Run this job alone when debugging only supplier training:

    databricks bundle run -t dev train_supplier_scoring

Predicts whether a lineitem will be delivered late
(``l_receiptdate > l_commitdate``) using supplier, part, and order
features. Uses SparkML RandomForestClassifier and logs metrics/model
to MLflow.
"""

from __future__ import annotations

import argparse
import os
from typing import Literal, cast

import ensure_src_path  # noqa: F401  # pylint: disable=unused-import  # must precede dsa_interview_packet imports
import mlflow
from dsa_interview_packet.categorical_hash import (
    SUPPLIER_SCORING_HASH_COLS,
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
from pyspark.ml.classification import (
    RandomForestClassificationModel,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import VectorAssembler
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
    """Join source tables and engineer features for supplier scoring."""
    print("Loading source tables ...")
    lineitem = spark.table(table_fqn(catalog, schema, "lineitem"))
    lineitem = sample_lineitem(lineitem, lineitem_sample_fraction)
    orders = spark.table(table_fqn(catalog, schema, "orders"))
    part = spark.table(table_fqn(catalog, schema, "part"))
    supplier = spark.table(table_fqn(catalog, schema, "supplier"))

    # --- Base join ----------------------------------------------------------
    print("Joining tables ...")
    df = (
        lineitem.alias("l")
        .join(orders.alias("o"), F.col("l.l_orderkey") == F.col("o.o_orderkey"))
        .join(part.alias("p"), F.col("l.l_partkey") == F.col("p.p_partkey"))
        .join(supplier.alias("s"), F.col("l.l_suppkey") == F.col("s.s_suppkey"))
    )

    # --- Label: late delivery -----------------------------------------------
    print("Creating label (late delivery) ...")
    df = df.withColumn(
        "label",
        F.when(F.col("l.l_receiptdate") > F.col("l.l_commitdate"), 1.0).otherwise(
            0.0
        ),
    )

    # --- Historical late delivery rate per supplier -------------------------
    print("Computing historical supplier late delivery rate ...")
    supp_window = (
        Window.partitionBy("l.l_suppkey")
        .orderBy(F.unix_timestamp(F.col("o.o_orderdate")))
        .rangeBetween(-90 * 86400, -1)
    )
    df = df.withColumn(
        "supp_late_rate_180d", F.avg("label").over(supp_window)
    ).fillna(0.0, subset=["supp_late_rate_180d"])

    # --- Select final feature columns --------------------------------------
    df = df.select(
        F.col("label"),
        F.col("o.o_orderdate").alias("order_date"),
        # Categorical
        F.col("p.p_brand").alias("p_brand"),
        F.col("p.p_type").alias("p_type"),
        F.col("o.o_orderpriority").alias("o_orderpriority"),
        F.col("l.l_shipmode").alias("l_shipmode"),
        F.col("l.l_shipinstruct").alias("l_shipinstruct"),
        # Numeric
        F.col("s.s_nationkey").cast("double").alias("s_nationkey"),
        F.col("s.s_acctbal").cast("double").alias("s_acctbal"),
        F.col("p.p_size").cast("double").alias("p_size"),
        F.col("l.l_quantity").cast("double").alias("l_quantity"),
        F.col("l.l_extendedprice").cast("double").alias("l_extendedprice"),
        F.col("l.l_discount").cast("double").alias("l_discount"),
        F.col("supp_late_rate_180d").cast("double"),
    )

    if log_row_counts:
        total = df.count()
        late = df.filter(F.col("label") == 1.0).count()
        print(f"Feature dataframe: {total} rows, {late} late ({100*late/total:.1f}%)")
    else:
        row = df.select(
            F.count(F.lit(1)).alias("total"),
            F.sum("label").alias("late"),
        ).collect()[0]
        total = int(row["total"])
        late = int(row["late"]) if row["late"] is not None else 0
        pct = 100.0 * late / total if total else 0.0
        print(
            f"Feature dataframe: {total} rows, {late} late ({pct:.1f}%) "
            "(one-pass aggregate; use --log-row-counts for naive two-pass counts)"
        )
    return cast(DataFrame, with_categorical_hash_buckets(df, SUPPLIER_SCORING_HASH_COLS))


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
    """Train supplier scoring model and log to MLflow.

    When ``register_to_uc`` is set, the model is also registered in the UC Model
    Registry as ``catalog.schema.supplier_scoring``; otherwise it is logged as a
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
    hashed_cat_cols = [f"{c}_hb" for c in SUPPLIER_SCORING_HASH_COLS]

    numeric_cols = [
        "s_nationkey",
        "s_acctbal",
        "p_size",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "supp_late_rate_180d",
    ]
    feature_cols = numeric_cols + hashed_cat_cols

    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features", handleInvalid="skip"
    )

    # maxBins must be >= distinct values per feature; hashed categoricals — ml_tree_config.
    rf_trees, rf_depth = 3, 2
    rf_bins = SPARK_TREE_MAX_BINS

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=rf_trees,
        maxDepth=rf_depth,
        maxBins=rf_bins,
        minInstancesPerNode=10,
        seed=42,
    )

    pipeline = Pipeline(stages=[assembler, rf])

    # --- Train with MLflow --------------------------------------------------
    # Prefer the bundle-managed experiment name passed by the job (resolves to the
    # tagged resource, including the [dev <user>] prefix in development mode). Fall
    # back to the flat name under the user home for standalone/local runs.
    if experiment_name is None:
        user_name = spark.sql("SELECT current_user()").collect()[0][0]
        experiment_name = f"/Users/{user_name}/dsa_interview_packet_supplier_scoring"
    mlflow.set_experiment(experiment_name)
    print(f"MLflow experiment: {experiment_name}")

    with mlflow.start_run(run_name="rf_supplier_scoring") as run:
        mlflow.log_param("model_type", "RandomForestClassifier")
        mlflow.log_param("num_trees", rf_trees)
        mlflow.log_param("max_depth", rf_depth)
        mlflow.log_param("max_bins", rf_bins)
        mlflow.log_param("min_instances_per_node", 10)
        mlflow.log_param("feature_cols", feature_cols)
        mlflow.log_param("train_cutoff", "1997-01-01")
        mlflow.log_param("lineitem_sample_fraction", lineitem_sample_fraction)

        print("Training RandomForestClassifier ...")
        model = pipeline.fit(train_df)

        # --- Evaluate -------------------------------------------------------
        print("Evaluating on test set ...")
        predictions = model.transform(test_df)

        # Binary metrics
        auc_evaluator = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
        )
        auc = auc_evaluator.evaluate(predictions)
        mlflow.log_metric("auc", auc)
        print(f"  AUC: {auc:.4f}")

        # Multiclass metrics (work for binary too)
        for metric_name in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]:
            metric_lit = cast(
                Literal[
                    "accuracy",
                    "weightedPrecision",
                    "weightedRecall",
                    "f1",
                ],
                metric_name,
            )
            evaluator = MulticlassClassificationEvaluator(
                labelCol="label", predictionCol="prediction", metricName=metric_lit
            )
            value = evaluator.evaluate(predictions)
            mlflow.log_metric(metric_name, value)
            print(f"  {metric_name}: {value:.4f}")

        # --- Feature importance ---------------------------------------------
        print("\nFeature importance (top 10):")
        rf_model = model.stages[-1]
        if not isinstance(rf_model, RandomForestClassificationModel):
            msg = "Expected RandomForestClassificationModel as final pipeline stage"
            raise TypeError(msg)
        importances = rf_model.featureImportances.toArray()
        feature_importance = sorted(
            zip(feature_cols, importances, strict=True),
            key=lambda x: x[1],
            reverse=True,
        )
        for feat_name, importance in feature_importance[:10]:
            mlflow.log_metric(f"importance_{feat_name}", importance)
            print(f"  {feat_name}: {importance:.4f}")

        # --- Log & register model -------------------------------------------
        # Always log the model to the run; additionally register it in the Unity
        # Catalog Model Registry unless registration was disabled via
        # --no-uc-model-registry, in which case log the run artifact only.
        registered_model_name = resolve_registered_model_name(
            catalog, schema, "supplier_scoring", register_to_uc=register_to_uc
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
            artifact_path="supplier_scoring_model",
            signature=signature,
            registered_model_name=registered_model_name,
            dfs_tmpdir=_dfs_tmp,
        )
        print(f"Model logged to MLflow run {run.info.run_id}")

    print("Supplier scoring training complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Train supplier reliability scoring model on supply chain data"
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
        help="Use extra df.count() passes for diagnostics (slow on large data).",
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
        "defaults to /Users/<user>/dsa_interview_packet_supplier_scoring for local runs).",
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
