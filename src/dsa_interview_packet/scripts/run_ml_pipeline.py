# pylint: disable=wrong-import-position
"""Run demand training, supplier training, and evaluation in one Spark job.

This is the **default** ML path after ``seed_source_tables`` and ``simulate_system_tables``.
A single serverless run reuses one SparkSession (fast E2E vs three separate jobs).
"""

from __future__ import annotations

import argparse
import os
import sys

import ensure_src_path  # must precede dsa_interview_packet imports

# spark_python_task may exec() without __file__; use ensure_src_path.__file__.
_scripts_dir = os.path.dirname(os.path.abspath(ensure_src_path.__file__))
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

import evaluate_models as evaluate_models_script  # noqa: E402
import train_demand_forecast as train_demand_script  # noqa: E402
import train_supplier_scoring as train_supplier_script  # noqa: E402


def main() -> None:
    """Parse CLI args and run demand train, supplier train, then evaluation."""
    parser = argparse.ArgumentParser(
        description="Train both ML models and evaluate in one Spark session",
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument(
        "--mlflow-volume-catalog",
        default=None,
        help="UC catalog for MLflow dfs_tmpdir (default: same as --catalog).",
    )
    parser.add_argument(
        "--mlflow-volume-schema",
        default=None,
        help="UC schema for MLflow volume (default: same as --schema).",
    )
    parser.add_argument(
        "--mlflow-volume-name",
        default="mlflow_scratch",
        help="Managed volume for mlflow.spark dfs_tmpdir.",
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
        help="Expensive row counts in training scripts (not used in evaluate).",
    )
    parser.add_argument(
        "--no-uc-model-registry",
        action="store_true",
        help="Disable Unity Catalog model registration in both training steps.",
    )
    # Managed experiment names passed by the bundle job (one per model + evaluation).
    parser.add_argument("--demand-experiment-name", default=None)
    parser.add_argument("--scoring-experiment-name", default=None)
    parser.add_argument("--evaluation-experiment-name", default=None)
    args = parser.parse_args()
    log_row_counts = args.log_row_counts or (
        os.environ.get("DSA_INTERVIEW_LOG_ROW_COUNTS") == "1"
    )
    register_to_uc = not args.no_uc_model_registry

    train_demand_script.main(
        args.catalog,
        args.schema,
        mlflow_volume_catalog=args.mlflow_volume_catalog,
        mlflow_volume_schema=args.mlflow_volume_schema,
        mlflow_volume_name=args.mlflow_volume_name,
        lineitem_sample_fraction=args.lineitem_sample_fraction,
        log_row_counts=log_row_counts,
        register_to_uc=register_to_uc,
        experiment_name=args.demand_experiment_name,
    )
    train_supplier_script.main(
        args.catalog,
        args.schema,
        mlflow_volume_catalog=args.mlflow_volume_catalog,
        mlflow_volume_schema=args.mlflow_volume_schema,
        mlflow_volume_name=args.mlflow_volume_name,
        lineitem_sample_fraction=args.lineitem_sample_fraction,
        log_row_counts=log_row_counts,
        register_to_uc=register_to_uc,
        experiment_name=args.scoring_experiment_name,
    )
    evaluate_models_script.main(
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


if __name__ == "__main__":
    main()
