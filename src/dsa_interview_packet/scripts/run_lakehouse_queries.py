"""Execute all 22 Lakehouse SQL queries and write results as Delta tables.

The bundle does not define a job for this script. After ``databricks bundle deploy``, run
it as a Python script task (in a job you create, on serverless) with::

    --catalog <catalog> --schema <schema> --queries-path /Workspace/Users/<your-email>/.bundle/dsa_interview_packet/dev/files/src/dsa_interview_packet/queries/lakehouse

It reads SQL files from that synced workspace path, executes each
against the source tables, and writes results as managed Delta tables
in the configured catalog/schema with a ``lakehouse_`` prefix.
"""

from __future__ import annotations

import argparse
import os
import sys

import ensure_src_path  # noqa: F401  # pylint: disable=unused-import  # must precede dsa_interview_packet imports
from dsa_interview_packet.sql_identifiers import quote_sql_ident, table_fqn
from pyspark.sql import SparkSession


def main(catalog: str, schema: str, queries_path: str) -> None:
    """Run all SQL files and materialise results."""
    spark = SparkSession.builder.getOrCreate()

    # Set default catalog/schema so unqualified table names resolve correctly
    spark.sql(f"USE CATALOG {quote_sql_ident(catalog)}")
    spark.sql(f"USE SCHEMA {quote_sql_ident(schema)}")

    # Find all .sql files
    sql_files = sorted(
        f for f in os.listdir(queries_path) if f.endswith(".sql")
    )

    if not sql_files:
        print(f"No SQL files found in {queries_path}")
        sys.exit(1)

    for sql_file in sql_files:
        query_name = sql_file.removesuffix(".sql")
        table_name = table_fqn(catalog, schema, f"lakehouse_{query_name}")
        sql_path = os.path.join(queries_path, sql_file)

        print(f"Running {query_name} -> {table_name}")
        with open(sql_path, encoding="utf-8") as f:
            sql = f.read()

        df = spark.sql(sql)
        df.write.mode("overwrite").saveAsTable(table_name)
        count = spark.table(table_name).count()
        print(f"  {table_name}: {count} rows")

    print(f"Done -- {len(sql_files)} queries executed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Lakehouse SQL supply chain queries")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--queries-path", required=True,
                        help="Workspace path to the queries/lakehouse directory")
    args = parser.parse_args()
    main(args.catalog, args.schema, args.queries_path)
