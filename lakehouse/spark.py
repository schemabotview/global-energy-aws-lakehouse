"""Spark session and catalog setup.

On Databricks the cluster is configured by Terraform (infra/terraform/databricks.tf): Iceberg runtime, GlueCatalog,
instance profile. Locally (LAKE_ENV=local) a Hadoop-catalog Iceberg warehouse in a folder is used instead.
"""
from __future__ import annotations

import os

from lakehouse.config import CATALOG, NAMESPACES, fq

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"

LANDING_FILES_DDL = f"""
CREATE TABLE IF NOT EXISTS {fq("ctl", "landing_files")} (
  file_key STRING, table_name STRING, run_id STRING, rows BIGINT, loaded_at TIMESTAMP
) USING iceberg
"""


def get_spark(warehouse: str | None = None):
    from pyspark.sql import SparkSession

    if os.getenv("LAKE_ENV") == "local":
        warehouse = warehouse or os.getenv("LAKE_WAREHOUSE", ".local/warehouse")
        spark = (
            SparkSession.builder.master("local[2]").appName("lakehouse-local")
            .config("spark.jars.packages", ICEBERG_PACKAGE)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
            .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    else:
        spark = SparkSession.builder.getOrCreate()
    # DMS parquet timestamps carry no zone; read them all as plain timestamps in UTC.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
    return spark


def ensure_catalog_objects(spark) -> None:
    for ns in NAMESPACES:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")
    spark.sql(LANDING_FILES_DDL)
