# Databricks notebook source
import os
import sys

# Notebooks run from a Git folder / Git job source: the repo root is two levels up.
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

# COMMAND ----------

from lakehouse.notebook import params

run_id, settings = params(dbutils)
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")

# COMMAND ----------

from lakehouse import silver
from lakehouse.contract import held_tables
from lakehouse.store import open_store

skip = held_tables(open_store(settings.ctl_uri))  # tables whose schema was quarantined are not processed
if skip:
    print("HELD (skipped):", sorted(skip))
print(silver.run(spark, run_id, skip))
