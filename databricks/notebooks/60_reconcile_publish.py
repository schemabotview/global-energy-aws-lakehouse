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

from lakehouse import reconcile

# Writes the evidence first, then raises ReconciliationBlocked if any control fails: nothing is published.
rows = reconcile.reconcile(spark, run_id, settings.recon_threshold_pct)
print(f"{len(rows)} controls PASSED")
print("published dates:", reconcile.publish(spark, run_id))
