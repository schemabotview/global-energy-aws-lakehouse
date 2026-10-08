# Databricks notebook source
# MAGIC %md
# MAGIC # Spike: Databricks writes Iceberg through the Glue Catalog, Athena reads it
# MAGIC Run this once on the job cluster (or any cluster with the Iceberg libraries + instance profile) before anything else.
# MAGIC Then run the query printed at the end in Athena (workgroup from `terraform output athena_workgroup`).

# COMMAND ----------

spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.spike")
spark.sql("CREATE TABLE IF NOT EXISTS lake.spike.hello (id INT, note STRING) USING iceberg")
spark.sql("INSERT INTO lake.spike.hello VALUES (1, 'written by databricks'), (2, 'read by athena')")
print(spark.table("lake.spike.hello").count(), "rows written")
print("Athena:  SELECT * FROM spike.hello;")
