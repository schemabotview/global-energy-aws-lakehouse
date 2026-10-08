"""Reconcile Gold daily consumption to the billing control totals and gate the published report.

For every control (latest control_version per source / period / control) the Gold total and interval count for that
settlement date must match: the total within recon_threshold_pct, the count exactly. Results are always written, so
the evidence exists for blocked runs too. Any non-PASSED row blocks publication and raises ReconciliationBlocked.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from lakehouse.config import fq
from lakehouse.keys import sk
from lakehouse.silver import canonical
from lakehouse.tables import TABLES

PASSED = "PASSED"


class ReconciliationBlocked(Exception):
    pass


def reconcile(spark, run_id: str, threshold_pct: float) -> list[dict]:
    raw = canonical(TABLES["reconciliation_control"], spark.table(fq("bronze", "reconciliation_control")))
    raw = raw.where(F.col("operation") != "DELETE")
    w = Window.partitionBy("source_name", "period", "control").orderBy(
        F.col("control_version").desc(), F.col("created_at").desc())
    ctl = raw.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")

    daily = spark.table(fq("gold", "fact_consumption_daily")).groupBy("date_key").agg(
        F.sum("daily_kwh").alias("target_total"), F.sum("valid_intervals").alias("target_count"))
    j = ctl.withColumn("date_key", F.date_format("period", "yyyyMMdd").cast("int")).join(daily, "date_key", "left")

    variance = F.col("target_total") - F.col("expected_total")
    pct = F.when(F.col("expected_total") != 0, F.abs(variance) / F.abs(F.col("expected_total")) * 100).otherwise(
        F.when(variance != 0, F.lit(100.0)).otherwise(F.lit(0.0)))
    status = (
        F.when(F.col("target_total").isNull(), "MISSING_TARGET")
        .when(F.col("expected_total").isNull(), "MISSING_CONTROL")
        .when(F.col("target_count") != F.col("expected_count"), "BLOCKED_COUNT")
        .when(pct > threshold_pct, "BLOCKED_TOTAL")
        .otherwise(PASSED)
    )
    out = j.select(
        sk("source_name", "period", "control", "run_id", "control_version").alias("reconciliation_sk"),
        "date_key", "source_name", "control", F.col("run_id").alias("control_run_id"), "control_version",
        F.col("expected_total").alias("source_total"), F.col("target_total"),
        variance.alias("variance_amount"), pct.alias("variance_pct"),
        F.col("expected_count").alias("source_count"), F.col("target_count"),
        F.lit(threshold_pct).alias("threshold_pct"), status.alias("status"), "source_cutoff",
        F.lit(run_id).alias("batch_run_id"), F.current_timestamp().alias("reconciled_at"),
    ).cache()
    try:
        target = fq("gold", "fact_reconciliation")
        if spark.catalog.tableExists(target):
            spark.sql(f"DELETE FROM {target} WHERE batch_run_id = '{run_id}'")
            out.writeTo(target).append()
        else:
            out.writeTo(target).using("iceberg").create()
        rows = [r.asDict() for r in out.collect()]
    finally:
        out.unpersist()

    blocked = [r for r in rows if r["status"] != PASSED]
    if blocked:
        detail = ", ".join(f"{r['date_key']}:{r['status']}({r['variance_pct']})" for r in blocked)
        raise ReconciliationBlocked(f"{len(blocked)} control(s) failed: {detail}")
    return rows


def publish(spark, run_id: str) -> int:
    """Refresh the report table for the dates reconciled in this run. Only called after reconcile() passed."""
    keys = [r.date_key for r in spark.table(fq("gold", "fact_reconciliation"))
            .where((F.col("batch_run_id") == run_id) & (F.col("status") == PASSED)).select("date_key").distinct().collect()]
    if not keys:
        return 0
    report = spark.table(fq("gold", "fact_consumption_daily")).where(F.col("date_key").isin(*keys)).select(
        "date_key", "meter_sk", "customer_sk", "tariff_sk", "daily_kwh", "valid_intervals", "expected_intervals",
        F.lit(run_id).alias("report_run_id"), F.lit("AUTO_RECONCILED").alias("approval_status"),
        F.current_timestamp().alias("published_at"))
    target = fq("gold", "report_daily_consumption")
    if not spark.catalog.tableExists(target):
        report.limit(0).writeTo(target).using("iceberg").partitionedBy(F.col("date_key")).create()
    report.writeTo(target).overwritePartitions()
    return len(keys)
