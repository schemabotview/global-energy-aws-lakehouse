"""Data-quality checks on Gold. Error-severity failures stop the run; warnings are recorded."""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from lakehouse.config import fq
from lakehouse.keys import UNKNOWN_SK

FK_CHECKS = [
    ("meter_sk", "dim_meter", "meter_sk"),
    ("customer_sk", "dim_customer", "customer_sk"),
    ("tariff_sk", "dim_tariff", "tariff_sk"),
    ("quality_sk", "dim_reading_quality", "quality_sk"),
    ("date_key", "dim_date", "date_key"),
    ("interval_key", "dim_settlement_interval", "interval_key"),
]


class DataQualityFailure(Exception):
    pass


def run(spark, run_id: str) -> list[dict]:
    g = lambda n: spark.table(fq("gold", n))  # noqa: E731
    fmr, daily, cust = g("fact_meter_reads"), g("fact_consumption_daily"), g("dim_customer")
    results: list[dict] = []

    def add(name: str, severity: str, value: float, ok: bool) -> None:
        results.append({"run_id": run_id, "check_name": name, "severity": severity, "value": float(value), "passed": ok})

    n = fmr.count()
    add("fact_meter_reads_grain_unique", "error", n - fmr.select("reading_sk").distinct().count(),
        n == fmr.select("reading_sk").distinct().count())
    nd = daily.count()
    add("fact_consumption_daily_grain_unique", "error", nd - daily.select("daily_consumption_sk").distinct().count(),
        nd == daily.select("daily_consumption_sk").distinct().count())
    add("fact_consumption_not_null", "error", fmr.where(F.col("consumption_kwh").isNull()).count(),
        fmr.where(F.col("consumption_kwh").isNull()).count() == 0)

    for fk, dim, key in FK_CHECKS:
        used = fmr.select(F.col(fk).alias("_k")).distinct()
        orphans = used.join(g(dim).select(F.col(key).alias("_k")), "_k", "left_anti").count()
        add(f"orphan_{fk}", "error", orphans, orphans == 0)

    real = cust.where(F.col("customer_sk") != UNKNOWN_SK)
    bad_current = real.groupBy("customer_id").agg(F.sum(F.col("is_current").cast("int")).alias("n")) \
        .where(F.col("n") != 1).count()
    add("dim_customer_one_current_per_customer", "error", bad_current, bad_current == 0)
    w = Window.partitionBy("customer_id").orderBy("valid_from")
    overlaps = real.withColumn("_next", F.lead("valid_from").over(w)).where(F.col("_next") < F.col("valid_to")).count()
    add("dim_customer_no_overlap", "error", overlaps, overlaps == 0)

    unknown_share = fmr.where(F.col("customer_sk") == UNKNOWN_SK).count() / n if n else 0.0
    add("fact_unknown_customer_share", "warn", unknown_share, unknown_share <= 0.01)

    out = spark.createDataFrame(results).withColumn("checked_at", F.current_timestamp())
    target = fq("ctl", "dq_result")
    if spark.catalog.tableExists(target):
        spark.sql(f"DELETE FROM {target} WHERE run_id = '{run_id}'")
        out.writeTo(target).append()
    else:
        out.writeTo(target).using("iceberg").create()

    failed = [r for r in results if r["severity"] == "error" and not r["passed"]]
    if failed:
        raise DataQualityFailure("; ".join(f"{r['check_name']}={r['value']}" for r in failed))
    return results
