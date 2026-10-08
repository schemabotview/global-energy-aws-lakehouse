"""Silver: typed, deduplicated, corrected, validated. Reads only Bronze, never the source, so any run can be replayed.

Reference tables are rebuilt as "latest row per key" each run (small). Readings are merged incrementally by
reading_id for the Bronze rows of one run. Rejected rows go to silver.dlq with a reason instead of being dropped.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from lakehouse.config import fq
from lakehouse.tables import OPEN_END_DATE, REFERENCE_TABLES, SPARK_TYPES, TABLES, Table

EXTRA = ("source_commit_ts", "ingested_at", "batch_run_id", "operation")
VALID_READING_TYPES = ("HALF_HOURLY",)


def canonical(tbl: Table, df, extra: tuple[str, ...] = EXTRA):
    cols = [F.col(c).cast(SPARK_TYPES[fam]).alias(c) for c, fam in tbl.columns.items()]
    return df.select(*cols, *extra)


def latest(df, pk):
    w = Window.partitionBy(*pk).orderBy(F.col("source_commit_ts").desc_nulls_last(), F.col("ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def _exists(spark, ns: str, name: str) -> bool:
    return spark.catalog.tableExists(fq(ns, name))


def write_dlq(spark, table_name: str, run_id: str, rejected) -> None:
    """rejected needs columns rejection_reason and payload. Re-running a run replaces its rejects."""
    target = fq("silver", "dlq")
    out = rejected.select(
        F.lit(table_name).alias("table_name"), F.lit(run_id).alias("batch_run_id"),
        "rejection_reason", "payload", F.current_timestamp().alias("rejected_at"))
    if spark.catalog.tableExists(target):
        spark.sql(f"DELETE FROM {target} WHERE batch_run_id = '{run_id}' AND table_name = '{table_name}'")
        out.writeTo(target).append()
    else:
        out.writeTo(target).using("iceberg").create()


def build_reference(spark, skip=frozenset()) -> None:
    for name in REFERENCE_TABLES:
        if name in skip or not _exists(spark, "bronze", name):
            continue
        t = TABLES[name]
        cur = latest(canonical(t, spark.table(fq("bronze", name))), t.pk).where(F.col("operation") != "DELETE")
        cur.drop(*EXTRA).writeTo(fq("silver", name)).using("iceberg").createOrReplace()


def clean_readings(spark, run_id: str) -> dict:
    t = TABLES["meter_reading"]
    src = spark.table(fq("bronze", "meter_reading")).where(F.col("batch_run_id") == run_id)
    c = canonical(t, src).where(F.col("operation") != "DELETE")
    meters = spark.table(fq("silver", "meter")).select("meter_id").distinct().withColumn("_known", F.lit(True))
    cal = spark.table(fq("silver", "settlement_calendar")).select(
        F.col("start_utc").alias("reading_ts"), "settlement_date", "period")
    j = c.join(meters, "meter_id", "left").join(F.broadcast(cal), "reading_ts", "left").cache()
    try:
        reason = (
            F.when(F.col("meter_id").isNull(), "NULL_METER_ID")
            .when(F.col("reading_ts").isNull(), "NULL_READING_TS")
            .when(F.col("consumption_kwh").isNull(), "NULL_CONSUMPTION")
            .when(F.col("consumption_kwh") < 0, "NEGATIVE_CONSUMPTION")
            .when(F.col("reading_type").isNull() | ~F.col("reading_type").isin(*VALID_READING_TYPES),
                  "INVALID_READING_TYPE")
            .when(F.col("_known").isNull(), "UNKNOWN_METER")
            .when(F.col("settlement_date").isNull(), "NO_SETTLEMENT_PERIOD")
        )
        rejected = j.where(reason.isNotNull()).select(
            reason.alias("rejection_reason"), F.to_json(F.struct(*t.columns)).alias("payload"))
        w = Window.partitionBy("reading_id").orderBy(
            F.col("version").desc(), F.col("source_commit_ts").desc_nulls_last(), F.col("ingested_at").desc())
        batch = (
            j.where(reason.isNull()).withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
            .select(
                "reading_id", "meter_id", "reading_ts", "interval_end", "version",
                F.col("consumption_kwh").alias("kwh_reported"), "consumption_kwh",
                "reading_type", "quality_code", "settlement_date", "period", "source_updated_at",
                F.lit(False).alias("correction_applied"), F.lit(run_id).alias("last_batch_run_id"))
        )
        write_dlq(spark, "meter_reading", run_id, rejected)

        target = fq("silver", "meter_reading")
        if not spark.catalog.tableExists(target):
            batch.limit(0).writeTo(target).using("iceberg").partitionedBy(F.col("settlement_date")).create()
        batch.createOrReplaceTempView("_reading_batch")
        spark.sql(f"""
            MERGE INTO {target} t USING _reading_batch s ON t.reading_id = s.reading_id
            WHEN MATCHED AND s.version >= t.version THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")
        return {"rows_in": j.count(), "rows_rejected": rejected.count()}
    finally:
        j.unpersist()


def apply_corrections(spark, run_id: str) -> dict:
    t = TABLES["reading_correction"]
    if not _exists(spark, "bronze", "reading_correction"):
        return {"rows_in": 0, "rows_rejected": 0}
    src = spark.table(fq("bronze", "reading_correction")).where(F.col("batch_run_id") == run_id)
    c = canonical(t, src).where(F.col("operation") != "DELETE")
    known = spark.table(fq("silver", "meter_reading")).select("reading_id").distinct().withColumn("_known", F.lit(True))
    j = c.join(known, "reading_id", "left").cache()
    try:
        reason = (
            F.when(F.col("reading_id").isNull(), "NULL_READING_ID")
            .when(F.col("replacement_kwh").isNull(), "NULL_REPLACEMENT")
            .when(F.col("replacement_kwh") < 0, "NEGATIVE_REPLACEMENT")
            .when(F.col("corrected_at").isNull(), "NULL_CORRECTED_AT")
            .when(F.col("_known").isNull(), "UNKNOWN_READING")
        )
        rejected = j.where(reason.isNotNull()).select(
            reason.alias("rejection_reason"), F.to_json(F.struct(*t.columns)).alias("payload"))
        w = Window.partitionBy("reading_id").orderBy(F.col("corrected_at").desc(), F.col("source_commit_ts").desc_nulls_last())
        batch = (
            j.where(reason.isNull()).withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
            .select("correction_id", "reading_id", "meter_id", "reading_ts", "version", "replacement_kwh",
                    "reason", "corrected_at", F.lit(run_id).alias("batch_run_id"))
        )
        write_dlq(spark, "reading_correction", run_id, rejected)

        target = fq("silver", "reading_correction")
        if not spark.catalog.tableExists(target):
            batch.limit(0).writeTo(target).using("iceberg").create()
        batch.createOrReplaceTempView("_correction_batch")
        spark.sql(f"""
            MERGE INTO {target} t USING _correction_batch s ON t.reading_id = s.reading_id
            WHEN MATCHED AND s.corrected_at > t.corrected_at THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")
        return {"rows_in": j.count(), "rows_rejected": rejected.count()}
    finally:
        j.unpersist()


def finalize_readings(spark, run_id: str) -> None:
    """Recompute the final kWh for readings touched by this run: a correction wins if it is newer than the reading."""
    readings = fq("silver", "meter_reading")
    corrections = fq("silver", "reading_correction")
    has_corr = spark.catalog.tableExists(corrections)
    join = f"LEFT JOIN {corrections} c ON c.reading_id = r.reading_id" if has_corr else ""
    corr_cols = (
        "CASE WHEN c.corrected_at > r.source_updated_at THEN c.replacement_kwh ELSE r.kwh_reported END",
        "coalesce(c.corrected_at > r.source_updated_at, false)",
        f"OR c.batch_run_id = '{run_id}'",
    ) if has_corr else ("r.kwh_reported", "false", "")
    spark.sql(f"""
        MERGE INTO {readings} t USING (
          SELECT r.reading_id, {corr_cols[0]} AS consumption_kwh, {corr_cols[1]} AS correction_applied
          FROM {readings} r {join}
          WHERE r.last_batch_run_id = '{run_id}' {corr_cols[2]}
        ) s ON t.reading_id = s.reading_id
        WHEN MATCHED THEN UPDATE SET t.consumption_kwh = s.consumption_kwh,
                                     t.correction_applied = s.correction_applied,
                                     t.last_batch_run_id = '{run_id}'""")


def build_customer_history(spark) -> None:
    """SCD Type 2 from the customer change stream. A new version only when tracked attributes change.
    The first version is open-ended at the start so any reading resolves to a customer version.
    Deletes (right to erasure) are out of scope for the MVP."""
    if not _exists(spark, "bronze", "customer"):
        return
    c = canonical(TABLES["customer"], spark.table(fq("bronze", "customer"))).where(F.col("operation") != "DELETE")
    w = Window.partitionBy("customer_id").orderBy("source_updated_at", "source_commit_ts")
    changed = (
        c.withColumn("_h", F.xxhash64("customer_type", "region")).withColumn("_prev", F.lag("_h").over(w))
        .where(F.col("_prev").isNull() | (F.col("_h") != F.col("_prev")))
    )
    open_end = F.lit(f"{OPEN_END_DATE} 00:00:00").cast("timestamp")
    hist = (
        changed.withColumn("valid_from", F.when(F.row_number().over(w) == 1, F.lit("1900-01-01").cast("timestamp"))
                           .otherwise(F.col("source_updated_at")))
        .withColumn("valid_to", F.coalesce(F.lead("source_updated_at").over(w), open_end))
        .select("customer_id", "customer_type", "region", "valid_from", "valid_to",
                (F.col("valid_to") == open_end).alias("is_current"))
    )
    hist.writeTo(fq("silver", "customer_history")).using("iceberg").createOrReplace()


def run(spark, run_id: str, skip=frozenset()) -> dict:
    build_reference(spark, skip)
    out = {}
    if "meter_reading" not in skip and _exists(spark, "bronze", "meter_reading"):
        out["readings"] = clean_readings(spark, run_id)
    if "reading_correction" not in skip and spark.catalog.tableExists(fq("silver", "meter_reading")):
        out["corrections"] = apply_corrections(spark, run_id)
    if spark.catalog.tableExists(fq("silver", "meter_reading")):
        finalize_readings(spark, run_id)
    if "customer" not in skip:
        build_customer_history(spark)
    return out
