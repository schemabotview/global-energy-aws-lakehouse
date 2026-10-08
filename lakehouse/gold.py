"""Gold: conformed dimensions and facts (a small star schema) on Iceberg, served through Athena.

Facts carry the surrogate key of the dimension version that was effective when the reading happened, resolved here
at load time, so queries never need a date-range join. Missing dimension members map to the UNKNOWN row (-1).
Customer PII lives only in dim_customer_pii.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.config import fq
from lakehouse.keys import UNKNOWN_SK, sk
from lakehouse.tables import OPEN_END_DATE

QUALITY = [("A", "Actual"), ("E", "Estimated"), ("UNKNOWN", "Unknown")]


def _silver(spark, name: str):
    return spark.table(fq("silver", name))


def _replace(df, name: str) -> None:
    df.writeTo(fq("gold", name)).using("iceberg").createOrReplace()


def with_unknown(spark, df, sk_col: str):
    """Append the UNKNOWN member: key -1, strings 'UNKNOWN', everything else null."""
    cols = []
    for f in df.schema.fields:
        if f.name == sk_col:
            cols.append(F.lit(UNKNOWN_SK).cast(f.dataType).alias(f.name))
        elif isinstance(f.dataType, T.StringType):
            cols.append(F.lit("UNKNOWN").alias(f.name))
        else:
            cols.append(F.lit(None).cast(f.dataType).alias(f.name))
    return df.unionByName(spark.range(1).select(*cols))


def build_dims(spark) -> None:
    m, sp = _silver(spark, "meter").alias("m"), _silver(spark, "supply_point").alias("sp")
    dim_meter = m.join(sp, F.col("m.supply_point_id") == F.col("sp.supply_point_id"), "left").select(
        sk("m.meter_id").alias("meter_sk"), F.col("m.meter_id").alias("meter_id"), "m.serial_no", "m.meter_type",
        F.col("m.status").alias("status"), F.col("m.supply_point_id").alias("supply_point_id"),
        F.col("sp.region").alias("region"), "sp.grid_zone")
    _replace(with_unknown(spark, dim_meter, "meter_sk"), "dim_meter")

    dim_tariff = _silver(spark, "tariff").select(
        sk("tariff_id").alias("tariff_sk"), "tariff_id", F.col("name").alias("tariff_name"), "tariff_type", "currency")
    _replace(with_unknown(spark, dim_tariff, "tariff_sk"), "dim_tariff")

    dim_customer = _silver(spark, "customer_history").select(
        sk("customer_id", "valid_from").alias("customer_sk"), "customer_id", "customer_type", "region",
        "valid_from", "valid_to", "is_current")
    _replace(with_unknown(spark, dim_customer, "customer_sk"), "dim_customer")

    # PII is split out so the star schema and its consumers never need it.
    pii = _silver(spark, "customer").select("customer_id", "name", "address")
    _replace(pii, "dim_customer_pii")

    cal = _silver(spark, "settlement_calendar")
    dim_date = cal.select("settlement_date").distinct().select(
        F.date_format("settlement_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("settlement_date").alias("date"), F.year("settlement_date").alias("year"),
        F.month("settlement_date").alias("month"), F.date_format("settlement_date", "EEEE").alias("day_name"),
        F.dayofweek("settlement_date").isin(1, 7).alias("is_weekend"))
    _replace(dim_date, "dim_date")

    intervals = spark.range(1, 51).select(
        F.col("id").cast("int").alias("interval_key"),
        F.format_string("SP-%02d", F.col("id").cast("int")).alias("interval_label"))
    _replace(intervals, "dim_settlement_interval")

    quality = spark.createDataFrame(QUALITY, "quality_code string, description string")
    _replace(quality.select(sk("quality_code").alias("quality_sk"), "quality_code", "description"),
             "dim_reading_quality")


def _date_key(col):
    return F.date_format(col, "yyyyMMdd").cast("int")


def build_facts(spark, run_id: str) -> dict:
    readings = _silver(spark, "meter_reading")
    affected = readings.where(F.col("last_batch_run_id") == run_id).select("settlement_date").distinct()
    dates = [r.settlement_date for r in affected.collect()]
    if not dates:
        return {"dates": 0}

    r = readings.join(F.broadcast(affected), "settlement_date", "inner").alias("r")
    asg = _silver(spark, "meter_assignment").select(
        F.col("meter_id").alias("a_meter_id"), "account_id", "tariff_id",
        F.col("valid_from").alias("a_from"),
        F.coalesce("valid_to", F.lit(OPEN_END_DATE).cast("date")).alias("a_to"),
        F.col("source_updated_at").alias("a_upd"))
    w = Window.partitionBy("reading_id").orderBy(F.col("a_upd").desc_nulls_last())
    j = (
        r.join(asg, (F.col("r.meter_id") == F.col("a_meter_id"))
               & (F.col("r.settlement_date") >= F.col("a_from")) & (F.col("r.settlement_date") < F.col("a_to")),
               "left")
        .withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
        .select("reading_id", F.col("r.meter_id").alias("meter_id"), "reading_ts", "version", "consumption_kwh",
                "reading_type", "quality_code", "settlement_date", "period", "correction_applied",
                "account_id", "tariff_id")
    )
    acc = _silver(spark, "account").select("account_id", "customer_id")
    ch = _silver(spark, "customer_history").select(
        F.col("customer_id").alias("ch_customer_id"), F.col("valid_from").alias("c_from"),
        F.col("valid_to").alias("c_to"))
    j = j.join(acc, "account_id", "left").join(
        ch, (F.col("customer_id") == F.col("ch_customer_id")) & (F.col("reading_ts") >= F.col("c_from"))
        & (F.col("reading_ts") < F.col("c_to")), "left")

    fact = j.select(
        sk("reading_id").alias("reading_sk"),
        sk("meter_id").alias("meter_sk"),
        F.when(F.col("c_from").isNull(), F.lit(UNKNOWN_SK)).otherwise(sk("customer_id", "c_from")).alias("customer_sk"),
        F.when(F.col("tariff_id").isNull(), F.lit(UNKNOWN_SK)).otherwise(sk("tariff_id")).alias("tariff_sk"),
        sk(F.coalesce("quality_code", F.lit("UNKNOWN"))).alias("quality_sk"),
        _date_key("settlement_date").alias("date_key"),
        F.col("period").alias("interval_key"),
        "reading_id", "reading_ts", "consumption_kwh", F.lit(1).alias("read_count"),
        "reading_type", "version", "correction_applied", F.lit(run_id).alias("batch_run_id"))
    target = fq("gold", "fact_meter_reads")
    if not spark.catalog.tableExists(target):
        fact.limit(0).writeTo(target).using("iceberg").partitionedBy(F.col("date_key")).create()
    fact.writeTo(target).overwritePartitions()

    keys = [int(d.strftime("%Y%m%d")) for d in dates]
    fmr = spark.table(target).where(F.col("date_key").isin(*keys))
    expected = _silver(spark, "settlement_calendar").groupBy("settlement_date").count().select(
        _date_key("settlement_date").alias("date_key"), F.col("count").cast("int").alias("expected_intervals"))
    daily = (
        fmr.groupBy("meter_sk", "date_key").agg(
            F.sum("consumption_kwh").alias("daily_kwh"),
            F.count("*").cast("int").alias("valid_intervals"),
            F.max_by("customer_sk", "reading_ts").alias("customer_sk"),
            F.max_by("tariff_sk", "reading_ts").alias("tariff_sk"))
        .join(expected, "date_key", "left")
        .select(sk("meter_sk", "date_key").alias("daily_consumption_sk"), "meter_sk", "customer_sk", "tariff_sk",
                "date_key", "daily_kwh", "valid_intervals", "expected_intervals",
                (F.col("valid_intervals") / F.col("expected_intervals")).alias("completeness"),
                F.lit(run_id).alias("batch_run_id"))
    )
    dtarget = fq("gold", "fact_consumption_daily")
    if not spark.catalog.tableExists(dtarget):
        daily.limit(0).writeTo(dtarget).using("iceberg").partitionedBy(F.col("date_key")).create()
    daily.writeTo(dtarget).overwritePartitions()
    return {"dates": len(keys)}
