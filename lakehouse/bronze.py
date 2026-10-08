"""Bronze: append-only Iceberg copy of the DMS files named in the contract manifest, plus audit columns.

Delivery is at-least-once. A file is recorded in ctl.landing_files only after its rows are appended, so a crash
between the two re-appends that file on the next run; Silver deduplicates, so this is safe.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from lakehouse.config import Settings, fq
from lakehouse.store import open_store
from lakehouse.tables import TABLES, Table

LANDING_FILES = fq("ctl", "landing_files")
OP_NAMES = {"I": "INSERT", "U": "UPDATE", "D": "DELETE"}


class SchemaMismatch(Exception):
    pass


def _family(dt) -> str:
    from pyspark.sql import types as T

    if isinstance(dt, T.StringType):
        return "string"
    if isinstance(dt, (T.ByteType, T.ShortType, T.IntegerType, T.LongType)):
        return "int"
    if isinstance(dt, T.DecimalType):
        return "decimal"
    if isinstance(dt, (T.FloatType, T.DoubleType)):
        return "double"
    if isinstance(dt, T.TimestampType):
        return "timestamp"
    if isinstance(dt, T.DateType):
        return "date"
    if isinstance(dt, T.BooleanType):
        return "bool"
    return dt.simpleString()


def _append(spark, target: str, df) -> None:
    if not spark.catalog.tableExists(target):
        df.writeTo(target).using("iceberg").partitionedBy(F.days("ingested_at")).create()
        return
    existing = {f.name: f.dataType for f in spark.table(target).schema.fields}
    for f in df.schema.fields:
        if f.name not in existing:  # additive change approved by the contract gate
            spark.sql(f"ALTER TABLE {target} ADD COLUMNS ({f.name} {f.dataType.simpleString()})")
        elif _family(existing[f.name]) != _family(f.dataType):
            raise SchemaMismatch(f"{target}.{f.name}: {existing[f.name]} vs {f.dataType}")
    aligned = spark.table(target).schema.fields
    cols = [(F.col(f.name) if f.name in df.columns else F.lit(None)).cast(f.dataType).alias(f.name) for f in aligned]
    df.select(*cols).writeTo(target).append()


def load_table(spark, tbl: Table, keys: list[str], landing_uri: str, run_id: str) -> dict:
    done = {r.file_key for r in spark.table(LANDING_FILES).where(F.col("table_name") == tbl.name)
            .select("file_key").collect()}
    todo = sorted(set(keys) - done)
    if not todo:
        return {"table": tbl.name, "files": 0, "rows": 0}

    base = landing_uri.rstrip("/")
    raw = spark.read.option("mergeSchema", "true").parquet(*[f"{base}/{k}" for k in todo])
    df = raw.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_source_file_ts"),
    )
    df = (
        df.withColumn("operation", F.coalesce(*[F.when(F.col("Op") == k, v) for k, v in OP_NAMES.items()]))
        .drop("Op")
        .withColumnRenamed("dms_commit_ts", "source_commit_ts")
        .withColumn("source_system", F.lit(tbl.source_system))
        .withColumn("source_record_id", F.concat_ws("|", *[F.col(c).cast("string") for c in tbl.pk]))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("batch_run_id", F.lit(run_id))
        .withColumn("_file_key", F.regexp_extract("_source_file", rf"/src/({tbl.name}/.*)$", 1))
    )
    if "source_updated_at" not in df.columns:
        df = df.withColumn("source_updated_at", F.lit(None).cast("timestamp"))
    df = df.cache()
    try:
        _append(spark, fq("bronze", tbl.name), df.drop("_file_key"))
        per_file = df.groupBy("_file_key").count().select(
            F.col("_file_key").alias("file_key"), F.lit(tbl.name).alias("table_name"),
            F.lit(run_id).alias("run_id"), F.col("count").alias("rows"), F.current_timestamp().alias("loaded_at"))
        per_file.writeTo(LANDING_FILES).append()
        rows = df.count()
    finally:
        df.unpersist()
    return {"table": tbl.name, "files": len(todo), "rows": rows}


def run(spark, settings: Settings, run_id: str) -> list[dict]:
    manifest = open_store(settings.ctl_uri).read_json(f"manifests/{run_id}.json")
    if manifest is None:
        raise FileNotFoundError(f"no contract manifest for run {run_id}; run the contract check first")
    return [
        load_table(spark, TABLES[table], keys, settings.landing_uri, run_id)
        for table, keys in manifest["passed"].items()
    ]
