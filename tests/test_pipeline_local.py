"""End-to-end on local Spark + Iceberg against a DMS imitation (no AWS, no Postgres).

Slow: the first run downloads the Iceberg runtime jar. Run with:  pytest -m spark
"""
import shutil
from decimal import Decimal

import pytest

pytestmark = [pytest.mark.spark, pytest.mark.skipif(shutil.which("java") is None, reason="needs Java")]

from generator.model import Config, build_scenario  # noqa: E402
from generator.sinks import DmsImitationSink  # noqa: E402
from lakehouse import contract  # noqa: E402
from lakehouse.config import NAMESPACES, Settings, fq  # noqa: E402
from lakehouse.pipeline import run_batch  # noqa: E402
from lakehouse.reconcile import ReconciliationBlocked  # noqa: E402
from lakehouse.spark import get_spark  # noqa: E402
from lakehouse.store import LocalStore  # noqa: E402

CFG = Config(customers=60)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    return get_spark(str(tmp_path_factory.mktemp("warehouse")))


def reset(spark):
    for ns in NAMESPACES:
        if spark.catalog.databaseExists(f"lake.{ns}"):
            for t in spark.sql(f"SHOW TABLES IN lake.{ns}").collect():
                spark.sql(f"DROP TABLE IF EXISTS lake.{ns}.{t.tableName} PURGE")


class Env:
    def __init__(self, tmp, scenario="baseline", bad_control=False):
        self.sc = build_scenario(Config(customers=60, bad_control=bad_control))
        self.landing, self.ctl, self.quar = tmp / "landing", tmp / "ctl", tmp / "quarantine"
        self.sink = DmsImitationSink(self.landing, scenario)
        self.settings = Settings(str(self.landing), str(self.ctl), str(self.quar))

    def check(self, run_id):
        return contract.run_check(LocalStore(self.landing), LocalStore(self.quar), LocalStore(self.ctl), run_id)

    def day1(self, spark):
        self.sink.write_day1(self.sc.day1, self.sc.day1_load_ts)
        self.check("run-day1")
        return run_batch(spark, self.settings, "run-day1")

    def day2_files(self):
        self.sink.write_day2(self.sc.day2, self.sc.day2_load_ts)
        return self.check("run-day2")

    def day2(self, spark):
        self.day2_files()
        return run_batch(spark, self.settings, "run-day2")


def count(spark, ns, name):
    return spark.table(fq(ns, name)).count()


def test_baseline_two_days_and_rerun(spark, tmp_path):
    reset(spark)
    env = Env(tmp_path)
    env.day1(spark)
    env.day2(spark)
    exp = env.sc.expected

    # Bronze keeps every landed row (full load + CDC)
    landed = len(env.sc.day1["meter_reading"]) + len(env.sc.day2["meter_reading"])
    assert count(spark, "bronze", "meter_reading") == landed

    # Silver/Gold hold exactly the valid, deduplicated readings
    want = sum(exp["final_counts"].values())
    assert count(spark, "silver", "meter_reading") == want
    assert count(spark, "gold", "fact_meter_reads") == want
    rejected = sum(v for d in ("day1", "day2") for k, v in exp[d]["defects"].items() if k != "newer_version_replays")
    assert spark.table(fq("silver", "dlq")).where("table_name = 'meter_reading'").count() == rejected

    # Totals per settlement date equal the independently computed control totals
    for date, total in exp["final_totals_kwh"].items():
        key = int(date.replace("-", ""))
        got = spark.table(fq("gold", "fact_consumption_daily")).where(f"date_key = {key}") \
            .groupBy().sum("daily_kwh").collect()[0][0]
        assert got == Decimal(total)

    # DST day has 50 expected intervals
    assert spark.table(fq("gold", "fact_consumption_daily")).where("date_key = 20261025") \
        .select("expected_intervals").distinct().collect()[0][0] == 50

    # Corrections applied
    assert spark.table(fq("silver", "meter_reading")).where("correction_applied").count() == exp["day2"]["corrections"]

    # SCD2: changed customers have two versions, exactly one current
    hist = spark.table(fq("silver", "customer_history"))
    assert hist.count() == CFG.customers + exp["day2"]["customer_changes"]
    assert hist.where("is_current").count() == CFG.customers

    # Reconciliation passed and published
    statuses = {r.status for r in spark.table(fq("gold", "fact_reconciliation")).collect()}
    assert statuses == {"PASSED"}
    assert count(spark, "gold", "report_daily_consumption") > 0

    # Re-running the same run is a no-op
    before = {t: count(spark, ns, t) for ns, t in [("bronze", "meter_reading"), ("silver", "meter_reading"),
                                                    ("gold", "fact_meter_reads"), ("silver", "dlq")]}
    run_batch(spark, env.settings, "run-day2")
    after = {t: count(spark, ns, t) for ns, t in [("bronze", "meter_reading"), ("silver", "meter_reading"),
                                                   ("gold", "fact_meter_reads"), ("silver", "dlq")]}
    assert before == after


def test_bad_control_blocks_publication(spark, tmp_path):
    reset(spark)
    env = Env(tmp_path, bad_control=True)
    env.day1(spark)
    with pytest.raises(ReconciliationBlocked):
        env.day2(spark)
    rows = spark.table(fq("gold", "fact_reconciliation")).where("batch_run_id = 'run-day2'").collect()
    assert any(r.status == "BLOCKED_TOTAL" for r in rows)  # evidence is kept
    published = {r.date_key for r in spark.table(fq("gold", "report_daily_consumption")).collect()}
    assert 20261025 not in published


def test_breaking_schema_is_quarantined_and_held(spark, tmp_path):
    reset(spark)
    env = Env(tmp_path, scenario="breaking_unit")
    env.day1(spark)
    manifest = env.day2_files()
    assert manifest["has_quarantine"]
    assert contract.held_tables(LocalStore(env.ctl)) == {"meter_reading"}
    assert "meter_reading" not in manifest["passed"]
    with pytest.raises(ReconciliationBlocked):  # day-2 readings never arrive, so day 2 cannot reconcile
        run_batch(spark, env.settings, "run-day2")


def test_additive_schema_change_flows_into_bronze(spark, tmp_path):
    reset(spark)
    env = Env(tmp_path, scenario="additive")
    env.day1(spark)
    manifest = env.day2_files()
    assert manifest["additive"] == [{"table": "meter", "added": ["firmware_version"]}]
    run_batch(spark, env.settings, "run-day2")
    meter = spark.table(fq("bronze", "meter"))
    assert "firmware_version" in meter.columns
    assert meter.where("firmware_version IS NOT NULL").count() > 0
    assert meter.where("firmware_version IS NULL").count() > 0  # day-1 rows are unchanged
