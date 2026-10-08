"""Source table registry: primary keys, column families, source system.

Families are deliberately coarse so a harmless widening (int32 -> int64) is not a schema break:
string, int, decimal, double, timestamp, date, bool.
Keep in sync with sql/postgres/001_schema.sql. This module must stay free of pyspark/pyarrow imports.
"""
from __future__ import annotations

from dataclasses import dataclass

# Columns DMS adds to every parquet file (see infra/terraform/dms.tf).
META_COLUMNS = {"Op": "string", "dms_commit_ts": "timestamp"}


@dataclass(frozen=True)
class Table:
    name: str
    pk: tuple[str, ...]
    columns: dict[str, str]
    source_system: str


def _t(name: str, pk: list[str], source: str, **cols: str) -> Table:
    return Table(name, tuple(pk), dict(cols), source)


TABLES: dict[str, Table] = {
    t.name: t
    for t in [
        _t("supply_point", ["supply_point_id"], "METER_REGISTRY",
           supply_point_id="string", supply_ref="string", region="string", grid_zone="string",
           supply_type="string", status="string", source_updated_at="timestamp"),
        _t("meter", ["meter_id"], "METER_REGISTRY",
           meter_id="string", supply_point_id="string", serial_no="string", meter_type="string",
           status="string", installed_at="date", source_updated_at="timestamp"),
        _t("customer", ["customer_id"], "CRM",
           customer_id="string", customer_type="string", region="string", name="string",
           address="string", valid_from="date", source_updated_at="timestamp"),
        _t("account", ["account_id"], "CRM",
           account_id="string", customer_id="string", account_ref="string", status="string",
           opened_at="date", closed_at="date", source_updated_at="timestamp"),
        _t("tariff", ["tariff_id"], "TARIFF_SYSTEM",
           tariff_id="string", name="string", tariff_type="string", currency="string",
           valid_from="date", valid_to="date", source_updated_at="timestamp"),
        _t("meter_assignment", ["assignment_id"], "METER_REGISTRY",
           assignment_id="string", meter_id="string", account_id="string", supply_point_id="string",
           tariff_id="string", valid_from="date", valid_to="date", source_updated_at="timestamp"),
        _t("meter_reading", ["reading_id", "version"], "HEADEND",
           reading_id="string", meter_id="string", reading_ts="timestamp", interval_end="timestamp",
           version="int", consumption_kwh="decimal", reading_type="string", quality_code="string",
           source_updated_at="timestamp"),
        _t("reading_correction", ["correction_id"], "HEADEND",
           correction_id="string", reading_id="string", meter_id="string", reading_ts="timestamp",
           version="int", replacement_kwh="decimal", reason="string", corrected_at="timestamp",
           source_updated_at="timestamp"),
        _t("settlement_calendar", ["settlement_date", "period"], "CALENDAR",
           settlement_date="date", period="int", start_utc="timestamp", end_utc="timestamp",
           local_start="timestamp", duration_minutes="int", dst_flag="bool", calendar_version="int"),
        _t("reconciliation_control", ["source_name", "period", "control", "run_id"], "BILLING",
           source_name="string", period="date", control="string", run_id="string",
           expected_count="int", expected_total="decimal", unit="string", source_cutoff="timestamp",
           created_at="timestamp", control_version="int"),
    ]
}

# Tables rebuilt as "current state" in Silver (latest row per key, deletes dropped).
REFERENCE_TABLES = [
    "supply_point", "meter", "account", "tariff", "settlement_calendar", "customer", "meter_assignment",
]

# Parent-before-child order for loading the source database.
LOAD_ORDER = [
    "settlement_calendar", "tariff", "supply_point", "customer", "account", "meter", "meter_assignment",
    "meter_reading", "reading_correction", "reconciliation_control",
]

SPARK_TYPES = {
    "string": "string", "int": "int", "decimal": "decimal(18,3)", "double": "double",
    "timestamp": "timestamp", "date": "date", "bool": "boolean",
}

OPEN_END_DATE = "9999-12-31"
