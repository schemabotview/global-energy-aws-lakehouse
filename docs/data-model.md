# Data model and rules

## Source (PostgreSQL schema `src`)
`supply_point`, `meter`, `customer` (PII: name, address), `account`, `tariff`, `meter_assignment` (tariff on the assignment,
`valid_to` exclusive, 9999-12-31 = open), `meter_reading` (PK `reading_id, version`), `reading_correction`,
`settlement_calendar`, `reconciliation_control` (billing control totals). DDL: `sql/postgres/001_schema.sql`.

## DMS output
`s3://<bucket>/dms/src/<table>/LOAD00000001.parquet` (full load) and `<table>/YYYY/MM/DD/*.parquet` (CDC). Every row has `Op`
(I/U/D) and `dms_commit_ts`. Deletes carry only the key.

## Layers (Iceberg, Glue Catalog `lake`)
| Layer | Tables | Rule |
|---|---|---|
| ctl | `landing_files`, `dq_result` | which files were loaded; check results |
| bronze | one per source table | append only; adds `operation`, `source_commit_ts`, `source_system`, `source_record_id`, `ingested_at`, `batch_run_id`, `_source_file`. Hidden partition `days(ingested_at)`. |
| silver | reference tables (latest row per key), `customer_history` (SCD2), `meter_reading`, `reading_correction`, `dlq` | typed, deduplicated, validated; rejects keep a reason |
| gold | dims, facts, `fact_reconciliation`, `report_daily_consumption` | star schema, surrogate keys, only reconciled data is published |

## Silver rules
- **Readings**: highest `version` per `reading_id` wins (tie: latest commit). Rejected to `silver.dlq` with a reason:
  `NULL_METER_ID`, `NULL_READING_TS`, `NULL_CONSUMPTION`, `NEGATIVE_CONSUMPTION`, `INVALID_READING_TYPE`, `UNKNOWN_METER`,
  `NO_SETTLEMENT_PERIOD`.
- **Corrections**: the latest correction per reading replaces the reported kWh if `corrected_at` is later than the reading's
  `source_updated_at`. Corrections for readings Silver has not seen are rejected (`UNKNOWN_READING`); a correction that arrives
  before its reading is therefore lost. Known MVP limitation.
- **Customer SCD2**: a new version only when `customer_type` or `region` changes, ordered by `source_updated_at`. The first version
  starts at 1900-01-01 so any reading resolves to a version.
- Silver reference tables and `customer_history` are rebuilt from all Bronze each run: simple and replay-safe, not for large data.

## Gold
`dim_meter`, `dim_tariff`, `dim_customer` (SCD2), `dim_customer_pii` (the only place with names and addresses), `dim_date`,
`dim_settlement_interval` (1..50), `dim_reading_quality`. Every dimension except the last two and `dim_customer_pii` has an
`UNKNOWN` member with key -1.

| Fact | Grain | Measures |
|---|---|---|
| `fact_meter_reads` | meter × settlement interval | `consumption_kwh`, `read_count` |
| `fact_consumption_daily` | meter × settlement date | `daily_kwh`, `valid_intervals`, `expected_intervals`, `completeness` |
| `fact_reconciliation` | source × period × control × version | source/target totals and counts, variance, status |

Facts take the dimension version effective at the reading time (assignment by settlement date, customer version by reading
timestamp), resolved when loading. In `fact_consumption_daily` the customer and tariff are those of the last interval of the
day. Interval and daily facts are never unioned.

## Reconciliation
For each control (latest `control_version`), Gold total must be within `recon_threshold_pct` (default 0.01%) and the interval count
must match exactly. Status is one of `PASSED`, `BLOCKED_TOTAL`, `BLOCKED_COUNT`, `MISSING_TARGET`, `MISSING_CONTROL`. Any
non-PASSED status fails the run and nothing is published. The control totals are computed by the generator independently of the
pipeline.

## Data quality (error severity stops the run)
Grain uniqueness of both facts; no NULL kWh; no orphan foreign keys for meter, customer, tariff, quality, date, interval;
exactly one current version per customer; no overlapping customer versions; warning if more than 1% of readings have an unknown customer.
