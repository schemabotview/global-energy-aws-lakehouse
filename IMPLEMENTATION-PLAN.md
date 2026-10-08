# Global Energy AWS Lakehouse: MVP Implementation Plan

Sources: `Case_Study_1_Global_Energy_AWS.pdf` (**Doc A**, STAR narrative) and `Global-Energy-AWS-Lakehouse-Platform.pdf` (**Doc B**, design guide).

## 1. Document review (what the MVP must work around)

| # | Finding | MVP decision |
|---|---|---|
| 1 | Dates conflict: Doc A says Nov 2025 to present, Doc B says Sep 2024 to Oct 2025. | Not a build issue. Fix before using either document externally. |
| 2 | Doc A puts the Bronze/Silver/Gold layers in one "curated zone", and also gives the raw zone its own prefix. | Use one Iceberg database per layer: `bronze`, `silver`, `gold`. |
| 3 | Doc A names Redshift + dbt for Gold. Doc B marks serving as a "guide" option and says Redshift/dbt are unconfirmed. | Gold lives in Iceberg and is served by Athena. dbt and Redshift are phase 2. |
| 4 | Doc A dedupes readings on `meter_id + reading_ts`. Doc B has `reading_id`, `version` and a separate `READING_CORRECTION` table. | Dedupe on `reading_id`, keep the highest `version`, then apply corrections. Cross-check on `meter_id + reading_ts`. |
| 5 | Volume: Doc A says 50M+ reads/day. Its stream sizing (450K events/hour peak) is about 10M/day at most. | Make volume a generator parameter. Prove correctness at 1M reads/day. Do not claim scale. |
| 6 | Doc A's results (6-8% to under 1%, 99.9% SLA, 2 min latency) are not backed by anything. Doc B says guide outcome numbers are unverified. | The MVP measures its own numbers (reject rate, variance, run time). Never quote Doc A's figures as achieved. |
| 7 | Doc A has a 0.01% reconciliation gate to billing. Doc B says thresholds are business-owned. | Make the threshold a config value, default 0.01%. |
| 8 | Doc A says a Lambda schema check fires on S3 PutObject. Doc B shows a "schema contract gate". | A contract check inside the pipeline, as a task before Silver, with the same quarantine behaviour. |

## 2. MVP outcome

One sentence: **a daily, replayable batch pipeline that turns synthetic smart-meter reads into a reconciled, SCD2-aware daily consumption star schema, and blocks the regulatory output if consumption does not reconcile to the billing control total.**

This covers the parts of both documents that matter most: Bronze/Silver/Gold on Iceberg, schema evolution vs quarantine, corrections, SCD2 customer/tariff, the reconciliation gate, and Terraform/CI.

### In scope
- Synthetic generator for these tables (Doc B names): `METER`, `SUPPLY_POINT`, `CUSTOMER`, `ACCOUNT`, `METER_ASSIGNMENT`, `TARIFF`, `METER_READING`, `READING_CORRECTION`, `BILL`/`BILL_LINE` (for the control total), `SETTLEMENT_CALENDAR`.
- Bronze: immutable raw Iceberg tables with audit columns (`source_system`, `source_record_id`, `source_updated_at`, `ingested_at`, `batch_run_id`, `operation`).
- Schema contract gate: additive change is auto-approved; breaking change (dropped column, kWh to Wh type/unit change) quarantines the partition and stops Silver for that source.
- Silver: UTC timestamps, dedupe, apply corrections, unit normalisation, null/invalid rows to a DLQ with `rejection_reason`.
- Gold star: `FACT_METER_READS` (meter × half-hour), `FACT_CONSUMPTION_DAILY` (meter × local settlement date), `FACT_RECONCILIATION`; `DIM_METER`, `DIM_CUSTOMER` (SCD2), `DIM_TARIFF`, `DIM_DATE`, `DIM_SETTLEMENT_INTERVAL`.
- Reconciliation: Gold daily total vs billing control total per scope and cutoff. A variance above the threshold sets the run to `BLOCKED` and no report is published.
- Terraform, GitHub Actions (plan on PR, apply with approval), Athena queries and a runbook.

### Out of scope (phase 2)
Kafka, Structured Streaming, Delta sink and DynamoDB hot path; Redshift, Spectrum, dbt and Power BI; MWAA; generation, trading and billing facts; Lake Formation column-level security and KMS CMK rotation; `FACT_REGULATORY_RESULT` and `BRIDGE_REPORT_CONTRIBUTION`; Great Expectations.

## 3. Architecture (MVP)

```
generator (Python) -> S3 landing/ (parquet, per-day manifest + checksum)
  -> Glue job: bronze_load      -> bronze.* (Iceberg, append only)
  -> Glue job: contract_check   -> pass / quarantine (S3 quarantine/ + control row)
  -> Glue job: silver_clean     -> silver.* (+ silver.dlq)
  -> Glue job: silver_scd2      -> silver.customer_tariff_history
  -> Glue job: gold_build       -> gold.dim_*, gold.fact_*
  -> Glue job: reconcile        -> gold.fact_reconciliation, run status
  -> Athena (workgroup) for queries
Step Functions orchestrates the chain; Terraform provisions; GitHub Actions deploys.
```

Choices to confirm (see section 7):
- **Glue PySpark for everything**, not Databricks. Doc A uses Databricks for ingestion, but one engine keeps the MVP cheap and simple. The notebooks/jobs are plain PySpark, so porting is easy.
- **Step Functions instead of MWAA** for the MVP. MWAA has a high fixed monthly cost. Dependencies, retries (3, backoff) and failure alerts via SNS are all still shown. Airflow DAGs are a phase-2 port.
- **Iceberg on S3 with the Glue Catalog**, as both documents specify.
- **Control tables** in Iceberg (`ctl.batch_audit`, `ctl.watermark`, `ctl.schema_contract`, `ctl.reconciliation_result`) instead of DynamoDB. Doc A uses DynamoDB for the watermark. Swap later if needed.

## 4. Key design rules

1. Silver reads only Bronze, never the source, so any day can be replayed.
2. Surrogate keys come from one shared helper used by both dimensions and facts. (The previous Azure project hit a bug where the fact and dimension hashed keys differently, which orphaned every foreign key. Add a test that fails on any orphaned FK.)
3. Facts take the SCD2 dimension version effective at event time, resolved at load, not by a runtime date join.
4. Store UTC plus the settlement calendar. DST days have 46 or 50 half-hours. The generator must include one DST day.
5. Do not union interval and daily facts. Aggregate each fact to its own grain before comparing.
6. Customer PII sits only in a restricted `dim_customer_pii`. Facts and `dim_customer` carry no names or addresses.
7. Every job is idempotent: re-running a `batch_run_id` produces the same result with no duplicates.

## 5. Build order and acceptance tests

Each step is done only when its test passes. Run locally on PySpark + Iceberg before any AWS deploy, and re-run after every fix. (The last project shipped with the fixed code, day 2 and the re-run path untested. Do not repeat that.)

| Step | Deliverable | Acceptance test |
|---|---|---|
| 0 | Repo skeleton, CI (lint, unit tests), pre-commit secret scan | CI green on an empty PR. |
| 1 | Generator: day 1 (full load) and day 2 (new reads, corrections, a tariff change, a unit change, duplicates, bad rows) | Row counts match the config. The injected defect counts are written to a file for later checks. |
| 2 | Local runner: Spark + Iceberg, `bronze_load` | Bronze counts equal landing counts. A re-run adds no rows. |
| 3 | `contract_check` | Additive column passes. Dropped column or Wh unit quarantines and halts Silver. |
| 4 | `silver_clean` | Duplicates removed, corrections applied, injected bad rows land in the DLQ with the right reasons. Counts match step 1. |
| 5 | `silver_scd2` and `gold_build` | One current row per customer. No overlapping ranges. Zero orphaned FKs. Day 2 closes the old tariff row and opens a new one. |
| 6 | `reconcile` | Clean data: variance under threshold, status `PASSED`. Injected 1% error: status `BLOCKED`. |
| 7 | Terraform: S3, Glue DB/jobs/roles, Athena workgroup, Step Functions, SNS | `terraform validate` + plan in CI. Apply to a dev account. |
| 8 | Deploy and run day 1, day 2 and a re-run in AWS | Same results as the local run. The re-run changes nothing. |
| 9 | Runbook, Athena sample queries, cost note | A new engineer can run it from the README. |

Suggested order of effort: steps 0 to 6 locally first (about 60% of the work), then 7 to 9.

## 6. Repo layout

```
infra/terraform/        modules: storage, glue, athena, orchestration, iam
generator/              synthetic data (day 1 / day 2, injected defects)
pipeline/jobs/          bronze_load, contract_check, silver_clean, silver_scd2, gold_build, reconcile
pipeline/common/        keys (sk), audit columns, contracts, config
orchestration/          state machine definition
tests/                  unit + local end-to-end (day 1, day 2, re-run)
sql/athena/             sample queries
docs/                   data model, runbook
.github/workflows/      ci.yml, deploy.yml (plan on PR, apply with approval)
```

## 7. Decisions needed

1. **Engine:** Glue only (recommended) or Glue + Databricks to mirror Doc A?
2. **Orchestration:** Step Functions (recommended) or MWAA?
3. **Serving:** Athena only (recommended) or add Redshift Serverless + dbt now?
4. **AWS account:** is there a dev account with permission to create IAM roles and Glue/Athena/S3, or should the MVP stay local until one exists?
5. **Interview use or real build?** If this is interview prep, keep the phase-2 streaming path as a design discussion. If it will be shown as working code, build only what passes the tests above.

## 8. Rough cost (dev, small data)

Under about 20 USD per month with Step Functions, small Glue runs (G.1X, few DPUs for minutes per day), S3 and Athena. MWAA alone would add several hundred USD per month, which is the main reason to defer it.

## 9. Status

Plan only. No code, Terraform or AWS resources exist yet.
