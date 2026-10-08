# Global Energy AWS Lakehouse: MVP Implementation Plan

Sources: `Case_Study_1_Global_Energy_AWS.pdf` (**Doc A**) and `Global-Energy-AWS-Lakehouse-Platform.pdf` (**Doc B**).
Status: steps 1-5 of the build order below are run and passing locally; steps 6-7 need an AWS account (see README).

## 1. Decisions

| Topic | Decision |
|---|---|
| Source | Synthetic data in **RDS PostgreSQL** (stands in for the metering, CRM and registry systems) |
| Ingestion | **AWS DMS**, full load + CDC, parquet to S3 with an `Op` column and commit timestamp |
| Processing | **Databricks** for Bronze, Silver and Gold; **Glue** for the Data Catalog and the schema-contract check |
| Table format | **Iceberg** on S3, registered in the Glue Catalog |
| Orchestration | **Step Functions** (Glue job, then Databricks Jobs API, then SNS on failure) |
| Serving | **Athena** only |
| Infra / CI | Terraform, GitHub Actions |

## 2. Outcome

A daily, replayable batch that turns smart-meter readings into a reconciled daily-consumption star schema, and **blocks publication
if consumption does not reconcile to the billing control total**.

In scope: DMS CDC ingestion, contract gate (additive passes, breaking is quarantined and the table held), Bronze with audit
columns, Silver (dedupe by version, corrections, validation with a reject table, SCD2 customer), Gold star schema, reconciliation
gate, DQ checks, Terraform, CI.
Out of scope: Kafka / streaming / DynamoDB, Redshift + dbt, MWAA, Lake Formation / KMS, billing, trading and generation facts,
regulatory result tables, Great Expectations, Unity Catalog.

## 3. Architecture

```
RDS PostgreSQL --DMS (full load + CDC)--> s3://lake/dms/src/<table>/...
  Step Functions:
    1. Glue  contract_check  -> ctl/manifests/<run_id>.json (files allowed into Bronze); breaking files -> quarantine/, table held
    2. Databricks job: setup -> bronze -> silver -> gold_dims -> gold_facts -> dq -> reconcile_publish
  Iceberg tables in Glue Catalog (bronze, silver, gold, ctl)  -->  Athena
  Any failure -> SNS email
```

Design rules: Silver reads only Bronze; one surrogate-key helper for dimensions and facts; facts use the dimension version
effective at event time; UTC plus a settlement calendar (the DST day has 50 periods); PII only in `dim_customer_pii`; every step
idempotent per `run_id`; control totals come from the generator, independent of the pipeline.

## 4. How AWS connects to Databricks

| Concern | Azure | AWS |
|---|---|---|
| Workspace | Azure resource | Created in the Databricks account console or with `databricks_mws_*` Terraform (`infra/terraform/databricks_account`); DBUs billed separately |
| Compute | managed resource group | Control plane in Databricks' AWS account; clusters (EC2) in your account |
| Launch permission | managed | Cross-account IAM role trusting Databricks, external ID = your Databricks account ID |
| Lake access | access connector | **Instance profile** on the cluster (MVP) or Unity Catalog storage credential + external location (phase 2) |
| Catalog | Unity Catalog / Hive | Open-source Iceberg runtime with `GlueCatalog` (MVP) |
| Orchestrator to Databricks | ADF activity | Step Functions HTTP task calling `jobs/run-now`, token held in an EventBridge connection |
| Data in | ADF copy | DMS writes S3; Databricks never connects to the source database |

**Risk to prove first:** Databricks writing Iceberg through Glue needs the Iceberg libraries on the cluster, and it bypasses Unity
Catalog governance. `databricks/notebooks/99_spike_glue_iceberg.py` tests it in one table. Fallback: Unity Catalog with Delta +
UniForm (Iceberg metadata) and Athena reading through Glue.

## 5. Deviations from the documents (and why)

| Document says | MVP does |
|---|---|
| Dates differ (Nov 2025 to present vs Sep 2024 to Oct 2025) | not a build issue; fix before external use |
| Doc A: schema check by Lambda on S3 PutObject | Glue job before Bronze, producing a manifest of allowed files |
| Doc A: dedupe on `meter_id + reading_ts`; Doc B has `reading_id` + `version` | dedupe on `reading_id`, highest `version` |
| Doc A: DynamoDB watermark | not needed: DMS CDC replaces watermark extraction; file tracking in `ctl.landing_files` |
| Doc A: MWAA retries 10/20/40 min | Step Functions retries only the Databricks API call; DQ and reconciliation failures are real and not retried |
| Doc A: 50M+ reads/day, 99.9% SLA, 6-8% to under 1%, 2 min latency | not claimed; Doc B marks outcome numbers unverified |
| Doc B: tariff via bill lines | tariff on `meter_assignment`, simplification |
| Doc B: `BILL`/`BILL_LINE` control source | `reconciliation_control` billing totals table |

## 6. Build order and what proves each step

| # | Step | Proof | State |
|---|---|---|---|
| 1 | Generator + Postgres DDL | `pytest -m "not spark"`: totals match an independent recomputation, DST day has 50 periods | passing |
| 2 | Contract gate | unit tests: additive passes, breaking quarantines and holds, idempotent per run | passing |
| 3 | Local pipeline on DMS imitation | `pytest -m spark`: counts, totals, SCD2, corrections, rerun is a no-op | passing |
| 4 | Negative paths | bad control total blocks publication; breaking schema is held; additive flows into Bronze | passing |
| 5 | Terraform | `terraform fmt`, `validate` on both stacks | validates; **not applied** |
| 6 | Spike | Databricks writes Iceberg via Glue, Athena reads it | not run (needs AWS) |
| 7 | AWS day 1, day 2, rerun | Athena totals equal `expected.json`; rerun changes nothing | not run (needs AWS) |

What the local suite does **not** reach, because each piece only exists against a real service: `PostgresSink` and
`sql/postgres/001_schema.sql` (first executed at deploy step 2), `S3Store` (the tests drive `LocalStore`), the Glue
entry point `glue/contract_check.py`, the notebooks and `lakehouse.notebook.params`, and the Step Functions
definition. The pipeline logic underneath them is covered; the wiring is not.

Bugs the first local run turned up, all fixed: the table registry could not be imported at all (a column called
`name` collided with the `_t()` helper's own parameter); the DMS imitation typed columns from a global
column-to-family map, which is wrong wherever two tables disagree (`period`); Bronze derived the file key that
`ctl.landing_files` is keyed on by matching `/src/` in the path, so outside the AWS layout it recorded an empty key
and the idempotency guard did nothing; and the contract gate's `OVERLAP` rewind re-listed files a previous run had
already checked, passing them into a second manifest.

## 7. Known limitations

- Corrections that arrive before their reading are rejected, not held.
- Silver reference tables and the customer history are fully rebuilt each run.
- The contract check reads whole parquet files from S3 to get the schema.
- Terraform state is local; the Databricks token is stored in Terraform state and an EventBridge connection (use OAuth service
  principals before anything beyond a dev account).
- DMS does not replicate DDL from PostgreSQL; schema changes need a table reload.
- No Lake Formation, KMS customer keys or CloudTrail retention.
