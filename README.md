# Global Energy AWS Lakehouse (MVP)

Synthetic smart-metering data in **RDS PostgreSQL** → **AWS DMS** (full load + CDC, parquet) → **S3** → Glue schema-contract
gate → **Databricks** (Bronze → Silver → Gold, Iceberg on S3, **Glue Catalog**) → **Athena**. **Step Functions** orchestrates,
**Terraform** provisions. Output: a reconciled daily-consumption star schema that is only published if it matches the
billing control totals.

Design and decisions: [IMPLEMENTATION-PLAN.md](IMPLEMENTATION-PLAN.md). Model and rules: [docs/data-model.md](docs/data-model.md).

> **Status: runs locally, never deployed.** `ruff check`, the full `pytest` suite (15 tests, including the
> Spark/Iceberg end-to-end) and `terraform validate` on both stacks all pass. Nothing has been applied to an AWS
> account, so the notebooks, the Step Functions definition and every resource argument are still unproven against
> the real services — deploy steps 4-7 of the build order in the plan remain to be run.

```
generator/      synthetic scenario (day 1 snapshot, day 2 changes) -> PostgreSQL, or a local DMS imitation
sql/postgres/   source schema (schema `src`)
lakehouse/      pipeline code: contract gate, bronze, silver (+SCD2), gold, dq, reconcile
glue/           Glue job: contract_check.py
databricks/     notebooks (thin wrappers over lakehouse/) + spike notebook
orchestration/  Step Functions definition (ASL)
infra/terraform main stack (VPC, RDS, DMS, S3, IAM, Glue, Athena, Databricks job, Step Functions)
infra/terraform/databricks_account   optional: create the Databricks workspace on AWS
tests/          contract + generator unit tests; local Spark/Iceberg end-to-end
scripts/        run_local.py
```

## Run it locally

Needs a JDK (17 is what this is run against; set `JAVA_HOME` if several are installed) and a virtualenv on
Python 3.10-3.12 — pyspark 3.5.3 has no wheel for 3.13+.

```bash
python3.12 -m venv .venv && .venv/bin/pip install -e ".[dev]"
export JAVA_HOME=$(/usr/libexec/java_home -v 17)      # macOS; skip where the JDK is already the default
.venv/bin/ruff check .
.venv/bin/pytest -m "not spark"                       # contract gate, generator, DMS-file shape
.venv/bin/pytest -m spark                             # full pipeline on local Spark + Iceberg (~75s, downloads the
                                                      # Iceberg jar on the first run)
```

The Spark suite drives the whole batch against a DMS imitation: baseline over two days plus a rerun, bad control
total (must block), breaking schema (must quarantine and hold), additive schema (must flow through).
`python scripts/run_local.py` runs the same pipeline outside pytest and leaves the warehouse on disk to inspect.

Terraform: `terraform fmt -check -recursive infra`, then `init -backend=false` and `validate` in both
`infra/terraform` and `infra/terraform/databricks_account`. Validation only type-checks the configuration; it says
nothing about whether the resources behave, so deploy and run the spike (below) before the full pipeline.

## Deploy

Prerequisites: AWS credentials for a dev account, a Databricks workspace on AWS (or apply `infra/terraform/databricks_account`
first), a token for a Databricks service principal, and this repo reachable from the workspace (private repo: add a Git
credential in the workspace).

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars        # fill in admin_cidrs, alert_email, databricks_host
export TF_VAR_databricks_token=...                  # never commit it
terraform init && terraform apply
```

If you create the workspace with `databricks_account/`, apply it first with `cluster_role_arn` set to
`arn:aws:iam::<account>:role/glenergy-dev-databricks-cluster` (the role this stack creates). If you already have a workspace,
make sure its cross-account role may `iam:PassRole` that same role, or `databricks_instance_profile` fails to register.

1. **Reboot RDS once** so `rds.logical_replication` applies: `aws rds reboot-db-instance --db-instance-identifier glenergy-dev-src`.
2. **Seed the source** (day 1) from an IP in `admin_cidrs`:
   ```bash
   export SRC_DSN="$(aws secretsmanager get-secret-value --secret-id glenergy-dev/source-db --query SecretString --output text \
     | jq -r '"postgresql://\(.username):\(.password)@\(.host):\(.port)/\(.dbname)"')"
   python -m generator.cli postgres --dsn "$SRC_DSN" --day 1 --reset --expected-out .local/expected.json
   ```
3. **Start DMS** (full load, then CDC continues): `aws dms start-replication-task --replication-task-arn $(terraform output -raw dms_task_arn) --start-replication-task-type start-replication`.
   Check `s3://<bucket>/dms/src/<table>/LOAD00000001.parquet` appears.
4. **Spike first**: run `databricks/notebooks/99_spike_glue_iceberg.py` on a cluster with the job's Spark config and libraries
   (copy them from `databricks.tf`). Then in Athena (workgroup `glenergy-dev`): `SELECT * FROM spike.hello;`.
   If Databricks cannot write Iceberg through Glue (library or access-mode issue), stop here and see the fallback in the plan.
5. **Run day 1**: `aws stepfunctions start-execution --state-machine-arn $(terraform output -raw state_machine_arn) --input '{"run_id":"day1"}'`.
6. **Apply day 2 changes**: `python -m generator.cli postgres --dsn "$SRC_DSN" --day 2`; wait about two minutes for DMS CDC files, then run
   execution `{"run_id":"day2"}`. Day 2 is 2026-10-25, the UK clock-change day: 50 settlement periods.
7. **Query** in Athena: `SELECT * FROM gold.report_daily_consumption LIMIT 10;` and `SELECT * FROM gold.fact_reconciliation;`.
   Compare totals with `.local/expected.json`.

Turn on the daily schedule with `schedule_enabled = true`.

## Operating notes

- **Blocked reconciliation**: `gold.fact_reconciliation` holds the evidence; nothing is published for that run. Fix the cause and run
  again with a new `run_id`.
- **Quarantined schema**: the Glue job fails, SNS emails, the file is in `s3://<bucket>/quarantine/<run_id>/...` and
  `s3://<bucket>/ctl/quarantine_flags/<table>.json` holds the table. Silver skips held tables. After deciding (update the contract in
  `lakehouse/tables.py`, or ask the source to revert), copy the quarantined files back under `dms/src/`, delete the flag
  (`lakehouse.contract.clear_hold`), and run again.
- **Replay**: Silver reads only Bronze. To rebuild, re-run the Databricks job with the same `run_id`s or truncate Silver/Gold tables.
- **DMS does not replicate DDL from PostgreSQL.** A new source column reaches S3 only after the table is reloaded
  (`aws dms reload-tables`), and the schema-change scenarios of the generator exist only in the DMS imitation.
- **Cost**: RDS `db.t4g.micro`, DMS `dms.t3.micro`, a single-node Databricks job cluster (spot), small Glue runs. Stop the DMS task and
  RDS when idle; `terraform destroy` removes everything (`force_destroy` is on for dev buckets).

## What is not built

Streaming (Kafka, Structured Streaming, DynamoDB), Redshift + dbt, MWAA/Airflow, Lake Formation and KMS key management,
billing / trading / generation facts, regulatory result tables, Great Expectations, Unity Catalog. See the plan.
