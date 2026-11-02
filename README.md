# Payments data platform

A settlement platform built on synthetic payments. It loads, models, checks and recovers data. You build and run it on your laptop first, then deploy it to AWS with Terraform from GitHub Actions, using Free plan services.

## Data

Synthetic, so you can publish everything. The template includes a ledger generator and a mock settlement API with pagination, rate limits, failures and a late-batch mode.


```bash
python tools/generate_ledger.py --rows 1000000 --out data   # use --rows 10000000 for the Spark milestone
python tools/settlement_api.py --ledger data/ledger.csv --fail-rate 0.05 --rate-limit 20
```

The ledger is seeded from your GitHub username, so your totals and late days are yours alone; pass `--seed 42` for the shared reference data.
These helpers are provided so you start with data; the milestone checks look at your own code, not at `tools/`.
Run the API with `--hold-days 7` for the capstone scenario, then `curl -X POST localhost:8080/admin/release` when the late batches "arrive".

Built step by step on the [Stacc Data Engineering roadmap](https://app.getstacc.org/learn/de). Each module adds one verified
milestone to this repository; the commit history is the build log.

## Milestones

| # | Module | Adds to this repo | Verified |
|---|---|---|---|
| 01 | Ingestion Pipeline | Write a one-page design doc, then load the payments API into PostgreSQL in Docker with a saved cursor, and rerun it without creating duplicates. | ☐ |
| 02 | Warehouse Modeling | Design a star schema for payments, keep one dimension’s history with SCD Type 2, build a cumulative activity table and record why this design fits. | ☐ |
| 03 | Transform with dbt | Turn raw tables into tested staging models, a merchant snapshot and an incremental payments mart. | ☐ |
| 04 | Scheduling with Airflow | Run ingestion, checks and dbt as an Airflow DAG with retries, an alert and a seven-day backfill. | ☐ |
| 05 | Cloud Data Lake | Build the lake locally with Parquet and DuckDB, then define it on AWS in Terraform: S3 layers, a Glue table, Athena and least-privilege roles, checked in CI. | ☐ |
| 06 | Big Data with Spark | Run a PySpark job on a large ledger, compare it with a plain baseline and fix one slow shuffle or skew. | ☐ |
| 07 | Streaming with Kafka | Stream payment events through Kafka, survive a consumer crash and replay without duplicate rows. | ☐ |
| 08 | Run and Monitor | Run the pipeline on a schedule locally, then deploy it to AWS from GitHub Actions: a scheduled Lambda, S3, and a freshness check at a live URL with alerts. | ☐ |
| 09 | Data Platform Capstone | Release the platform live on AWS with green CI, lineage, alerts, a recovery runbook, an updated design doc and a walkthrough video. | ☐ |

## Acceptance evidence

Use the guided fixture and the independent variation in each module. Fix failures, then rerun every earlier test on the same candidate commit.
The filenames below are the submission contract, not supplied solutions. Implement the tests and artifacts yourself; do not commit generated caches or personal data.

### Ingestion Pipeline

- `README.md`
- `docker-compose.yml`
- `src/ingestion.py`
- `tests/test_ingestion.py`
- `docs/design.md`

### Warehouse Modeling

- `docs/bus_matrix.md`
- `models/schema.sql`
- `tests/test_modeling.py`

### Transform with dbt

- `dbt_project.yml`
- `models/schema.yml`
- `tests/test_dbt_acceptance.py`

### Scheduling with Airflow

- `dags/pipeline.py`
- `docs/backfill.md`
- `tests/test_backfill.py`

### Cloud Data Lake

- `infra/main.tf`
- `infra/iam.tf`
- `infra/budget.tf`
- `infra/lake.tf`
- `docs/lake.md`
- `infra/variables.tf`
- `docs/cost.md`
- `.github/workflows/ci.yml`
- `infra/tests/security.tftest.hcl`
- `tests/test_cost_comparison.py`

### Big Data with Spark

- `src/spark_job.py`
- `docs/spark_plan.md`
- `tests/test_spark_equivalence.py`

### Streaming with Kafka

- `src/streaming.py`
- `docs/streaming.md`
- `tests/test_streaming_recovery.py`

### Run and Monitor

- `infra/schedule.tf`
- `infra/alarms.tf`
- `docs/ops.md`
- `tests/test_ops.py`
- `infra/tests/ops.tftest.hcl`
- `.github/workflows/deploy.yml`
- `README.md`

### Data Platform Capstone

- `README.md`
- `docs/architecture.md`
- `docs/data_dictionary.md`
- `docs/runbook.md`
- `tests/test_platform_acceptance.py`
- `docs/design.md`

Keep the assessment job named `test` in GitHub Actions. It must run the actual acceptance tests; a green empty workflow does not establish correctness.
For non-Python artifacts, add the appropriate validation/integration commands to CI and retain their outputs.
Stacc's automatic checks establish artifact structure and CI status. Use the module criteria to assess numerical correctness, design quality and independent understanding.

Tick a row when Stacc verifies the milestone. Replace this section's intro, but keep the table:
the first milestone checks that the plan is here.

<!--
Final demo question (answer it in the capstone walkthrough):
A partner API failed and seven days of payments arrived late. Show how your platform loads them, checks the totals and recovers without counting anything twice.

The capstone requires a "## Walkthrough" section in this README linking a
walkthrough of five minutes or less, and CI passing on the submitted commit.
-->
