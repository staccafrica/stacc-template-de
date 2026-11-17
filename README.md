# Payments data platform

A settlement platform built on synthetic payments. It loads, models, checks and recovers data. You build and run it on your laptop first, then deploy it to AWS with Terraform from GitHub Actions, using Free plan services.

## Data

Synthetic, so you can publish everything, but built to behave like a real pan-African payment processor: five currencies, cards, transfers, USSD and mobile money, fees, payouts, late refunds and a dirty partner feed. The template includes the generator and a partner API with keys, rate limits, failures and a late-batch mode.


```bash
python tools/generate_ledger.py --rows 1000000 --out data    # about 200 MB; use --rows 10000000 for the Spark milestone
export STACC_API_KEY=sk_test_local_123                       # any value; put it in .env, never in Git
python tools/settlement_api.py --ledger data/ledger.csv      # http://127.0.0.1:8080, contract in tools/openapi.yaml
```

The data is a pan-African payment processor's year: five countries and currencies, card, bank transfer, USSD and mobile money,
lunch and evening peaks, paydays and December, next-business-day settlement with fees, and refunds and chargebacks that land days later.
It is seeded from your GitHub username, so your totals and late days are yours alone; pass `--seed 42` for the shared reference data.
These helpers are provided so you start with data; the milestone checks look at your own code, not at `tools/`.

### Files the generator writes

| File | What it is | Used from |
|---|---|---|
| `data/ledger.csv` | One row per payment in its final state: amount, fee, net, currency, method, status, created, settled, reversed and updated times, payout batch | Ingestion (through the API) |
| `data/merchants.csv` | Each merchant as it was on 1 January: name, category, country, city, risk tier, onboarding date | Warehouse Modeling: the merchant dimension |
| `data/merchant_changes.csv` | Every later change, one full new version per row, with valid_from and the reason | Warehouse Modeling: your Slowly Changing Dimension Type 2 input |
| `data/payouts.csv` | One payout per merchant per settlement day: gross, fees, net, count | Reconciliation: your totals must match these |
| `data/events.jsonl` | Authorization, capture, decline, settlement, refund and chargeback events per payment, in arrival order | Streaming: your Kafka input |

Amounts are integers in minor units: divide by 100 for naira, shillings, cedis, rand or pounds, and never add two currencies together.
Like any real partner feed it has a few defects. Some payments are sent twice, a few have no account or a lower-case currency, one field
appears halfway through the year, and some timestamps carry a local offset. Events can arrive late, out of order or twice.
Finding and handling these is part of the work.

### Settlement API

The full contract is `tools/openapi.yaml`. The short version:

- Send `Authorization: Bearer $STACC_API_KEY` on every `/v1` request. Without it you get `401`.
- `GET /v1/transactions?limit=500` returns `{"object": "list", "data": [...], "has_more": true, "next_cursor": "cur_..."}`. `limit` is at most 1000.
- Ask for the next page with `&cursor=<next_cursor>` and the same filters. The cursor is opaque, so never build one. Save the last one you finished so a rerun resumes there.
- Filters: `created_from` and `created_to` for a day or a backfill, `updated_since` to pick up payments that changed after you loaded them, plus `merchant_id` and `status`.
- `GET /v1/payouts` lists settlement batches, with `settlement_date_from` and `settlement_date_to`.
- `429`: you went over the per-key rate limit. Wait `Retry-After` seconds. `X-RateLimit-Remaining` tells you how close you are.
- `500` and `503`: temporary failures. Retry with exponential backoff and jitter. Some responses are just slow, so set a timeout.
- `400`: a bad parameter or cursor. Fix the request; don't retry it. Every error has a `request_id`.
- A page can repeat the last payment of the page before, so write with an upsert on `txn_id`.
- Start with `--profile calm` (no failures or limits) while you build, then use the default `realistic` one. `--profile harsh` is for your tests.
- For the capstone, run with `--hold-days 7`: the last week is held back and older payments show as they were then. Then
  `curl -X POST localhost:8080/admin/release` when the late batches "arrive", and load them with `updated_since`.

### Before you start

- **Docker:** Docker Desktop's disk image grows with every image and volume. Keep about 20 GB free and run `docker system prune` between milestones.
- **Tests import your code:** `pytest.ini` sets `pythonpath = .`, so tests can `from src.ingestion import ...`. Keep it.

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

Each list is exactly what Check my work looks for, including the section headings each file needs.

### Ingestion Pipeline

Task: https://app.getstacc.org/roadmap/de-etl

- `README.md` with sections: Setup, Configuration, Safe reruns
- `docker-compose.yml`
- `src/ingestion.py`
- `tests/test_ingestion.py`
- `docs/design.md` with sections: Components, Data flow, Failure modes, Cost
- GitHub Actions job `test` passes on the submitted commit

### Warehouse Modeling

Task: https://app.getstacc.org/roadmap/de-modeling

- `docs/bus_matrix.md` with sections: Grain, Facts, Dimensions, Slowly changing, Design choice
- `models/schema.sql`
- `tests/test_modeling.py`
- GitHub Actions job `test` passes on the submitted commit

### Transform with dbt

Task: https://app.getstacc.org/roadmap/de-dbt

- `dbt_project.yml`
- `models/schema.yml` with sections: tests, description
- `tests/test_dbt_acceptance.py`
- GitHub Actions job `test` passes on the submitted commit

### Scheduling with Airflow

Task: https://app.getstacc.org/roadmap/de-orchestration

- `dags/pipeline.py`
- `docs/backfill.md` with sections: Procedure, Validation, Recovery
- `tests/test_backfill.py`
- GitHub Actions job `test` passes on the submitted commit

### Cloud Data Lake

Task: https://app.getstacc.org/roadmap/de-cloud

- `infra/main.tf` with sections: aws_s3_bucket
- `infra/iam.tf` with sections: aws_iam_role
- `infra/budget.tf` with sections: aws_budgets_budget
- `infra/lake.tf` with sections: aws_glue_catalog_table, aws_athena_workgroup
- `docs/lake.md` with sections: Layers, Partitioning, Query
- `infra/variables.tf`
- `docs/cost.md` with sections: Baseline, Optimisation, Result
- `.github/workflows/ci.yml` with sections: terraform validate, checkov
- `infra/tests/security.tftest.hcl`
- `tests/test_cost_comparison.py`
- GitHub Actions job `test` passes on the submitted commit

### Big Data with Spark

Task: https://app.getstacc.org/roadmap/de-spark

- `src/spark_job.py`
- `docs/spark_plan.md` with sections: Baseline, Explain plan, Partitioning, Result
- `tests/test_spark_equivalence.py`
- GitHub Actions job `test` passes on the submitted commit

### Streaming with Kafka

Task: https://app.getstacc.org/roadmap/de-streaming

- `src/streaming.py`
- `docs/streaming.md` with sections: Ordering, Delivery semantics, Replay, Recovery
- `tests/test_streaming_recovery.py`
- GitHub Actions job `test` passes on the submitted commit

### Run and Monitor

Task: https://app.getstacc.org/roadmap/de-ops

- `infra/schedule.tf` with sections: aws_scheduler_schedule
- `infra/alarms.tf` with sections: aws_cloudwatch_metric_alarm
- `docs/ops.md` with sections: SLA, Alerts, Failure drill, Recovery
- `tests/test_ops.py`
- `infra/tests/ops.tftest.hcl`
- `.github/workflows/deploy.yml`
- `README.md` with sections: Live
- GitHub Actions job `test` passes on the submitted commit
- the link under the README Live section answers when checked (a health endpoint)

### Data Platform Capstone

Task: https://app.getstacc.org/roadmap/de-vectordb

- `README.md` with sections: Setup, Validation, What broke, Cost, Walkthrough, Live
- `docs/architecture.md`
- `docs/data_dictionary.md`
- `docs/runbook.md` with sections: Alerts, Backfill, Recovery
- `tests/test_platform_acceptance.py`
- `docs/design.md` with sections: What changed, At 10x
- GitHub Actions job `test` passes on the submitted commit
- README Walkthrough section links an HTTPS demonstration
- the link under the README Live section answers when checked (a health endpoint)

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
