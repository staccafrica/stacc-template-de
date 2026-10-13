# Enterprise Financial Settlement & Clickstream Lakehouse

An enterprise-grade lakehouse platform that ingests high-volume transactions, payment settlement ledgers, and real-time user clickstream telemetry to guarantee data SLAs, audit reconciliation, and financial reporting.

## Data

Synthetic, so you can publish everything. The template includes a ledger generator and a mock settlement API with pagination, rate limits, failures and a late-batch mode.


```bash
python tools/generate_ledger.py --rows 1000000 --out data   # use --rows 10000000 for the Spark milestone
python tools/settlement_api.py --ledger data/ledger.csv --fail-rate 0.05 --rate-limit 20
```

These helpers are provided so you start with data; the milestone checks look at your own code, not at `tools/`.
Run the API with `--hold-days 7` for the capstone scenario, then `curl -X POST localhost:8080/admin/release` when the late batches "arrive".

Built step by step on the [Stacc Data Engineering roadmap](https://app.getstacc.org/learn/de). Each module adds one verified
milestone to this repository; the commit history is the build log.

## Milestones

| # | Module | Adds to this repo | Verified |
|---|---|---|---|
| 01 | Local Data Platform | Run local PostgreSQL in Docker; ingest paginated transactional API data with stateful cursors and duplicate-safe idempotency. | ☐ |
| 02 | Warehouse Modeling | Design a Kimball Star Schema for financial transactions with conformed dimensions and an SCD Type 2 history table. | ☐ |
| 03 | Analytics Engineering with dbt | Transform raw staging tables into tested intermediate models and decision-ready financial marts. | ☐ |
| 04 | Workflow Orchestration | Schedule ingestion, validation, and dbt execution as an observable Airflow DAG with failure alerts. | ☐ |
| 05 | Cloud & Infrastructure as Code | Deploy cloud storage zones, datasets, service accounts, and Secret Manager configs via declarative Terraform. | ☐ |
| 06 | Distributed Batch with Spark | Process multi-gigabyte historical settlement ledgers with PySpark; inspect physical plans and eliminate shuffle skew. | ☐ |
| 07 | Event Streaming with Kafka | Stream real-time payment authorization events into Kafka with keyed partitions and consumer group rebalance safety. | ☐ |
| 08 | End-to-End Data Platform Product | Harden and release the entire financial settlement lakehouse with green CI, lineage, alerts, runbooks, and a demo walkthrough. | ☐ |

Tick a row when Stacc verifies the milestone. Replace this section's intro, but keep the table:
the first milestone checks that the plan is here.

<!--
Final demo question (answer it in the capstone walkthrough):
A settlement partner API failed and seven days of transactional batches arrived late—demonstrate how the platform ingests, reconciles, and recovers without duplicating balances or violating SLAs.

The capstone requires a "## Walkthrough" section in this README linking a
walkthrough of five minutes or less, and CI passing on the submitted commit.
-->
