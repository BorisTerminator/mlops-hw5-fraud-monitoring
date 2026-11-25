# Fraud monitoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox syntax for tracking.

**Goal:** Deliver all Seminar 11 requirements, running locally with evidence and a Russian walkthrough.
**Architecture:** Reuse HW-3 contracts and training pipeline in an independent repository. Instrument successful processing and committed writes; query business dimensions in PostgreSQL. Provision monitoring declaratively.
**Tech Stack:** Python 3.11, CatBoost, Kafka, PostgreSQL 15, Airflow 2.10.5, prometheus-client, Prometheus 3.2.1, Grafana 11.6.0, Node Exporter 1.9.0.
**Spec:** ../specs/2026-11-25-fraud-monitoring-design.md

## Global Constraints

- Do not modify previous HW directories. Preserve input/output contracts; add analytics fields.
- Live Kafka traffic, CPU inference, hot model reload, independent dashboards and Prometheus configuration.
- Prometheus scrape interval 1 second. Never label metrics by transaction_id or merchant.
- Four dashboards, two provisioned alerts, SQL filters, category analysis of latest 1000 records.
- Local PostgreSQL port 5433, Prometheus 9091 (9090 is AdGuard), other ports configurable and bound to 127.0.0.1.
- User approved design and delegated subsequent technical decisions; execute inline without further design gates.

## Review Focus

- Malformed messages and scoring failures must be counted without crashing or silently advancing successful counters.
- Failed model reload must retain a consistent old model/preprocessor pair.
- Duplicate Kafka delivery must not duplicate rows or successful-write metrics.
- SQL filter values including apostrophes must be quoted safely; empty segment must render valid empty results.
- Alerts must distinguish silence, exporter outage and low-volume fraud ratios.

### Task 1: Instrument streaming scorer
**Files:** fraud_detector/src/{service,scorer,metrics}.py, requirements.txt; tests/test_scoring.py.
**Interfaces:** score_batch(scorer, ids, rows) -> list[dict] includes segment fields and event timestamps; ScoringMetrics(registry).observe(result, duration); VersionedScorer.refresh() -> bool.
- [x] Write tests: output preserves CA / merchant with apostrophe / category / amount; malformed input increments errors; failed reload retains old predictions and valid version.
- [x] Run failing tests in container derived from cached HW-3 scorer (install prometheus-client and pytest).
- [x] Implement counters/histograms, last-success time, fraud ratio, reload counter by version and safe reload.
- [x] Run tests green and commit.

### Task 2: Commit-aware writer and schema
**Files:** scoring_writer/{app,metrics}.py, postgres/init.sql, tests/test_writer.py.
**Interfaces:** insert_score(conn, data, metrics) -> bool (true only for new committed row). Unique transaction_id, segment columns and TIMESTAMPTZ processing times; read-only grafana_reader.
- [x] Test against actual PostgreSQL: duplicate delivery inserts once, stored dimensions exact, DB failure doesn't increment metrics, rollback permits next write.
- [x] Verify RED, implement explicit commit offsets, DB rollback/retry on transient failure, successful-only metrics and readonly role.
- [x] Verify GREEN and commit.

### Task 3: Reproducible monitoring infrastructure
**Files:** docker-compose.yml, prometheus/prometheus.yml, grafana/provisioning/{datasources,dashboards,alerting}/*, grafana/dashboards/*.json, scripts/generate_dashboards.py.
**Interfaces:** datasource UIDs fraud-prometheus / fraud-postgres; dashboard UIDs fraud-scoring / fraud-storage / fraud-host / fraud-business.
- [x] Implement independent provisioning, local isolated network/volumes and healthchecks; preserve Airflow training.
- [x] Build dashboards using small Python generator, store both generator and JSON. Use :sqlstring for state/merch, Grafana PostgreSQL time macros.
- [x] Add stream-stalled (>30s for 15s) and high-fraud (>20% in 1m, minimum 20 messages, for 20s) Grafana rules.
- [x] Run compose config, promtool, provisioned datasource health and every panel query via Grafana API; verify nonempty data. Commit.

### Task 4: Live acceptance and model publication
**Files:** scripts/{verify_stack,demo}.py, tests/test_business.py, docs/verification.md.
**Interfaces:** python scripts/verify_stack.py checks actual running endpoints; docker compose exec airflow airflow dags trigger retrain_catboost.
- [x] Start all containers and check growing traffic, all 3 scrape targets, DB inserts, nonempty panels and SELECT-only datasource.
- [x] Trigger real retraining. Capture old/new model version and unchanged scorer start time; query annotation expression and rows for both versions.
- [x] Pause simulator to demonstrate alert firing, resume and verify recovery. Demonstrate high-fraud via configurable threshold and restore normal settings.
- [x] Test SQL filtering and latest-1000 semantics with known rows in real PostgreSQL, including apostrophes. Save evidence and commit.

### Task 5: Explain and package
**Files:** README.md, docs/walkthrough.md, .gitignore, .env.example.
- [x] Russian README: setup, credentials, dashboard links, all metrics, novel metrics vs seminar, alert thresholds/tests, retrain/publication demo, limitations.
- [x] Step-by-step walkthrough: follow one transaction, explain counters/rate/histograms/SQL vs Prometheus, guided practical experiments.
- [x] Final test suite, fresh reviewer, fix material issues, clean git state, prepare public repository after reviewing publish authorization.
