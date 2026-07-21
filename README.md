# OpenSky Flight Streaming Pipeline

[![CI](https://github.com/npgb2505/opensky-flight-streaming/actions/workflows/ci.yml/badge.svg)](https://github.com/npgb2505/opensky-flight-streaming/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/python-3.12-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Kafka compatible](https://img.shields.io/badge/stream-Redpanda%20%2F%20Kafka-E2231A.svg)](https://redpanda.com/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

An event-driven aircraft telemetry pipeline with a Redpanda/Kafka transport, replayable JSONL raw store, DuckDB bronze/silver/gold models, invalid-event quarantine, and tested aviation data products.

![OpenSky Flight Streaming architecture](docs/architecture-etl.svg)

## Why this project

Aircraft state vectors arrive as high-frequency operational events. They need buffering, a stable contract, replay, validation, deduplication, and curated outputs before analysts can trust them. This repository implements that lifecycle and keeps a broker-free demo path so every commit remains reproducible in CI.

The design uses streaming ingestion and micro-batch warehouse curation. It deliberately does not claim end-to-end sub-second analytics.

## Verified demo

CI runs the same deterministic workload shown below. Five malformed events are injected intentionally to verify quarantine and row reconciliation.

| Result | Verified value |
|---|---:|
| Raw position events | 500 |
| Accepted / quarantined | 495 / 5 |
| Acceptance rate | 99.0% |
| Data-quality gates | 9 passed |
| Distinct aircraft | 40 |
| Origin countries | 7 |
| Altitude bands | 4 |
| Reconciled events | 500 / 500 |

These values prove deterministic behavior; they are not current air-traffic statistics. Live mode calls the public [OpenSky Network API](https://openskynetwork.github.io/opensky-api/).

## Quick start

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python scripts/run_demo.py --rows 500 --invalid-events 5
```

This writes a replayable JSONL event log, builds the warehouse and three marts, applies all release gates, and emits Markdown plus JSON quality evidence.

## Event flow

```text
OpenSky API / deterministic event generator
  -> Python producer
  -> Redpanda topic: opensky.aircraft_states      (streaming path)
  -> Kafka consumer -> replayable JSONL raw log   (streaming path)
  -> direct JSONL raw log                         (CI/local path)
  -> bronze.aircraft_state_events
  -> silver.aircraft_positions_clean
     + silver.aircraft_positions_rejected (reason attached)
  -> gold.mart_country_airspace
     gold.mart_altitude_distribution
     gold.mart_aircraft_latest_state
```

| Data product | Grain | Purpose |
|---|---|---|
| Bronze events | Source event | Preserve the replayable source payload |
| Silver clean | Valid position event | Type and validate telemetry; derive speed and altitude band |
| Silver rejected | Invalid position event | Retain every rejected payload with rule failures |
| Country airspace | Origin country | Aircraft count, events, altitude, speed, freshness |
| Altitude distribution | Altitude band | Event and aircraft distribution by operating band |
| Latest aircraft state | Aircraft (`icao24`) | One latest position per aircraft using a window function |

Kafka messages use `icao24` as the key and a normalized event JSON object as the value. The consumer group can replay the topic into a new raw log.

## Reliability controls

- Live API calls use retry/backoff and explicit connect/read timeouts.
- JSONL files are written with atomic temporary-file replacement.
- Invalid identity, coordinate, velocity, and altitude events are quarantined.
- A reconciliation gate enforces `raw = clean + rejected`.
- Latest-state uniqueness is tested after the window-function deduplication.
- Nine SQL quality gates validate curated telemetry and all gold marts.
- dbt tests assert required fields and unique aircraft state keys.
- pytest covers deterministic generation, invalid input, quarantine, and the end-to-end build.
- GitHub Actions rebuilds the project and uploads quality reports for every run.

Quality evidence:

```text
reports/data_quality_report.md
reports/data_quality_report.json
```

## Run the streaming path

```bash
docker compose up -d
pip install -r requirements-kafka.txt
python -m flight_pipeline.producer --mode sample --sink kafka --rows 500
python -m flight_pipeline.consume_kafka --max-messages 500
python -m flight_pipeline.build_marts
python -m flight_pipeline.data_quality
```

Redpanda Console is available at `http://localhost:8081` for topic and message inspection.

Run a live API snapshot without Kafka:

```bash
python -m flight_pipeline.producer --mode live --sink jsonl
python -m flight_pipeline.build_marts
python -m flight_pipeline.data_quality
```

## dbt models

```bash
cd dbt
dbt debug --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

The dbt project mirrors the curated contract with staging, latest-state, country, and altitude models.

## Repository map

```text
flight_pipeline/               producer, consumer, build, and quality logic
dbt/models/                    staging, intermediate, and mart SQL
docs/                          architecture and event contract
scripts/run_demo.py            deterministic broker-free execution
tests/                         unit and end-to-end contract tests
docker-compose.yml             Redpanda and Redpanda Console
.github/workflows/ci.yml       automated verification
```

Generated outputs:

```text
data/raw/events/*.jsonl
data/warehouse/opensky_streaming.duckdb
data/marts/*.csv
reports/data_quality_report.{md,json}
```

## Design choices and scope

- Redpanda supplies Kafka semantics with a lightweight local runtime.
- JSONL is the replay boundary and makes the pipeline debuggable without the broker.
- DuckDB keeps warehouse transformations portable for reviewers and CI.
- Deterministic sample mode removes public-API availability from automated tests.
- A production deployment would use schema-registry compatibility checks, long-running consumers, checkpointed object storage, incremental marts, and external metrics/alerting.

## Author

Nguyen Phuc Gia Bao - [GitHub](https://github.com/npgb2505) - [LinkedIn](https://www.linkedin.com/in/gia-bao-nguyen-phuc-27a6682b6/)
