# OpenSky Flight Tracking Streaming Pipeline

A real-time data engineering portfolio project that ingests aircraft position
events, streams them through a Kafka-compatible architecture, validates the data,
and publishes curated aviation analytics marts.

The project is intentionally built with two execution paths:

1. A lightweight local demo using deterministic sample events.
2. A streaming path using OpenSky live API events and Redpanda/Kafka.

That makes the repository easy for recruiters to run while still showing the
architecture patterns expected in a real event-driven platform.

## Business Problem

Aircraft state vectors are high-frequency operational events. Raw events are not
directly useful for analytics because they need validation, normalization,
deduplication, enrichment and aggregation.

This pipeline turns aircraft position events into answers such as:

- How many aircraft are active by country?
- What altitude bands dominate current traffic?
- What is the latest known state of each aircraft?
- Are incoming events fresh, valid and usable for downstream analytics?

## Architecture

```text
OpenSky API or deterministic sample generator
        |
        v
Producer
        |
        +--> JSONL demo sink
        |
        +--> Redpanda/Kafka topic: opensky.aircraft_states
                 |
                 v
              Consumer
        |
        v
data/raw/events/*.jsonl
        |
        v
DuckDB bronze.aircraft_state_events
        |
        v
silver.aircraft_positions_clean
        |
        v
gold marts + quality report
```

## Tech Stack

| Layer | Tools |
|---|---|
| Source | OpenSky Network API |
| Streaming | Redpanda/Kafka, Python producer/consumer |
| Storage | JSONL raw event log, DuckDB |
| Transformation | SQL, dbt project structure |
| Quality | Python quality checks, dbt tests |
| Packaging | Docker Compose |
| Testing | pytest |

## Repository Structure

```text
.
|-- flight_pipeline/
|   |-- producer.py              # Sample/live OpenSky producer
|   |-- consume_kafka.py         # Kafka to JSONL sink
|   |-- build_marts.py           # Bronze, silver, gold DuckDB models
|   |-- data_quality.py          # Quality gates and markdown report
|   `-- sample_events.py         # Deterministic event generator
|-- dbt/models/                  # Staging, intermediate, marts
|-- docs/data_contract.md
|-- scripts/run_demo.py
|-- tests/
|-- docker-compose.yml
`-- README.md
```

## Quick Start

Create a virtual environment and install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Run the local demo:

```powershell
python scripts\run_demo.py --rows 250
```

Run tests:

```powershell
pytest
```

The demo creates:

- `data/raw/events/opensky_events.jsonl`
- `data/warehouse/opensky_streaming.duckdb`
- `data/marts/mart_country_airspace.csv`
- `data/marts/mart_altitude_distribution.csv`
- `data/marts/mart_aircraft_latest_state.csv`
- `reports/data_quality_report.md`

## Run with Live OpenSky Data

OpenSky API docs: https://openskynetwork.github.io/opensky-api/

```powershell
python -m flight_pipeline.producer --mode live --sink jsonl
python -m flight_pipeline.build_marts
python -m flight_pipeline.data_quality
```

The public API can be rate-limited, so the sample generator is the recommended
path for repeatable local tests.

## Run with Redpanda/Kafka

Start Redpanda:

```powershell
docker compose up -d
pip install -r requirements-kafka.txt
```

Produce sample events to Kafka:

```powershell
python -m flight_pipeline.producer --mode sample --sink kafka --rows 500
```

Consume Kafka events into the raw event log:

```powershell
python -m flight_pipeline.consume_kafka --max-messages 500
python -m flight_pipeline.build_marts
python -m flight_pipeline.data_quality
```

Open Redpanda Console at `http://localhost:8081`.

## Data Quality Gates

| Check | Purpose |
|---|---|
| `silver_has_events` | Blocks empty datasets |
| `icao24_present` | Ensures every event has an aircraft identifier |
| `valid_coordinates` | Protects map/location analytics |
| `valid_speed` | Flags impossible velocity values |
| `valid_altitude` | Flags invalid altitude readings |

## Gold Marts

| Mart | Business Use |
|---|---|
| `gold.mart_country_airspace` | Country-level aircraft activity and traffic volume |
| `gold.mart_altitude_distribution` | Operational view of ground, low, medium and cruise traffic |
| `gold.mart_aircraft_latest_state` | Latest known state per aircraft for monitoring use cases |

## Portfolio Talking Points

- Built a streaming-style pipeline with producer, Kafka-compatible broker option, consumer and curated marts.
- Added deterministic sample events so reviewers can run the project without API keys or rate-limit surprises.
- Implemented quality gates for aircraft identifiers, coordinates, speed and altitude.
- Modeled event data into analytics tables for country airspace, altitude distribution and latest aircraft state.
- Included dbt models and tests to show analytics engineering discipline on top of event data.

## CV Bullet

Designed a real-time aircraft tracking data pipeline using Python, Redpanda/Kafka,
DuckDB and dbt to ingest OpenSky aircraft state events, validate event quality,
build curated aviation marts, and support country-level and altitude-band
airspace analytics.
