from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from flight_pipeline.sample_events import generate_events

ROOT = Path(__file__).resolve().parents[1]
OPENSKY_URL = "https://opensky-network.org/api/states/all"


def normalize_state(state: list, observed_at: datetime) -> dict:
    return {
        "observed_at": observed_at.isoformat(),
        "icao24": state[0],
        "callsign": state[1].strip() if state[1] else None,
        "origin_country": state[2],
        "longitude": state[5],
        "latitude": state[6],
        "baro_altitude": state[7],
        "velocity": state[9],
        "true_track": state[10],
        "vertical_rate": state[11],
        "on_ground": state[8],
        "source": "opensky_live",
    }


def fetch_live_events() -> list[dict]:
    observed_at = datetime.now(UTC)
    retry = Retry(total=4, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    response = session.get(OPENSKY_URL, timeout=(10, 45))
    response.raise_for_status()
    payload = response.json()
    states = payload.get("states") or []
    return [normalize_state(state, observed_at) for state in states]


def write_jsonl(events: list[dict], target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".part")
    with temporary.open("w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")
    os.replace(temporary, target)


def produce_to_kafka(events: list[dict], bootstrap_servers: str, topic: str) -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    for event in events:
        producer.send(topic, key=event.get("icao24") or "invalid-aircraft", value=event)
    producer.flush()


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce OpenSky aircraft state events.")
    parser.add_argument("--mode", choices=["sample", "live"], default="sample")
    parser.add_argument("--sink", choices=["jsonl", "kafka"], default="jsonl")
    parser.add_argument("--rows", type=int, default=250)
    parser.add_argument(
        "--invalid-events",
        type=int,
        default=0,
        help="Number of deterministic invalid events used to demonstrate quarantine logic.",
    )
    parser.add_argument("--bootstrap-servers", default="localhost:19092")
    parser.add_argument("--topic", default="opensky.aircraft_states")
    parser.add_argument("--output", default="data/raw/events/opensky_events.jsonl")
    args = parser.parse_args()

    if args.mode == "live" and args.invalid_events:
        parser.error("--invalid-events is available only in sample mode")
    events = generate_events(args.rows, args.invalid_events) if args.mode == "sample" else fetch_live_events()
    if args.sink == "jsonl":
        target = ROOT / args.output
        write_jsonl(events, target)
        print(f"Wrote {len(events)} events to {target}")
    else:
        produce_to_kafka(events, args.bootstrap_servers, args.topic)
        print(f"Produced {len(events)} events to Kafka topic {args.topic}")


if __name__ == "__main__":
    main()
