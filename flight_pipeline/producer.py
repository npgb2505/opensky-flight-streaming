from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import requests

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
    observed_at = datetime.now(timezone.utc)
    response = requests.get(OPENSKY_URL, timeout=45)
    response.raise_for_status()
    payload = response.json()
    states = payload.get("states") or []
    return [normalize_state(state, observed_at) for state in states]


def write_jsonl(events: list[dict], target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")


def produce_to_kafka(events: list[dict], bootstrap_servers: str, topic: str) -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    for event in events:
        producer.send(topic, key=event["icao24"], value=event)
    producer.flush()


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce OpenSky aircraft state events.")
    parser.add_argument("--mode", choices=["sample", "live"], default="sample")
    parser.add_argument("--sink", choices=["jsonl", "kafka"], default="jsonl")
    parser.add_argument("--rows", type=int, default=250)
    parser.add_argument("--bootstrap-servers", default="localhost:19092")
    parser.add_argument("--topic", default="opensky.aircraft_states")
    parser.add_argument("--output", default="data/raw/events/opensky_events.jsonl")
    args = parser.parse_args()

    events = generate_events(args.rows) if args.mode == "sample" else fetch_live_events()
    if args.sink == "jsonl":
        target = ROOT / args.output
        write_jsonl(events, target)
        print(f"Wrote {len(events)} events to {target}")
    else:
        produce_to_kafka(events, args.bootstrap_servers, args.topic)
        print(f"Produced {len(events)} events to Kafka topic {args.topic}")


if __name__ == "__main__":
    main()
