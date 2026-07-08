from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume OpenSky events from Kafka into JSONL.")
    parser.add_argument("--bootstrap-servers", default="localhost:19092")
    parser.add_argument("--topic", default="opensky.aircraft_states")
    parser.add_argument("--group-id", default="opensky-duckdb-sink")
    parser.add_argument("--max-messages", type=int, default=500)
    parser.add_argument("--output", default="data/raw/events/opensky_events_from_kafka.jsonl")
    args = parser.parse_args()

    from kafka import KafkaConsumer

    target = ROOT / args.output
    target.parent.mkdir(parents=True, exist_ok=True)

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=15000,
    )

    count = 0
    with target.open("w", encoding="utf-8") as fh:
        for message in consumer:
            fh.write(json.dumps(message.value) + "\n")
            count += 1
            if count >= args.max_messages:
                break

    consumer.close()
    print(f"Wrote {count} Kafka messages to {target}")


if __name__ == "__main__":
    main()
