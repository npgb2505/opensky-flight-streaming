from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(args: list[str]) -> None:
    subprocess.run([sys.executable, *args], cwd=ROOT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local OpenSky streaming demo.")
    parser.add_argument("--rows", type=int, default=250)
    parser.add_argument("--invalid-events", type=int, default=5)
    args = parser.parse_args()

    if args.invalid_events < 0 or args.invalid_events > args.rows:
        parser.error("--invalid-events must be between zero and --rows")
    run([
        "-m",
        "flight_pipeline.producer",
        "--mode",
        "sample",
        "--sink",
        "jsonl",
        "--rows",
        str(args.rows),
        "--invalid-events",
        str(args.invalid_events),
    ])
    run(["-m", "flight_pipeline.build_marts"])
    run(["-m", "flight_pipeline.data_quality"])


if __name__ == "__main__":
    main()
