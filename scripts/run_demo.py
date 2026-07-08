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
    args = parser.parse_args()

    run(["-m", "flight_pipeline.producer", "--mode", "sample", "--sink", "jsonl", "--rows", str(args.rows)])
    run(["-m", "flight_pipeline.build_marts"])
    run(["-m", "flight_pipeline.data_quality"])


if __name__ == "__main__":
    main()
