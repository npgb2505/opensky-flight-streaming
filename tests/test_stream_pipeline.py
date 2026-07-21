from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from flight_pipeline.sample_events import generate_events

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "warehouse" / "opensky_streaming.duckdb"


def test_streaming_demo_builds_airspace_marts() -> None:
    subprocess.run(
        [sys.executable, "scripts/run_demo.py", "--rows", "120", "--invalid-events", "4"],
        cwd=ROOT,
        check=True,
    )

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        countries = con.execute("SELECT count(*) FROM gold.mart_country_airspace").fetchone()[0]
        bands = con.execute("SELECT count(*) FROM gold.mart_altitude_distribution").fetchone()[0]
        aircraft = con.execute("SELECT count(*) FROM gold.mart_aircraft_latest_state").fetchone()[0]
        raw_events, clean_events, rejected_events = con.execute(
            "SELECT raw_events, clean_events, rejected_events FROM gold.pipeline_run_summary"
        ).fetchone()
        rejection_reasons = con.execute(
            "SELECT count(DISTINCT rejection_reason) FROM silver.aircraft_positions_rejected"
        ).fetchone()[0]

    assert countries > 0
    assert bands > 0
    assert aircraft > 0
    assert (raw_events, clean_events, rejected_events) == (120, 116, 4)
    assert rejection_reasons == 4


def test_sample_events_are_deterministic_and_validate_arguments() -> None:
    first = generate_events(12, invalid_events=2)
    second = generate_events(12, invalid_events=2)
    assert first == second
    assert first[0]["longitude"] == 200.0
    assert first[1]["icao24"] is None
    with pytest.raises(ValueError):
        generate_events(3, invalid_events=4)
