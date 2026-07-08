from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "warehouse" / "opensky_streaming.duckdb"


def test_streaming_demo_builds_airspace_marts() -> None:
    subprocess.run([sys.executable, "scripts/run_demo.py", "--rows", "120"], cwd=ROOT, check=True)

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        countries = con.execute("SELECT count(*) FROM gold.mart_country_airspace").fetchone()[0]
        bands = con.execute("SELECT count(*) FROM gold.mart_altitude_distribution").fetchone()[0]
        aircraft = con.execute("SELECT count(*) FROM gold.mart_aircraft_latest_state").fetchone()[0]
        raw_events = con.execute("SELECT count(*) FROM bronze.aircraft_state_events").fetchone()[0]

    assert countries > 0
    assert bands > 0
    assert aircraft > 0
    assert raw_events == 120
