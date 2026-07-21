from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "warehouse" / "opensky_streaming.duckdb"
MART_DIR = ROOT / "data" / "marts"
RAW_GLOB = (ROOT / "data" / "raw" / "events" / "*.jsonl").as_posix()


def q(path: str) -> str:
    return path.replace("'", "''")


def build_marts(
    db_path: Path = DB_PATH,
    mart_dir: Path = MART_DIR,
    raw_glob: str = RAW_GLOB,
) -> dict[str, float]:
    if not list((ROOT / "data" / "raw" / "events").glob("*.jsonl")):
        raise FileNotFoundError("No JSONL events found. Run the producer or Kafka consumer first.")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    mart_dir.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute(
            f"""
            CREATE OR REPLACE TABLE bronze.aircraft_state_events AS
            SELECT *
            FROM read_json_auto(
                '{q(raw_glob)}',
                format = 'newline_delimited',
                union_by_name = true
            );
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE silver.aircraft_positions_rejected AS
            SELECT
                *,
                concat_ws('; ',
                    CASE WHEN observed_at IS NULL THEN 'missing_observed_at' END,
                    CASE WHEN icao24 IS NULL OR trim(icao24) = '' THEN 'missing_icao24' END,
                    CASE WHEN longitude IS NULL OR longitude NOT BETWEEN -180 AND 180 THEN 'invalid_longitude' END,
                    CASE WHEN latitude IS NULL OR latitude NOT BETWEEN -90 AND 90 THEN 'invalid_latitude' END,
                    CASE WHEN velocity IS NULL OR velocity NOT BETWEEN 0 AND 388.89 THEN 'invalid_velocity' END,
                    CASE WHEN baro_altitude IS NULL OR baro_altitude NOT BETWEEN 0 AND 20000 THEN 'invalid_altitude' END
                ) AS rejection_reason
            FROM bronze.aircraft_state_events
            WHERE observed_at IS NULL
               OR icao24 IS NULL OR trim(icao24) = ''
               OR longitude IS NULL OR longitude NOT BETWEEN -180 AND 180
               OR latitude IS NULL OR latitude NOT BETWEEN -90 AND 90
               OR velocity IS NULL OR velocity NOT BETWEEN 0 AND 388.89
               OR baro_altitude IS NULL OR baro_altitude NOT BETWEEN 0 AND 20000;
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE silver.aircraft_positions_clean AS
            SELECT
                CAST(observed_at AS TIMESTAMPTZ) AS observed_at,
                lower(trim(icao24)) AS icao24,
                nullif(trim(callsign), '') AS callsign,
                origin_country,
                CAST(longitude AS DOUBLE) AS longitude,
                CAST(latitude AS DOUBLE) AS latitude,
                CAST(baro_altitude AS DOUBLE) AS baro_altitude_m,
                round(CAST(velocity AS DOUBLE) * 3.6, 2) AS speed_kmh,
                CAST(true_track AS DOUBLE) AS true_track,
                CAST(vertical_rate AS DOUBLE) AS vertical_rate,
                CAST(on_ground AS BOOLEAN) AS on_ground,
                CASE
                    WHEN CAST(on_ground AS BOOLEAN) THEN 'ground'
                    WHEN CAST(baro_altitude AS DOUBLE) < 3000 THEN 'low'
                    WHEN CAST(baro_altitude AS DOUBLE) < 9000 THEN 'medium'
                    ELSE 'cruise'
                END AS altitude_band,
                source
            FROM bronze.aircraft_state_events
            WHERE observed_at IS NOT NULL
              AND icao24 IS NOT NULL AND trim(icao24) <> ''
              AND longitude BETWEEN -180 AND 180
              AND latitude BETWEEN -90 AND 90
              AND velocity BETWEEN 0 AND 388.89
              AND baro_altitude BETWEEN 0 AND 20000;
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE gold.mart_country_airspace AS
            SELECT
                origin_country,
                count(DISTINCT icao24) AS aircraft_count,
                count(*) AS position_events,
                round(avg(baro_altitude_m), 2) AS avg_altitude_m,
                round(avg(speed_kmh), 2) AS avg_speed_kmh,
                sum(CASE WHEN on_ground THEN 0 ELSE 1 END) AS airborne_events,
                max(observed_at) AS latest_event_at
            FROM silver.aircraft_positions_clean
            GROUP BY 1
            ORDER BY aircraft_count DESC, position_events DESC;
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE gold.mart_altitude_distribution AS
            SELECT
                altitude_band,
                count(*) AS position_events,
                count(DISTINCT icao24) AS aircraft_count,
                round(avg(speed_kmh), 2) AS avg_speed_kmh
            FROM silver.aircraft_positions_clean
            GROUP BY 1
            ORDER BY position_events DESC;
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE gold.mart_aircraft_latest_state AS
            SELECT * EXCLUDE (latest_rank)
            FROM (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY icao24
                        ORDER BY observed_at DESC
                    ) AS latest_rank
                FROM silver.aircraft_positions_clean
            )
            WHERE latest_rank = 1;
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE gold.pipeline_run_summary AS
            WITH metrics AS (
                SELECT
                    (SELECT count(*) FROM bronze.aircraft_state_events) AS raw_events,
                    (SELECT count(*) FROM silver.aircraft_positions_clean) AS clean_events,
                    (SELECT count(*) FROM silver.aircraft_positions_rejected) AS rejected_events,
                    (SELECT count(DISTINCT icao24) FROM silver.aircraft_positions_clean) AS distinct_aircraft,
                    (SELECT count(DISTINCT origin_country) FROM silver.aircraft_positions_clean) AS countries,
                    (SELECT count(DISTINCT altitude_band) FROM silver.aircraft_positions_clean) AS altitude_bands
            )
            SELECT
                *,
                round(100.0 * clean_events / nullif(raw_events, 0), 2) AS acceptance_rate_pct,
                current_timestamp AS completed_at
            FROM metrics;
            """
        )

        for table in [
            "mart_country_airspace",
            "mart_altitude_distribution",
            "mart_aircraft_latest_state",
            "pipeline_run_summary",
        ]:
            out = (mart_dir / f"{table}.csv").as_posix()
            con.execute(f"COPY gold.{table} TO '{q(out)}' (HEADER, DELIMITER ',')")

        columns = [
            "raw_events",
            "clean_events",
            "rejected_events",
            "distinct_aircraft",
            "countries",
            "altitude_bands",
            "acceptance_rate_pct",
        ]
        values = con.execute(
            """
            SELECT raw_events, clean_events, rejected_events, distinct_aircraft,
                   countries, altitude_bands, acceptance_rate_pct
            FROM gold.pipeline_run_summary
            """
        ).fetchone()

    return dict(zip(columns, values, strict=True))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build DuckDB marts from OpenSky JSONL events.")
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--mart-dir", type=Path, default=MART_DIR)
    parser.add_argument("--raw-glob", default=RAW_GLOB)
    args = parser.parse_args()

    summary = build_marts(args.db_path, args.mart_dir, args.raw_glob)
    print(f"Built {args.db_path}")
    print(f"Exported marts to {args.mart_dir}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
