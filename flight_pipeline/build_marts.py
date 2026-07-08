from __future__ import annotations

from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "warehouse" / "opensky_streaming.duckdb"
MART_DIR = ROOT / "data" / "marts"
RAW_GLOB = (ROOT / "data" / "raw" / "events" / "*.jsonl").as_posix()


def q(path: str) -> str:
    return path.replace("'", "''")


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    MART_DIR.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(DB_PATH)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute(
            f"""
            CREATE OR REPLACE TABLE bronze.aircraft_state_events AS
            SELECT * FROM read_json_auto('{q(RAW_GLOB)}', format = 'newline_delimited');
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
            WHERE icao24 IS NOT NULL
              AND observed_at IS NOT NULL
              AND longitude BETWEEN -180 AND 180
              AND latitude BETWEEN -90 AND 90;
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
            SELECT *
            FROM (
                SELECT
                    *,
                    row_number() OVER (PARTITION BY icao24 ORDER BY observed_at DESC) AS rn
                FROM silver.aircraft_positions_clean
            )
            WHERE rn = 1;
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE gold.data_quality_summary AS
            SELECT 'raw_events' AS metric, count(*)::DOUBLE AS value FROM bronze.aircraft_state_events
            UNION ALL
            SELECT 'clean_events', count(*)::DOUBLE FROM silver.aircraft_positions_clean
            UNION ALL
            SELECT 'distinct_aircraft', count(DISTINCT icao24)::DOUBLE FROM silver.aircraft_positions_clean
            UNION ALL
            SELECT 'countries', count(DISTINCT origin_country)::DOUBLE FROM silver.aircraft_positions_clean;
            """
        )

        for table in [
            "mart_country_airspace",
            "mart_altitude_distribution",
            "mart_aircraft_latest_state",
            "data_quality_summary",
        ]:
            out = (MART_DIR / f"{table}.csv").as_posix()
            con.execute(f"COPY gold.{table} TO '{q(out)}' (HEADER, DELIMITER ',')")

    print(f"Built {DB_PATH}")
    print(f"Exported marts to {MART_DIR}")


if __name__ == "__main__":
    main()
