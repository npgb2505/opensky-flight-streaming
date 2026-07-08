from __future__ import annotations

from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "warehouse" / "opensky_streaming.duckdb"
REPORT_PATH = ROOT / "reports" / "data_quality_report.md"


CHECKS = {
    "silver_has_events": "SELECT CASE WHEN count(*) > 0 THEN 0 ELSE 1 END FROM silver.aircraft_positions_clean",
    "icao24_present": "SELECT count(*) FROM silver.aircraft_positions_clean WHERE icao24 IS NULL OR icao24 = ''",
    "valid_coordinates": "SELECT count(*) FROM silver.aircraft_positions_clean WHERE longitude NOT BETWEEN -180 AND 180 OR latitude NOT BETWEEN -90 AND 90",
    "valid_speed": "SELECT count(*) FROM silver.aircraft_positions_clean WHERE speed_kmh < 0 OR speed_kmh > 1400",
    "valid_altitude": "SELECT count(*) FROM silver.aircraft_positions_clean WHERE baro_altitude_m < 0 OR baro_altitude_m > 20000",
}


def main() -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    failures: list[tuple[str, int]] = []

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = []
        for name, sql in CHECKS.items():
            failed_rows = int(con.execute(sql).fetchone()[0])
            status = "PASS" if failed_rows == 0 else "FAIL"
            rows.append((name, status, failed_rows))
            if failed_rows:
                failures.append((name, failed_rows))

        mart_counts = con.execute(
            """
            SELECT 'gold.mart_country_airspace' AS table_name, count(*) AS rows FROM gold.mart_country_airspace
            UNION ALL
            SELECT 'gold.mart_altitude_distribution', count(*) FROM gold.mart_altitude_distribution
            UNION ALL
            SELECT 'gold.mart_aircraft_latest_state', count(*) FROM gold.mart_aircraft_latest_state
            """
        ).fetchall()

    lines = [
        "# Data Quality Report",
        "",
        "| Check | Status | Failed Rows |",
        "|---|---:|---:|",
    ]
    lines += [f"| {name} | {status} | {failed_rows} |" for name, status, failed_rows in rows]
    lines += ["", "## Mart Row Counts", "", "| Table | Rows |", "|---|---:|"]
    lines += [f"| {table} | {count} |" for table, count in mart_counts]
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {REPORT_PATH}")

    if failures:
        for name, failed_rows in failures:
            print(f"FAIL {name}: {failed_rows}")
        raise SystemExit(1)
    print("All data quality checks passed")


if __name__ == "__main__":
    main()
