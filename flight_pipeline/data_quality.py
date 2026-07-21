from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "warehouse" / "opensky_streaming.duckdb"
REPORT_PATH = ROOT / "reports" / "data_quality_report.md"
JSON_REPORT_PATH = ROOT / "reports" / "data_quality_report.json"


CHECKS = {
    "silver_has_events": "SELECT CASE WHEN count(*) > 0 THEN 0 ELSE 1 END FROM silver.aircraft_positions_clean",
    "icao24_present": "SELECT count(*) FROM silver.aircraft_positions_clean WHERE icao24 IS NULL OR icao24 = ''",
    "valid_coordinates": (
        "SELECT count(*) FROM silver.aircraft_positions_clean "
        "WHERE longitude NOT BETWEEN -180 AND 180 OR latitude NOT BETWEEN -90 AND 90"
    ),
    "valid_speed": "SELECT count(*) FROM silver.aircraft_positions_clean WHERE speed_kmh < 0 OR speed_kmh > 1400",
    "valid_altitude": (
        "SELECT count(*) FROM silver.aircraft_positions_clean "
        "WHERE baro_altitude_m < 0 OR baro_altitude_m > 20000"
    ),
    "raw_events_reconciled": (
        "SELECT abs((SELECT count(*) FROM bronze.aircraft_state_events) "
        "- (SELECT count(*) FROM silver.aircraft_positions_clean) "
        "- (SELECT count(*) FROM silver.aircraft_positions_rejected))"
    ),
    "latest_state_unique": "SELECT count(*) - count(DISTINCT icao24) FROM gold.mart_aircraft_latest_state",
    "altitude_band_known": (
        "SELECT count(*) FROM silver.aircraft_positions_clean "
        "WHERE altitude_band NOT IN ('ground', 'low', 'medium', 'cruise')"
    ),
    "gold_marts_have_rows": (
        "SELECT CASE WHEN (SELECT count(*) FROM gold.mart_country_airspace) > 0 "
        "AND (SELECT count(*) FROM gold.mart_altitude_distribution) > 0 "
        "AND (SELECT count(*) FROM gold.mart_aircraft_latest_state) > 0 THEN 0 ELSE 1 END"
    ),
}


def run_checks(
    db_path: Path = DB_PATH,
    report_path: Path = REPORT_PATH,
    json_report_path: Path = JSON_REPORT_PATH,
) -> dict:
    if not db_path.exists():
        raise FileNotFoundError(f"Warehouse not found: {db_path}")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    json_report_path.parent.mkdir(parents=True, exist_ok=True)
    failures: list[tuple[str, int]] = []

    with duckdb.connect(str(db_path), read_only=True) as con:
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
        metrics_row = con.execute(
            """
            SELECT raw_events, clean_events, rejected_events, distinct_aircraft,
                   countries, altitude_bands, acceptance_rate_pct
            FROM gold.pipeline_run_summary
            """
        ).fetchone()

    generated_at = datetime.now(UTC).isoformat()
    metric_names = [
        "raw_events",
        "clean_events",
        "rejected_events",
        "distinct_aircraft",
        "countries",
        "altitude_bands",
        "acceptance_rate_pct",
    ]
    metrics = dict(zip(metric_names, metrics_row, strict=True))
    payload = {
        "generated_at": generated_at,
        "status": "FAIL" if failures else "PASS",
        "checks": [
            {"name": name, "status": status, "failed_rows": failed_rows}
            for name, status, failed_rows in rows
        ],
        "metrics": metrics,
        "mart_row_counts": {table: count for table, count in mart_counts},
    }

    lines = [
        "# Data Quality Report",
        "",
        f"Generated at: `{generated_at}`",
        "",
        f"Overall status: **{payload['status']}**",
        "",
        "| Check | Status | Failed Rows |",
        "|---|---:|---:|",
    ]
    lines += [f"| {name} | {status} | {failed_rows} |" for name, status, failed_rows in rows]
    lines += ["", "## Mart Row Counts", "", "| Table | Rows |", "|---|---:|"]
    lines += [f"| {table} | {count} |" for table, count in mart_counts]
    lines += ["", "## Pipeline Metrics", "", "| Metric | Value |", "|---|---:|"]
    lines += [f"| {name} | {value} |" for name, value in metrics.items()]
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    json_report_path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    print(f"Wrote {report_path}")
    print(f"Wrote {json_report_path}")

    if failures:
        for name, failed_rows in failures:
            print(f"FAIL {name}: {failed_rows}")
        raise SystemExit(1)
    print("All data quality checks passed")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate OpenSky streaming data contracts.")
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--report-path", type=Path, default=REPORT_PATH)
    parser.add_argument("--json-report-path", type=Path, default=JSON_REPORT_PATH)
    args = parser.parse_args()
    run_checks(args.db_path, args.report_path, args.json_report_path)


if __name__ == "__main__":
    main()
