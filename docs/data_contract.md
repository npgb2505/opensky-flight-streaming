# OpenSky Aircraft-State Event Contract

## Ownership and service expectations

| Item | Definition |
|---|---|
| Producer | OpenSky Network API or deterministic sample generator |
| Pipeline owner | Nguyen Phuc Gia Bao |
| Topic | `opensky.aircraft_states` |
| Message key | `icao24` aircraft identifier |
| Raw format | Newline-delimited JSON |
| Event grain | One observed aircraft state at one timestamp |
| Failure policy | Quarantine invalid events; fail release on reconciliation or curated-quality failure |

## Contracted fields

| Field | Type | Rule |
|---|---|---|
| `observed_at` | ISO-8601 timestamp | Required |
| `icao24` | string | Required, trimmed and normalized to lowercase |
| `callsign` | string/null | Optional |
| `origin_country` | string | Required in curated events |
| `longitude` | number | Required, -180 to 180 |
| `latitude` | number | Required, -90 to 90 |
| `baro_altitude` | number | Required, 0 to 20,000 meters |
| `velocity` | number | Required, 0 to 388.89 meters/second |
| `true_track` | number/null | Source telemetry |
| `vertical_rate` | number/null | Source telemetry |
| `on_ground` | boolean | Drives altitude-band classification |
| `source` | string | `sample` or `opensky_live` |

Events that violate one or more rules are written to `silver.aircraft_positions_rejected`. The `rejection_reason` field retains every failed rule.

## Reconciliation invariant

```text
count(bronze.aircraft_state_events)
  = count(silver.aircraft_positions_clean)
  + count(silver.aircraft_positions_rejected)
```

## Published data products

| Mart | Grain | Primary measures |
|---|---|---|
| `gold.mart_country_airspace` | Origin country | aircraft, events, altitude, speed, airborne events, freshness |
| `gold.mart_altitude_distribution` | Altitude band | events, aircraft, average speed |
| `gold.mart_aircraft_latest_state` | Aircraft | latest valid telemetry per `icao24` |
| `gold.pipeline_run_summary` | Pipeline execution | raw, clean, rejected, coverage, acceptance rate |

## Compatibility policy

The raw JSON reader uses name-based unioning for additive fields. Curated SQL explicitly selects and casts the contracted fields, so incompatible changes fail during the warehouse build rather than silently changing downstream data products.
