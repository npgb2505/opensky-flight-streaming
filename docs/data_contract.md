# Data Contract

## Source

OpenSky aircraft state vectors are represented as event records. The project can
use either live OpenSky API responses or deterministic sample events.

## Event Grain

One row represents one observed aircraft state at one event timestamp.

## Critical Rules

| Field | Rule |
|---|---|
| `observed_at` | Not null |
| `icao24` | Not null |
| `longitude` | Between -180 and 180 |
| `latitude` | Between -90 and 90 |
| `speed_kmh` | Between 0 and 1400 |
| `baro_altitude_m` | Between 0 and 20000 |

## Gold Marts

| Mart | Purpose |
|---|---|
| `gold.mart_country_airspace` | Aircraft count, event volume, average altitude and speed by country |
| `gold.mart_altitude_distribution` | Flight activity by altitude band |
| `gold.mart_aircraft_latest_state` | Latest known state per aircraft |
