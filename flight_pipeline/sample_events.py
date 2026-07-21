from __future__ import annotations

from datetime import UTC, datetime, timedelta
from random import Random

COUNTRIES = ["United States", "Germany", "France", "United Kingdom", "Japan", "Singapore", "Australia"]
CALLSIGNS = ["UAL120", "DLH401", "AFR083", "BAW216", "JAL006", "SIA322", "QFA012"]


def generate_events(rows: int = 250, invalid_events: int = 0) -> list[dict]:
    if rows <= 0:
        raise ValueError("rows must be greater than zero")
    if invalid_events < 0 or invalid_events > rows:
        raise ValueError("invalid_events must be between zero and rows")

    rng = Random(42)
    start = datetime(2026, 7, 8, 9, 0, tzinfo=UTC)
    events: list[dict] = []

    for i in range(rows):
        aircraft_id = i % 40
        country = COUNTRIES[aircraft_id % len(COUNTRIES)]
        observed_at = start + timedelta(seconds=i * 15)
        altitude = 1000 + ((aircraft_id % 15) * 700) + rng.randint(-120, 120)
        on_ground = aircraft_id % 23 == 0
        if on_ground:
            altitude = 0
        events.append(
            {
                "observed_at": observed_at.isoformat(),
                "icao24": f"abc{aircraft_id:03x}",
                "callsign": CALLSIGNS[aircraft_id % len(CALLSIGNS)],
                "origin_country": country,
                "longitude": round(-125 + rng.random() * 250, 6),
                "latitude": round(-55 + rng.random() * 110, 6),
                "baro_altitude": float(altitude),
                "velocity": 0.0 if on_ground else round(170 + rng.random() * 90, 2),
                "true_track": round(rng.random() * 360, 2),
                "vertical_rate": round(-8 + rng.random() * 16, 2),
                "on_ground": on_ground,
                "source": "sample",
            }
        )
    for i in range(invalid_events):
        scenario = i % 4
        if scenario == 0:
            events[i]["longitude"] = 200.0
        elif scenario == 1:
            events[i]["icao24"] = None
        elif scenario == 2:
            events[i]["velocity"] = -1.0
        else:
            events[i]["baro_altitude"] = 25000.0
    return events
