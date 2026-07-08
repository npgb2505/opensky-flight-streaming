from __future__ import annotations

from datetime import datetime, timedelta, timezone
from random import Random


COUNTRIES = ["United States", "Germany", "France", "United Kingdom", "Japan", "Singapore", "Australia"]
CALLSIGNS = ["UAL120", "DLH401", "AFR083", "BAW216", "JAL006", "SIA322", "QFA012"]


def generate_events(rows: int = 250) -> list[dict]:
    rng = Random(42)
    start = datetime(2026, 7, 8, 9, 0, tzinfo=timezone.utc)
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
    return events
