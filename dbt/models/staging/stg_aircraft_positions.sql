select
    observed_at,
    icao24,
    callsign,
    origin_country,
    longitude,
    latitude,
    baro_altitude_m,
    speed_kmh,
    true_track,
    vertical_rate,
    on_ground,
    altitude_band,
    source
from silver.aircraft_positions_clean
