select
    origin_country,
    count(distinct icao24) as aircraft_count,
    count(*) as position_events,
    round(avg(baro_altitude_m), 2) as avg_altitude_m,
    round(avg(speed_kmh), 2) as avg_speed_kmh,
    sum(case when on_ground then 0 else 1 end) as airborne_events,
    max(observed_at) as latest_event_at
from {{ ref('stg_aircraft_positions') }}
group by 1
