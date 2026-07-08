select
    altitude_band,
    count(*) as position_events,
    count(distinct icao24) as aircraft_count,
    round(avg(speed_kmh), 2) as avg_speed_kmh
from {{ ref('stg_aircraft_positions') }}
group by 1
