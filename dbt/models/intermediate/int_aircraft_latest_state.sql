select *
from (
    select
        *,
        row_number() over (partition by icao24 order by observed_at desc) as latest_rank
    from {{ ref('stg_aircraft_positions') }}
)
where latest_rank = 1
