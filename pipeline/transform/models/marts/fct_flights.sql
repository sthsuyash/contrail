{{
    config(
        materialized='table',
        order_by='(departure_time, icao24)',
    )
}}

-- One row per reconstructed flight.
--
-- Aggregates the sessionized observations into a flight-level fact, and
-- attributes the first and last positions to airports by proximity.
--
-- Every flight here is an inference, not a record. The pipeline never sees a
-- flight plan; it sees positions and decides where one flight ends and the
-- next begins. The `is_complete` flag below is what separates a flight whose
-- start and end were both actually observed from one that was merely clipped
-- by the edge of coverage, and consumers that do not respect it will compute
-- badly wrong durations.

with sessions as (

    select *
    from {{ ref('int_flight_sessions') }}
    where has_usable_position

),

aggregated as (

    select
        flight_id,
        icao24,
        session_seq,

        -- Callsigns can change mid-session (crews correct typos, and an
        -- airframe re-registers between legs). The most frequent value over the
        -- session is more robust than first or last.
        topK(1)(callsign)[1]        as callsign,
        any(origin_country)         as origin_country,

        min(observed_at)            as departure_time,
        max(observed_at)            as arrival_time,
        dateDiff('second', min(observed_at), max(observed_at)) as duration_seconds,

        count()                     as observation_count,
        max(altitude_m)             as max_altitude_m,
        avg(altitude_m)             as avg_altitude_m,
        max(velocity)               as max_velocity_ms,
        avg(velocity)               as avg_velocity_ms,

        -- Endpoints. argMin/argMax pick the value at the extreme of the
        -- ordering column, which is exactly "position at first/last sighting".
        argMin(latitude, observed_at)  as first_latitude,
        argMin(longitude, observed_at) as first_longitude,
        argMax(latitude, observed_at)  as last_latitude,
        argMax(longitude, observed_at) as last_longitude,

        -- Whether the ground phases at each end were actually observed. A
        -- session that begins already airborne started before coverage picked
        -- it up, so its "departure time" is when this pipeline first saw it,
        -- not when it left the ground.
        argMin(on_ground, observed_at) as started_on_ground,
        argMax(on_ground, observed_at) as ended_on_ground,

        countIf(flight_phase = 'climb')   as climb_observations,
        countIf(flight_phase = 'cruise')  as cruise_observations,
        countIf(flight_phase = 'descent') as descent_observations,
        countIf(flight_phase = 'ground')  as ground_observations,

        groupUniqArray(region)      as regions_seen,

        -- The longest unobserved stretch inside the session. A flight stitched
        -- across a 25-minute hole is a weaker inference than one sampled every
        -- 30 seconds, and this is what lets a consumer tell the two apart.
        max(seconds_since_previous) as max_gap_seconds

    from sessions
    group by flight_id, icao24, session_seq

),

-- Nearest airport to each endpoint, within the match radius.
--
-- Expressed as a cross join rather than a correlated subquery, which ClickHouse
-- does not support in a join expression. The cross product is affordable
-- precisely because the airport seed is a curated list of order 100 rows: the
-- intermediate is flights x airports, and argMin then collapses it back to one
-- row per flight. Against a full worldwide airport table of ~80k rows this
-- shape would need a geospatial index or a dictionary instead.

departure_candidates as (

    select
        aggregated.flight_id,
        airports.icao,
        airports.name,
        {{ haversine_km('aggregated.first_latitude', 'aggregated.first_longitude',
                        'airports.latitude', 'airports.longitude') }} as distance_km
    from aggregated
    cross join {{ ref('airports') }} as airports

),

departure_match as (

    select
        flight_id,
        -- The radius filter runs before aggregation, so a flight with no
        -- airport inside it produces no row at all and falls out of the left
        -- join below as null, rather than matching something implausibly far.
        argMin(icao, distance_km) as departure_airport,
        argMin(name, distance_km) as departure_airport_name
    from departure_candidates
    where distance_km <= {{ var('airport_match_km') }}
    group by flight_id

),

arrival_candidates as (

    select
        aggregated.flight_id,
        airports.icao,
        airports.name,
        {{ haversine_km('aggregated.last_latitude', 'aggregated.last_longitude',
                        'airports.latitude', 'airports.longitude') }} as distance_km
    from aggregated
    cross join {{ ref('airports') }} as airports

),

arrival_match as (

    select
        flight_id,
        argMin(icao, distance_km) as arrival_airport,
        argMin(name, distance_km) as arrival_airport_name
    from arrival_candidates
    where distance_km <= {{ var('airport_match_km') }}
    group by flight_id

),

with_airports as (

    -- Columns are listed rather than expanded with `aggregated.*`. ClickHouse's
    -- analyzer keeps star-expanded columns qualified once a join is present, so
    -- the outer query could not then refer to them unqualified. Listing them
    -- also makes this model's output contract explicit.
    select
        aggregated.flight_id            as flight_id,
        aggregated.icao24               as icao24,
        aggregated.callsign             as callsign,
        aggregated.origin_country       as origin_country,
        aggregated.departure_time       as departure_time,
        aggregated.arrival_time         as arrival_time,
        aggregated.duration_seconds     as duration_seconds,
        aggregated.observation_count    as observation_count,
        aggregated.max_altitude_m       as max_altitude_m,
        aggregated.avg_altitude_m       as avg_altitude_m,
        aggregated.max_velocity_ms      as max_velocity_ms,
        aggregated.avg_velocity_ms      as avg_velocity_ms,
        aggregated.first_latitude       as first_latitude,
        aggregated.first_longitude      as first_longitude,
        aggregated.last_latitude        as last_latitude,
        aggregated.last_longitude       as last_longitude,
        aggregated.started_on_ground    as started_on_ground,
        aggregated.ended_on_ground      as ended_on_ground,
        aggregated.climb_observations   as climb_observations,
        aggregated.cruise_observations  as cruise_observations,
        aggregated.descent_observations as descent_observations,
        aggregated.ground_observations  as ground_observations,
        aggregated.regions_seen         as regions_seen,
        aggregated.max_gap_seconds      as max_gap_seconds,
        departure_match.departure_airport      as departure_airport,
        departure_match.departure_airport_name as departure_airport_name,
        arrival_match.arrival_airport          as arrival_airport,
        arrival_match.arrival_airport_name     as arrival_airport_name
    from aggregated
    left join departure_match on departure_match.flight_id = aggregated.flight_id
    left join arrival_match on arrival_match.flight_id = aggregated.flight_id

)

select
    flight_id,
    icao24,
    callsign,
    origin_country,

    departure_time,
    arrival_time,
    duration_seconds,
    round(duration_seconds / 60.0, 1) as duration_minutes,

    departure_airport,
    departure_airport_name,
    arrival_airport,
    arrival_airport_name,

    first_latitude,
    first_longitude,
    last_latitude,
    last_longitude,

    -- Straight-line distance between endpoints. Deliberately not called
    -- "distance flown": it ignores the actual track, so a holding pattern or a
    -- weather diversion is invisible to it. Track distance would need summing
    -- leg by leg, which the observation gaps make unreliable.
    round(
        {{ haversine_km('first_latitude', 'first_longitude',
                        'last_latitude', 'last_longitude') }},
        1
    ) as endpoint_distance_km,

    round(max_altitude_m)              as max_altitude_m,
    round(avg_altitude_m)              as avg_altitude_m,
    round(max_velocity_ms * 1.94384)   as max_velocity_knots,
    round(avg_velocity_ms * 1.94384)   as avg_velocity_knots,

    observation_count,
    climb_observations,
    cruise_observations,
    descent_observations,
    ground_observations,
    max_gap_seconds,
    regions_seen,

    started_on_ground,
    ended_on_ground,

    -- Both ends observed on the ground: the full flight is within coverage and
    -- its duration is a real flight time rather than an observation window.
    (started_on_ground and ended_on_ground) as is_complete,

    -- A crude confidence signal for consumers that need to rank flights by how
    -- well evidenced they are.
    case
        when started_on_ground and ended_on_ground and max_gap_seconds <= 300 then 'high'
        when observation_count >= 10 and max_gap_seconds <= 900 then 'medium'
        else 'low'
    end as reconstruction_quality

from with_airports

-- A session of one or two pings is an artefact of coverage rather than a
-- flight; there is nothing to reconstruct from it.
where observation_count >= 3
