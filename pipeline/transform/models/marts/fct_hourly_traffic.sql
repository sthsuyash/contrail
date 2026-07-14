{{
    config(
        materialized='table',
        order_by='(traffic_hour, region)',
    )
}}

-- Traffic volume per region per hour.
--
-- The measure that matters here is distinct aircraft, not observation count.
-- Observations are an artefact of how often the scheduler chose to poll a
-- region, so counting them would report the pipeline's own behaviour rather
-- than the traffic: a region polled twice as often would appear twice as busy.
-- Distinct airframes is invariant to polling frequency, up to coverage.

with observations as (

    select
        toStartOfHour(observed_at) as traffic_hour,
        region,
        icao24,
        on_ground,
        altitude_m,
        velocity,
        source_row_count
    from {{ ref('stg_state_vectors') }}

)

select
    traffic_hour,
    region,

    uniqExact(icao24)                                as distinct_aircraft,
    uniqExactIf(icao24, on_ground = false)           as distinct_airborne,
    uniqExactIf(icao24, on_ground = true)            as distinct_on_ground,

    count()                                          as observations,

    -- Polling intensity, kept alongside the counts so the two are never
    -- confused. A drop in observations with flat distinct_aircraft means the
    -- scheduler deprioritised the region, not that traffic fell.
    round(count() / nullIf(uniqExact(icao24), 0), 1) as observations_per_aircraft,

    round(avgIf(altitude_m, on_ground = false))      as avg_airborne_altitude_m,
    round(maxIf(altitude_m, on_ground = false))      as max_altitude_m,
    round(avgIf(velocity, on_ground = false) * 1.94384) as avg_airborne_velocity_knots,

    -- Raw rows that collapsed into these observations. Surfaced as a mart
    -- column so the duplicate rate stays a measurement rather than a claim.
    sum(source_row_count)                            as raw_rows,
    round(
        (sum(source_row_count) - count()) / nullIf(sum(source_row_count), 0) * 100,
        1
    )                                                as duplicate_rate_pct

from observations
group by traffic_hour, region
