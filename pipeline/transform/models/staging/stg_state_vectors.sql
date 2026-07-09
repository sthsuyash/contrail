{{
    config(
        materialized='view',
    )
}}

-- Silver: one row per distinct observation, cleaned and typed.
--
-- The bronze table is a ReplacingMergeTree keyed on (icao24, observed_at), so
-- duplicates are collapsed during background merges, eventually. Between
-- merges the table still holds them, and a query that simply selects rows would
-- count the same observation more than once. Measured against recorded traffic
-- that is 38.5% of grounded vectors, which is more than enough to visibly skew
-- an aircraft count.
--
-- Aggregating by the sort key makes deduplication explicit and immediate rather
-- than depending on merge timing. `FINAL` would do the same but forces a full
-- merge at read time on every query; at roughly 10M rows a day this GROUP BY
-- reads the same data once and is deterministic regardless of merge state.

with deduplicated as (

    select
        icao24,
        observed_at,

        -- argMax resolves the duplicate: keep the most recently ingested copy
        -- of every column. Re-ingesting a batch after a crash therefore
        -- corrects a row rather than adding one.
        argMax(callsign, ingested_at)             as callsign,
        argMax(origin_country, ingested_at)       as origin_country,
        argMax(time_position, ingested_at)        as time_position,
        argMax(last_contact, ingested_at)         as last_contact,
        argMax(longitude, ingested_at)            as longitude,
        argMax(latitude, ingested_at)             as latitude,
        argMax(baro_altitude, ingested_at)        as baro_altitude,
        argMax(on_ground, ingested_at)            as on_ground,
        argMax(velocity, ingested_at)             as velocity,
        argMax(true_track, ingested_at)           as true_track,
        argMax(vertical_rate, ingested_at)        as vertical_rate,
        argMax(geo_altitude, ingested_at)         as geo_altitude,
        argMax(squawk, ingested_at)               as squawk,
        argMax(spi, ingested_at)                  as spi,
        argMax(position_source, ingested_at)      as position_source,
        argMax(category, ingested_at)             as category,
        argMax(region, ingested_at)               as region,
        argMax(poll_time, ingested_at)            as poll_time,
        argMax(position_age_seconds, ingested_at) as position_age_seconds,

        -- Deliberately not aliased `ingested_at`. ClickHouse resolves column
        -- aliases inside the same SELECT, so an alias matching the ordering
        -- column would be substituted into every argMax above and rejected as
        -- an aggregate nested inside an aggregate. The outer query renames it.
        max(ingested_at)                          as last_ingested_at,

        -- How many raw rows collapsed into this observation. Carried forward so
        -- the duplicate rate is measurable downstream instead of being an
        -- assumption about the pipeline.
        count()                                   as source_row_count

    from {{ source('bronze', 'bronze_state_vectors') }}
    group by icao24, observed_at

)

select
    icao24,
    observed_at,
    callsign,
    origin_country,
    time_position,
    last_contact,
    longitude,
    latitude,
    baro_altitude,
    on_ground,
    velocity,
    true_track,
    vertical_rate,
    geo_altitude,
    squawk,
    spi,
    position_source,
    category,
    region,
    poll_time,
    last_ingested_at as ingested_at,
    position_age_seconds,
    source_row_count,

    -- Whether this observation can be trusted to place the aircraft.
    --
    -- A vector always reports where the aircraft *was* at time_position, but
    -- that can lag last_contact by minutes. Rows failing this are kept, not
    -- dropped: they are still evidence the aircraft was contactable, and
    -- discarding them would understate coverage. Geospatial models filter on
    -- this flag; availability models do not.
    (
        latitude is not null
        and longitude is not null
        and position_age_seconds <= {{ var('max_position_age_seconds') }}
    ) as has_usable_position,

    -- Preferred altitude source. Geometric altitude is GNSS-derived and does
    -- not drift with atmospheric pressure, so it is used when present;
    -- barometric is the fallback, and it is missing about 11% of the time.
    coalesce(geo_altitude, baro_altitude) as altitude_m

from deduplicated

-- Vectors with no position at all cannot contribute to any model built on top
-- of this one. They are excluded here rather than in every downstream model.
where latitude is not null
  and longitude is not null
