{{
    config(
        materialized='table',
        order_by='(icao24, observed_at)',
    )
}}

-- Assigns every observation to a flight.
--
-- The API reports aircraft, not flights. There is no field saying "this is
-- flight 4 of the day for this airframe", only a stream of positions keyed by
-- transponder address. Reconstructing flights from that stream is the central
-- analytical problem in this pipeline, and everything in the marts depends on
-- getting it right.
--
-- Two signals mark a boundary:
--
--   1. A gap in observation. If an aircraft is not seen for longer than the
--      session threshold, the next sighting starts a new flight. This is the
--      weaker signal, because a gap is ambiguous: it might mean the aircraft
--      landed, or merely that it flew through airspace this pipeline was not
--      polling. Coverage is regional and rotates on a ceiling of 10 minutes,
--      so gaps of that order are routine and mean nothing. The threshold is set
--      well above them.
--
--   2. A ground-to-air transition. This is unambiguous: an aircraft that was
--      on the ground and is now airborne has taken off, whatever the gap was.
--      Where both signals are available this one governs.
--
-- Sessions are numbered by a running sum of boundary flags within each
-- aircraft, which is the standard way to turn a sparse set of "a new group
-- starts here" markers into a dense group identifier.

with observations as (

    select
        icao24,
        observed_at,
        callsign,
        origin_country,
        latitude,
        longitude,
        altitude_m,
        on_ground,
        velocity,
        vertical_rate,
        true_track,
        squawk,
        region,
        has_usable_position
    from {{ ref('stg_state_vectors') }}

),

with_previous as (

    select
        *,
        -- observation_seq exists to identify an aircraft's first row.
        --
        -- lagInFrame does not return NULL when there is no preceding row: it
        -- returns the column type's default, which for DateTime is the epoch.
        -- A null check against it therefore never fires, and the gap for a
        -- first observation comes out as the seconds since 1970, a number
        -- large enough to still trigger a session boundary, so sessions look
        -- correct while every gap-derived measure downstream is silently
        -- wrong. An explicit row number is the unambiguous test.
        row_number() over aircraft_history            as observation_seq,
        lagInFrame(observed_at) over aircraft_history as previous_observed_at,
        lagInFrame(on_ground) over aircraft_history   as previous_on_ground
    from observations
    window aircraft_history as (
        partition by icao24
        order by observed_at
        rows between unbounded preceding and current row
    )

),

boundaries as (

    select
        *,
        -- Null for a first observation: there is no previous sighting to
        -- measure from, which is different from a gap of zero.
        if(
            observation_seq = 1,
            null,
            dateDiff('second', previous_observed_at, observed_at)
        ) as seconds_since_previous,

        case
            -- The aircraft's first ever observation opens its first session.
            when observation_seq = 1 then 1

            -- Unambiguous takeoff.
            when previous_on_ground = true and on_ground = false then 1

            -- Observation gap long enough that continuity cannot be assumed.
            when dateDiff('second', previous_observed_at, observed_at)
                 > {{ var('session_gap_minutes') }} * 60 then 1

            else 0
        end as is_session_start
    from with_previous

),

numbered as (

    select
        *,
        sum(is_session_start) over (
            partition by icao24
            order by observed_at
            rows between unbounded preceding and current row
        ) as session_seq
    from boundaries

)

select
    -- A session identifier stable across rebuilds: it is derived from the data
    -- itself rather than from a row number, so re-running the model on the same
    -- input produces the same identifiers.
    concat(icao24, '-', toString(session_seq)) as flight_id,
    icao24,
    session_seq,
    observed_at,
    callsign,
    origin_country,
    latitude,
    longitude,
    altitude_m,
    on_ground,
    velocity,
    vertical_rate,
    true_track,
    squawk,
    region,
    has_usable_position,
    seconds_since_previous,
    is_session_start,

    -- Vertical phase, from the reported climb rate.
    --
    -- The 1.5 m/s deadband keeps ordinary turbulence in cruise out of the climb
    -- and descent buckets; without it level flight flickers between phases on
    -- every minor altitude correction.
    case
        when on_ground then 'ground'
        when vertical_rate is null then 'unknown'
        when vertical_rate > 1.5 then 'climb'
        when vertical_rate < -1.5 then 'descent'
        else 'cruise'
    end as flight_phase

from numbered
