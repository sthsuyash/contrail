-- Bronze layer: raw state vectors, one row per observation.
--
-- Runs once on first container start via docker-entrypoint-initdb.d.

CREATE DATABASE IF NOT EXISTS contrail;

-- Deduplication is delegated to the storage engine rather than done in the
-- consumer.
--
-- The observation key is (icao24, time_position): consecutive polls return the
-- same aircraft repeatedly, and an aircraft whose transponder has not reported
-- since the last poll comes back carrying an identical time_position. Measured
-- against recorded traffic that is 38.5% of grounded vectors and 6.6% of
-- airborne ones.
--
-- ReplacingMergeTree collapses rows sharing the ORDER BY key during background
-- merges, keeping the one with the highest version column. Making the sort key
-- the observation key therefore makes deduplication a property of the table
-- instead of state the consumer has to carry. That matters for restarts: a
-- consumer that dedups in memory loses its history on restart and re-admits
-- everything it had already filtered, whereas the table's guarantee survives.
--
-- The cost is that collapsing is eventual, not immediate. Between merges the
-- table can hold duplicates, so any query that counts observations must say so
-- explicitly: see the silver models, which aggregate rather than trusting the
-- row count. FINAL would force it at read time but scans far more data; at
-- ~10M rows a day that trade is not worth making for every query.
CREATE TABLE IF NOT EXISTS contrail.bronze_state_vectors
(
    -- Observation identity.
    icao24               LowCardinality(String) COMMENT 'ICAO 24-bit transponder address',
    observed_at          DateTime               COMMENT 'time_position, or last_contact when there is no fix',

    -- Reported state. Nullable columns mean "not reported", which is
    -- distinct from zero: a parked aircraft genuinely reports velocity 0,
    -- while one with no fix reports nothing. Collapsing the two would
    -- silently bias every average downstream.
    callsign             Nullable(String)       COMMENT 'trimmed of the API 8-char padding',
    origin_country       LowCardinality(String),
    time_position        Nullable(DateTime)     COMMENT 'null when the aircraft has no position fix',
    last_contact         DateTime,
    longitude            Nullable(Float64),
    latitude             Nullable(Float64),
    baro_altitude        Nullable(Float64)      COMMENT 'metres',
    on_ground            Bool,
    velocity             Nullable(Float64)      COMMENT 'metres/second over ground',
    true_track           Nullable(Float64)      COMMENT 'degrees clockwise from north',
    vertical_rate        Nullable(Float64)      COMMENT 'metres/second, positive climbing',
    geo_altitude         Nullable(Float64)      COMMENT 'metres',
    squawk               Nullable(String),
    spi                  Bool,
    position_source      UInt8                  COMMENT '0=ADS-B 1=ASTERIX 2=MLAT 3=FLARM',
    category             Nullable(UInt8)        COMMENT 'only present when extended=1 was requested',

    -- Ingestion provenance.
    region               LowCardinality(String) COMMENT 'which configured region polled this',
    poll_time            DateTime               COMMENT 'the response envelope timestamp',
    ingested_at          DateTime               COMMENT 'doubles as the ReplacingMergeTree version',

    -- How far the position fix lags last contact. Measured p50 0s, p99 101s,
    -- max 5497s. Geospatial models filter on this; without it an aircraft
    -- reported as current could be placed an hour and a half from where it was.
    position_age_seconds Int32,

    -- Skip indexes, not a second sort order. The table is sorted by aircraft,
    -- so a query filtering only on region or time would otherwise read every
    -- part; these let ClickHouse discard granules first.
    INDEX idx_region region TYPE set(0) GRANULARITY 4,
    INDEX idx_poll_time poll_time TYPE minmax GRANULARITY 4
)
ENGINE = ReplacingMergeTree(ingested_at)
-- Daily partitions match how the data is queried and expired. Hourly would
-- produce 24x the parts for no benefit at this volume and slow merges down.
PARTITION BY toYYYYMMDD(observed_at)
-- Aircraft first, then time: sessionization walks one aircraft's history in
-- order, which is the dominant read pattern, and it makes the sort key
-- identical to the observation key so replacement does the deduplication.
ORDER BY (icao24, observed_at)
TTL observed_at + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
