-- Ingestion metrics, written by the bronze sink.
--
-- This table exists because of a property of the bronze layer that is easy to
-- miss: ReplacingMergeTree destroys the evidence of its own work. Once merges
-- have collapsed duplicate (icao24, observed_at) rows, `count()` and
-- `uniqExact((icao24, observed_at))` agree by construction, and the duplicate
-- rate, a genuinely useful signal about upstream behaviour, reads as exactly
-- zero no matter what it actually was.
--
-- The rate is therefore only observable at the moment of ingestion, before the
-- warehouse deduplicates. Recording it here keeps a measurement that would
-- otherwise be irrecoverable, and turns "38% of grounded vectors are
-- duplicates" from a one-off analysis into a continuously tracked metric.
CREATE TABLE IF NOT EXISTS contrail.ingest_batches
(
    batch_at          DateTime COMMENT 'when the batch was flushed',
    messages_consumed UInt32   COMMENT 'Kafka messages read for this batch',
    rows_written      UInt32   COMMENT 'rows handed to the warehouse',
    invalid_messages  UInt32   COMMENT 'messages rejected by schema validation',
    -- Distinct observations within the batch. Comparing this to rows_written
    -- gives the in-batch duplicate rate; it understates the total, since
    -- duplicates also occur across batch boundaries.
    distinct_observations UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(batch_at)
ORDER BY batch_at
TTL batch_at + INTERVAL 90 DAY;
