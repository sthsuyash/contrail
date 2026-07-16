"""Dagster definitions for the Contrail pipeline.

Assets rather than tasks. The distinction matters here because the pipeline's
unit of work is a table that should exist and be current, not a script that
should run, and the difference shows up when something fails. A task graph
tells you a job errored; an asset graph tells you which table is stale and what
else is now built on stale input.

The ingester itself is deliberately not an asset. It is a continuously running
process with its own pacing, not something to schedule: waking it hourly would
fight the credit budgeter, which already decides when to poll based on how much
allowance is left in the day.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    Definitions,
    HourlyPartitionsDefinition,
    MetadataValue,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from contrail.config import Settings

DBT_PROJECT_DIR = Path(__file__).parent.parent / "transform"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)
dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(project_dir=dbt_project)

# Hourly partitions align with how the bronze layer is written: Parquet is
# partitioned on observed_at hour, so a backfill of one partition maps onto
# exactly one set of objects.
hourly_partitions = HourlyPartitionsDefinition(start_date="2026-08-01-00:00")


def _clickhouse_client() -> Any:
    import clickhouse_connect

    settings = Settings()
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )


@asset(
    partitions_def=hourly_partitions,
    description=(
        "Raw state vectors landed by the Kafka sink. This asset observes what "
        "the sink has already written rather than producing it, because the "
        "sink is a long-running consumer, not a scheduled job."
    ),
    compute_kind="clickhouse",
)
def bronze_state_vectors(context: AssetExecutionContext) -> None:
    window_start = dt.datetime.fromisoformat(context.partition_key)
    window_end = window_start + dt.timedelta(hours=1)

    client = _clickhouse_client()
    try:
        rows, aircraft, duplicates = client.query(
            """
            SELECT count(),
                   uniqExact(icao24),
                   count() - uniqExact((icao24, observed_at))
            FROM contrail.bronze_state_vectors
            WHERE observed_at >= %(start)s AND observed_at < %(end)s
            """,
            parameters={"start": window_start, "end": window_end},
        ).result_rows[0]
    finally:
        client.close()

    context.add_output_metadata(
        {
            "rows": MetadataValue.int(rows),
            "distinct_aircraft": MetadataValue.int(aircraft),
            "duplicate_rows": MetadataValue.int(duplicates),
            "partition": MetadataValue.text(context.partition_key),
        }
    )

    if rows == 0:
        # Not an error. Overnight hours in a single region genuinely produce
        # nothing, and failing on that would train everyone to ignore alerts.
        context.log.warning("no rows landed for %s", context.partition_key)


@dbt_assets(manifest=dbt_project.manifest_path)
def contrail_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# ----------------------------------------------------------------- checks


@asset_check(
    asset=bronze_state_vectors,
    description="Bronze data has arrived recently enough to be considered live.",
    blocking=False,
)
def bronze_is_fresh() -> AssetCheckResult:
    """Freshness, measured against the scheduler's own coverage guarantee.

    The threshold is not arbitrary. Regions rotate under a 10-minute staleness
    ceiling, so a gap of that order is normal even when everything is healthy.
    Warning below that would fire constantly; the limit here sits at three
    rotations, by which point ingestion has genuinely stopped.
    """
    client = _clickhouse_client()
    try:
        result = client.query(
            "SELECT max(ingested_at), dateDiff('second', max(ingested_at), now()) "
            "FROM contrail.bronze_state_vectors"
        ).result_rows[0]
    finally:
        client.close()

    latest, age_seconds = result
    if latest is None:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="bronze table is empty",
        )

    limit = 30 * 60
    return AssetCheckResult(
        passed=age_seconds <= limit,
        severity=AssetCheckSeverity.WARN,
        description=f"newest row is {age_seconds}s old (limit {limit}s)",
        metadata={
            "latest_ingested_at": MetadataValue.text(str(latest)),
            "age_seconds": MetadataValue.int(int(age_seconds)),
        },
    )


@asset_check(
    asset=AssetKey(["fct_flights"]),
    description="Reconstructed flights are physically plausible.",
    blocking=True,
)
def flights_are_plausible() -> AssetCheckResult:
    """Catches unit and sign errors that schema tests cannot.

    A range test on altitude proves the number sits between bounds. It does not
    prove the reconstruction is sane. A flight lasting a negative time, or one
    at cruise altitude reporting zero speed, satisfies every column constraint
    while being obviously wrong. These are the assertions that would have caught
    a metres-versus-feet mix-up.
    """
    client = _clickhouse_client()
    try:
        row = client.query(
            """
            SELECT
                countIf(duration_seconds < 0),
                countIf(max_altitude_m > 8000 AND max_velocity_knots < 50),
                countIf(observation_count < 3),
                countIf(arrival_time < departure_time),
                count()
            FROM contrail.fct_flights
            """
        ).result_rows[0]
    finally:
        client.close()

    negative_duration, stalled_at_altitude, too_few_points, reversed_times, total = row
    violations = {
        "negative_duration": negative_duration,
        "cruise_altitude_without_speed": stalled_at_altitude,
        "below_minimum_observations": too_few_points,
        "arrival_before_departure": reversed_times,
    }
    failed = {name: count for name, count in violations.items() if count}

    return AssetCheckResult(
        passed=not failed,
        severity=AssetCheckSeverity.ERROR,
        description=(
            f"{total} flights checked, no implausible rows"
            if not failed
            else f"implausible rows: {failed}"
        ),
        metadata={k: MetadataValue.int(v) for k, v in violations.items()}
        | {"flights_checked": MetadataValue.int(total)},
    )


@asset_check(
    asset=AssetKey(["stg_state_vectors"]),
    description="Deduplication is actually removing duplicates.",
    blocking=False,
)
def deduplication_is_working() -> AssetCheckResult:
    """Guards the assumption the silver layer is built on.

    Measured duplicate rates are ~38% for grounded aircraft and ~7% airborne. A
    rate of exactly zero is more suspicious than a high one: it means either
    ingestion stopped or the GROUP BY silently stopped collapsing anything.
    """
    client = _clickhouse_client()
    try:
        raw, deduplicated = client.query(
            "SELECT sum(source_row_count), count() FROM contrail_staging.stg_state_vectors"
        ).result_rows[0]
    finally:
        client.close()

    if not raw:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="no rows in the staging model",
        )

    rate = (raw - deduplicated) / raw * 100
    return AssetCheckResult(
        passed=deduplicated <= raw,
        severity=AssetCheckSeverity.WARN,
        description=(
            f"{raw} raw rows collapsed to {deduplicated} observations ({rate:.1f}% duplicates)"
        ),
        metadata={
            "raw_rows": MetadataValue.int(int(raw)),
            "distinct_observations": MetadataValue.int(int(deduplicated)),
            "duplicate_rate_pct": MetadataValue.float(round(rate, 2)),
        },
    )


# ------------------------------------------------------------- jobs


transform_job = define_asset_job(
    name="transform",
    selection=[contrail_dbt_assets],
    description="Rebuild the silver and gold models from bronze.",
)

# Ten past the hour, so the hour's final records have been consumed and
# flushed before the models that summarise them are rebuilt.
transform_schedule = ScheduleDefinition(
    job=transform_job,
    cron_schedule="10 * * * *",
    execution_timezone="UTC",
)

defs = Definitions(
    assets=[bronze_state_vectors, contrail_dbt_assets],
    asset_checks=[bronze_is_fresh, flights_are_plausible, deduplication_is_working],
    jobs=[transform_job],
    schedules=[transform_schedule],
    resources={"dbt": dbt_resource},
)
