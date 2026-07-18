"""Read-only HTTP API over the Contrail gold marts."""

from __future__ import annotations

import datetime as dt
import os
import threading
from contextlib import asynccontextmanager
from typing import Annotated, Any, AsyncIterator

import clickhouse_connect
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

CLICKHOUSE = {
    "host": os.getenv("CONTRAIL_CLICKHOUSE_HOST", "localhost"),
    "port": int(os.getenv("CONTRAIL_CLICKHOUSE_PORT", "8123")),
    "username": os.getenv("CONTRAIL_CLICKHOUSE_USER", "contrail"),
    "password": os.getenv("CONTRAIL_CLICKHOUSE_PASSWORD", "contrail"),
    "database": os.getenv("CONTRAIL_CLICKHOUSE_DATABASE", "contrail"),
}

# One client per thread, not one per process.
#
# A clickhouse-connect client owns a session and refuses concurrent queries on
# it: "Attempt to execute concurrent queries within the same session". FastAPI
# runs synchronous endpoint functions in a threadpool, so a single shared client
# fails as soon as two requests overlap. The dashboard issues three calls at once
# on every refresh, so this is the normal case rather than an edge case, and the
# resulting 500s surface in the browser as opaque CORS errors because the failed
# response carries no headers.
#
# Thread-local storage gives each threadpool worker its own session while still
# reusing connections across requests, since those threads are long-lived.
_local = threading.local()
_clients: list[Any] = []
_clients_lock = threading.Lock()


def _new_client() -> Any:
    client = clickhouse_connect.get_client(**CLICKHOUSE)
    # Tracked so shutdown can close every session, not just the one belonging to
    # whichever thread happens to run the lifespan handler.
    with _clients_lock:
        _clients.append(client)
    return client


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    # Fail fast at startup if the warehouse is unreachable, rather than serving
    # 500s on the first request.
    _new_client().query("SELECT 1")
    try:
        yield
    finally:
        with _clients_lock:
            for client in _clients:
                try:
                    client.close()
                except Exception:  # noqa: BLE001 - shutdown is best effort
                    pass
            _clients.clear()


app = FastAPI(
    title="Contrail API",
    description="Flight telemetry reconstructed from OpenSky Network state vectors.",
    version="0.1.0",
    lifespan=lifespan,
)

# The dashboard is served from a different origin in development.
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CONTRAIL_CORS_ORIGINS", "http://localhost:3000").split(","),
    allow_methods=["GET"],
    allow_headers=["*"],
)


def get_client() -> Any:
    """Return this thread's client, creating one on first use."""
    client = getattr(_local, "client", None)
    if client is None:
        client = _new_client()
        _local.client = client
    return client


Client = Annotated[Any, Depends(get_client)]


# ------------------------------------------------------------------ models


class Aircraft(BaseModel):
    icao24: str
    callsign: str | None
    origin_country: str
    latitude: float
    longitude: float
    altitude_m: float | None
    velocity_knots: float | None
    true_track: float | None
    on_ground: bool
    observed_at: dt.datetime
    position_age_seconds: int = Field(
        description="Seconds between the position fix and last contact; high values mean a stale position."
    )


class Flight(BaseModel):
    flight_id: str
    icao24: str
    callsign: str | None
    departure_time: dt.datetime
    arrival_time: dt.datetime
    duration_minutes: float
    departure_airport: str | None
    arrival_airport: str | None
    max_altitude_m: float | None
    max_velocity_knots: float | None
    endpoint_distance_km: float | None
    observation_count: int
    is_complete: bool
    reconstruction_quality: str


class TrafficPoint(BaseModel):
    traffic_hour: dt.datetime
    region: str
    distinct_aircraft: int
    distinct_airborne: int
    observations: int
    duplicate_rate_pct: float | None


class PipelineStats(BaseModel):
    raw_rows: int
    distinct_observations: int
    duplicate_rate_pct: float
    distinct_aircraft: int
    flights_reconstructed: int
    complete_flights: int
    latest_observation: dt.datetime | None
    regions: list[str]


def rows_to_dicts(result: Any) -> list[dict[str, Any]]:
    return [dict(zip(result.column_names, row, strict=True)) for row in result.result_rows]


# ---------------------------------------------------------------- endpoints


@app.get("/health")
def health(client: Client) -> dict[str, str]:
    client.query("SELECT 1")
    return {"status": "ok"}


@app.get("/api/aircraft/live", response_model=list[Aircraft])
def live_aircraft(
    client: Client,
    limit: int = Query(default=2000, ge=1, le=10000),
    max_age_seconds: int = Query(
        default=900,
        ge=0,
        description="Ignore observations older than this.",
    ),
) -> list[dict[str, Any]]:
    """Most recent position per aircraft.

    Positions are filtered on `has_usable_position`, so an aircraft whose last
    fix is badly stale is omitted rather than drawn in the wrong place. That
    filter is the reason this endpoint can be plotted directly.
    """
    # The filter lives in a subquery rather than a WHERE beside the aggregates.
    # ClickHouse resolves column aliases within the same SELECT, so an outer
    # `WHERE observed_at >= ...` would bind to the `max(observed_at)` alias and
    # be rejected as an aggregate in WHERE. Filtering first is also what we
    # want semantically: stale fixes are excluded before the "latest position
    # per aircraft" is chosen, so a discarded fix cannot win the argMax.
    result = client.query(
        """
        SELECT
            icao24,
            argMax(callsign, seen_at)             AS callsign,
            argMax(origin_country, seen_at)       AS origin_country,
            argMax(latitude, seen_at)             AS latitude,
            argMax(longitude, seen_at)            AS longitude,
            argMax(altitude_m, seen_at)           AS altitude_m,
            round(argMax(velocity, seen_at) * 1.94384) AS velocity_knots,
            argMax(true_track, seen_at)           AS true_track,
            argMax(on_ground, seen_at)            AS on_ground,
            max(seen_at)                          AS observed_at,
            argMax(position_age_seconds, seen_at) AS position_age_seconds
        FROM (
            SELECT
                icao24, callsign, origin_country, latitude, longitude,
                altitude_m, velocity, true_track, on_ground,
                position_age_seconds,
                observed_at AS seen_at
            FROM contrail_staging.stg_state_vectors
            WHERE has_usable_position
              AND observed_at >= now() - INTERVAL %(max_age)s SECOND
        )
        GROUP BY icao24
        ORDER BY observed_at DESC
        LIMIT %(limit)s
        """,
        parameters={"limit": limit, "max_age": max_age_seconds},
    )
    return rows_to_dicts(result)


@app.get("/api/flights", response_model=list[Flight])
def flights(
    client: Client,
    limit: int = Query(default=100, ge=1, le=1000),
    min_quality: str = Query(
        default="low",
        pattern="^(low|medium|high)$",
        description="Minimum reconstruction confidence.",
    ),
    complete_only: bool = Query(
        default=False,
        description="Only flights whose start and end were both observed on the ground.",
    ),
) -> list[dict[str, Any]]:
    """Reconstructed flights, newest first."""
    ranks = {"low": 0, "medium": 1, "high": 2}
    allowed = [name for name, rank in ranks.items() if rank >= ranks[min_quality]]

    result = client.query(
        f"""
        SELECT
            flight_id, icao24, callsign,
            departure_time, arrival_time, duration_minutes,
            nullIf(departure_airport, '') AS departure_airport,
            nullIf(arrival_airport, '')   AS arrival_airport,
            max_altitude_m, max_velocity_knots, endpoint_distance_km,
            observation_count, is_complete, reconstruction_quality
        FROM contrail.fct_flights
        WHERE reconstruction_quality IN %(allowed)s
          {"AND is_complete" if complete_only else ""}
        ORDER BY departure_time DESC
        LIMIT %(limit)s
        """,
        parameters={"limit": limit, "allowed": allowed},
    )
    return rows_to_dicts(result)


@app.get("/api/flights/{flight_id}/track")
def flight_track(client: Client, flight_id: str) -> dict[str, Any]:
    """Every observed position for one flight, in order."""
    result = client.query(
        """
        SELECT observed_at, latitude, longitude, altitude_m,
               round(velocity * 1.94384) AS velocity_knots,
               vertical_rate, flight_phase, seconds_since_previous
        FROM contrail.int_flight_sessions
        WHERE flight_id = %(flight_id)s AND has_usable_position
        ORDER BY observed_at
        """,
        parameters={"flight_id": flight_id},
    )
    points = rows_to_dicts(result)
    if not points:
        raise HTTPException(status_code=404, detail=f"no track for flight {flight_id}")
    return {"flight_id": flight_id, "points": points}


@app.get("/api/traffic/hourly", response_model=list[TrafficPoint])
def hourly_traffic(
    client: Client,
    hours: int = Query(default=48, ge=1, le=720),
) -> list[dict[str, Any]]:
    """Traffic per region per hour, measured in distinct aircraft."""
    result = client.query(
        """
        SELECT traffic_hour, region, distinct_aircraft, distinct_airborne,
               observations, duplicate_rate_pct
        FROM contrail.fct_hourly_traffic
        WHERE traffic_hour >= now() - INTERVAL %(hours)s HOUR
        ORDER BY traffic_hour, region
        """,
        parameters={"hours": hours},
    )
    return rows_to_dicts(result)


@app.get("/api/stats", response_model=PipelineStats)
def pipeline_stats(client: Client) -> dict[str, Any]:
    """Headline pipeline measurements, including the observed duplicate rate."""
    bronze = client.query(
        """
        SELECT count(), uniqExact((icao24, observed_at)), uniqExact(icao24),
               max(observed_at), groupUniqArray(region)
        FROM contrail.bronze_state_vectors
        """
    ).result_rows[0]
    raw_rows, distinct_observations, distinct_aircraft, latest, regions = bronze

    # The duplicate rate cannot be read from bronze. ReplacingMergeTree collapses
    # duplicate (icao24, observed_at) rows on merge, after which count() and
    # uniqExact agree by construction and the rate always reads as zero. It is
    # only observable at ingestion, which is what ingest_batches records.
    ingest = client.query(
        "SELECT sum(rows_written), sum(distinct_observations) FROM contrail.ingest_batches"
    ).result_rows[0]
    ingested_rows, ingested_distinct = ingest[0] or 0, ingest[1] or 0

    flights_row = client.query(
        "SELECT count(), countIf(is_complete) FROM contrail.fct_flights"
    ).result_rows[0]

    return {
        "raw_rows": raw_rows,
        "distinct_observations": distinct_observations,
        "duplicate_rate_pct": round(
            (ingested_rows - ingested_distinct) / ingested_rows * 100, 2
        )
        if ingested_rows
        else 0.0,
        "distinct_aircraft": distinct_aircraft,
        "flights_reconstructed": flights_row[0],
        "complete_flights": flights_row[1],
        "latest_observation": latest,
        "regions": sorted(regions),
    }
