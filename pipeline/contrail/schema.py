"""The state vector record schema, shared by the Parquet and ClickHouse sinks.

This is the consuming half of the contract the Go ingester writes. The field
names and types here must match ``ingester/internal/sink/record.go``; the
round-trip test in ``tests/test_schema.py`` pins that against a real message.

Nullability is the part worth being careful with. Almost every measurement in a
state vector is optional, and an absent one is not zero: an aircraft parked at
a gate genuinely reports ``velocity: 0`` while one with no fix reports nothing
at all. Measured against recorded traffic, 10-20% of vectors are missing
altitude, vertical rate or squawk. Coercing those to zero on the way in is
unrecoverable and biases every average computed downstream, so nullability is
preserved all the way into the warehouse.
"""

from __future__ import annotations

from typing import Any, Final

import pyarrow as pa

# Column order is fixed so Parquet files written by different runs share a
# schema and can be read as one dataset.
BRONZE_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        # Observation identity.
        pa.field("icao24", pa.string(), nullable=False),
        pa.field("observed_at", pa.timestamp("s", tz="UTC"), nullable=False),
        # Reported state.
        pa.field("callsign", pa.string(), nullable=True),
        pa.field("origin_country", pa.string(), nullable=False),
        pa.field("time_position", pa.timestamp("s", tz="UTC"), nullable=True),
        pa.field("last_contact", pa.timestamp("s", tz="UTC"), nullable=False),
        pa.field("longitude", pa.float64(), nullable=True),
        pa.field("latitude", pa.float64(), nullable=True),
        pa.field("baro_altitude", pa.float64(), nullable=True),
        pa.field("on_ground", pa.bool_(), nullable=False),
        pa.field("velocity", pa.float64(), nullable=True),
        pa.field("true_track", pa.float64(), nullable=True),
        pa.field("vertical_rate", pa.float64(), nullable=True),
        pa.field("geo_altitude", pa.float64(), nullable=True),
        pa.field("squawk", pa.string(), nullable=True),
        pa.field("spi", pa.bool_(), nullable=False),
        pa.field("position_source", pa.uint8(), nullable=False),
        pa.field("category", pa.uint8(), nullable=True),
        # Ingestion provenance.
        pa.field("region", pa.string(), nullable=False),
        pa.field("poll_time", pa.timestamp("s", tz="UTC"), nullable=False),
        pa.field("ingested_at", pa.timestamp("s", tz="UTC"), nullable=False),
        pa.field("position_age_seconds", pa.int32(), nullable=False),
    ]
)

# ClickHouse column order for the bronze insert. Kept separate from the Parquet
# schema because clickhouse-connect inserts positionally, so this list is the
# authority on ordering for that path.
CLICKHOUSE_COLUMNS: Final[list[str]] = [field.name for field in BRONZE_SCHEMA]

# Fields the ingester sends that the warehouse does not store.
#
# dedup_key is one of them: it exists so the ingester and the tests can talk
# about observation identity, but in ClickHouse that identity is expressed by
# the ReplacingMergeTree sort key instead. Storing it too would be a second,
# silently divergent definition of the same thing.
DROPPED_FIELDS: Final[frozenset[str]] = frozenset({"dedup_key"})

# Epoch seconds outside this range indicate a corrupt or misparsed message
# rather than an unusual aircraft. The lower bound predates ADS-B deployment;
# the upper bound is far enough out to never reject live data.
_MIN_EPOCH: Final[int] = 946_684_800  # 2000-01-01
_MAX_EPOCH: Final[int] = 4_102_444_800  # 2100-01-01


class SchemaError(ValueError):
    """A message did not match the expected record schema."""


def _epoch(value: Any, field: str, *, required: bool) -> int | None:
    """Validate an epoch-seconds field."""
    if value is None:
        if required:
            raise SchemaError(f"{field} is required but was null")
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise SchemaError(f"{field} must be an integer, got {type(value).__name__}")
    if not _MIN_EPOCH <= value <= _MAX_EPOCH:
        raise SchemaError(f"{field}={value} is outside the plausible epoch range")
    return value


def _number(value: Any, field: str) -> float | None:
    """Validate an optional numeric measurement.

    JSON numbers arrive as both ``0`` and ``5.62`` for the same field, since the
    API emits integers whenever a float happens to be whole, so ints are
    accepted and widened rather than rejected.
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SchemaError(f"{field} must be numeric, got {type(value).__name__}")
    return float(value)


def _text(value: Any, field: str, *, required: bool) -> str | None:
    if value is None:
        if required:
            raise SchemaError(f"{field} is required but was null")
        return None
    if not isinstance(value, str):
        raise SchemaError(f"{field} must be a string, got {type(value).__name__}")
    return value


def _flag(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise SchemaError(f"{field} must be a boolean, got {type(value).__name__}")
    return value


def parse_record(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate one decoded message and project it onto the bronze schema.

    Raises SchemaError if the message cannot be trusted. The caller decides what
    to do with that: the consumer routes failures to a dead-letter path rather
    than stopping, because one malformed record should not cost the other
    several hundred in the same batch.
    """
    if not isinstance(payload, dict):
        raise SchemaError(f"record must be an object, got {type(payload).__name__}")

    icao24 = _text(payload.get("icao24"), "icao24", required=True)
    assert icao24 is not None  # narrowed by required=True
    if not icao24.strip():
        raise SchemaError("icao24 is empty")

    latitude = _number(payload.get("latitude"), "latitude")
    longitude = _number(payload.get("longitude"), "longitude")
    if latitude is not None and not -90 <= latitude <= 90:
        raise SchemaError(f"latitude={latitude} is out of range")
    if longitude is not None and not -180 <= longitude <= 180:
        raise SchemaError(f"longitude={longitude} is out of range")

    category = payload.get("category")
    if category is not None and (not isinstance(category, int) or not 0 <= category <= 20):
        raise SchemaError(f"category={category!r} is not a valid aircraft category")

    source = payload.get("position_source")
    if not isinstance(source, int) or not 0 <= source <= 3:
        raise SchemaError(f"position_source={source!r} is not one of 0-3")

    return {
        "icao24": icao24,
        "observed_at": _epoch(payload.get("observed_at"), "observed_at", required=True),
        "callsign": _text(payload.get("callsign"), "callsign", required=False),
        "origin_country": _text(payload.get("origin_country"), "origin_country", required=True)
        or "",
        "time_position": _epoch(payload.get("time_position"), "time_position", required=False),
        "last_contact": _epoch(payload.get("last_contact"), "last_contact", required=True),
        "longitude": longitude,
        "latitude": latitude,
        "baro_altitude": _number(payload.get("baro_altitude"), "baro_altitude"),
        "on_ground": _flag(payload.get("on_ground"), "on_ground"),
        "velocity": _number(payload.get("velocity"), "velocity"),
        "true_track": _number(payload.get("true_track"), "true_track"),
        "vertical_rate": _number(payload.get("vertical_rate"), "vertical_rate"),
        "geo_altitude": _number(payload.get("geo_altitude"), "geo_altitude"),
        "squawk": _text(payload.get("squawk"), "squawk", required=False),
        "spi": _flag(payload.get("spi"), "spi"),
        "position_source": source,
        "category": category,
        "region": _text(payload.get("region"), "region", required=True) or "",
        "poll_time": _epoch(payload.get("poll_time"), "poll_time", required=True),
        "ingested_at": _epoch(payload.get("ingested_at"), "ingested_at", required=True),
        "position_age_seconds": int(payload.get("position_age_seconds") or 0),
    }


def to_arrow(records: list[dict[str, Any]]) -> pa.Table:
    """Build an Arrow table from parsed records.

    Columns are assembled explicitly rather than via ``from_pylist`` so the
    declared schema governs types. Inference would otherwise pick a type from
    whatever happens to be in the batch. An all-null altitude column in one
    batch would land as null-typed and fail to merge with a float column in the
    next.
    """
    if not records:
        return BRONZE_SCHEMA.empty_table()

    columns = []
    for field in BRONZE_SCHEMA:
        values = [record[field.name] for record in records]
        columns.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(columns, schema=BRONZE_SCHEMA)
