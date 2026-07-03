"""Schema tests, including a round-trip against real ingester output.

``tests/data/sample_records.ndjson`` was produced by the Go ingester from
recorded OpenSky traffic. It is deliberately weighted towards the awkward
cases (grounded aircraft, missing altitudes, genuine zero velocities and badly
stale position fixes), because those are where a schema quietly loses meaning.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from contrail.schema import (
    BRONZE_SCHEMA,
    CLICKHOUSE_COLUMNS,
    DROPPED_FIELDS,
    SchemaError,
    parse_record,
    to_arrow,
)

SAMPLE_PATH = Path(__file__).parent / "data" / "sample_records.ndjson"


@pytest.fixture(scope="module")
def sample_records() -> list[dict[str, Any]]:
    return [json.loads(line) for line in SAMPLE_PATH.read_text().splitlines() if line.strip()]


def minimal_record(**overrides: Any) -> dict[str, Any]:
    """A valid record, so each test can perturb exactly one field."""
    record: dict[str, Any] = {
        "icao24": "4b1815",
        "dedup_key": "4b1815:1786114045",
        "observed_at": 1786114045,
        "callsign": "SWR123",
        "origin_country": "Switzerland",
        "time_position": 1786114045,
        "last_contact": 1786114045,
        "longitude": 8.5554,
        "latitude": 47.4515,
        "baro_altitude": 11277.6,
        "on_ground": False,
        "velocity": 231.5,
        "true_track": 89.2,
        "vertical_rate": 0.0,
        "geo_altitude": 11582.4,
        "squawk": "1000",
        "spi": False,
        "position_source": 0,
        "category": None,
        "region": "swiss-alps",
        "poll_time": 1786114175,
        "ingested_at": 1786114175,
        "position_age_seconds": 0,
    }
    record.update(overrides)
    return record


# --------------------------------------------------------------- contract


def test_every_real_record_parses(sample_records: list[dict[str, Any]]) -> None:
    assert sample_records, "sample fixture is empty"
    for record in sample_records:
        parse_record(record)


def test_producer_fields_match_the_schema(sample_records: list[dict[str, Any]]) -> None:
    """The Go producer and this schema must not drift apart silently."""
    produced = set(sample_records[0])
    expected = {field.name for field in BRONZE_SCHEMA} | DROPPED_FIELDS

    assert not produced - expected, (
        f"producer sends fields the schema ignores: {produced - expected}"
    )
    assert not expected - produced, (
        f"schema expects fields the producer omits: {expected - produced}"
    )


def test_clickhouse_columns_track_the_arrow_schema() -> None:
    assert [field.name for field in BRONZE_SCHEMA] == CLICKHOUSE_COLUMNS


# ------------------------------------------------------ null vs zero


def test_absent_measurements_stay_null(sample_records: list[dict[str, Any]]) -> None:
    """The distinction the whole schema exists to preserve."""
    with_nulls = [r for r in sample_records if r["baro_altitude"] is None]
    assert with_nulls, "fixture should contain vectors with no altitude"

    for record in with_nulls:
        assert parse_record(record)["baro_altitude"] is None


def test_genuine_zeros_are_not_nulls(sample_records: list[dict[str, Any]]) -> None:
    stationary = [r for r in sample_records if r["velocity"] == 0]
    assert stationary, "fixture should contain aircraft genuinely reporting zero velocity"

    for record in stationary:
        parsed = parse_record(record)
        assert parsed["velocity"] == 0.0
        assert parsed["velocity"] is not None


def test_integer_valued_floats_are_accepted() -> None:
    """The API emits ``0`` and ``5.62`` for the same field."""
    parsed = parse_record(minimal_record(velocity=0, baro_altitude=1000))
    assert parsed["velocity"] == 0.0
    assert isinstance(parsed["velocity"], float)
    assert parsed["baro_altitude"] == 1000.0


# ----------------------------------------------------------- validation


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"icao24": None}, "icao24 is required"),
        ({"icao24": ""}, "empty icao24"),
        ({"observed_at": None}, "observed_at is required"),
        ({"observed_at": 100}, "epoch far before ADS-B existed"),
        ({"observed_at": 99_999_999_999}, "epoch far in the future"),
        ({"latitude": 91.0}, "latitude out of range"),
        ({"latitude": -91.0}, "latitude out of range"),
        ({"longitude": 181.0}, "longitude out of range"),
        ({"position_source": 9}, "unknown position source"),
        ({"position_source": None}, "position source is required"),
        ({"category": 99}, "invalid aircraft category"),
        ({"on_ground": "yes"}, "on_ground must be boolean"),
        ({"velocity": "fast"}, "velocity must be numeric"),
        ({"callsign": 42}, "callsign must be a string"),
    ],
)
def test_invalid_records_are_rejected(overrides: dict[str, Any], reason: str) -> None:
    with pytest.raises(SchemaError):
        parse_record(minimal_record(**overrides))


def test_booleans_are_not_accepted_as_numbers() -> None:
    """``True`` is an int in Python; letting it through would store 1.0 m/s."""
    with pytest.raises(SchemaError):
        parse_record(minimal_record(velocity=True))


def test_non_object_payload_is_rejected() -> None:
    with pytest.raises(SchemaError):
        parse_record(["not", "an", "object"])  # type: ignore[arg-type]


def test_positionless_vector_is_valid() -> None:
    """An aircraft with no fix is a real observation, not a broken record."""
    parsed = parse_record(
        minimal_record(
            latitude=None,
            longitude=None,
            time_position=None,
            baro_altitude=None,
            geo_altitude=None,
            velocity=None,
        )
    )
    assert parsed["latitude"] is None
    assert parsed["time_position"] is None
    assert parsed["observed_at"] is not None


def test_stale_position_is_preserved(sample_records: list[dict[str, Any]]) -> None:
    """Badly stale fixes must survive parsing so models can filter on them."""
    stale = max(sample_records, key=lambda r: r["position_age_seconds"])
    assert stale["position_age_seconds"] > 60, "fixture should contain a stale fix"

    parsed = parse_record(stale)
    assert parsed["position_age_seconds"] == stale["position_age_seconds"]
    # The observation instant must come from the fix, not from arrival time.
    assert parsed["observed_at"] == stale["observed_at"]
    assert parsed["observed_at"] < parsed["poll_time"]


# ---------------------------------------------------------------- arrow


def test_to_arrow_matches_the_declared_schema(sample_records: list[dict[str, Any]]) -> None:
    table = to_arrow([parse_record(r) for r in sample_records])

    assert table.schema.equals(BRONZE_SCHEMA)
    assert table.num_rows == len(sample_records)


def test_to_arrow_handles_an_all_null_column() -> None:
    """Type inference would make this column null-typed and unmergeable."""
    records = [
        parse_record(minimal_record(baro_altitude=None, icao24=f"aaaa{i:02d}")) for i in range(3)
    ]
    table = to_arrow(records)

    assert table.schema.field("baro_altitude").type == pa.float64()
    assert table.column("baro_altitude").null_count == 3


def test_to_arrow_on_empty_input() -> None:
    table = to_arrow([])
    assert table.num_rows == 0
    assert table.schema.equals(BRONZE_SCHEMA)


def test_timestamps_are_utc(sample_records: list[dict[str, Any]]) -> None:
    table = to_arrow([parse_record(r) for r in sample_records])

    for name in ("observed_at", "last_contact", "poll_time", "ingested_at"):
        assert table.schema.field(name).type == pa.timestamp("s", tz="UTC")
