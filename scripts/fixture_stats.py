#!/usr/bin/env python3
"""Measure the data-quality properties the pipeline is designed around.

Every number the README claims about OpenSky data comes from this script, run
against the fixtures in the repository. Keeping the analysis in version control
rather than in prose means the claims can be re-checked against a fresh capture
instead of being taken on trust, and if a future recording contradicts them,
the discrepancy shows up immediately.

Usage:
    python3 scripts/fixture_stats.py [fixtures_dir]
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

# State vector indices, per the OpenSky REST API. The response encodes each
# aircraft as a positional array rather than an object, so these are the schema.
ICAO24 = 0
CALLSIGN = 1
TIME_POSITION = 3
LAST_CONTACT = 4
LONGITUDE = 5
LATITUDE = 6
BARO_ALTITUDE = 7
ON_GROUND = 8
VELOCITY = 9
VERTICAL_RATE = 11
SENSORS = 12
GEO_ALTITUDE = 13
SQUAWK = 14

FIELD_NAMES = {
    ICAO24: "icao24",
    CALLSIGN: "callsign",
    TIME_POSITION: "time_position",
    LAST_CONTACT: "last_contact",
    LONGITUDE: "longitude",
    LATITUDE: "latitude",
    BARO_ALTITUDE: "baro_altitude",
    ON_GROUND: "on_ground",
    VELOCITY: "velocity",
    10: "true_track",
    VERTICAL_RATE: "vertical_rate",
    SENSORS: "sensors",
    GEO_ALTITUDE: "geo_altitude",
    SQUAWK: "squawk",
    15: "spi",
    16: "position_source",
    17: "category",
}


@dataclass
class Bucket:
    """Duplicate counts for one population of vectors."""

    total: int = 0
    duplicates: int = 0

    @property
    def rate(self) -> float:
        return self.duplicates / self.total * 100 if self.total else 0.0


@dataclass
class Stats:
    polls: int = 0
    rows: int = 0
    distinct: int = 0
    ground: Bucket = field(default_factory=Bucket)
    airborne: Bucket = field(default_factory=Bucket)
    staleness: list[int] = field(default_factory=list)
    null_counts: dict[int, int] = field(default_factory=dict)
    field_widths: set[int] = field(default_factory=set)
    interval_seconds: list[int] = field(default_factory=list)


def dedup_key(vector: list) -> tuple:
    """Identify a distinct observation.

    Consecutive polls return the same aircraft repeatedly. Only a changed
    time_position marks a genuinely new report; a bumped last_contact means the
    aircraft is still reachable, not that it has moved. Vectors with no position
    fix fall back to last_contact and are tagged so they cannot collide with a
    real fix recorded at the same instant.
    """
    if vector[TIME_POSITION] is not None:
        return (vector[ICAO24], vector[TIME_POSITION])
    return (vector[ICAO24], "no-fix", vector[LAST_CONTACT])


def percentile(values: list[int], p: float) -> int:
    if not values:
        return 0
    index = min(int(len(values) * p / 100), len(values) - 1)
    return values[index]


def collect(paths: list[Path]) -> Stats:
    stats = Stats()
    seen: set[tuple] = set()
    previous_time: int | None = None

    for path in paths:
        with path.open() as handle:
            payload = json.load(handle)

        vectors = payload.get("states") or []
        stats.polls += 1

        if previous_time is not None:
            stats.interval_seconds.append(payload["time"] - previous_time)
        previous_time = payload["time"]

        for vector in vectors:
            stats.rows += 1
            stats.field_widths.add(len(vector))

            for index in range(len(vector)):
                if vector[index] is None:
                    stats.null_counts[index] = stats.null_counts.get(index, 0) + 1

            key = dedup_key(vector)
            bucket = stats.ground if vector[ON_GROUND] else stats.airborne
            bucket.total += 1
            if key in seen:
                bucket.duplicates += 1
            else:
                seen.add(key)

            if vector[TIME_POSITION] is not None:
                stats.staleness.append(vector[LAST_CONTACT] - vector[TIME_POSITION])

    stats.distinct = len(seen)
    stats.staleness.sort()
    return stats


def report(stats: Stats) -> None:
    print(f"polls                 {stats.polls}")
    print(f"raw rows              {stats.rows}")
    print(f"distinct observations {stats.distinct}")

    if stats.interval_seconds:
        avg = sum(stats.interval_seconds) / len(stats.interval_seconds)
        print(f"poll interval         {avg:.0f}s (mean)")

    widths = ", ".join(str(w) for w in sorted(stats.field_widths))
    print(f"fields per vector     {widths}")

    if not stats.rows:
        print("\nno vectors found")
        return

    duplicates = stats.rows - stats.distinct
    print()
    print("DEDUPLICATION")
    print(f"  overall duplicate rate  {duplicates / stats.rows * 100:5.1f}%")
    print(f"  on ground               {stats.ground.rate:5.1f}%  "
          f"({stats.ground.duplicates}/{stats.ground.total})")
    print(f"  airborne                {stats.airborne.rate:5.1f}%  "
          f"({stats.airborne.duplicates}/{stats.airborne.total})")
    print("  Parked aircraft stop refreshing time_position while remaining")
    print("  contactable, so duplicates concentrate almost entirely on the ground.")

    print()
    print("POSITION STALENESS  (last_contact - time_position)")
    for p in (50, 90, 99):
        print(f"  p{p:<2}                    {percentile(stats.staleness, p)}s")
    print(f"  max                     {stats.staleness[-1] if stats.staleness else 0}s")
    stale = sum(1 for s in stats.staleness if s > 10)
    share = stale / len(stats.staleness) * 100 if stats.staleness else 0
    print(f"  share older than 10s    {share:.1f}%")
    print("  Timestamping observations with the response time rather than")
    print("  time_position would misplace the worst of these by minutes.")

    print()
    print("NULL RATES")
    for index in sorted(stats.null_counts):
        count = stats.null_counts[index]
        name = FIELD_NAMES.get(index, f"index {index}")
        marker = "  <- documented as non-nullable" if index == SENSORS else ""
        print(f"  {name:<16} {count / stats.rows * 100:5.1f}%{marker}")


def main() -> int:
    directory = Path(sys.argv[1] if len(sys.argv) > 1 else "fixtures")
    paths = sorted(directory.glob("states-*.json"))
    if not paths:
        print(f"no fixtures matched {directory}/states-*.json", file=sys.stderr)
        print("run `make record-fixtures` first", file=sys.stderr)
        return 1

    report(collect(paths))
    return 0


if __name__ == "__main__":
    sys.exit(main())
