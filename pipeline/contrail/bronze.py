"""Bronze writers: Parquet on object storage, and ClickHouse."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from contrail.config import Settings
from contrail.schema import BRONZE_SCHEMA, CLICKHOUSE_COLUMNS, to_arrow

log = structlog.get_logger(__name__)


class Writer(Protocol):
    """A destination for a batch of parsed records."""

    def write(self, records: list[dict[str, Any]]) -> int:
        """Persist the batch, returning the number of rows written."""

    def close(self) -> None: ...

    def describe(self) -> str: ...


class ParquetWriter:
    """Writes hour-partitioned Parquet to an S3-compatible store.

    Partitioning is by ``observed_at`` rather than by ingestion time. That is
    the whole point of keeping the two timestamps apart: a record whose position
    fix is an hour and a half stale (and measurements show that happening)
    belongs to the hour it was observed, not the hour it happened to arrive.
    Partitioning by arrival would scatter one hour of flight across several
    partitions and make any backfill non-reproducible.

    Each flush writes one new file with a unique name, so a re-run after a crash
    adds files rather than corrupting existing ones. Duplicate rows that result
    are removed by the deduplicating table in ClickHouse and by the silver
    models, which is why the writer itself can stay this simple.
    """

    def __init__(self, settings: Settings) -> None:
        import s3fs

        self._bucket = settings.s3_bucket
        self._fs = s3fs.S3FileSystem(
            key=settings.s3_access_key,
            secret=settings.s3_secret_key,
            endpoint_url=settings.s3_endpoint,
            # MinIO does not implement bucket-region lookups the way S3 does.
            client_kwargs={"region_name": "us-east-1"},
        )
        self._endpoint = settings.s3_endpoint

    def describe(self) -> str:
        return f"parquet s3://{self._bucket} via {self._endpoint}"

    def write(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0

        table = to_arrow(records)
        written = 0
        for partition, chunk in _split_by_hour(table):
            path = f"{self._bucket}/{partition}/{uuid.uuid4().hex}.parquet"
            with self._fs.open(path, "wb") as handle:
                pq.write_table(
                    chunk,
                    handle,
                    # zstd beats snappy substantially here: a batch is dominated
                    # by repeated country names, callsign prefixes and region
                    # labels, which dictionary-encode and then compress well.
                    compression="zstd",
                    compression_level=3,
                    use_dictionary=True,
                )
            written += chunk.num_rows
        return written

    def close(self) -> None:  # pragma: no cover - nothing buffered
        return None


def _split_by_hour(table: pa.Table) -> list[tuple[str, pa.Table]]:
    """Split a table into Hive-style hour partitions keyed on observed_at."""
    if table.num_rows == 0:
        return []

    observed = table.column("observed_at").to_pylist()
    buckets: dict[str, list[int]] = {}
    for index, value in enumerate(observed):
        moment = (
            value if isinstance(value, dt.datetime) else dt.datetime.fromtimestamp(value, tz=dt.UTC)
        )
        key = f"date={moment:%Y-%m-%d}/hour={moment:%H}"
        buckets.setdefault(key, []).append(index)

    return [(key, table.take(indices)) for key, indices in sorted(buckets.items())]


class ClickHouseWriter:
    """Inserts records into the bronze table."""

    def __init__(self, settings: Settings) -> None:
        import clickhouse_connect

        self._target = settings.clickhouse_target
        self._table = settings.clickhouse_table
        self._database = settings.clickhouse_database
        self._client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )

    def describe(self) -> str:
        return f"clickhouse {self._target}"

    def write(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0

        rows = [
            [_coerce_for_clickhouse(name, record[name]) for name in CLICKHOUSE_COLUMNS]
            for record in records
        ]
        self._client.insert(
            table=self._table,
            data=rows,
            column_names=CLICKHOUSE_COLUMNS,
            database=self._database,
        )
        return len(rows)

    def record_batch_metrics(
        self, *, consumed: int, written: int, invalid: int, records: list[dict[str, Any]]
    ) -> None:
        """Persist what this batch looked like before the warehouse deduplicated it.

        Once ReplacingMergeTree has merged, the duplicate rate is unrecoverable
        from the bronze table: count() and uniqExact agree by construction. The
        only place the number is observable is here, at the moment of insert.
        """
        distinct = len({(r["icao24"], r["observed_at"]) for r in records})
        try:
            self._client.insert(
                table="ingest_batches",
                data=[
                    [
                        dt.datetime.now(tz=dt.UTC),
                        consumed,
                        written,
                        invalid,
                        distinct,
                    ]
                ],
                column_names=[
                    "batch_at",
                    "messages_consumed",
                    "rows_written",
                    "invalid_messages",
                    "distinct_observations",
                ],
                database=self._database,
            )
        except Exception as exc:
            # Metrics must never take ingestion down with them. Losing a
            # measurement is an inconvenience; losing observations is not.
            log.warning("recording batch metrics", error=str(exc))

    def close(self) -> None:
        self._client.close()


def _coerce_for_clickhouse(column: str, value: Any) -> Any:
    """Convert epoch seconds to datetimes for DateTime columns.

    The schema keeps timestamps as integers through parsing so validation can
    range-check them; ClickHouse wants datetime objects.
    """
    if value is None:
        return None
    field = BRONZE_SCHEMA.field(column)
    if pa.types.is_timestamp(field.type) and isinstance(value, int):
        return dt.datetime.fromtimestamp(value, tz=dt.UTC)
    return value


class MultiWriter:
    """Fans a batch out to several writers.

    Bronze is written to object storage and to ClickHouse for different
    reasons: Parquet is the durable, reprocessable record of what arrived, and
    ClickHouse is the queryable copy. If the warehouse is lost or a schema
    changes, the Parquet is what the silver layer is rebuilt from.

    A failure in one writer is not allowed to hide a success in another, so all
    writers are attempted and errors are collected rather than short-circuited.
    """

    def __init__(self, writers: list[Writer]) -> None:
        self._writers = writers

    def describe(self) -> str:
        return " + ".join(w.describe() for w in self._writers)

    def write(self, records: list[dict[str, Any]]) -> int:
        written = 0
        failures: list[tuple[str, Exception]] = []
        for writer in self._writers:
            try:
                written = max(written, writer.write(records))
            except Exception as exc:
                failures.append((writer.describe(), exc))
                log.error("writer failed", writer=writer.describe(), error=str(exc))
        if failures and len(failures) == len(self._writers):
            # Every destination failed, so nothing was persisted. Raising lets
            # the consumer withhold the offset commit and retry the batch.
            raise RuntimeError(
                "all bronze writers failed: "
                + "; ".join(f"{name}: {exc}" for name, exc in failures)
            )
        return written

    def record_batch_metrics(
        self, *, consumed: int, written: int, invalid: int, records: list[dict[str, Any]]
    ) -> None:
        """Forward batch metrics to whichever writer can persist them."""
        for writer in self._writers:
            recorder = getattr(writer, "record_batch_metrics", None)
            if recorder is not None:
                recorder(consumed=consumed, written=written, invalid=invalid, records=records)

    def close(self) -> None:
        for writer in self._writers:
            try:
                writer.close()
            except Exception as exc:
                log.warning("closing writer", writer=writer.describe(), error=str(exc))


def build_writer(settings: Settings) -> MultiWriter:
    """Assemble the configured bronze writers."""
    writers: list[Writer] = []
    if settings.write_parquet:
        writers.append(ParquetWriter(settings))
    if settings.write_clickhouse:
        writers.append(ClickHouseWriter(settings))
    if not writers:
        raise ValueError(
            "no bronze destination enabled; set CONTRAIL_WRITE_PARQUET or CONTRAIL_WRITE_CLICKHOUSE"
        )
    return MultiWriter(writers)
