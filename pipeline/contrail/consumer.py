"""Kafka consumer that batches state vectors into the bronze layer."""

from __future__ import annotations

import json
import signal
import time
from dataclasses import dataclass, field
from types import FrameType
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException

from contrail.bronze import MultiWriter
from contrail.config import Settings
from contrail.schema import SchemaError, parse_record

log = structlog.get_logger(__name__)


@dataclass
class Metrics:
    """Counters for one consumer run."""

    messages: int = 0
    written: int = 0
    invalid: int = 0
    batches: int = 0
    invalid_samples: list[str] = field(default_factory=list)

    @property
    def invalid_ratio(self) -> float:
        return self.invalid / self.messages if self.messages else 0.0

    def record_invalid(self, reason: str) -> None:
        self.invalid += 1
        # Keep a handful of examples: an operator debugging a schema break needs
        # to see what actually arrived, but retaining every bad payload in a
        # flood would be its own memory problem.
        if len(self.invalid_samples) < 5:
            self.invalid_samples.append(reason)


class BronzeSink:
    """Consumes state vectors and writes them to the bronze layer.

    Offsets are committed only after a batch has been durably written. That
    ordering makes the sink at-least-once: a crash between write and commit
    replays the batch and produces duplicate rows. That is the deliberate
    choice. The alternative, committing first, loses data on the same crash,
    and duplicates are already handled by the ReplacingMergeTree sort key and
    by the silver models. Losing observations is unrecoverable; duplicating
    them is not.
    """

    def __init__(self, settings: Settings, writer: MultiWriter) -> None:
        self._settings = settings
        self._writer = writer
        self._metrics = Metrics()
        self._running = True

        self._consumer = Consumer(
            {
                "bootstrap.servers": ",".join(settings.broker_list),
                "group.id": settings.kafka_group_id,
                "auto.offset.reset": settings.kafka_auto_offset_reset,
                # Manual commits are what make the write-then-commit ordering
                # above possible.
                "enable.auto.commit": False,
                "session.timeout.ms": 45_000,
                "max.poll.interval.ms": 300_000,
            }
        )

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    def stop(self, *_: object) -> None:
        """Request a graceful shutdown."""
        self._running = False

    def install_signal_handlers(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_signal)

    def _handle_signal(self, signum: int, _frame: FrameType | None) -> None:
        log.info("shutdown requested", signal=signal.Signals(signum).name)
        self.stop()

    def run(self, max_batches: int | None = None) -> Metrics:
        """Consume until stopped, or until max_batches have been written."""
        self._consumer.subscribe([self._settings.kafka_topic])
        log.info(
            "bronze sink started",
            topic=self._settings.kafka_topic,
            group=self._settings.kafka_group_id,
            destination=self._writer.describe(),
        )

        batch: list[dict[str, Any]] = []
        deadline = time.monotonic() + self._settings.batch_timeout_seconds

        try:
            while self._running:
                message = self._consumer.poll(timeout=1.0)
                now = time.monotonic()

                if message is None:
                    # A quiet topic must still flush what is already buffered,
                    # or the last partial batch waits for traffic that may not
                    # come for hours.
                    if batch and now >= deadline:
                        self._flush(batch)
                        batch = []
                        deadline = now + self._settings.batch_timeout_seconds
                        if self._reached_limit(max_batches):
                            break
                    continue

                error = message.error()
                if error is not None:
                    self._handle_error(error)
                    continue

                self._metrics.messages += 1
                parsed = self._parse(message.value())
                if parsed is not None:
                    batch.append(parsed)

                if len(batch) >= self._settings.batch_size or now >= deadline:
                    self._flush(batch)
                    batch = []
                    deadline = now + self._settings.batch_timeout_seconds
                    if self._reached_limit(max_batches):
                        break

                self._guard_invalid_ratio()

            if batch:
                self._flush(batch)
        finally:
            self._consumer.close()
            self._writer.close()
            log.info(
                "bronze sink stopped",
                messages=self._metrics.messages,
                written=self._metrics.written,
                invalid=self._metrics.invalid,
                batches=self._metrics.batches,
            )
        return self._metrics

    def _reached_limit(self, max_batches: int | None) -> bool:
        return max_batches is not None and self._metrics.batches >= max_batches

    def _parse(self, raw: bytes | None) -> dict[str, Any] | None:
        if raw is None:
            self._metrics.record_invalid("empty message value")
            return None
        try:
            return parse_record(json.loads(raw))
        except (json.JSONDecodeError, SchemaError) as exc:
            # One malformed record must not cost the several hundred valid ones
            # alongside it in the same poll, so this is counted and skipped.
            self._metrics.record_invalid(str(exc))
            return None

    def _flush(self, batch: list[dict[str, Any]]) -> None:
        if not batch:
            return
        written = self._writer.write(batch)
        self._metrics.written += written
        self._metrics.batches += 1
        # Recorded before the warehouse merges, which is the only moment the
        # in-batch duplicate rate is still observable.
        self._writer.record_batch_metrics(
            consumed=len(batch),
            written=written,
            invalid=self._metrics.invalid,
            records=batch,
        )
        # Committing only after a successful write is what makes the sink
        # at-least-once rather than at-most-once.
        self._consumer.commit(asynchronous=False)
        log.info("batch written", rows=written, total=self._metrics.written)

    def _handle_error(self, error: KafkaError) -> None:
        if error.code() == KafkaError._PARTITION_EOF:
            return
        # Fatal errors will not resolve by retrying; anything else is treated as
        # transient so a broker restart does not take the sink down with it.
        if error.fatal():
            raise KafkaException(error)
        log.warning("kafka error", error=str(error))

    def _guard_invalid_ratio(self) -> None:
        """Stop if malformed messages stop being an exception.

        A steady trickle of bad records is normal. A sustained high rate means
        the producer's schema has moved and the sink is quietly discarding real
        data. Better to fail loudly than to keep an empty warehouse looking
        healthy. The sample threshold avoids tripping on the first few messages.
        """
        if self._metrics.messages < 100:
            return
        if self._metrics.invalid_ratio > self._settings.max_invalid_ratio:
            raise RuntimeError(
                f"invalid message ratio {self._metrics.invalid_ratio:.1%} exceeds "
                f"the {self._settings.max_invalid_ratio:.1%} threshold; "
                f"examples: {self._metrics.invalid_samples}"
            )
