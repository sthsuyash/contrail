"""Entry point for the bronze sink."""

from __future__ import annotations

import argparse
import logging
import sys

import structlog

from contrail.bronze import build_writer
from contrail.config import Settings
from contrail.consumer import BronzeSink


def configure_logging(level: str) -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=level.upper())
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[level.upper()]
        ),
        cache_logger_on_first_use=True,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="contrail-sink",
        description="Consume state vectors from Kafka into the bronze layer.",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="stop after this many batches (used by tests and smoke runs)",
    )
    parser.add_argument("--no-parquet", action="store_true", help="skip the object-storage writer")
    parser.add_argument("--no-clickhouse", action="store_true", help="skip the warehouse writer")
    args = parser.parse_args(argv)

    settings = Settings()
    if args.no_parquet:
        settings.write_parquet = False
    if args.no_clickhouse:
        settings.write_clickhouse = False

    configure_logging(settings.log_level)

    sink = BronzeSink(settings, build_writer(settings))
    sink.install_signal_handlers()
    metrics = sink.run(max_batches=args.max_batches)

    # A run that consumed nothing but was asked to do bounded work has not
    # succeeded. It usually means the topic name is wrong or the ingester
    # never produced. Exiting non-zero makes that visible to CI.
    if args.max_batches is not None and metrics.written == 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
