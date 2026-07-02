"""Sink configuration, resolved from the environment."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings for the bronze sink.

    Defaults target the local Compose stack, so the sink runs with no
    environment set at all once `docker compose up` is running.
    """

    model_config = SettingsConfigDict(
        env_prefix="CONTRAIL_",
        env_file=".env",
        extra="ignore",
    )

    # --- Kafka -------------------------------------------------------------
    kafka_brokers: str = "localhost:19092"
    kafka_topic: str = "opensky.states.raw"
    kafka_group_id: str = "contrail-bronze-sink"
    # Start from the beginning so a fresh stack replays everything the ingester
    # has already produced rather than silently skipping it.
    kafka_auto_offset_reset: str = "earliest"

    # --- Batching ----------------------------------------------------------
    # A poll yields a few hundred to a few thousand vectors. Batching well above
    # that keeps Parquet row groups a sensible size and amortises the ClickHouse
    # round trip, rather than writing one tiny file per poll.
    batch_size: int = Field(default=5000, ge=1)
    # Bound on how long a partial batch waits before being flushed, so a quiet
    # period cannot leave records stranded in memory indefinitely.
    batch_timeout_seconds: float = Field(default=30.0, gt=0)

    # --- Object storage (bronze) -------------------------------------------
    s3_endpoint: str = "http://localhost:9002"
    s3_access_key: str = "contrail"
    s3_secret_key: str = "contrail123"
    s3_bucket: str = "contrail-bronze"
    write_parquet: bool = True

    # --- ClickHouse --------------------------------------------------------
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "contrail"
    clickhouse_password: str = "contrail"
    clickhouse_database: str = "contrail"
    clickhouse_table: str = "bronze_state_vectors"
    write_clickhouse: bool = True

    # --- Behaviour ---------------------------------------------------------
    log_level: str = "INFO"
    # Cap on malformed messages tolerated before the sink stops. A trickle of
    # bad records is normal and gets counted; a flood means the producer's
    # schema has changed and continuing would quietly fill the warehouse with
    # nothing.
    max_invalid_ratio: float = Field(default=0.05, ge=0, le=1)

    @property
    def broker_list(self) -> list[str]:
        return [b.strip() for b in self.kafka_brokers.split(",") if b.strip()]

    @property
    def clickhouse_target(self) -> str:
        return f"{self.clickhouse_database}.{self.clickhouse_table}"
