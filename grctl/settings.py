from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class EngineSettings(BaseSettings):
    nats_servers: list[str] = ["nats://localhost:4225"]
    nats_connect_timeout: float = 2.0
    nats_request_timeout: float = 5.0
    nats_max_reconnect_attempts: int = -1
    nats_reconnect_time_wait: float = 2.0
    nats_fetch_max_wait: float = 5.0

    # Maximum history events requested from JetStream in a single pull.
    # Override with ENGINE_NATS_HISTORY_FETCH_BATCH_SIZE.
    nats_history_fetch_batch_size: int = Field(default=256, ge=1)

    # Maximum time (in seconds) to wait for one history pull before checking
    # the overall read deadline. Override with ENGINE_NATS_HISTORY_FETCH_TIMEOUT_SECONDS.
    nats_history_fetch_timeout_seconds: float = Field(default=0.25, gt=0)

    # Deadline (in seconds) for reading the stable history prefix of a run.
    # Override with ENGINE_NATS_HISTORY_READ_TIMEOUT_SECONDS.
    nats_history_read_timeout_seconds: float = Field(default=10.0, gt=0)

    # How long an idle, temporary history consumer remains before JetStream removes it.
    # Override with ENGINE_NATS_HISTORY_CONSUMER_INACTIVE_THRESHOLD_SECONDS.
    nats_history_consumer_inactive_threshold_seconds: float = Field(default=1.0, gt=0)

    # Number of attempts to register a worker after a transport failure.
    # Override with ENGINE_NATS_WORKER_REGISTRATION_MAX_ATTEMPTS.
    nats_worker_registration_max_attempts: int = Field(default=5, ge=1)

    # Base retry delay (in seconds) for worker registration. Each retry increases
    # this delay linearly. Override with ENGINE_NATS_WORKER_REGISTRATION_RETRY_BASE_DELAY_SECONDS.
    nats_worker_registration_retry_base_delay_seconds: float = Field(default=0.5, ge=0)

    # Redelivery deadline (in seconds) for NATS JetStream messages. If no ACK, NAK,
    # or in_progress heartbeat is received within this window (e.g. worker process crash or hang),
    # NATS redelivers the message to another available worker.
    nats_worker_ack_wait: float = 8.0

    # Interval (in seconds) at which an active worker sends in_progress() progress heartbeats to NATS.
    # Keeps ack_wait from expiring for long-running steps while worker is alive and responsive.
    progress_ack_interval_seconds: float = 3.0

    # Defaults used by a task RetryPolicy when its corresponding delay field is omitted.
    # Override with ENGINE_TASK_RETRY_INITIAL_DELAY_MS.
    task_retry_initial_delay_ms: int = 100

    # Override with ENGINE_TASK_RETRY_BACKOFF_MULTIPLIER.
    task_retry_backoff_multiplier: float = 2.0

    # Override with ENGINE_TASK_RETRY_MAX_DELAY_MS.
    task_retry_max_delay_ms: int = 5000

    model_config = SettingsConfigDict(
        env_prefix="ENGINE_",
    )


@lru_cache
def get_settings() -> EngineSettings:
    """Get engine settings from environment variables."""
    return EngineSettings()
