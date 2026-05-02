from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class EngineSettings(BaseSettings):
    nats_servers: list[str] = ["nats://localhost:4225"]
    nats_connect_timeout: float = 2.0
    nats_request_timeout: float = 5.0
    nats_max_reconnect_attempts: int = 10
    nats_reconnect_time_wait: float = 2.0
    nats_worker_ack_wait: float = 5.0

    model_config = SettingsConfigDict(
        env_prefix="ENGINE_",
    )


@lru_cache
def get_settings() -> EngineSettings:
    """Get engine settings from environment variables."""
    return EngineSettings()
