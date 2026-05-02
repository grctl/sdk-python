from dataclasses import dataclass
from pathlib import Path

import yaml
from ulid import ULID

from grctl.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class StreamConfig:
    name: str
    type: str
    consumers: dict[str, dict[str, str]] | None = None
    consumer: dict[str, dict[str, str]] | None = None


@dataclass
class SubjectConfig:
    stream: str | None
    subject_pattern: str | None = None
    listener_pattern: str | None = None
    subject_patterns: dict[str, str] | None = None


@dataclass
class ManifestConfig:
    version: int
    streams: dict[str, StreamConfig]
    subjects: dict[str, SubjectConfig]


class NatsManifest:
    """Centralized NATS configuration from nats_manifest.yaml.

    Provides type-safe access to stream names, subject patterns,
    bucket names, and key patterns.
    """

    def __init__(self, config: ManifestConfig) -> None:
        self._config = config

    @classmethod
    def load(cls, yaml_path: str | Path | None = None) -> "NatsManifest":
        yaml_path = Path(__file__).parent / "nats_manifest.yaml" if yaml_path is None else Path(yaml_path)

        if not yaml_path.exists():
            raise FileNotFoundError(f"NATS manifest not found: {yaml_path}")

        try:
            with yaml_path.open() as f:
                raw_config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.exception("Failed to parse NATS manifest YAML")
            raise RuntimeError(f"Invalid YAML in {yaml_path}: {e}") from e

        config = cls._parse_config(raw_config)
        logger.debug(f"Loaded NATS manifest from {yaml_path}")
        return cls(config)

    @staticmethod
    def _parse_config(raw: dict) -> ManifestConfig:
        streams = {name: StreamConfig(**cfg) for name, cfg in raw.get("streams", {}).items()}
        subjects = {name: SubjectConfig(**cfg) for name, cfg in raw.get("subjects", {}).items()}
        return ManifestConfig(
            version=raw["version"],
            streams=streams,
            subjects=subjects,
        )

    def _stream_consumers(self, stream_key: str) -> dict[str, dict[str, str]]:
        stream = self._config.streams.get(stream_key)
        if stream is None:
            return {}
        if stream.consumers:
            return stream.consumers
        return stream.consumer or {}

    def _subject_pattern(self, subject_key: str, pattern_key: str) -> str:
        subject = self._config.subjects.get(subject_key)
        if subject is None:
            return ""
        if subject.subject_patterns:
            return subject.subject_patterns.get(pattern_key, "")
        if pattern_key == "publish":
            return subject.subject_pattern or ""
        if pattern_key == "listen":
            return subject.listener_pattern or ""
        return ""

    def _subject_stream(self, subject_key: str) -> str:
        subject = self._config.subjects.get(subject_key)
        if subject is None or subject.stream is None:
            return ""
        return subject.stream

    @staticmethod
    def _substitute_params(pattern: str, **params: str | ULID) -> str:
        result = pattern
        for key, value in params.items():
            result = result.replace(f"{{{key}}}", str(value))
        return result

    def directive_stream_name(self) -> str:
        return self._config.streams["directive"].name

    def history_stream_name(self) -> str:
        return self._subject_stream("history")

    def timers_stream_name(self) -> str:
        return self._subject_stream("timer")

    def state_stream_name(self) -> str:
        return self._config.streams["state"].name

    def timer_fired_consumer_name(self) -> str:
        consumers = self._stream_consumers("state")
        if not consumers:
            raise ValueError("State stream has no consumers configured")
        return consumers["grctl_timer_fired"]["name"]

    def directive_subject(self, wf_type: str, wf_id: str | ULID, run_id: str | ULID) -> str:
        pattern = self._subject_pattern("directive", "publish")
        return self._substitute_params(pattern, wf_type=wf_type, wf_id=wf_id, run_id=run_id)

    def directive_listener_pattern(self) -> str:
        return self._subject_pattern("directive", "listen")

    def history_subject(self, wf_id: str | ULID, run_id: str | ULID) -> str:
        pattern = self._subject_pattern("history", "publish")
        return self._substitute_params(pattern, wf_id=wf_id, run_id=run_id)

    def history_listener_pattern(self) -> str:
        return self._subject_pattern("history", "listen")

    def api_subject(self, wf_id: str | ULID) -> str:
        pattern = self._subject_pattern("api", "publish")
        return self._substitute_params(pattern, wf_id=wf_id)

    def api_listener_pattern(self) -> str:
        return self._subject_pattern("api", "listen")

    def worker_task_filter_subject(self, wf_type: str) -> str:
        pattern = self._subject_pattern("worker_task", "filter")
        return self._substitute_params(pattern, wf_type=wf_type)

    def worker_task_queue_group(self, wf_type: str) -> str:
        pattern = self._subject_pattern("worker_task", "queue_group")
        return self._substitute_params(pattern, wf_type=wf_type)

    def run_state_subject(self, wf_id: str | ULID, run_id: str | ULID) -> str:
        pattern = self._subject_pattern("run_state", "publish")
        return self._substitute_params(pattern, wf_id=wf_id, run_id=run_id)

    def cancel_subject(self, wf_id: str | ULID) -> str:
        pattern = self._subject_pattern("cancel", "publish")
        return self._substitute_params(pattern, wf_id=wf_id)

    def inflight_subject(self, worker_id: str, wf_id: str | ULID) -> str:
        pattern = self._subject_pattern("inflight", "publish")
        return self._substitute_params(pattern, worker_id=worker_id, wf_id=wf_id)

    def timer_subject(self, wf_id: str | ULID, kind: str) -> str:
        pattern = self._subject_pattern("timer", "publish")
        return self._substitute_params(pattern, wf_id=wf_id, kind=kind)

    def timer_listener_pattern(self) -> str:
        return self._subject_pattern("timer", "listen")

    def timer_fired_subject(self) -> str:
        return self._subject_pattern("timer_fired", "publish")

    def timer_fired_listener_pattern(self) -> str:
        return self._subject_pattern("timer_fired", "listen")

    def run_info_key(self, wf_type: str, wf_id: str, run_id: str) -> str:
        pattern = self._subject_pattern("run_store", "info")
        return self._substitute_params(pattern, wf_type=wf_type, wf_id=wf_id, run_id=run_id)

    def list_run_info_by_wf_id_pattern(self, wf_type: str, wf_id: str) -> str:
        pattern = self._subject_pattern("run_store", "info")
        return self._substitute_params(pattern, wf_type=wf_type, wf_id=wf_id, run_id="*")

    def list_run_info_by_run_id_pattern(self, run_id: str) -> str:
        pattern = self._subject_pattern("run_store", "info")
        return self._substitute_params(pattern, wf_type="*", wf_id="*", run_id=run_id)

    def run_input_key(self, wf_id: str, run_id: str) -> str:
        pattern = self._subject_pattern("run_store", "input")
        return self._substitute_params(pattern, wf_id=wf_id, run_id=run_id)

    def run_output_key(self, wf_id: str, run_id: str) -> str:
        pattern = self._subject_pattern("run_store", "output")
        return self._substitute_params(pattern, wf_id=wf_id, run_id=run_id)

    def wf_kv_store_bucket_name(self) -> str:
        return self._subject_stream("kv_store")

    def wf_key_prefix(self, wf_id: str) -> str:
        pattern = self._subject_pattern("kv_store", "kv")
        prefix = self._substitute_params(pattern, wf_id=wf_id, run_id="{run_id}", key="{key}")
        return prefix.removesuffix(".{run_id}.{key}")

    def wf_run_key_prefix(self, wf_id: str, run_id: str) -> str:
        pattern = self._subject_pattern("kv_store", "kv")
        prefix = self._substitute_params(pattern, wf_id=wf_id, run_id=run_id, key="{key}")
        return prefix.removesuffix(".{key}")

    def wf_kv_key(self, wf_id: str, run_id: str, key: str) -> str:
        pattern = self._subject_pattern("kv_store", "kv")
        return self._substitute_params(pattern, wf_id=wf_id, run_id=run_id, key=key)

    def all_runs_info_key_pattern(self) -> str:
        pattern = self._subject_pattern("run_store", "info")
        pattern = pattern.replace("{wf_type}", "*")
        pattern = pattern.replace("{wf_id}", "*")
        return pattern.replace("{run_id}", "*")

    def all_runs_input_key_pattern(self) -> str:
        pattern = self._subject_pattern("run_store", "input")
        pattern = pattern.replace("{wf_id}", "*")
        return pattern.replace("{run_id}", "*")

    def all_runs_output_key_pattern(self) -> str:
        pattern = self._subject_pattern("run_store", "output")
        pattern = pattern.replace("{wf_id}", "*")
        return pattern.replace("{run_id}", "*")

    def all_wf_kv_key_pattern(self) -> str:
        pattern = self._subject_pattern("kv_store", "kv")
        pattern = pattern.replace("{wf_id}", "*")
        pattern = pattern.replace("{run_id}", "*")
        return pattern.replace("{key}", "*")
