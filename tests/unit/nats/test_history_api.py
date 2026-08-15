from types import SimpleNamespace
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock

import pytest

if TYPE_CHECKING:
    from nats.jetstream import JetStream

from grctl.models import HistoryReadError
from grctl.nats import history_api
from grctl.nats.codec import MsgspecCodec
from grctl.nats.history_api import NatsHistoryAPI
from grctl.settings import get_settings


class FakeMessageBatch:
    def __init__(self, messages: list[object]) -> None:
        self._messages = messages
        self.error: Exception | None = None

    async def __aiter__(self):
        for message in self._messages:
            yield message


class FakeConsumer:
    def __init__(self, responses: list[list[object] | BaseException]) -> None:
        self.name = "history-reader"
        self._responses = iter(responses)
        self.fetch_calls: list[dict[str, object]] = []

    async def fetch(self, **_kwargs: object) -> FakeMessageBatch:
        self.fetch_calls.append(_kwargs)
        response = next(self._responses)
        if isinstance(response, BaseException):
            raise response
        return FakeMessageBatch(response)  # type: ignore[arg-type]


class FakeJetStream:
    def __init__(self, last_sequence: int | BaseException, consumer: FakeConsumer | None = None) -> None:
        self._last_sequence = last_sequence
        self._consumer = consumer
        self.create_consumer = AsyncMock(return_value=consumer)
        self.delete_consumer = AsyncMock()

    async def get_last_message_for_subject(self, _stream: str, _subject: str) -> object:
        if isinstance(self._last_sequence, BaseException):
            raise self._last_sequence
        return SimpleNamespace(sequence=self._last_sequence)


def history_message(sequence: int, pending: int, data: bytes = b"event") -> object:
    return SimpleNamespace(
        data=data,
        metadata=SimpleNamespace(
            sequence=SimpleNamespace(stream=sequence),
            num_pending=pending,
        ),
    )


def api_for(jetstream: FakeJetStream) -> NatsHistoryAPI:
    return NatsHistoryAPI(cast("JetStream", jetstream), codec=cast("MsgspecCodec", None))


async def test_get_run_history_returns_empty_without_creating_a_consumer() -> None:
    jetstream = FakeJetStream(history_api.MessageNotFoundError("no history", error_code=10037))

    assert await api_for(jetstream).get_run_history("workflow", "run") == []

    jetstream.create_consumer.assert_not_awaited()
    jetstream.delete_consumer.assert_not_awaited()


async def test_get_run_history_retries_silence_until_target_sequence(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = FakeConsumer(
        [
            TimeoutError(),
            [history_message(4, pending=1), history_message(5, pending=0)],
        ]
    )
    jetstream = FakeJetStream(5, consumer)
    decoded = [object(), object()]
    monkeypatch.setattr(history_api, "history_decoder", lambda _data: decoded.pop(0))

    events = await api_for(jetstream).get_run_history("workflow", "run")

    assert len(events) == 2
    create_args = jetstream.create_consumer.await_args
    assert create_args is not None
    assert create_args.kwargs["name"].startswith("history-run-")
    assert create_args.kwargs["filter_subject"].endswith(".workflow.run")
    assert create_args.kwargs["ack_policy"] == "none"
    assert create_args.kwargs["inactive_threshold"].total_seconds() == 1
    jetstream.delete_consumer.assert_awaited_once()


async def test_history_api_uses_environment_configured_read_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENGINE_NATS_HISTORY_FETCH_BATCH_SIZE", "10")
    monkeypatch.setenv("ENGINE_NATS_HISTORY_FETCH_TIMEOUT_SECONDS", "0.1")
    monkeypatch.setenv("ENGINE_NATS_HISTORY_READ_TIMEOUT_SECONDS", "2.0")
    monkeypatch.setenv("ENGINE_NATS_HISTORY_CONSUMER_INACTIVE_THRESHOLD_SECONDS", "4.0")
    get_settings.cache_clear()

    consumer = FakeConsumer([[history_message(5, pending=0)]])
    jetstream = FakeJetStream(5, consumer)
    monkeypatch.setattr(history_api, "history_decoder", lambda _data: object())
    try:
        assert len(await api_for(jetstream).get_run_history("workflow", "run")) == 1
    finally:
        get_settings.cache_clear()

    assert consumer.fetch_calls == [{"max_messages": 10, "max_wait": 0.1}]
    create_args = jetstream.create_consumer.await_args
    assert create_args is not None
    assert create_args.kwargs["inactive_threshold"].total_seconds() == 4.0


async def test_history_read_raises_when_server_reports_history_ended_before_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer = FakeConsumer([[history_message(4, pending=0)]])
    jetstream = FakeJetStream(5, consumer)
    monkeypatch.setattr(history_api, "history_decoder", lambda _data: object())

    with pytest.raises(HistoryReadError, match="before target sequence 5"):
        await api_for(jetstream).get_run_history("workflow", "run")

    jetstream.delete_consumer.assert_awaited_once()


async def test_history_read_identifies_the_stream_sequence_of_a_decode_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer = FakeConsumer([[history_message(5, pending=0)]])
    jetstream = FakeJetStream(5, consumer)

    def fail_decode(_data: bytes) -> object:
        raise ValueError("corrupt event")

    monkeypatch.setattr(history_api, "history_decoder", fail_decode)

    with pytest.raises(HistoryReadError, match="stream sequence 5"):
        await api_for(jetstream).get_run_history("workflow", "run")

    jetstream.delete_consumer.assert_awaited_once()


async def test_step_history_keeps_observability_events_in_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = FakeConsumer([[history_message(4, pending=1), history_message(5, pending=0)]])
    jetstream = FakeJetStream(5, consumer)
    observability_event = SimpleNamespace(operation_id="")
    replay_event = SimpleNamespace(operation_id="now:1")
    decoded = [observability_event, replay_event]
    monkeypatch.setattr(history_api, "history_decoder", lambda _data: decoded.pop(0))

    events = await api_for(jetstream).fetch_step_history("workflow", "run", history_seq_id=4)

    assert events == [observability_event, replay_event]
    create_args = jetstream.create_consumer.await_args
    assert create_args is not None
    assert create_args.kwargs["deliver_policy"] == "by_start_sequence"
    assert create_args.kwargs["opt_start_seq"] == 4
