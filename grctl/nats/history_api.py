import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Never, cast

from nats.jetstream import JetStream
from nats.jetstream.errors import MessageNotFoundError
from nats.jetstream.message import Message
from ulid import ULID

from grctl.models import (
    HistoryEvent,
    HistoryReadError,
    history_decoder,
    history_encoder,
)
from grctl.nats.codec import MsgspecCodec
from grctl.nats.manifest import manifest
from grctl.settings import EngineSettings, get_settings

if TYPE_CHECKING:
    from nats.jetstream.consumer.pull import PullConsumer

logger = logging.getLogger(__name__)


@dataclass
class HistoryReadProgress:
    """Tracks a bounded read of one stable prefix of a run's history."""

    run_id: str
    target_sequence: int
    started_at: float
    fetch_timeout_seconds: float
    read_timeout_seconds: float
    events: list[HistoryEvent] = field(default_factory=list)
    reached_sequence: int | None = None

    def fetch_timeout(self) -> float:
        remaining = self.read_timeout_seconds - self.elapsed
        if remaining <= 0:
            self.raise_incomplete("timed out")
        return min(self.fetch_timeout_seconds, remaining)

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.started_at

    def raise_incomplete(self, reason: str) -> Never:
        logger.error(
            "History read incomplete events=%d target_sequence=%d reached_sequence=%s elapsed=%.3fs reason=%s run=%s",
            len(self.events),
            self.target_sequence,
            self.reached_sequence,
            self.elapsed,
            reason,
            self.run_id,
        )
        raise HistoryReadError(
            f"history read {reason} before target sequence {self.target_sequence} for run {self.run_id}; "
            f"reached sequence {self.reached_sequence}"
        )


class NatsHistoryAPI:
    """NATS impl of the history reader and writer ports.

    Reader side (get_run_history / fetch_step_history) pull-subscribes over the
    run's history subject; writer side (append) publishes onto it. The domain
    depends only on the narrow reader/writer protocols this satisfies.
    """

    def __init__(self, jetstream: JetStream, codec: MsgspecCodec) -> None:
        self.jetstream = jetstream
        self.codec = codec

    async def get_run_history(self, wf_id: str, run_id: str) -> list[HistoryEvent]:
        return await self.read(wf_id, run_id)

    async def fetch_step_history(
        self,
        wf_id: str,
        run_id: str,
        history_seq_id: int,
    ) -> list[HistoryEvent]:
        if history_seq_id <= 0:
            return []

        return await self.read(wf_id, run_id, history_seq_id)

    async def read(self, wf_id: str, run_id: str, start_sequence: int | None = None) -> list[HistoryEvent]:
        """Read a complete, fixed history prefix or fail without returning it."""
        settings = get_settings()
        history_subject = manifest.history_subject(wf_id=wf_id, run_id=run_id)
        history_stream = manifest.history_stream_name()
        target_sequence = await self.target_sequence(history_stream, history_subject)
        if target_sequence is None:
            return []

        consumer = await self.new_consumer(history_stream, history_subject, run_id, start_sequence, settings)
        try:
            return await self.collect(consumer, target_sequence, run_id, settings)
        finally:
            await self.jetstream.delete_consumer(history_stream, consumer.name)

    async def target_sequence(self, stream: str, subject: str) -> int | None:
        try:
            last_message = await self.jetstream.get_last_message_for_subject(stream, subject)
        except MessageNotFoundError:
            return None
        return last_message.sequence

    async def new_consumer(
        self,
        stream: str,
        subject: str,
        run_id: str,
        start_sequence: int | None,
        settings: EngineSettings,
    ) -> "PullConsumer":
        return cast(
            "PullConsumer",
            await self.jetstream.create_consumer(
                stream,
                name=f"history-{run_id}-{ULID()}",
                filter_subject=subject,
                deliver_policy="all" if start_sequence is None else "by_start_sequence",
                opt_start_seq=start_sequence,
                ack_policy="none",
                inactive_threshold=timedelta(seconds=settings.nats_history_consumer_inactive_threshold_seconds),
            ),
        )

    async def collect(
        self, consumer: "PullConsumer", target_sequence: int, run_id: str, settings: EngineSettings
    ) -> list[HistoryEvent]:
        progress = HistoryReadProgress(
            run_id,
            target_sequence,
            time.monotonic(),
            settings.nats_history_fetch_timeout_seconds,
            settings.nats_history_read_timeout_seconds,
        )
        while True:
            max_wait = progress.fetch_timeout()
            messages = await self.fetch(consumer, settings.nats_history_fetch_batch_size, max_wait, run_id)
            if messages is None:
                continue

            batch, progress.reached_sequence, pending = self.decode_batch(messages, run_id)
            progress.events.extend(batch)
            if progress.reached_sequence >= progress.target_sequence:
                logger.info(
                    "History read completed events=%d target_sequence=%d reached_sequence=%d elapsed=%.3fs",
                    len(progress.events),
                    progress.target_sequence,
                    progress.reached_sequence,
                    progress.elapsed,
                )
                return progress.events
            if pending == 0:
                progress.raise_incomplete("server reported no pending messages")

    @staticmethod
    async def fetch(consumer: "PullConsumer", max_messages: int, max_wait: float, run_id: str) -> list[Message] | None:
        try:
            message_batch = await consumer.fetch(max_messages=max_messages, max_wait=max_wait)
            messages = [message async for message in message_batch]
        except Exception:
            logger.warning("History read fetch failed; retrying run=%s", run_id, exc_info=True)
            return None
        if message_batch.error is not None:
            logger.warning("History read fetch failed; retrying run=%s error=%s", run_id, message_batch.error)
            return None
        return messages

    @staticmethod
    def decode_batch(messages: Sequence[Message], run_id: str) -> tuple[list[HistoryEvent], int, int]:
        events: list[HistoryEvent] = []
        reached_sequence = 0
        pending = 1
        for message in messages:
            stream_sequence = message.metadata.sequence.stream
            if not message.data:
                raise HistoryReadError(f"empty history event at stream sequence {stream_sequence} for run {run_id}")
            try:
                events.append(history_decoder(message.data))
            except Exception as exc:
                raise HistoryReadError(
                    f"failed to decode history event at stream sequence {stream_sequence} for run {run_id}"
                ) from exc
            reached_sequence = stream_sequence
            pending = message.metadata.num_pending
        return events, reached_sequence, pending

    async def append(self, event: HistoryEvent) -> None:
        subject = manifest.history_subject(wf_id=event.wf_id, run_id=event.run_id)
        data = history_encoder(event, enc_hook=self.codec.enc_hook)
        await self.jetstream.publish(subject, data)
