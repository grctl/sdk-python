import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import cast

from nats.jetstream import JetStream
from nats.jetstream.consumer import ConsumerConfig
from nats.jetstream.consumer.pull import PullConsumer
from nats.jetstream.message import Message

from grctl.models import Directive, directive_decoder
from grctl.models.errors import WorkflowStepAlreadyExecutedError
from grctl.nats.manifest import NatsManifest
from grctl.settings import get_settings

DirectiveHandler = Callable[[Directive], Awaitable[asyncio.Task]]
"""Dispatches one directive and returns the task running it.

A callable rather than an import from exec/ so nats/ stays decoupled;
ExecutionManager.handle_next_directive satisfies it.
"""


class Subscriber:
    def __init__(
        self,
        js: JetStream,
        manifest: NatsManifest,
        wf_types: list[str],
        directive_handler: DirectiveHandler,
        logger: logging.Logger,
    ) -> None:
        self._jetstream = js
        self._manifest = manifest
        self._wf_types = wf_types
        self._directive_handler = directive_handler
        self._consume_tasks: list[asyncio.Task] = []
        self.tasks_in_progress: set[asyncio.Task] = set()
        self.logger = logger
        self.settings = get_settings()

    async def start(self) -> None:

        for wf_type in self._wf_types:
            task = asyncio.create_task(self._consume_loop(wf_type))
            self._consume_tasks.append(task)

            self.logger.info(
                "Starting subscription wf_type=%s consumer=%s",
                wf_type,
                self._manifest.worker_task_queue_group(wf_type),
            )

    async def _consume_loop(self, wf_type: str) -> None:
        while True:
            try:
                consumer = await self._create_consumer(wf_type)
                self.logger.info(
                    "Subscribed to worker tasks wf_type=%s consumer=%s",
                    wf_type,
                    self._manifest.worker_task_queue_group(wf_type),
                )
                while True:
                    batch = await consumer.fetch(
                        max_messages=100,
                        max_wait=self.settings.nats_fetch_max_wait,
                    )
                    async for msg in batch:
                        num_pending = msg.metadata.num_pending if msg.metadata else -1
                        self.logger.info(
                            "msg fetched | tasks_in_progress=%d num_pending=%d attempt=%d",
                            len(self.tasks_in_progress),
                            num_pending,
                            msg.metadata.num_delivered if msg.metadata else -1,
                        )
                        task = asyncio.create_task(self._process_message(msg))
                        self.tasks_in_progress.add(task)
                        task.add_done_callback(self.tasks_in_progress.discard)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception(
                    "Consume loop error wf_type=%s, restarting in %.1fs",
                    wf_type,
                    self.settings.nats_reconnect_time_wait,
                )
                await asyncio.sleep(self.settings.nats_reconnect_time_wait)

    async def _create_consumer(self, wf_type: str) -> PullConsumer:
        config = ConsumerConfig(
            name=self._manifest.worker_task_queue_group(wf_type),
            durable_name=self._manifest.worker_task_queue_group(wf_type),
            filter_subject=self._manifest.worker_task_filter_subject(wf_type),
            ack_policy="explicit",
            ack_wait=timedelta(seconds=get_settings().nats_worker_ack_wait),
        )
        stream = await self._jetstream.get_stream(self._manifest.state_stream_name())
        consumer = await stream.create_or_update_consumer(config)
        return cast("PullConsumer", consumer)

    async def _process_message(self, msg: Message) -> None:
        try:
            directive = directive_decoder(msg.data)
            self.logger.debug(
                "Received worker job: %s workflow_type=%s wf_id=%s",
                directive.kind,
                directive.run_info.wf_type,
                directive.run_info.wf_id,
            )
        except Exception:
            self.logger.exception("Failed to decode directive, NAKing message subject=%s", msg.subject)
            await msg.nak()
            return

        # num_delivered is 1-based; map to 0-based attempt so NATS redeliveries
        # are treated as retries even when the server-side attempt counter hasn't advanced.
        nats_attempt = msg.metadata.num_delivered - 1
        directive.attempt = max(directive.attempt, nats_attempt)

        self.logger.debug(
            "Received worker job wf_type=%s wf_id=%s",
            directive.run_info.wf_type,
            directive.run_info.wf_id,
        )

        # Start in_progress heartbeat immediately so the ack_wait timer never expires
        # while we initialise the runner (includes _load_step_history for attempt > 0).
        ack_progress_task = asyncio.create_task(self._send_ack_progress(msg))
        t_heartbeat = time.monotonic()
        try:
            await self.execute_directive(msg, directive)
        finally:
            ack_progress_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ack_progress_task

            self.logger.debug(
                "Job run exited directive kind=%s wf_type=%s wf_id=%s total_ms=%.0f",
                directive.kind,
                directive.run_info.wf_type,
                directive.run_info.wf_id,
                (time.monotonic() - t_heartbeat) * 1000,
            )

    async def execute_directive(self, msg: Message, directive: Directive) -> None:
        t_execute = time.monotonic()
        self.logger.info(
            "stage=executing run_id=%.8s kind=%s attempt=%d",
            directive.run_info.id,
            directive.kind,
            directive.attempt,
        )
        try:
            task = await self._directive_handler(directive)
        except WorkflowStepAlreadyExecutedError:
            # run_id already executing — ACK to prevent redelivery of a duplicate
            self.logger.warning(
                "Directive already executing directive_id=%s run_id=%s, ACKing duplicate",
                directive.id,
                directive.run_info.id,
            )
            await msg.ack()
            return
        except Exception:
            self.logger.exception(
                "Failed to initialize runner directive_id=%s wf_type=%s, NAKing",
                directive.id,
                directive.run_info.wf_type,
            )
            await msg.nak()
            return

        init_ms = (time.monotonic() - t_execute) * 1000
        self.logger.info(
            "stage=running run_id=%.8s kind=%s init_ms=%.1f",
            directive.run_info.id,
            directive.kind,
            init_ms,
        )
        try:
            await task
            await msg.ack()
            self.logger.debug(
                "ACK sent directive kind=%s wf_type=%s wf_id=%s",
                directive.kind,
                directive.run_info.wf_type,
                directive.run_info.wf_id,
            )
        except asyncio.CancelledError:
            self.logger.info(
                "Job cancelled directive kind=%s wf_type=%s wf_id=%s",
                directive.kind,
                directive.run_info.wf_type,
                directive.run_info.wf_id,
            )
            await msg.ack()
        except Exception:
            # Fail directive already published by workflow_error_handler inside the runner
            self.logger.exception(
                "Runner job raised exception directive kind=%s wf_type=%s wf_id=%s",
                directive.kind,
                directive.run_info.wf_type,
                directive.run_info.wf_id,
            )
            await msg.ack()

    async def _send_ack_progress(self, msg: Message) -> None:
        seconds = self.settings.progress_ack_interval_seconds
        while True:
            await asyncio.sleep(seconds)
            await msg.in_progress()

    async def stop(self) -> None:
        for task in self._consume_tasks:
            task.cancel()
        await asyncio.gather(*self._consume_tasks, return_exceptions=True)
        self._consume_tasks.clear()

        if self.tasks_in_progress:
            await asyncio.gather(*self.tasks_in_progress, return_exceptions=True)
