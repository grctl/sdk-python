import asyncio
import contextlib
import json

import msgspec
from nats.aio.msg import Msg
from nats.js.api import ConsumerConfig
from nats.js.client import JetStreamContext

from grctl.logging_config import get_logger
from grctl.models import directive_decoder
from grctl.nats.manifest import NatsManifest
from grctl.settings import get_settings
from grctl.worker.run_manager import RunManager

logger = get_logger(__name__)

_ACK_PROGRESS_INTERVAL_SECONDS = 2.0


class Subscriber:
    def __init__(
        self,
        js: JetStreamContext,
        manifest: NatsManifest,
        wf_types: list[str],
        run_manager: RunManager,
    ) -> None:
        self._js = js
        self._manifest = manifest
        self._wf_types = wf_types
        self._run_manager = run_manager
        self._subscriptions: list[JetStreamContext.PushSubscription] = []

    async def start(self) -> None:
        for wf_type in self._wf_types:
            filter_subject = self._manifest.worker_task_filter_subject(wf_type)
            queue_group = self._manifest.worker_task_queue_group(wf_type)
            # In nats-py, queue sets both the deliver group AND the durable name,
            # giving us a persistent competing-consumer subscription shared across
            # all workers with the same wf_type.
            sub = await self._js.subscribe(
                filter_subject,
                queue=queue_group,
                manual_ack=True,
                config=ConsumerConfig(ack_wait=get_settings().nats_worker_ack_wait),
                cb=self._handle_message,
            )
            self._subscriptions.append(sub)
            logger.info(
                "Subscribed to worker tasks wf_type=%s queue=%s",
                wf_type,
                queue_group,
            )

    async def stop(self) -> None:
        for sub in self._subscriptions:
            await sub.unsubscribe()
        self._subscriptions.clear()

    async def _handle_message(self, msg: Msg) -> None:
        try:
            directive = directive_decoder(msg.data)
            logger.info("Received worker task: %s", directive)
        except Exception:
            logger.exception("Failed to decode directive, NAKing message subject=%s", msg.subject)
            await msg.nak()
            return

        # num_delivered is 1-based; map to 0-based attempt so NATS redeliveries
        # are treated as retries even when the server-side attempt counter hasn't advanced.
        nats_attempt = msg.metadata.num_delivered - 1
        directive.attempt = max(directive.attempt, nats_attempt)

        payload = json.dumps(msgspec.to_builtins(directive), indent=2, sort_keys=True)
        logger.info(
            "Received worker task subject=%s wf_type=%s: %s",
            msg.subject,
            directive.run_info.wf_type,
            payload,
        )

        try:
            task = await self._run_manager.handle_next_directive(directive)
        except Exception:
            logger.exception(
                "Failed to initialize runner directive_id=%s wf_type=%s, NAKing",
                directive.id,
                directive.run_info.wf_type,
            )
            await msg.nak()
            return

        if task is None:
            # run_id already executing — ACK to prevent redelivery of a duplicate
            logger.warning(
                "Directive already executing directive_id=%s run_id=%s, ACKing duplicate",
                directive.id,
                directive.run_info.id,
            )
            await msg.ack()
            return

        ack_progress_task = asyncio.create_task(self._send_ack_progress(msg))
        try:
            await task
            await msg.ack()
        except asyncio.CancelledError:
            logger.info("Task cancelled directive_id=%s", directive.id)
        except Exception:
            # Fail directive already published by workflow_error_handler inside the runner
            logger.debug("Runner task raised exception directive_id=%s", directive.id)
            await msg.ack()
        finally:
            ack_progress_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ack_progress_task

            logger.info("Task completed directive_id=%s", directive.id)

    async def _send_ack_progress(self, msg: Msg) -> None:
        while True:
            await asyncio.sleep(_ACK_PROGRESS_INTERVAL_SECONDS)
            await msg.in_progress()
