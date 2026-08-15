# NATS Core and JetStream Python Integration Guide

This guide describes the APIs in `nats-core 0.2.0` and `nats-jetstream 0.3.0`
needed for four common messaging roles:

1. synchronous command or workflow APIs, using core NATS request/reply;
2. command intake, using a core NATS subscription;
3. durable history watching, using a JetStream consumer that starts at the
   latest retained message; and
4. durable work consumption, using a pull consumer with explicit
   acknowledgements.

`nats-core` provides the connection and core NATS messaging. JetStream is a
separate package layered on top of that connection:

```python
from nats.client import connect
from nats.jetstream import new as new_jetstream

client = await connect("nats://localhost:4222")
jetstream = new_jetstream(client)
```

All payloads are `bytes`. Encode and decode at the application boundary, for
example with MessagePack, JSON, or Protobuf.

## Connection lifecycle

The connection entry point and the operations used here have these shapes:

```python
from collections.abc import Callable
from nats.client import Client, connect
from nats.client.message import Message
from nats.client.subscription import Subscription

async def connect(
    url: str = "nats://localhost:4222",
    *,
    timeout: float = 2.0,
    allow_reconnect: bool = True,
    reconnect_max_attempts: int = 10,  # 0 means unlimited
    reconnect_time_wait: float = 2.0,
    reconnect_time_wait_max: float = 10.0,
    reconnect_jitter: float = 0.1,
    reconnect_timeout: float | None = None,
    token: str | Callable[[], str] | None = None,
    user: str | Callable[[], str] | None = None,
    password: str | Callable[[], str] | None = None,
    nkey=...,
    jwt=...,
) -> Client: ...

await client.publish(
    subject: str | bytes,
    payload: bytes,
    *,
    reply: str | bytes | None = None,
    headers: dict[str, str | list[str]] | None = None,
) -> None

await client.request(
    subject: str,
    payload: bytes,
    *,
    timeout: float = 2.0,
    headers: dict[str, str | list[str]] | None = None,
    return_on_error: bool = False,
) -> Message

await client.subscribe(
    subject: str | bytes,
    *,
    queue: str | bytes = "",
    max_pending_messages: int | None = 65_536,
    max_pending_bytes: int | None = 67_108_864,
) -> Subscription

await client.flush(timeout: float | None = None) -> None
await client.drain(timeout: float = 30.0) -> None
await client.close() -> None
```

`connect()` takes one `url`, not a `servers` list. Configure the client with
one seed URL and allow the server's advertised peer URLs to populate its
connection pool.

Connection callbacks are synchronous functions:

```python
def on_disconnected() -> None:
    ...

def on_reconnected() -> None:
    ...

def on_error(error: Exception | str) -> None:
    ...

client.add_disconnected_callback(on_disconnected)
client.add_reconnected_callback(on_reconnected)
client.add_error_callback(on_error)
```

Do not await in these callbacks or perform slow work in them. Schedule
asynchronous recovery work with `asyncio.create_task()` if needed.

Use `drain()` for normal process shutdown. It stops new publishes, drains
subscriptions, flushes buffered writes, and closes the connection. Use
`close()` only when immediate shutdown is acceptable.

## Core request/reply APIs

Request/reply suits a caller that needs one bounded response from a service.
The response body is application-defined.

```python
import asyncio
from nats.client.errors import NoRespondersError, StatusError

async def call_service(client, subject: str, request_bytes: bytes) -> bytes:
    try:
        reply = await client.request(subject, request_bytes, timeout=5.0)
    except TimeoutError as exc:
        # A responder may be overloaded, disconnected, or unavailable.
        raise RuntimeError("request timed out") from exc
    except NoRespondersError as exc:
        # NATS status 503: there was no current subscription for the subject.
        raise RuntimeError("service has no active responder") from exc
    except StatusError as exc:
        raise RuntimeError(f"NATS status {exc.status}: {exc.description}") from exc

    return reply.data
```

The request API creates and multiplexes inbox subscriptions internally. It
raises `TimeoutError` when the deadline is reached. By default, it raises
`StatusError` for an error status response, including `NoRespondersError` for
status `503`. Set `return_on_error=True` only when the caller needs to inspect
the returned status message itself.

### Writing a responder

`nats-core` subscriptions are pull-based. There is no `cb=` parameter on
`subscribe()`, and core `Message` has no `respond()` method. Consume messages
from the subscription and publish a reply to `message.reply`.

```python
from nats.client.message import Message

async def serve(client, subject: str) -> None:
    subscription = await client.subscribe(subject)
    try:
        async for message in subscription:
            response_bytes = await handle_request(message.data)
            if message.reply:
                await client.publish(message.reply, response_bytes)
    finally:
        await subscription.unsubscribe()

async def handle_request(request_bytes: bytes) -> bytes:
    return b"ok"
```

A failing handler must still reply when the request contract requires a reply.
Encode an application error response, or let the requester time out only when
no reliable error response is possible. Check `message.reply` before replying:
ordinary publishes have no reply subject.

### Retry and idempotency

A timeout means the requester does not know whether the service processed the
command. Retrying can therefore create duplicate work. Include an operation or
command ID in every mutating request, and make the responder deduplicate that
ID durably. Retry only transport-level failures such as timeout or
`NoRespondersError`; do not retry a valid application rejection.

## Core command subscriptions

A core subscription receives only messages published while it is active. It is
not durable and has no acknowledgement protocol.

```python
async def consume_commands(client, subject: str, queue_group: str | None = None) -> None:
    subscription = await client.subscribe(
        subject,
        queue=queue_group or "",
        max_pending_messages=1_000,
        max_pending_bytes=8 * 1024 * 1024,
    )
    try:
        async for message in subscription:
            await dispatch_command(message)
    finally:
        await subscription.unsubscribe()

async def dispatch_command(message) -> None:
    ...
```

Use no queue group when each active listener must receive every message. Use
the same non-empty `queue` value on several listeners when exactly one active
member should receive each message for load balancing. Queue groups provide
load distribution, not durability and not exactly-once delivery.

Bound pending messages and bytes. When a subscription cannot keep up, the
client drops messages to protect memory and invokes registered error callbacks
with `SlowConsumerError`. A command that cannot be dropped belongs in
JetStream, not a core subscription.

For one-shot consumption, `Subscription.next()` has this shape:

```python
message = await subscription.next(timeout: float | None = None)
```

It raises `asyncio.TimeoutError` on a timed wait and `RuntimeError` after a
closed subscription has been drained.

## JetStream setup and publish

JetStream persists messages in streams and tracks delivery through consumers.
Its entry points relevant to this guide are:

```python
from nats.jetstream import JetStream, new as new_jetstream

jetstream: JetStream = new_jetstream(client)

await jetstream.publish(
    subject: str,
    payload: bytes,
    *,
    headers: dict[str, str] | None = None,
    retry_attempts: int = 2,
    retry_wait: float = 0.25,
    timeout: float = 5.0,
)  # -> PublishAck

await jetstream.get_stream(name: str)  # -> Stream
await jetstream.create_consumer(
    stream_name: str,
    name: str,
    durable_name: str | None = None,
    **config,
)  # -> Consumer
await jetstream.create_or_update_consumer(stream_name: str, **config)  # -> Consumer
await jetstream.delete_consumer(stream_name: str, consumer_name: str)  # -> bool
await jetstream.get_last_message_for_subject(stream: str, subject: str)
```

`JetStream.publish()` waits for the server's publish acknowledgement and
retries `NoRespondersError` twice by default. Treat a returned `PublishAck` as
the persistence confirmation. If the producer can retry after an uncertain
outcome, use the stream's de-duplication configuration and a stable
`Nats-Msg-Id` header.

## Durable worker consumption with pull consumers

Use a durable pull consumer for work that must survive process restarts. A
durable consumer shared by multiple processes load-balances deliveries among
those processes.

```python
from datetime import timedelta
from nats.jetstream.consumer import ConsumerConfig
from nats.jetstream.consumer.pull import PullConsumer

stream = await jetstream.get_stream("WORK")
consumer = await stream.create_or_update_consumer(
    ConsumerConfig(
        name="workers",
        durable_name="workers",
        filter_subject="work.>",
        ack_policy="explicit",
        ack_wait=timedelta(seconds=60),
        max_ack_pending=100,
    )
)
assert isinstance(consumer, PullConsumer)

while True:
    batch = await consumer.fetch(max_messages=100, max_wait=1.0)
    async for message in batch:
        await process_work(message.data)
        await message.ack()

    if batch.error is not None:
        raise batch.error
```

The consumer configuration is a `ConsumerConfig` dataclass. The fields most
relevant to durable work are:

| Field | Purpose |
| --- | --- |
| `name` | Required stable consumer name. |
| `durable_name` | Stable durable identity, retained for server compatibility. |
| `filter_subject` | Limits this consumer to one subject pattern. |
| `ack_policy="explicit"` | Requires an acknowledgement per message. |
| `ack_wait` | Redelivery deadline after delivery or the last progress signal. |
| `max_ack_pending` | Backpressure limit for outstanding unacknowledged work. |
| `max_deliver` | Maximum delivery attempts before the server stops redelivering. |
| `deliver_policy` | Start position, such as `"all"`, `"last"`, or `"by_start_sequence"`. |
| `opt_start_seq` | Required starting stream sequence for `"by_start_sequence"`. |
| `inactive_threshold` | Server cleanup deadline for an inactive ephemeral consumer. |

The pull API is:

```python
await consumer.fetch(
    *,
    max_messages: int | None = None,
    max_bytes: int | None = None,
    max_wait: float | None = 30.0,
    heartbeat: float | None = None,
    min_ack_pending: int | None = None,
    min_pending: int | None = None,
)  # -> MessageBatch
```

Specify exactly one of `max_messages` or `max_bytes`. Iterate the returned
batch, then inspect `batch.error`; an empty batch after `max_wait` is normal.
The JetStream message exposes `data`, `subject`, `headers`, and `metadata`.
`metadata.num_delivered` is one-based, so `metadata.num_delivered - 1` is the
zero-based redelivery count.

### Acknowledgement decisions

For an explicitly acknowledged message:

```python
await message.ack()          # completed successfully
await message.in_progress()  # reset the ack timer while still processing
await message.nak()          # request immediate redelivery
await message.nak_with_delay(seconds)  # request delayed redelivery
await message.term()         # never redeliver this message
```

Acknowledgements are themselves publishes. A crash after the side effect but
before `ack()` produces redelivery, so handlers must be idempotent. Send
`in_progress()` periodically for work that can exceed `ack_wait`; otherwise
the server may redeliver while the first attempt is still running. Use `term()`
only for a permanently invalid message that has been recorded for operators.

## Latest-value durable history watching

To watch a subject's current outcome and later updates, create an ephemeral
pull consumer that starts with the latest retained matching message:

```python
from datetime import timedelta

consumer = await jetstream.create_consumer(
    "HISTORY",
    name=f"watch-{watch_id}",
    filter_subject="history.run-123",
    deliver_policy="last",
    ack_policy="explicit",
    inactive_threshold=timedelta(minutes=1),
)

try:
    async for message in await consumer.messages(
        max_messages=1,
        max_wait=30.0,
        heartbeat=5.0,
    ):
        try:
            await handle_history_event(message.data)
        except Exception:
            await message.nak_with_delay(1.0)
        else:
            await message.ack()
finally:
    await jetstream.delete_consumer("HISTORY", consumer.name)
```

`deliver_policy="last"` supplies the newest retained matching message and
subsequent messages. It is appropriate when the latest event is a complete
state or terminal outcome. It is not appropriate when every historical event
matters; use `"all"` or `"by_start_sequence"` for that case.

Every independent watcher needs its own consumer. Sharing a durable consumer
would distribute messages among watchers rather than give each watcher a full
view. An ephemeral consumer with `inactive_threshold` provides server cleanup
if a crashed watcher cannot delete it.

## Migration differences to account for

The new client has a materially different surface from `nats-py`:

| Area | `nats-core` / `nats-jetstream` approach |
| --- | --- |
| Core import | `from nats.client import connect` |
| Connection creation | `await connect(url, ...)`, one URL rather than `nats.connect(servers=[...])` |
| Core subscription | `subscription = await client.subscribe(...)`, then `await subscription.next()` or `async for message in subscription` |
| Callback subscriptions | No `cb=` argument to `subscribe()`; use iteration or `subscription.add_callback()` for a small synchronous callback |
| Responding | `await client.publish(message.reply, response)` after checking `message.reply`; core messages have no `respond()` |
| JetStream construction | `jetstream = new_jetstream(client)` |
| JetStream API | `nats-jetstream` is required in addition to `nats-core` |
| Pull message type | `nats.jetstream.message.Message`, with `ack()`, `nak()`, `in_progress()`, and `term()` |
| Time values | New JetStream consumer configuration uses `datetime.timedelta` for durations such as `ack_wait` |

Validate the exact installed versions before migrating. This guide reflects
`nats-core 0.2.0` and `nats-jetstream 0.3.0`.
