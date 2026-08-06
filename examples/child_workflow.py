import asyncio
import logging
import uuid
from datetime import timedelta

import ulid

from grctl.client import Client, setup_logging
from grctl.nats import Connection
from grctl.worker import Worker
from grctl.workflow import Context, Directive, Workflow, task

setup_logging(level=logging.DEBUG)
logger = logging.getLogger(__name__)


payment_wf = Workflow(workflow_type="Payment")
order_wf = Workflow(workflow_type="Order")


@task
async def validate_order(order_id: str, amount: float) -> tuple[str, float]:
    logger.info(f"Validating order {order_id} for amount ${amount}")
    await asyncio.sleep(0.1)
    return (order_id, amount)


@task
async def process_payment(amount: float) -> str:
    logger.info(f"Processing payment for amount ${amount}")
    await asyncio.sleep(0.2)
    return str(uuid.uuid4())[:8]


@task
async def record_transaction(transaction_id: str, amount: float) -> str:
    logger.info(f"Recording transaction {transaction_id} for ${amount}")
    await asyncio.sleep(0.1)
    return "success"


# ============================================================================
# PAYMENT WORKFLOW (Child)
# ============================================================================


@payment_wf.step(start=True)
async def payment_start(ctx: Context, amount: float) -> Directive:
    ctx.store.set("amount", amount)
    logger.info(f"Payment workflow started for amount ${amount}")
    return ctx.next.step(payment_process)


@payment_wf.step()
async def payment_process(ctx: Context) -> Directive:
    amount = await ctx.store.get("amount", float)
    transaction_id = await process_payment(amount)
    ctx.store.set("transaction_id", transaction_id)
    return ctx.next.step(payment_record)


@payment_wf.step()
async def payment_record(ctx: Context) -> Directive:
    transaction_id = await ctx.store.get("transaction_id", str)
    amount = await ctx.store.get("amount", float)

    status = await record_transaction(transaction_id, amount)
    ctx.store.set("status", status)

    logger.info(f"Payment completed with status: {status}")

    res = {"status": status, "transaction_id": transaction_id}
    await ctx.send_to_parent(event_name="payment_completed", payload=res)
    return ctx.next.complete(res)


# ============================================================================
# ORDER WORKFLOW (Parent)
# ============================================================================


@order_wf.step(start=True)
async def order_start(ctx: Context, order_id: str, amount: float) -> Directive:
    validated_id, validated_amount = await validate_order(order_id, amount)
    ctx.store.set("order_id", validated_id)
    ctx.store.set("amount", validated_amount)
    logger.info(f"Order workflow started for order {order_id}")

    payment_workflow_id = f"payment-{validated_id}-{ulid.ULID()}"
    payment_handle = await ctx.start_child(
        payment_wf.workflow_type,
        workflow_id=payment_workflow_id,
        workflow_input={"amount": validated_amount},
        workflow_timeout=timedelta(minutes=1),
    )

    ctx.store.set("payment_workflow_id", payment_handle.run_info.id)
    logger.info(
        "Started child payment workflow %s for order %s",
        payment_handle.run_info.id,
        validated_id,
    )
    return ctx.next.wait()


@order_wf.step(event=True, name="payment_completed")
async def handle_payment_result(ctx: Context, status: str, transaction_id: str) -> Directive:
    order_id = await ctx.store.get("order_id", str)

    logger.info(f"Order {order_id} received payment result: {status}")

    ctx.store.set("payment_status", status)

    message = f"Order {order_id} completed with payment status: {status} transaction_id: {transaction_id}"
    ctx.store.set("message", message)

    logger.info(f"Final message: {message}")
    return ctx.next.complete(message)


async def main() -> None:
    """Run an order workflow that starts a child payment workflow."""
    logger.info("Starting worker...")
    connection = await Connection.connect()
    worker = Worker(
        workflows=[order_wf, payment_wf],
        connection=connection,
    )

    worker_task = asyncio.create_task(worker.run())

    logger.info("Worker is ready. Sending test request...")

    client = Client(connection=connection)

    try:
        order_handle = await client.start_workflow(
            type=order_wf.workflow_type,
            id=str(ulid.ULID()),
            input={"order_id": "ORDER-001", "amount": 99.99},
            timeout=timedelta(seconds=60),
        )
        logger.info(f"Order workflow started with handle: {order_handle}")

        result = await asyncio.wait_for(order_handle.future, timeout=10)
        logger.info(f"Order workflow result: {result}")

    except Exception:
        logger.exception("Workflow execution failed")
    finally:
        await worker.stop()
        if not worker_task.done():
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
