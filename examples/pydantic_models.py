"""Example: using Pydantic models as workflow input, event payload, task input/output, and result.

Demonstrates that grctl serializes Pydantic models transparently at every boundary:
  - workflow_input  — OrderRequest instance
  - start step arg  — start(ctx, order: OrderRequest)
  - task input      — enrich_order(order: OrderRequest)
  - task output     — returns EnrichedOrder
  - store           — ctx.store.put/get with Pydantic types
  - event send      — send("confirm_payment", PaymentConfirmation(...))
  - event step arg  — confirm_payment(ctx, confirmation: PaymentConfirmation)
  - result          — ctx.next.complete(OrderResult(...))
"""

import asyncio
import logging
from datetime import timedelta

import ulid
from pydantic import BaseModel

from grctl.client import Client, Connection, get_logger, setup_logging
from grctl.worker import Context, Worker, task
from grctl.workflow import Directive, Workflow

setup_logging(level=logging.DEBUG)
logger = get_logger(__name__)


class OrderRequest(BaseModel):
    order_id: str
    customer: str
    amount: float
    currency: str = "USD"


class EnrichedOrder(BaseModel):
    order_id: str
    customer: str
    amount: float
    currency: str
    tax: float
    total: float


class PaymentConfirmation(BaseModel):
    transaction_id: str
    status: str
    paid_amount: float


class OrderResult(BaseModel):
    order_id: str
    customer: str
    total: float
    payment_status: str
    transaction_id: str


orders = Workflow(workflow_type="Orders")


@task
async def enrich_order(order: OrderRequest) -> EnrichedOrder:
    logger.info(f"Enriching order {order.order_id} for {order.customer}")
    tax = round(order.amount * 0.08, 2)
    return EnrichedOrder(
        order_id=order.order_id,
        customer=order.customer,
        amount=order.amount,
        currency=order.currency,
        tax=tax,
        total=round(order.amount + tax, 2),
    )


@orders.start()
async def start(ctx: Context, order: OrderRequest) -> Directive:
    enriched = await enrich_order(order)
    logger.info(f"Enriched order: total={enriched.total} {enriched.currency}")
    ctx.store.put("enriched_order", enriched)
    return ctx.next.wait_for_event()


@orders.event()
async def confirm_payment(ctx: Context, confirmation: PaymentConfirmation) -> Directive:
    enriched = await ctx.store.get("enriched_order", EnrichedOrder)
    logger.info(f"Payment {confirmation.status} for order {enriched.order_id}: {confirmation.paid_amount}")

    result = OrderResult(
        order_id=enriched.order_id,
        customer=enriched.customer,
        total=enriched.total,
        payment_status=confirmation.status,
        transaction_id=confirmation.transaction_id,
    )
    return ctx.next.complete(result)


async def main() -> None:
    connection = await Connection.connect()
    worker = Worker(workflows=[orders], connection=connection)
    worker_task = asyncio.create_task(worker.start())

    logger.info("Worker ready. Starting pydantic models workflow...")

    client = Client(connection=connection)
    try:
        wf_handle = await client.start_workflow(
            type=orders.workflow_type,
            id=str(ulid.ULID()),
            input=OrderRequest(order_id="ORD-001", customer="Alice", amount=99.99),
            timeout=timedelta(seconds=30),
        )

        await asyncio.sleep(1)

        await wf_handle.send(
            "confirm_payment",
            PaymentConfirmation(transaction_id="TXN-XYZ", status="approved", paid_amount=107.99),
        )

        result = await asyncio.wait_for(wf_handle.future, timeout=30)
        logger.info(f"Workflow result: {result}")

    except Exception:
        logger.exception("Workflow execution failed")
    finally:
        await worker.stop()
        if not worker_task.done():
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
