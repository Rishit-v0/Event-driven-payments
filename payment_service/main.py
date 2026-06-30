import asyncio
from contextlib import asynccontextmanager

import stripe
from fastapi import FastAPI

from common.config import settings
from common.db import Base, make_session_factory
from common.events import Event, EventType
from common.outbox import run_outbox_relay
from common.idempotency import Idempotency
from common.kafka_io import EventConsumer
from payment_service.models import Payment, Outbox

stripe.api_key = settings.stripe_api_key
engine, Session = make_session_factory(settings.payment_db_url)
idem = Idempotency(settings.redis_url)


def _charge(order_id: str, amount_cents: int, decline: bool):
    pm = "pm_card_chargeDeclined" if decline else "pm_card_visa"
    return stripe.PaymentIntent.create(
        amount=amount_cents,
        currency="usd",
        payment_method=pm,
        confirm=True,
        automatic_payment_methods={"enabled": True, "allow_redirects": "never"},
        idempotency_key=f"charge-{order_id}",  # Stripe never double-charges
    )


def _refund(order_id: str, intent_id: str):
    return stripe.Refund.create(
        payment_intent=intent_id,
        idempotency_key=f"refund-{order_id}",
    )


async def handle_order_created(event: Event):
    if event.event_type != EventType.ORDER_CREATED:
        return
    p = event.payload
    order_id = p["order_id"]
    decline = p.get("simulate_decline", False)

    if settings.mock_payments:
        # Skip the real Stripe call during load testing.
        succeeded = not decline
        intent_id = None if decline else f"mock_pi_{order_id}"
    else:
        succeeded = True
        intent_id = None
        try:
            intent = await asyncio.to_thread(
                _charge, order_id, p["amount_cents"], decline
            )
            intent_id = intent.id
            succeeded = intent.status == "succeeded"
        except stripe.error.CardError:
            succeeded = False

    async with Session() as session:
        payment = Payment(
            id=order_id,
            stripe_payment_intent=intent_id,
            status="SUCCEEDED" if succeeded else "FAILED",
            amount_cents=p["amount_cents"],
        )
        result = Event(
            event_type=EventType.PAYMENT_SUCCEEDED if succeeded else EventType.PAYMENT_FAILED,
            aggregate_id=order_id,
            payload={
                "order_id": order_id,
                "amount_cents": p["amount_cents"],
                "product_id": p["product_id"],
                "quantity": p["quantity"],
            },
        )
        await session.merge(payment)
        session.add(
            Outbox(
                topic="payment.events",
                event_type=result.event_type.value,
                aggregate_id=order_id,
                payload=result.to_json(),
            )
        )
        await session.commit()


async def handle_compensation(event: Event):
    if event.event_type != EventType.INVENTORY_FAILED:
        return
    order_id = event.aggregate_id
    async with Session() as session:
        payment = await session.get(Payment, order_id)
        if not payment or payment.status != "SUCCEEDED":
            return
        if not settings.mock_payments:
            await asyncio.to_thread(_refund, order_id, payment.stripe_payment_intent)
        payment.status = "REFUNDED"
        refund = Event(
            event_type=EventType.PAYMENT_REFUNDED,
            aggregate_id=order_id,
            payload={"order_id": order_id},
        )
        session.add(
            Outbox(
                topic="payment.events",
                event_type=refund.event_type.value,
                aggregate_id=order_id,
                payload=refund.to_json(),
            )
        )
        await session.commit()


async def router(event: Event):
    await handle_order_created(event)
    await handle_compensation(event)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    relay = asyncio.create_task(run_outbox_relay(Session, Outbox, settings.kafka_bootstrap))
    consumer = EventConsumer(
        topics=["order.events", "inventory.events"],
        group_id="payment-service",
        bootstrap=settings.kafka_bootstrap,
        handler=router,
        idempotency=idem,
    )
    await consumer.start()
    yield
    relay.cancel()
    await consumer.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}