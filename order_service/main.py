import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI
from pydantic import BaseModel

from common.config import settings
from common.db import Base, make_session_factory
from common.events import Event, EventType
from common.outbox import run_outbox_relay
from common.idempotency import Idempotency
from common.kafka_io import EventConsumer
from order_service.models import Order, Outbox

engine, Session = make_session_factory(settings.order_db_url)
idem = Idempotency(settings.redis_url)


class CreateOrderRequest(BaseModel):
    customer_email: str
    product_id: str
    quantity: int
    amount_cents: int
    simulate_decline: bool = False


async def handle_event(event: Event):
    async with Session() as session:
        order = await session.get(Order, event.aggregate_id)
        if not order:
            return
        if event.event_type == EventType.INVENTORY_RESERVED:
            order.status = "CONFIRMED"
        elif event.event_type in (
            EventType.PAYMENT_FAILED,
            EventType.INVENTORY_FAILED,
            EventType.PAYMENT_REFUNDED,
        ):
            order.status = "CANCELLED"
        await session.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    relay = asyncio.create_task(run_outbox_relay(Session, Outbox, settings.kafka_bootstrap))
    consumer = EventConsumer(
        topics=["payment.events", "inventory.events"],
        group_id="order-service",
        bootstrap=settings.kafka_bootstrap,
        handler=handle_event,
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


@app.post("/orders")
async def create_order(req: CreateOrderRequest):
    order_id = str(uuid4())
    async with Session() as session:
        order = Order(
            id=order_id,
            customer_email=req.customer_email,
            product_id=req.product_id,
            quantity=req.quantity,
            amount_cents=req.amount_cents,
            status="PENDING",
        )
        event = Event(
            event_type=EventType.ORDER_CREATED,
            aggregate_id=order_id,
            payload={
                "order_id": order_id,
                "customer_email": req.customer_email,
                "product_id": req.product_id,
                "quantity": req.quantity,
                "amount_cents": req.amount_cents,
                "simulate_decline": req.simulate_decline,
            },
        )
        outbox = Outbox(
            topic="order.events",
            event_type=event.event_type.value,
            aggregate_id=order_id,
            payload=event.to_json(),
        )
        session.add(order)
        session.add(outbox)
        await session.commit()  # order row and event written atomically
    return {"order_id": order_id, "status": "PENDING"}


@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    async with Session() as session:
        order = await session.get(Order, order_id)
        if not order:
            return {"error": "not found"}
        return {
            "order_id": order.id,
            "status": order.status,
            "amount_cents": order.amount_cents,
        }