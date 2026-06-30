import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import select

from common.config import settings
from common.db import Base, make_session_factory
from common.events import Event, EventType
from common.outbox import run_outbox_relay
from common.idempotency import Idempotency
from common.kafka_io import EventConsumer
from inventory_service.models import Stock, Reservation, Outbox

engine, Session = make_session_factory(settings.inventory_db_url)
idem = Idempotency(settings.redis_url)


async def handle_payment_succeeded(event: Event):
    if event.event_type != EventType.PAYMENT_SUCCEEDED:
        return
    p = event.payload
    order_id, product_id, qty = p["order_id"], p["product_id"], p["quantity"]
    async with Session() as session:
        row = (
            await session.execute(
                select(Stock).where(Stock.product_id == product_id).with_for_update()
            )
        ).scalar_one_or_none()

        reserved = row is not None and row.available >= qty
        if reserved:
            row.available -= qty
            session.add(
                Reservation(order_id=order_id, product_id=product_id, quantity=qty)
            )

        result = Event(
            event_type=EventType.INVENTORY_RESERVED if reserved else EventType.INVENTORY_FAILED,
            aggregate_id=order_id,
            payload={"order_id": order_id, "product_id": product_id, "quantity": qty},
        )
        session.add(
            Outbox(
                topic="inventory.events",
                event_type=result.event_type.value,
                aggregate_id=order_id,
                payload=result.to_json(),
            )
        )
        await session.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with Session() as session:  # seed stock once
        if not await session.get(Stock, "widget-001"):
            session.add(Stock(product_id="widget-001", available=10000))
            await session.commit()
    relay = asyncio.create_task(run_outbox_relay(Session, Outbox, settings.kafka_bootstrap))
    consumer = EventConsumer(
        topics=["payment.events"],
        group_id="inventory-service",
        bootstrap=settings.kafka_bootstrap,
        handler=handle_payment_succeeded,
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


@app.get("/stock/{product_id}")
async def stock(product_id: str):
    async with Session() as session:
        row = await session.get(Stock, product_id)
        return {"product_id": product_id, "available": row.available if row else 0}