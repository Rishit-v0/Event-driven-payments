import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from common.config import settings
from common.events import Event, EventType
from common.idempotency import Idempotency
from common.kafka_io import EventConsumer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("notification")
idem = Idempotency(settings.redis_url)


async def handle(event: Event):
    if event.event_type == EventType.INVENTORY_RESERVED:
        log.info("Order %s confirmed. Sending confirmation.", event.aggregate_id)
    elif event.event_type in (EventType.PAYMENT_FAILED, EventType.PAYMENT_REFUNDED):
        log.info("Order %s could not complete. Notifying customer.", event.aggregate_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer = EventConsumer(
        topics=["payment.events", "inventory.events"],
        group_id="notification-service",
        bootstrap=settings.kafka_bootstrap,
        handler=handle,
        idempotency=idem,
    )
    await consumer.start()
    yield
    await consumer.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}