import asyncio
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from sqlalchemy import String, DateTime, Integer, Text, select
from sqlalchemy.orm import Mapped, mapped_column


class OutboxMixin:
    __tablename__ = "outbox"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String(100))
    event_type: Mapped[str] = mapped_column(String(50))
    aggregate_id: Mapped[str] = mapped_column(String(64))
    payload: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )



async def run_outbox_relay(session_factory, outbox_model, bootstrap, poll_interval=0.1):
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        while True:
            async with session_factory() as session:
                rows = (
                    await session.execute(
                        select(outbox_model)
                        .where(outbox_model.published_at.is_(None))
                        .order_by(outbox_model.id)
                        .limit(200)
                    )
                ).scalars().all()
                for row in rows:
                    await producer.send(            # batched, does not block per row
                        row.topic,
                        row.payload.encode("utf-8"),
                        key=row.aggregate_id.encode("utf-8"),
                    )
                    row.published_at = datetime.now(timezone.utc)
                if rows:
                    await producer.flush()          # wait once for the whole batch
                    await session.commit()
            await asyncio.sleep(poll_interval)
    finally:
        await producer.stop()