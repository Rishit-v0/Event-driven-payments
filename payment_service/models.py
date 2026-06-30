from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Integer
from sqlalchemy.orm import Mapped, mapped_column

from common.db import Base
from common.outbox import OutboxMixin


class Payment(Base):
    __tablename__ = "payments"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)  # order_id
    stripe_payment_intent: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str] = mapped_column(String(20))  # SUCCEEDED, FAILED, REFUNDED
    amount_cents: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class Outbox(Base, OutboxMixin):
    pass