from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class EventType(str, Enum):
    ORDER_CREATED = "OrderCreated"
    PAYMENT_SUCCEEDED = "PaymentSucceeded"
    PAYMENT_FAILED = "PaymentFailed"
    PAYMENT_REFUNDED = "PaymentRefunded"
    INVENTORY_RESERVED = "InventoryReserved"
    INVENTORY_FAILED = "InventoryFailed"


class Event(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType
    aggregate_id: str  # the order_id this event concerns
    occurred_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    payload: dict[str, Any] = {}

    def to_json(self) -> str:
        return self.model_dump_json()

    @staticmethod
    def from_bytes(data: bytes) -> "Event":
        return Event.model_validate_json(data)