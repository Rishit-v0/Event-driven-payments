from sqlalchemy import String, Integer
from sqlalchemy.orm import Mapped, mapped_column

from common.db import Base
from common.outbox import OutboxMixin


class Stock(Base):
    __tablename__ = "stock"
    product_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    available: Mapped[int] = mapped_column(Integer)


class Reservation(Base):
    __tablename__ = "reservations"
    order_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    product_id: Mapped[str] = mapped_column(String(64))
    quantity: Mapped[int] = mapped_column(Integer)


class Outbox(Base, OutboxMixin):
    pass