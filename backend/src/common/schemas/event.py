"""Kafka topic "product-events" ke message ki shakal."""

from datetime import UTC, datetime
from typing import Literal, Self
from uuid import uuid4

from pydantic import AwareDatetime, BaseModel, Field, model_validator

from common.schemas.product import Product

EventType = Literal["product.created", "product.updated", "product.deleted"]


class ProductEvent(BaseModel):
    # Har event ki apni pehchan: ek event ko logs mein poore system mein trace kar sakte hain
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType
    product_id: str = Field(min_length=1)  # Yahi Kafka key bhi hai: ek product ke events order mein rehte hain
    timestamp: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    payload: Product | None = None  # Delete mein sirf id kaafi hai

    @model_validator(mode="after")
    def check_payload(self) -> Self:
        # Ye ghalti worker mein pakadni mushkil hoti, is liye darwaze par hi rok do. Worker mein ye event DLQ jayega.
        if self.event_type != "product.deleted" and self.payload is None:
            raise ValueError(f"{self.event_type} ke liye payload zaroori hai")
        if self.payload is not None and self.payload.product_id != self.product_id:
            raise ValueError("payload ka product_id event ke product_id se alag hai")
        return self
