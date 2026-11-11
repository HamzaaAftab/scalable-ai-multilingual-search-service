"""HTTP layer: product create. Asli kaam services/products.py mein."""

from typing import Annotated, Literal

from fastapi import APIRouter, Header, HTTPException, status
from pydantic import BaseModel

from api.services.products import create_product
from common.clients.kafka_client import KafkaPublishError
from common.schemas.product import ProductCreate

router = APIRouter(prefix="/products", tags=["products"])


class ProductAccepted(BaseModel):
    product_id: str
    # "saved" nahi, "accepted": Kafka ne le liya, workers thodi der mein likhenge (eventual consistency)
    status: Literal["accepted"] = "accepted"
    duplicate: bool


# 202 Accepted, 201 Created nahi: product abhi Mongo/Typesense mein nahi hai, sirf Kafka mein hai
@router.post("", status_code=status.HTTP_202_ACCEPTED)
def add_product(
    product: ProductCreate,
    # Required: bina key ke request 422. Warna kisi client ki ek bhool se duplicate ki guarantee khatam.
    # FastAPI khud "idempotency_key" ko "Idempotency-Key" header se jodta hai.
    idempotency_key: Annotated[str, Header(min_length=8, max_length=100)],
) -> ProductAccepted:
    try:
        result = create_product(product, idempotency_key)
    except KafkaPublishError:
        # 503 = "abhi nahi, thodi der baad try karo". Admin wahi key ke saath retry kar sakta hai.
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Queue abhi available nahi, dobara koshish karein")
    return ProductAccepted(product_id=result.product_id, duplicate=result.duplicate)
