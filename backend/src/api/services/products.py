"""Product create: duplicate request roko (idempotency), event banao, Kafka mein bhejo.

Idempotency-Key admin panel har FORM ke liye ek dafa banata hai (har click par nahi).
Retry ya double click par wahi key aati hai, to hum wahi product_id wapas dete hain aur Kafka mein kuch nahi jata.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from redis import RedisError

from common.clients.kafka_client import publish_event
from common.clients.redis_client import get_redis
from common.config import settings
from common.schemas.event import ProductEvent
from common.schemas.product import Product, ProductCreate, new_product_id

logger = logging.getLogger(__name__)


@dataclass
class CreateResult:
    product_id: str
    duplicate: bool  # True = ye key pehle aa chuki thi, naya product nahi bana


def _claim_key(key: str, product_id: str) -> str | None:
    """Key apne naam karo. Wapas: None agar hum pehle hain, warna pehle wala product_id."""
    redis_key = f"idem:{key}"
    try:
        # NX: sirf tab likho jab key na ho. Ye ek hi atomic command hai: do requests bilkul ek saath
        # aayen to bhi sirf ek jeetegi. ("GET karo, phir SET karo" do commands mein ye guarantee nahi.)
        if get_redis().set(redis_key, product_id, nx=True, ex=settings.idempotency_ttl_s):
            return None
        return get_redis().get(redis_key)
    except RedisError as exc:
        # Faisla: Redis down ho to admin ka kaam nahi rokna. Bas duplicate ka khatra hai, wo bhi chhota.
        logger.warning("idempotency check skipped, redis error: %r", exc)
        return None


def _release_key(key: str) -> None:
    # Kafka ne event nahi liya: key chhod do, taake admin ka retry asli koshish kare.
    # Na chhodte to retry par "duplicate, 202" milta, lekin product kabhi bana hi nahi hota.
    try:
        get_redis().delete(f"idem:{key}")
    except RedisError as exc:
        logger.warning("idempotency key release failed: %r", exc)


def create_product(data: ProductCreate, idempotency_key: str) -> CreateResult:
    product_id = new_product_id()

    existing_id = _claim_key(idempotency_key, product_id)
    if existing_id is not None:
        logger.info("duplicate request: key=%s product_id=%s", idempotency_key, existing_id)
        return CreateResult(product_id=existing_id, duplicate=True)

    now = datetime.now(UTC)
    product = Product(**data.model_dump(), product_id=product_id, created_at=now, updated_at=now)
    event = ProductEvent(event_type="product.created", product_id=product_id, payload=product)

    try:
        partition, offset = publish_event(event)
    except Exception:
        _release_key(idempotency_key)
        raise

    logger.info("product accepted: product_id=%s event_id=%s partition=%s offset=%s",
                product_id, event.event_id, partition, offset)
    return CreateResult(product_id=product_id, duplicate=False)
