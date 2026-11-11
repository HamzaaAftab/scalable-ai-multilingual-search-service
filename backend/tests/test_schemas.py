"""Event schema: API aur workers ke beech ka contract. Yahan ghalti ho to poori pipeline mein kachra jata hai."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from common.schemas.event import ProductEvent
from common.schemas.product import Product, ProductCreate, new_product_id

VALID_INPUT = {"name": "Amul Taaza Toned Milk 1L", "category": "dairy", "brand": "Amul", "price": 270}


def make_product(product_id: str = "p_1") -> Product:
    now = datetime.now(UTC)
    return Product(**VALID_INPUT, product_id=product_id, created_at=now, updated_at=now)


def test_product_ids_are_unique():
    ids = {new_product_id() for _ in range(10_000)}
    assert len(ids) == 10_000


@pytest.mark.parametrize(
    "bad_field",
    [
        {"name": "   "},  # spaces hat kar khali
        {"price": 0},
        {"price": -5},
        {"price": "abc"},
        {"price": float("inf")},
        {"image_url": "not-a-url"},
    ],
)
def test_bad_product_input_is_rejected(bad_field):
    with pytest.raises(ValidationError):
        ProductCreate(**{**VALID_INPUT, **bad_field})


def test_product_needs_timezone():
    with pytest.raises(ValidationError):
        Product(**VALID_INPUT, product_id="p_1", created_at=datetime(2026, 1, 1), updated_at=datetime(2026, 1, 1))


def test_event_survives_kafka_round_trip():
    # Kafka sirf bytes jaanta hai: JSON ban kar jaye aur wapas wahi event bane, Urdu/Hindi tags samait
    product = make_product().model_copy(update={"tags": ["doodh", "دودھ", "दूध"]})
    event = ProductEvent(event_type="product.created", product_id="p_1", payload=product)

    restored = ProductEvent.model_validate_json(event.model_dump_json().encode("utf-8"))

    assert restored == event


def test_created_event_needs_payload():
    with pytest.raises(ValidationError):
        ProductEvent(event_type="product.created", product_id="p_1")


def test_deleted_event_needs_no_payload():
    assert ProductEvent(event_type="product.deleted", product_id="p_1").payload is None


def test_payload_id_must_match_event_id():
    with pytest.raises(ValidationError):
        ProductEvent(event_type="product.updated", product_id="p_1", payload=make_product("p_2"))


def test_unknown_event_type_is_rejected():
    with pytest.raises(ValidationError):
        ProductEvent(event_type="product.exploded", product_id="p_1")
