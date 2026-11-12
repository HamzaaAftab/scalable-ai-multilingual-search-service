"""BaseConsumer: retry aur DLQ ke rules. Kafka/Mongo ke baghair, fake messages se."""

from datetime import UTC, datetime

import pytest
from pymongo.errors import BulkWriteError, NetworkTimeout, ServerSelectionTimeoutError

from common.schemas.event import ProductEvent
from common.schemas.product import Product
from workers.base_consumer import BaseConsumer
from workers.mongo_writer import MongoWriter


class TransientError(Exception):
    """Test ke liye 'Mongo down' jaisi temporary ghalti."""


class FakeMessage:
    """confluent_kafka.Message jaisa: sirf wo methods jo BaseConsumer use karta hai."""

    def __init__(self, value: bytes, offset: int, partition: int = 0, key: bytes | None = b"p_1"):
        self._value, self._offset, self._partition, self._key = value, offset, partition, key

    def value(self) -> bytes:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def error(self):
        return None

    def topic(self) -> str:
        return "product-events"

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


def event_message(product_id: str, offset: int) -> FakeMessage:
    now = datetime.now(UTC)
    product = Product(name="Amul Taaza Toned Milk 1L", category="dairy", brand="Amul", price=270,
                      product_id=product_id, created_at=now, updated_at=now)
    event = ProductEvent(event_type="product.created", product_id=product_id, payload=product)
    return FakeMessage(event.model_dump_json().encode("utf-8"), offset, key=product_id.encode())


class FakeWorker(BaseConsumer):
    group_id = "test-worker"

    def __init__(self, behaviour=lambda events: None, dlq_fails: bool = False):
        self.dlq: list[tuple[bytes | None, bytes, dict[str, str]]] = []
        self.sleeps: list[float] = []
        self.calls: list[list[str]] = []
        self.behaviour = behaviour

        def dlq_publish(key, value, headers):
            if dlq_fails:
                raise RuntimeError("DLQ bhi down")
            self.dlq.append((key, value, headers))

        super().__init__(dlq_publish=dlq_publish, sleep=self.sleeps.append)

    def handle_batch(self, events: list[ProductEvent]) -> None:
        self.calls.append([e.product_id for e in events])
        self.behaviour(events)

    def is_transient(self, exc: Exception) -> bool:
        return isinstance(exc, TransientError)


def test_valid_batch_is_handled_once_without_dlq():
    worker = FakeWorker()
    worker.process([event_message("p_1", 0), event_message("p_2", 1)])
    assert worker.calls == [["p_1", "p_2"]]
    assert worker.dlq == []
    assert worker.sleeps == []


def test_broken_json_goes_to_dlq_with_original_bytes_and_context():
    worker = FakeWorker()
    worker.process([event_message("p_1", 0), FakeMessage(b"not json", 7, partition=2), event_message("p_2", 8)])

    assert worker.calls == [["p_1", "p_2"]]  # baqi events nahi ruke
    assert len(worker.dlq) == 1
    key, value, headers = worker.dlq[0]
    assert value == b"not json"  # asli bytes bilkul waise hi: baad mein dobara chala saken
    assert key == b"p_1"
    assert headers["dlq.reason"] == "invalid_event"
    assert headers["dlq.group_id"] == "test-worker"
    assert headers["dlq.source.topic"] == "product-events"
    assert headers["dlq.source.partition"] == "2"
    assert headers["dlq.source.offset"] == "7"
    assert headers["dlq.error"]


def test_price_abc_goes_to_dlq_without_any_retry():
    good = event_message("p_1", 0)
    bad = FakeMessage(good.value().replace(b'"price":270.0', b'"price":"abc"'), 1)
    assert bad.value() != good.value()  # sach mein price badla

    worker = FakeWorker()
    worker.process([bad])
    assert worker.calls == []  # handle_batch tak pohancha hi nahi
    assert [h["dlq.reason"] for _, _, h in worker.dlq] == ["invalid_event"]
    assert worker.sleeps == []


def test_transient_error_is_retried_with_backoff_then_succeeds():
    failures = iter([TransientError("mongo down"), TransientError("mongo down")])

    def behaviour(events):
        error = next(failures, None)
        if error:
            raise error

    worker = FakeWorker(behaviour)
    worker.process([event_message("p_1", 0)])
    assert len(worker.calls) == 3  # 2 fail, teesri dafa kamyab
    assert worker.sleeps == [1.0, 2.0]  # exponential backoff
    assert worker.dlq == []


def test_transient_error_that_never_recovers_raises_and_never_goes_to_dlq():
    def behaviour(events):
        raise TransientError("mongo 10 minute se down")

    worker = FakeWorker(behaviour)
    with pytest.raises(TransientError):
        worker.process([event_message("p_1", 0), event_message("p_2", 1)])
    assert len(worker.calls) == 4  # 1 asli + 3 retry
    assert worker.sleeps == [1.0, 2.0, 4.0]
    assert worker.dlq == []  # theek events ko DLQ mein phenkna ghalat hota


def test_poison_event_is_isolated_and_only_it_goes_to_dlq():
    def behaviour(events):
        if any(e.product_id == "p_bad" for e in events):
            raise ValueError("ye document kabhi save nahi hoga")

    worker = FakeWorker(behaviour)
    worker.process([event_message("p_1", 0), event_message("p_bad", 1), event_message("p_2", 2)])

    # Pehle poora batch fail, phir ek ek kar ke: sirf p_bad fail
    assert worker.calls == [["p_1", "p_bad", "p_2"], ["p_1"], ["p_bad"], ["p_2"]]
    assert len(worker.dlq) == 1
    key, _, headers = worker.dlq[0]
    assert key == b"p_bad"
    assert headers["dlq.reason"] == "handler_failed"
    assert headers["dlq.source.offset"] == "1"
    assert worker.sleeps == []  # permanent ghalti par intezar bekaar


def test_transient_error_during_isolation_still_raises():
    state = {"mongo_up": True}

    def behaviour(events):
        if len(events) > 1:
            state["mongo_up"] = False  # batch ke baad Mongo gir gaya
            raise ValueError("poison")
        if not state["mongo_up"]:
            raise TransientError("mongo down")

    worker = FakeWorker(behaviour)
    with pytest.raises(TransientError):
        worker.process([event_message("p_1", 0), event_message("p_2", 1)])
    assert worker.dlq == []


def test_dlq_failure_raises_so_batch_is_not_committed():
    worker = FakeWorker(dlq_fails=True)
    with pytest.raises(RuntimeError):
        worker.process([FakeMessage(b"not json", 0)])


def test_mongo_writer_knows_which_errors_are_temporary():
    writer = MongoWriter(dlq_publish=lambda *a: None, sleep=lambda s: None)
    assert writer.is_transient(ServerSelectionTimeoutError("mongo nahi mila"))
    assert writer.is_transient(NetworkTimeout("network slow"))
    assert not writer.is_transient(BulkWriteError({"writeErrors": [{"errmsg": "document too large"}]}))
    assert not writer.is_transient(ValueError("kuch aur"))
