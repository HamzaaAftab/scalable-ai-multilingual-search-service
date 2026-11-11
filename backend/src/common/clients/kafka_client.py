"""Kafka producer: event ko "product-events" topic mein pakka save karwana."""

from functools import lru_cache

from confluent_kafka import KafkaError, Message, Producer

from common.config import settings
from common.schemas.event import ProductEvent


class KafkaPublishError(Exception):
    """Kafka ne event save nahi kiya (down, timeout). API isko 503 mein badlegi."""


@lru_cache(maxsize=1)  # Ek hi producer poore app mein: connection aur andar ka buffer reuse hota hai
def get_producer() -> Producer:
    timeout_ms = int(settings.kafka_publish_timeout_s * 1000)
    return Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": "blinkit-api",
        # Network ki wajah se retry ho to bhi Kafka mein event do baar nahi likha jayega
        "enable.idempotence": True,
        # "Save ho gaya" tab, jab saare replicas ne likh liya ho (idempotence ke saath ye waise bhi lazmi hai)
        "acks": "all",
        # librdkafka khud bhi utni hi der koshish kare jitni der hum intezar karte hain. Warna hum admin ko
        # "fail" bata dein aur event baad mein chupke se chala jaye: admin dobara dabaye, product do baar.
        "delivery.timeout.ms": timeout_ms,
    })


def publish_event(event: ProductEvent) -> tuple[int, int]:
    """Event bhejo aur Kafka ke "haan" ka intezar karo. Wapas: (partition, offset)."""
    result: dict[str, KafkaError | Message | None] = {}

    def on_delivery(err: KafkaError | None, msg: Message) -> None:
        # Ye produce() ke andar nahi, baad mein flush() ke andar chalta hai, jab Kafka jawab de
        result["err"] = err
        result["msg"] = msg

    producer = get_producer()
    producer.produce(
        settings.kafka_topic,
        # Key = product_id: ek product ke saare events ek hi partition mein, yani sahi order mein
        key=event.product_id.encode("utf-8"),
        value=event.model_dump_json().encode("utf-8"),
        on_delivery=on_delivery,
    )
    # produce() sirf andar ke buffer mein daalta hai. flush() tab tak rukta hai jab tak Kafka jawab na de.
    producer.flush(settings.kafka_publish_timeout_s)

    if "msg" not in result:
        raise KafkaPublishError(f"Kafka ne {settings.kafka_publish_timeout_s}s mein jawab nahi diya")
    if result["err"] is not None:
        raise KafkaPublishError(str(result["err"]))
    return result["msg"].partition(), result["msg"].offset()
