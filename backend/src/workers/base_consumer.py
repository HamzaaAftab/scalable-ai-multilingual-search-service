"""Saare workers ka common hissa: Kafka se batch parho, kaam karo, PHIR commit.

Har worker (mongo-writer, typesense-indexer, redis-cacher) sirf do cheezein likhta hai:
  handle_batch(): asli kaam
  is_transient(): kaunsi ghalti temporary hai (retry se theek ho jayegi)
Kafka, commit, retry, DLQ aur shutdown ka logic ek hi jagah hai.

Ghalti ke do qisam, do ilaaj:
  Temporary (Mongo down): ruk kar retry. Phir bhi na chale to crash, commit NAHI. Restart par wahi batch.
  Permanent (kharab data): retry bekaar. Seedha DLQ, baqi queue chalti rahe.
"""

import logging
import signal
import time
from collections.abc import Callable
from datetime import UTC, datetime

from confluent_kafka import Consumer, Message
from pydantic import ValidationError

from common.config import settings
from common.schemas.event import ProductEvent

logger = logging.getLogger(__name__)

# (key, value, headers): DLQ mein bhejne wala function. Test mein fake, asal mein Kafka.
DlqPublisher = Callable[[bytes | None, bytes, dict[str, str]], None]


class BaseConsumer:
    group_id: str = ""  # Har worker ka apna group: har group apni speed se poora topic parhta hai
    batch_size: int = 500  # Ek dafa mein itne events: 1-1 likhne se 10 lakh mein ghante lagte
    max_retries: int = 3
    # Intezar 1s, 2s, 4s (exponential backoff): girti hui service par foran baar baar hathora na maarein.
    # Kul ~7s + har koshish ka timeout: Kafka ke max.poll.interval (5 min) se bohat kam, group se nahi nikalte.
    backoff_s: float = 1.0

    def handle_batch(self, events: list[ProductEvent]) -> None:
        raise NotImplementedError

    def is_transient(self, exc: Exception) -> bool:
        """True = temporary ghalti (retry karo). False = data ka masla (DLQ). Har worker apna batata hai."""
        return False

    def __init__(self, dlq_publish: DlqPublisher | None = None,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self._running = True
        if dlq_publish is None:
            # Yahan import: tests ko asli Kafka producer ki zarurat na pade
            from common.clients.kafka_client import publish_to_dlq
            dlq_publish = publish_to_dlq
        self._dlq_publish = dlq_publish
        self._sleep = sleep  # Test mein fake: 7 sec intezar ki jagah sirf yaad rakhta hai

    def _stop(self, signum: int, _frame: object) -> None:
        # Foran band nahi: abhi wala batch poora karo aur commit karo, phir niklo (graceful shutdown)
        logger.info("signal %s mila, current batch ke baad band ho raha hoon", signum)
        self._running = False

    # ---------- ek batch ka poora faisla (Kafka ke baghair test hota hai) ----------

    def process(self, messages: list[Message]) -> None:
        """Batch ko anjaam tak pohanchao. Exception aaye to caller commit NA kare."""
        pairs: list[tuple[Message, ProductEvent]] = []
        for msg in messages:
            try:
                pairs.append((msg, ProductEvent.model_validate_json(msg.value())))
            except ValidationError as exc:
                # Kharab JSON / price "abc": 100 dafa parho, 100 dafa kharab. Retry nahi, seedha DLQ.
                self._send_to_dlq(msg, "invalid_event", exc.errors()[0]["msg"])

        if not pairs:
            return
        try:
            self._handle_with_retry([event for _, event in pairs])
        except Exception as exc:
            if self.is_transient(exc):
                raise  # Retry khatam, service ab bhi down: crash, commit nahi. Restart par yahi batch.
            # Permanent ghalti, lekin 500 mein se kaunsa event? Ek ek kar ke chalao (isolation).
            logger.warning("batch fail (%s), %d events ek ek kar ke chala raha hoon", exc, len(pairs))
            self._handle_one_by_one(pairs)

    def _handle_with_retry(self, events: list[ProductEvent]) -> None:
        for attempt in range(self.max_retries + 1):
            try:
                self.handle_batch(events)
                return
            except Exception as exc:
                if not self.is_transient(exc) or attempt == self.max_retries:
                    raise
                wait = self.backoff_s * 2 ** attempt
                logger.warning("temporary ghalti (%s), %.0fs baad retry %d/%d",
                               exc, wait, attempt + 1, self.max_retries)
                self._sleep(wait)

    def _handle_one_by_one(self, pairs: list[tuple[Message, ProductEvent]]) -> None:
        # Tarteeb wahi: ek product ke events aage peeche na hon
        for msg, event in pairs:
            try:
                self._handle_with_retry([event])
            except Exception as exc:
                if self.is_transient(exc):
                    raise  # Beech mein service gir gayi: ye data ka qasoor nahi, DLQ nahi
                self._send_to_dlq(msg, "handler_failed", str(exc))

    def _send_to_dlq(self, msg: Message, reason: str, error: str) -> None:
        # Value bilkul asli bytes: masla theek karke wahi message dobara main topic mein daal saken (replay).
        # Kya hua, kahan se aaya: sab headers mein.
        headers = {
            "dlq.reason": reason,
            "dlq.error": error[:1000],  # Bohat lambi ghalti se message bhari na ho
            "dlq.group_id": self.group_id,  # Teen groups hain: kis worker ko masla hua
            "dlq.source.topic": msg.topic(),
            "dlq.source.partition": str(msg.partition()),
            "dlq.source.offset": str(msg.offset()),
            "dlq.failed_at": datetime.now(UTC).isoformat(),
        }
        # DLQ bhi fail ho to exception upar jayega: commit nahi hoga, event khoyega nahi
        self._dlq_publish(msg.key(), msg.value(), headers)
        logger.error("DLQ: reason=%s partition=%s offset=%s error=%s",
                     reason, msg.partition(), msg.offset(), error)

    # ---------- Kafka loop ----------

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._stop)  # Ctrl+C
        signal.signal(signal.SIGTERM, self._stop)  # docker stop

        consumer = Consumer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": self.group_id,
            # Khud commit karenge, kaam ke BAAD. Auto commit timer par hota hai: kaam se pehle bhi ho sakta hai,
            # phir crash par event hamesha ke liye kho jata.
            "enable.auto.commit": False,
            # Is group ka koi commit na ho (pehli dafa chala) to topic ke shuru se parho, kuch miss na ho
            "auto.offset.reset": "earliest",
        })
        consumer.subscribe([settings.kafka_topic])
        logger.info("%s shuru: topic=%s", self.group_id, settings.kafka_topic)

        try:
            while self._running:
                # Chhota timeout zaroori: consume() ke dauraan Ctrl+C nahi sunta, sirf iske wapas aane par
                messages = consumer.consume(num_messages=self.batch_size, timeout=1.0)
                if not messages:
                    continue

                good = []
                for msg in messages:
                    if msg.error():
                        logger.warning("kafka message error: %s", msg.error())
                    else:
                        good.append(msg)

                # Yahan exception aaye (Mongo down, retry khatam) to commit NAHI hoga. Restart par yahi batch.
                self.process(good)
                consumer.commit(asynchronous=False)  # asynchronous=False: Kafka ke "haan" ka intezar
        finally:
            # close() group chhodta hai, to doosra worker foran is ke partitions le leta hai.
            # Auto commit band hai, is liye close() khud koi commit nahi karta.
            consumer.close()
            logger.info("%s band", self.group_id)
