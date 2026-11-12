"""mongo-writer: product events ko MongoDB (source of truth) mein likhta hai.

Chalane ka tareeqa (backend/ folder se):  uv run python -m workers.mongo_writer
"""

import logging
import time

from pymongo import DeleteOne, ReplaceOne
from pymongo.errors import ConnectionFailure, PyMongoError

from common.clients.mongo_client import products_collection
from common.schemas.event import ProductEvent
from workers.base_consumer import BaseConsumer

logger = logging.getLogger(__name__)


def to_mongo_operation(event: ProductEvent) -> DeleteOne | ReplaceOne:
    # _id = product_id: alag index ki zarurat nahi, aur Mongo khud ek id par ek hi document rakhta hai
    if event.event_type == "product.deleted":
        return DeleteOne({"_id": event.product_id})  # Jo hai hi nahi uska delete: kuch nahi hota (idempotent)

    doc = event.payload.model_dump(exclude={"product_id"})  # datetime asli date hi rahe, string nahi (sort ke liye)
    doc["_id"] = event.product_id
    if doc["image_url"] is not None:
        doc["image_url"] = str(doc["image_url"])  # Pydantic ka Url object Mongo nahi samajhta
    # Upsert: nahi hai to bana do, hai to poora naya version rakh do. Event 10 dafa aaye, result ek hi document.
    return ReplaceOne({"_id": event.product_id}, doc, upsert=True)


class MongoWriter(BaseConsumer):
    group_id = "mongo-writer"

    def is_transient(self, exc: Exception) -> bool:
        # ConnectionFailure: Mongo tak pohanch hi nahi sake (down, network). Iske andar AutoReconnect,
        # NetworkTimeout, ServerSelectionTimeoutError bhi aate hain.
        # exc.timeout: Mongo ne waqt par jawab nahi diya (timeoutMS=5000).
        # Baqi (BulkWriteError: document ghalat/bohat bara): data ka masla, retry se kuch nahi badlega.
        return isinstance(exc, ConnectionFailure) or (isinstance(exc, PyMongoError) and exc.timeout)

    def handle_batch(self, events: list[ProductEvent]) -> None:
        start = time.perf_counter()
        # ordered=True: ek hi product ke "created" ke baad "deleted" ho to isi tarteeb mein chalein
        result = products_collection().bulk_write([to_mongo_operation(e) for e in events], ordered=True)
        logger.info("batch=%d upserted(naye)=%d modified(badle)=%d matched=%d deleted=%d in %.0f ms",
                    len(events), result.upserted_count, result.modified_count, result.matched_count,
                    result.deleted_count, (time.perf_counter() - start) * 1000)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    MongoWriter().run()
