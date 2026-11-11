"""Kafka producer ko chala kar dekho: event kis partition mein gaya, kitna waqt laga.

Chalane ka tareeqa (backend/ folder se):  uv run python scripts/try_publish.py

"product.deleted" events bhejte hain (aise ids ke jo hain hi nahi), taake baad mein workers
inhein parhein to koi nakli product na bane. Delete ka delete bhi idempotent hai: kuch nahi hoga.
"""

import time

from common.clients.kafka_client import publish_event
from common.schemas.event import ProductEvent

# Har id 3 dafa: dekhna hai ke ek id hamesha ek hi partition mein jati hai
PRODUCT_IDS = ["p_demo_amul", "p_demo_tapal", "p_demo_lays", "p_demo_dettol"]

for product_id in PRODUCT_IDS:
    for _ in range(3):
        start = time.perf_counter()
        partition, offset = publish_event(ProductEvent(event_type="product.deleted", product_id=product_id))
        ms = (time.perf_counter() - start) * 1000
        print(f"{product_id:<14} -> partition {partition}, offset {offset:<4} ({ms:.0f} ms)")
