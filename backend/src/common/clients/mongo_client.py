from functools import lru_cache

from pymongo import MongoClient
from pymongo.collection import Collection

from common.config import settings


@lru_cache(maxsize=1)  # Ek hi client: pymongo andar connection pool rakhta hai
def get_mongo() -> MongoClient:
    if not settings.mongo_url:
        raise RuntimeError("MONGO_URL missing: backend/.env mein likho")
    # timeoutMS: har operation (connect + likhna) 5 sec mein ho, warna error. Worker atakne ki jagah retry karega.
    # tz_aware: Mongo UTC mein rakhta hai, lekin pymongo by default timezone hata kar deta hai. Hamara schema
    # (AwareDatetime) bina timezone wala waqt reject karta hai, is liye wapas bhi UTC ke saath chahiye.
    return MongoClient(settings.mongo_url, timeoutMS=5000, tz_aware=True, appname="blinkit-backend")


def products_collection() -> Collection:
    return get_mongo()[settings.mongo_db]["products"]
