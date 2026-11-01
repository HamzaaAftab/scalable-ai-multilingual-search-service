from functools import lru_cache

from groq import Groq

from common.config import settings


@lru_cache(maxsize=1)  # Ek hi client poore app mein: connection pool reuse hota hai
def get_groq_client() -> Groq:
    return Groq(
        api_key=settings.groq_api_key,
        timeout=settings.groq_timeout_s,
        # SDK khud 429 / 5xx / network errors par backoff ke saath retry karta hai
        max_retries=settings.groq_max_retries,
    )
