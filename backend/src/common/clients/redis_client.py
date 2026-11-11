from functools import lru_cache

from redis import Redis

from common.config import settings


@lru_cache(maxsize=1)  # Ek hi client: andar connection pool hai, har request par naya connection nahi khulta
def get_redis() -> Redis:
    return Redis.from_url(
        settings.redis_url,
        # Dono timeouts zaroori: connect na ho, ya connect ho kar jawab na aaye. Bina timeout ke request atak jati.
        socket_connect_timeout=settings.redis_timeout_s,
        socket_timeout=settings.redis_timeout_s,
        decode_responses=True,  # bytes ki jagah str wapas: b"p_abc" nahi, "p_abc"
    )
