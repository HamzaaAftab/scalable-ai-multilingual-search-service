"""Saari settings ek jagah, env vars se. Code mein koi value hardcode nahi."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)  # frozen: chalte program mein koi galti se setting na badal de
class Settings:
    groq_api_key: str
    groq_model: str
    # Admin form par intezar kar raha hai: der tak atakne se tez fail hona behtar hai
    groq_timeout_s: float
    groq_max_retries: int
    kafka_bootstrap_servers: str
    kafka_topic: str
    # Admin ko 202 tabhi milega jab Kafka save kar le. Kafka itni der mein jawab na de to 503: atka hua form behtar nahi.
    kafka_publish_timeout_s: float
    redis_url: str
    # Redis RAM mein hai, 1-2 ms mein jawab deta hai. Itni der mein na de to masla hai: intezar mat karo.
    redis_timeout_s: float
    # Itni der tak wahi Idempotency-Key dobara aaye to wahi product_id wapas milega
    idempotency_ttl_s: int


def load_settings() -> Settings:
    return Settings(
        groq_api_key=os.environ["GROQ_API_KEY"],  # na mile to foran crash: chupi ghalti se behtar
        groq_model=os.getenv("GROQ_MODEL", "openai/gpt-oss-120b"),
        groq_timeout_s=float(os.getenv("GROQ_TIMEOUT_S", "8")),
        groq_max_retries=int(os.getenv("GROQ_MAX_RETRIES", "1")),
        # PC se "localhost:9092", Docker container ke andar se "kafka:29092"
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "product-events"),
        kafka_publish_timeout_s=float(os.getenv("KAFKA_PUBLISH_TIMEOUT_S", "5")),
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        redis_timeout_s=float(os.getenv("REDIS_TIMEOUT_S", "0.5")),
        idempotency_ttl_s=int(os.getenv("IDEMPOTENCY_TTL_S", "86400")),  # 24 ghante
    )


settings = load_settings()
