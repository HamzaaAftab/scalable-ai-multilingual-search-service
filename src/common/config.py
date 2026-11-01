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


def load_settings() -> Settings:
    return Settings(
        groq_api_key=os.environ["GROQ_API_KEY"],  # na mile to foran crash: chupi ghalti se behtar
        groq_model=os.getenv("GROQ_MODEL", "openai/gpt-oss-120b"),
        groq_timeout_s=float(os.getenv("GROQ_TIMEOUT_S", "8")),
        groq_max_retries=int(os.getenv("GROQ_MAX_RETRIES", "1")),
    )


settings = load_settings()
