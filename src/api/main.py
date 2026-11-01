"""FastAPI app: saare routes yahan jud-te hain.

Chalane ka tareeqa (backend/ folder se):  uv run fastapi dev src/api/main.py
Docs (browser mein try karo):              http://127.0.0.1:8000/docs
"""

import logging

from fastapi import FastAPI

from api.routes import tags

# Abhi simple logs. Structured (JSON) logs baad mein common/logging.py mein aayenge.
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastAPI(title="Blinkit Search API")
app.include_router(tags.router)


@app.get("/health")
def health() -> dict[str, str]:
    # Abhi sirf "process zinda hai". Kafka aa jaye to yahan uska check bhi hoga.
    return {"status": "ok"}
