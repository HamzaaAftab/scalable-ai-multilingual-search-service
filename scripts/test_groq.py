"""Phase 1 smoke test: kya Groq se baat ho rahi hai? Kitna waqt lagta hai? Kitni limit baqi hai?

Chalane ka tareeqa (backend/ folder se):  uv run python scripts/test_groq.py
"""

import os
import sys
import time

from dotenv import load_dotenv
from groq import Groq, APIStatusError, APIConnectionError

load_dotenv()  # backend/.env se GROQ_API_KEY aur GROQ_MODEL env mein aa jate hain

api_key = os.getenv("GROQ_API_KEY", "")
model = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b")

if not api_key or api_key == "paste-your-key-here":
    sys.exit("GROQ_API_KEY missing: backend/.env mein apni key paste karo.")

# Timeout 60s (default) bohat zyada hai. Admin intezar nahi karega, is liye 10s.
# Retries yahan 0 hain taake asli error seedha nazar aaye.
client = Groq(api_key=api_key, timeout=10.0, max_retries=0)

try:
    # 1) Kaunse models available hain
    model_ids = sorted(m.id for m in client.models.list().data)
    print(f"Available models ({len(model_ids)}):")
    for mid in model_ids:
        print("  -", mid)

    # 2) Ek chhoti call, aur time napo
    start = time.perf_counter()
    # with_raw_response: jawab ke saath HTTP headers bhi milte hain (rate limit wahin hoti hai)
    raw = client.chat.completions.with_raw_response.create(
        model=model,
        messages=[
            {"role": "system", "content": "Reply with only a comma-separated list of search tags."},
            {"role": "user", "content": "Product: Amul Milk 1L. Give tags in English, Roman Urdu and Hindi."},
        ],
    )
    elapsed_ms = (time.perf_counter() - start) * 1000
    completion = raw.parse()

    print(f"\nModel: {model}")
    print(f"Answer: {completion.choices[0].message.content.strip()}")
    print(f"Time: {elapsed_ms:.0f} ms | tokens: {completion.usage.total_tokens}")

    # 3) Rate limit: kitni requests/tokens baqi hain
    h = raw.headers
    print("\nRate limit:")
    print(f"  requests left: {h.get('x-ratelimit-remaining-requests')} / {h.get('x-ratelimit-limit-requests')}")
    print(f"  tokens left:   {h.get('x-ratelimit-remaining-tokens')} / {h.get('x-ratelimit-limit-tokens')}")

except APIConnectionError as e:
    sys.exit(f"Groq tak pahunch nahi paye (internet/timeout): {e}")
except APIStatusError as e:
    sys.exit(f"Groq ne error diya: HTTP {e.status_code}: {e.message}")
