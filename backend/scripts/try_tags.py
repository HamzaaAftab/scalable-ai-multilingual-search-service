"""Tag generator ko asli products par chala kar dekho.

Chalane ka tareeqa (backend/ folder se):
    uv run python scripts/try_tags.py                        # .env wala model
    uv run python scripts/try_tags.py openai/gpt-oss-120b    # koi aur model
"""

import sys

from api.services.tagging import suggest_tags

# Windows terminal default "cp1252" Hindi/Urdu print nahi kar sakta
sys.stdout.reconfigure(encoding="utf-8")

PRODUCTS = [
    "Amul Taaza Toned Milk 1L",
    "Tapal Danedar Chai 950g",
    "Lay's Masala Chips 52g",
    "Dettol Original Soap 125g",
]

model = sys.argv[1] if len(sys.argv) > 1 else None
total_ms = total_tokens = 0

for name in PRODUCTS:
    result = suggest_tags(name, model=model)
    total_ms += result.latency_ms
    total_tokens += result.total_tokens
    shown = sum(len(tags) for tags in result.by_language.values())
    print(f"\n=== {name}  [{result.source} | {result.latency_ms:.0f} ms | {result.total_tokens} tokens"
          f" | admin ko {shown} tags | Typesense mein {len(result.tags)} unique]")
    if result.error:
        print("  error:", result.error)
    for lang, tags in result.by_language.items():
        print(f"  {lang:<11}: {', '.join(tags)}")
    if result.removed:
        print(f"  {'hataye':<11}: {', '.join(result.removed)}")

print(f"\nModel: {result.model} | avg {total_ms / len(PRODUCTS):.0f} ms | avg {total_tokens / len(PRODUCTS):.0f} tokens")
