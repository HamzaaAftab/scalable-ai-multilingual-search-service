"""Product naam se multilingual search tags banana (Groq LLM).

Flow: prompt -> Groq (strict JSON, har zubaan ki alag list) -> validate -> saaf karo -> 10 zubaanen x 4 tags.
Groq fail ho to exception nahi, khali tags + reason: admin khud tags likh lega.
"""

import json
import logging
import re
import time
from dataclasses import dataclass, field

from groq import APIError

from common.clients.groq_client import get_groq_client
from common.config import settings

logger = logging.getLogger(__name__)

# Single source of truth: nayi zubaan add karni ho to sirf yahan ek line. Prompt aur schema dono isi se bante hain.
LANGUAGES: dict[str, str] = {
    "english": "English",
    "roman_urdu": "Urdu/Hindi words in English letters, the way people type them "
                  '("doodh", not "milk"; "sabun", not "soap"). Include common spelling variants '
                  '("doodh", "dudh"). Never copy the English tags.',
    "urdu": "Urdu script",
    "hindi": "Hindi in Devanagari script",
    "punjabi": "Punjabi in Shahmukhi script (as written in Pakistan)",
    "sindhi": "Sindhi in Sindhi Arabic script",
    "pashto": "Pashto script",
    "italian": "Italian",
    "spanish": "Spanish",
    "german": "German",
}
TAGS_PER_LANGUAGE = 4  # 10 zubaanen x 4 = 40 tags
MAX_TAG_LENGTH = 40  # Lambe "tags" asal mein jumle hote hain, search ke kaam ke nahi
# Jawab ki had. Na batao to Groq bada andaza laga kar rate limit (OTPM) se pehle hi request rok deta hai.
# 40 tags (kai scripts mein) ka JSON ~700-900 tokens: 1200 mein jagah hai, faltu reservation nahi.
MAX_OUTPUT_TOKENS = 1200

# Size/weight wale tags ("950g", "1 litre", "52 ग्राम", "125 گرام", "1 litro"): size alag field hai, tag nahi
SIZE_PATTERN = re.compile(
    r"\d+(\.\d+)?\s*(g|gm|gms|gram|grams|gramm|grammi|gramos|kg|ml|l|ltr|litre|liter|litro|litros|pcs|pack"
    r"|ग्राम|किलो|लीटर|मिली|گرام|کلو|لیٹر)\b",
    re.IGNORECASE,
)

# Doosre brands ke product names jo hum jaante hain. Prompt mana karta hai phir bhi model de deta hai,
# is liye code se guarantee. Naya collision mile to yahan add karo.
BLOCKED_TAGS = {"dairy milk"}

# Few-shot example: jaan boojh kar test products (Amul, Tapal...) se ALAG product, taake model ratta na maare
EXAMPLE_PRODUCT = "National Pink Salt 800g"
EXAMPLE_TAGS = {
    "english": ["pink salt", "rock salt", "himalayan salt", "salt"],
    "roman_urdu": ["namak", "gulabi namak", "lahori namak", "sendha namak"],
    "urdu": ["نمک", "گلابی نمک", "لاہوری نمک", "نیشنل نمک"],
    "hindi": ["नमक", "गुलाबी नमक", "सेंधा नमक", "नेशनल नमक"],
    "punjabi": ["لون", "گلابی لون", "سیندھا لون", "نیشنل لون"],
    "sindhi": ["لوڻ", "گلابي لوڻ", "نيشنل لوڻ", "هماليائي لوڻ"],
    "pashto": ["مالګه", "ګلابي مالګه", "نېشنل مالګه", "هماليايي مالګه"],
    "italian": ["sale rosa", "sale dell'himalaya", "salgemma", "sale"],
    "spanish": ["sal rosada", "sal del himalaya", "sal de roca", "sal"],
    "german": ["rosa salz", "himalaya salz", "steinsalz", "salz"],
}

_language_lines = "\n".join(f"- {key}: {desc}" for key, desc in LANGUAGES.items())
_example_json = json.dumps(EXAMPLE_TAGS, ensure_ascii=False)  # ensure_ascii=False: Urdu/Hindi asli shakal mein

SYSTEM_PROMPT = f"""You create search tags for a grocery e-commerce app. Shoppers search in many languages
and scripts, and your tags are how they find the product.

Goal: the product name is ALREADY searchable in English letters. Tags must add the OTHER words a
shopper might type: the product type, category, synonyms, and translations.

Give exactly {TAGS_PER_LANGUAGE} tags in EVERY language:
{_language_lines}

Example for "{EXAMPLE_PRODUCT}":
{_example_json}

Rules:
1. Facts only: use only what the product name/description says. NEVER invent attributes
   (organic, low fat, sugar free, whole, flavours, colours, ingredients).
2. Correct translations with correct spelling. If unsure, use a simpler word you are sure of.
3. Brand and variant names: in non-Latin scripts you MAY write the brand phonetically (National -> نیشنل).
   Never translate variant names word by word ("Original", "Taaza", "Danedar" stay as they sound).
4. Never use another brand's product name as a tag ("dairy milk" is a Cadbury chocolate).
5. No sizes, weights or quantities (1l, 950g, 52 gram).
6. Every tag adds a new word. No combinations that only repeat other tags.
7. No generic single words that also mean a different product ("potato chips", not "potato").
   A single word is fine when it IS the product type ("chai", "salt").
8. Each tag is 1-3 words. Latin-script tags in lowercase."""

# Strict mode ki shart: saare fields "required" aur additionalProperties false.
# Har zubaan required hai, is liye model koi zubaan chhod hi nahi sakta.
TAGS_SCHEMA = {
    "type": "object",
    "properties": {lang: {"type": "array", "items": {"type": "string"}} for lang in LANGUAGES},
    "required": list(LANGUAGES),
    "additionalProperties": False,
}


@dataclass
class TagResult:
    tags: list[str]  # Flat list: Typesense mein yahi jayegi
    by_language: dict[str, list[str]]  # Admin UI mein zubaan ke hisaab se dikhane ke liye
    source: str  # "llm" ya "fallback": admin UI ko pata chale AI ne diye ya nahi
    model: str
    latency_ms: float
    total_tokens: int = 0
    error: str | None = None
    removed: list[str] = field(default_factory=list)  # Saaf karte waqt kya hataya (debugging ke liye)


def clean_tags(raw_tags: list[str], removed: list[str]) -> list[str]:
    """Ek zubaan ke tags saaf karo. LLM ke output par bharosa nahi."""
    seen: set[str] = set()
    cleaned: list[str] = []
    for tag in raw_tags:
        if not isinstance(tag, str):
            continue
        tag = " ".join(tag.split()).lower()  # extra spaces hatao, lowercase (Hindi/Urdu par asar nahi)
        if not tag or tag in seen:
            continue
        if len(tag) > MAX_TAG_LENGTH or SIZE_PATTERN.search(tag) or tag in BLOCKED_TAGS:
            removed.append(tag)
            continue
        seen.add(tag)
        cleaned.append(tag)
    return cleaned[:TAGS_PER_LANGUAGE]


def unique_tags(by_language: dict[str, list[str]]) -> list[str]:
    """Search index ke liye flat list: "صابن" Urdu aur Punjabi dono mein ho to bhi ek dafa kaafi hai."""
    return list(dict.fromkeys(tag for tags in by_language.values() for tag in tags))


def _build_request_options(model: str) -> dict:
    """Har model family ki apni settings hain. Ghalat value bhejo to Groq 400 deta hai."""
    if model.startswith("openai/gpt-oss"):
        # Kam "sochna" = kam tokens = kam rate limit kharch
        return {"reasoning_effort": "low", "include_reasoning": False}
    if model.startswith("qwen/"):
        # Tags ke liye sochne ki zarurat nahi: reasoning band (Qwen 3.8 ka default bhi yahi hai, saaf likh diya)
        return {"reasoning_effort": "none"}
    return {}


def _fallback(model: str, start: float, exc: Exception) -> TagResult:
    latency_ms = (time.perf_counter() - start) * 1000
    logger.warning("tag suggestion failed: model=%s error=%r", model, exc)
    return TagResult(
        tags=[],
        by_language={lang: [] for lang in LANGUAGES},
        source="fallback",
        model=model,
        latency_ms=latency_ms,
        error=repr(exc),
    )


def suggest_tags(name: str, description: str = "", model: str | None = None) -> TagResult:
    model = model or settings.groq_model
    user_message = f"Product name: {name}"
    if description:
        user_message += f"\nDescription: {description}"

    start = time.perf_counter()
    try:
        completion = get_groq_client().chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {"name": "product_tags", "strict": True, "schema": TAGS_SCHEMA},
            },
            temperature=0.2,  # Kam creativity: tags har dafa milte julte aayein, hallucination kam
            max_completion_tokens=MAX_OUTPUT_TOKENS,
            **_build_request_options(model),
        )
        latency_ms = (time.perf_counter() - start) * 1000
        data = json.loads(completion.choices[0].message.content)

        removed: list[str] = []
        by_language = {lang: clean_tags(data[lang], removed) for lang in LANGUAGES}
    except (APIError, json.JSONDecodeError, KeyError, TypeError) as exc:
        # Kuch bhi ho (timeout, 429, kharab JSON): pipeline nahi rukni chahiye
        return _fallback(model, start, exc)

    return TagResult(
        tags=unique_tags(by_language),
        by_language=by_language,
        source="llm",
        model=model,
        latency_ms=latency_ms,
        total_tokens=completion.usage.total_tokens if completion.usage else 0,
        removed=removed,
    )
