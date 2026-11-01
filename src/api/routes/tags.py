"""HTTP layer: request lo, validate karo, service ko do, response wapas. Business logic yahan nahi."""

from fastapi import APIRouter
from pydantic import BaseModel, ConfigDict, Field

from api.services.tagging import suggest_tags

router = APIRouter(prefix="/tags", tags=["tags"])


class TagSuggestRequest(BaseModel):
    # "  Amul Milk  " -> "Amul Milk": khali spaces wala naam bhi pakda jayega (min_length)
    model_config = ConfigDict(str_strip_whitespace=True)

    name: str = Field(min_length=2, max_length=200)
    # Had zaroori hai: lamba text = zyada tokens = Groq ki rate limit jaldi khatam
    description: str = Field(default="", max_length=1000)


class TagSuggestResponse(BaseModel):
    tags: list[str]  # Unique flat list: Typesense mein yahi jayegi
    by_language: dict[str, list[str]]  # Admin UI zubaan ke hisaab se dikhayega
    source: str  # "llm" ya "fallback": fallback par UI admin ko khud likhne ko kahega
    model: str
    latency_ms: float


# "async def" nahi, sirf "def": Groq SDK ki call blocking hai (~2 sec). FastAPI "def" wale route ko
# threadpool mein chalata hai, to ek admin ka intezar doosri requests (jaise search) ko nahi rokta.
@router.post("/suggest")
def suggest(request: TagSuggestRequest) -> TagSuggestResponse:
    result = suggest_tags(request.name, request.description)
    # Groq ka asli error (result.error) sirf logs mein, client ko andar ki details nahi dikhani
    return TagSuggestResponse(
        tags=result.tags,
        by_language=result.by_language,
        source=result.source,
        model=result.model,
        latency_ms=round(result.latency_ms),
    )
