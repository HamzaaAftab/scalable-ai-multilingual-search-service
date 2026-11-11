"""Product ki shakal. API aur workers dono yahi use karte hain, taake dono kabhi alag shakal na samjhein."""

from uuid import uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, HttpUrl


def new_product_id() -> str:
    # Backend banata hai, kyunke event Mongo se PEHLE Kafka mein jata hai, aur Kafka key ke liye id abhi chahiye.
    # Random id ka faida: Kafka ke partitions mein products barabar batt-te hain.
    return f"p_{uuid4().hex}"


class ProductCreate(BaseModel):
    """Jo admin bhejta hai. Is mein id aur timestamps nahi: wo backend khud lagata hai, client par bharosa nahi."""

    model_config = ConfigDict(str_strip_whitespace=True)

    name: str = Field(min_length=2, max_length=200)
    description: str = Field(default="", max_length=1000)
    category: str = Field(min_length=1, max_length=100)
    brand: str = Field(min_length=1, max_length=100)
    # allow_inf_nan=False: warna "Infinity" bhi gt=0 pass kar jata, aur search index mein kachra jata
    price: float = Field(gt=0, allow_inf_nan=False)
    image_url: HttpUrl | None = None
    tags: list[str] = Field(default_factory=list, max_length=100)  # Admin 40 AI tags ke upar apne bhi jod sakta hai


class Product(ProductCreate):
    """Poora record: yahi Kafka se guzar kar Mongo aur Typesense tak jata hai."""

    product_id: str = Field(min_length=1)
    # AwareDatetime: timezone ke baghair waqt reject. Warna "10:00" kis mulk ka hai, koi nahi jaanta.
    created_at: AwareDatetime
    updated_at: AwareDatetime
