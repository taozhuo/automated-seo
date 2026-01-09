from dataclasses import dataclass, field, asdict
from typing import Optional
import json


@dataclass
class Post:
    id: int
    username: str
    content_raw: str
    content_html: str
    created_at: str
    likes: int = 0
    post_number: int = 1

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Topic:
    id: int
    title: str
    slug: str
    url: str
    category_id: int
    category_name: str
    created_at: str
    last_posted_at: Optional[str] = None
    views: int = 0
    reply_count: int = 0
    like_count: int = 0
    tags: list[str] = field(default_factory=list)
    posts: list[Post] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["posts"] = [p.to_dict() if isinstance(p, Post) else p for p in self.posts]
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)


@dataclass
class Category:
    id: int
    name: str
    slug: str
    topic_count: int = 0
    description: str = ""

    def to_dict(self) -> dict:
        return asdict(self)
