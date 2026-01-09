import json
import os
from pathlib import Path
from typing import Generator, Optional

from tqdm import tqdm

import config
from .client import DiscourseClient
from .models import Category, Topic, Post


class DevForumScraper:
    def __init__(self, client: Optional[DiscourseClient] = None):
        self.client = client or DiscourseClient()
        self.data_dir = Path(config.DATA_DIR)
        self.state_file = Path(config.STATE_FILE)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._state = self._load_state()

    def _load_state(self) -> dict:
        if self.state_file.exists():
            with open(self.state_file) as f:
                return json.load(f)
        return {"scraped_topics": [], "last_category": None, "last_page": 0}

    def _save_state(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, "w") as f:
            json.dump(self._state, f)

    def get_categories(self, include_subcategories: bool = True) -> list[Category]:
        data = self.client.get_categories()
        if not data:
            return []

        categories = []
        for cat_list in data.get("category_list", {}).get("categories", []):
            categories.append(Category(
                id=cat_list["id"],
                name=cat_list["name"],
                slug=cat_list["slug"],
                topic_count=cat_list.get("topic_count", 0),
                description=cat_list.get("description_text", ""),
            ))
            # Include subcategories
            if include_subcategories:
                for subcat in cat_list.get("subcategory_list", []):
                    categories.append(Category(
                        id=subcat["id"],
                        name=subcat["name"],
                        slug=subcat["slug"],
                        topic_count=subcat.get("topic_count", 0),
                        description=subcat.get("description_text", ""),
                    ))
        return categories

    def scrape_pain_points(self, limit: Optional[int] = None) -> int:
        """Scrape only pain point categories defined in config."""
        categories = [
            Category(
                id=cat["id"],
                name=cat["name"],
                slug=cat["slug"],
            )
            for cat in config.PAIN_POINT_CATEGORIES
        ]
        return self._scrape_categories(categories, limit)

    def get_category_topic_ids(
        self, category: Category, max_pages: int = 100
    ) -> Generator[int, None, None]:
        for page in range(max_pages):
            data = self.client.get_category_topics(category.slug, category.id, page)
            if not data:
                break

            topics = data.get("topic_list", {}).get("topics", [])
            if not topics:
                break

            for topic in topics:
                topic_id = topic["id"]
                if topic_id not in self._state["scraped_topics"]:
                    yield topic_id

    def scrape_topic(self, topic_id: int, category_name: str = "") -> Optional[Topic]:
        data = self.client.get_topic(topic_id)
        if not data:
            return None

        posts = []
        for post_data in data.get("post_stream", {}).get("posts", []):
            posts.append(Post(
                id=post_data["id"],
                username=post_data.get("username", ""),
                content_raw=post_data.get("raw", ""),
                content_html=post_data.get("cooked", ""),
                created_at=post_data.get("created_at", ""),
                likes=post_data.get("like_count", 0),
                post_number=post_data.get("post_number", 1),
            ))

        topic = Topic(
            id=data["id"],
            title=data.get("title", ""),
            slug=data.get("slug", ""),
            url=f"{config.BASE_URL}/t/{data.get('slug', '')}/{data['id']}",
            category_id=data.get("category_id", 0),
            category_name=category_name,
            created_at=data.get("created_at", ""),
            last_posted_at=data.get("last_posted_at"),
            views=data.get("views", 0),
            reply_count=data.get("reply_count", 0),
            like_count=data.get("like_count", 0),
            tags=data.get("tags", []),
            posts=posts,
        )
        return topic

    def save_topic(self, topic: Topic) -> Path:
        category_dir = self.data_dir / topic.category_name.lower().replace(" ", "_")
        category_dir.mkdir(parents=True, exist_ok=True)
        filepath = category_dir / f"{topic.id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(topic.to_json())
        return filepath

    def _scrape_categories(
        self, categories: list[Category], limit: Optional[int] = None
    ) -> int:
        """Internal method to scrape a list of categories."""
        if not categories:
            print("No categories found")
            return 0

        print(f"Found {len(categories)} categories to scrape")
        total_scraped = 0

        for category in categories:
            print(f"\nScraping category: {category.name} ({category.topic_count} topics)")
            topic_ids = list(self.get_category_topic_ids(category))

            if limit:
                remaining = limit - total_scraped
                if remaining <= 0:
                    break
                topic_ids = topic_ids[:remaining]

            for topic_id in tqdm(topic_ids, desc=category.name):
                topic = self.scrape_topic(topic_id, category.name)
                if topic:
                    self.save_topic(topic)
                    self._state["scraped_topics"].append(topic_id)
                    total_scraped += 1

                    if total_scraped % 10 == 0:
                        self._save_state()

        self._save_state()
        return total_scraped

    def scrape(
        self,
        categories: Optional[list[str]] = None,
        limit: Optional[int] = None,
        resume: bool = True,
    ) -> int:
        all_categories = self.get_categories()
        if categories:
            all_categories = [c for c in all_categories if c.slug in categories or c.name in categories]

        return self._scrape_categories(all_categories, limit)

    def close(self) -> None:
        self.client.close()
