#!/usr/bin/env python3
"""
Pain Point Analyzer for Roblox DevForum content.
Extracts common issues, questions, and complaints for SEO content generation.
"""

import json
import re
from collections import Counter
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
import html

import config


@dataclass
class PainPoint:
    title: str
    category: str
    url: str
    views: int
    replies: int
    keywords: list[str] = field(default_factory=list)
    question_type: str = ""  # "how-to", "error", "why", "bug", etc.


def clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_question_type(title: str) -> str:
    """Classify the type of question/issue."""
    title_lower = title.lower()

    if any(p in title_lower for p in ["how do i", "how to", "how can"]):
        return "how-to"
    elif any(p in title_lower for p in ["why does", "why is", "why won't", "why can't"]):
        return "why"
    elif any(p in title_lower for p in ["error", "bug", "crash", "broken"]):
        return "error"
    elif any(p in title_lower for p in ["not working", "doesn't work", "won't work"]):
        return "not-working"
    elif any(p in title_lower for p in ["help", "need help", "please help"]):
        return "help-request"
    elif title_lower.startswith("what ") or "what is" in title_lower:
        return "what-is"
    elif any(p in title_lower for p in ["can i", "is it possible", "can you"]):
        return "possibility"
    else:
        return "general"


def extract_keywords(title: str, content: str) -> list[str]:
    """Extract relevant Roblox-specific keywords."""
    text = f"{title} {content}".lower()

    # Roblox-specific terms to look for
    roblox_terms = [
        "script", "localscript", "modulescript", "serverscript",
        "remotevent", "remotefunction", "bindableevent",
        "datastore", "datastoreservice", "savedata",
        "tween", "tweenservice", "animation",
        "gui", "screengui", "surfacegui", "billboardgui",
        "part", "meshpart", "union", "model",
        "player", "character", "humanoid", "tool",
        "workspace", "replicatedstorage", "serverstorage",
        "lighting", "soundservice", "debris",
        "raycast", "collision", "physics",
        "touched", "click", "input", "userinputservice",
        "filtering", "sanity check", "exploiter", "anticheat",
        "lag", "performance", "optimize", "memory",
        "studio", "plugin", "command bar",
        "roblox api", "http", "httpservice", "webhook",
        "pcall", "coroutine", "async", "wait", "task.wait",
        "loop", "while", "for", "function", "return",
        "table", "array", "dictionary", "metatables",
        "oop", "class", "module", "require",
        "pathfinding", "navmesh", "ai", "npc",
        "chat", "textchatservice", "filter",
        "marketplace", "gamepass", "devproduct", "robux",
    ]

    found = []
    for term in roblox_terms:
        if term in text:
            found.append(term)

    return found[:10]  # Limit to top 10


class PainPointAnalyzer:
    def __init__(self, data_dir: str = config.DATA_DIR):
        self.data_dir = Path(data_dir)
        self.pain_points: list[PainPoint] = []

    def load_topics(self) -> list[dict]:
        """Load all scraped topic JSON files."""
        topics = []
        for json_file in self.data_dir.rglob("*.json"):
            if json_file.name == "scraper_state.json":
                continue
            try:
                with open(json_file) as f:
                    topics.append(json.load(f))
            except json.JSONDecodeError:
                continue
        return topics

    def analyze(self) -> list[PainPoint]:
        """Analyze all topics for pain points."""
        topics = self.load_topics()

        for topic in topics:
            # Get first post content
            first_post_content = ""
            if topic.get("posts"):
                first_post = topic["posts"][0]
                first_post_content = clean_html(
                    first_post.get("content_html", "") or first_post.get("content_raw", "")
                )

            pain_point = PainPoint(
                title=topic.get("title", ""),
                category=topic.get("category_name", ""),
                url=topic.get("url", ""),
                views=topic.get("views", 0),
                replies=topic.get("reply_count", 0),
                keywords=extract_keywords(topic.get("title", ""), first_post_content),
                question_type=extract_question_type(topic.get("title", "")),
            )
            self.pain_points.append(pain_point)

        return self.pain_points

    def get_top_keywords(self, n: int = 30) -> list[tuple[str, int]]:
        """Get most common keywords across all pain points."""
        all_keywords = []
        for pp in self.pain_points:
            all_keywords.extend(pp.keywords)
        return Counter(all_keywords).most_common(n)

    def get_question_type_distribution(self) -> dict[str, int]:
        """Get distribution of question types."""
        types = [pp.question_type for pp in self.pain_points]
        return dict(Counter(types))

    def get_high_engagement_topics(self, min_views: int = 1000) -> list[PainPoint]:
        """Get topics with high engagement (good SEO targets)."""
        return sorted(
            [pp for pp in self.pain_points if pp.views >= min_views],
            key=lambda x: x.views,
            reverse=True,
        )

    def export_for_seo(self, output_file: str = "data/seo_pain_points.json"):
        """Export analyzed pain points for SEO content generation."""
        output = {
            "total_topics": len(self.pain_points),
            "top_keywords": self.get_top_keywords(30),
            "question_types": self.get_question_type_distribution(),
            "high_engagement": [
                {
                    "title": pp.title,
                    "url": pp.url,
                    "views": pp.views,
                    "replies": pp.replies,
                    "keywords": pp.keywords,
                    "question_type": pp.question_type,
                }
                for pp in self.get_high_engagement_topics(100)
            ],
            "all_topics": [
                {
                    "title": pp.title,
                    "category": pp.category,
                    "url": pp.url,
                    "views": pp.views,
                    "replies": pp.replies,
                    "keywords": pp.keywords,
                    "question_type": pp.question_type,
                }
                for pp in sorted(self.pain_points, key=lambda x: x.views, reverse=True)
            ],
        }

        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(output, f, indent=2)

        return output


def main():
    print("Analyzing scraped topics for pain points...")

    analyzer = PainPointAnalyzer()
    pain_points = analyzer.analyze()

    if not pain_points:
        print("No topics found. Run the scraper first.")
        return

    print(f"\nAnalyzed {len(pain_points)} topics")

    print("\n--- Top Keywords (SEO targets) ---")
    for keyword, count in analyzer.get_top_keywords(20):
        print(f"  {keyword}: {count}")

    print("\n--- Question Type Distribution ---")
    for qtype, count in analyzer.get_question_type_distribution().items():
        print(f"  {qtype}: {count}")

    print("\n--- High Engagement Topics (100+ views) ---")
    for pp in analyzer.get_high_engagement_topics(100)[:10]:
        print(f"  [{pp.views} views] {pp.title}")

    # Export for SEO
    output = analyzer.export_for_seo()
    print(f"\nExported {len(output['all_topics'])} topics to data/seo_pain_points.json")


if __name__ == "__main__":
    main()
