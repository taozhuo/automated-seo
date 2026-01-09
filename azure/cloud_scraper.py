#!/usr/bin/env python3
"""
Cloud-based bulk scraper for Azure Container Instances.
Scrapes DevForum and Reddit, stores in Azure Blob Storage.
"""

import json
import os
import re
import time
import warnings
from datetime import datetime

warnings.filterwarnings("ignore")

import requests
from azure.storage.blob import BlobServiceClient
from google import genai

# Configuration from environment
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
STORAGE_CONNECTION = os.environ.get("STORAGE_CONNECTION")
RESULTS_CONTAINER = os.environ.get("RESULTS_CONTAINER", "scraper-results")
WORKER_ID = os.environ.get("WORKER_ID", "cloud-worker")
SOURCE = os.environ.get("SOURCE", "devforum")  # devforum or reddit
PAGES = int(os.environ.get("PAGES", "50"))

# Initialize
client = genai.Client(api_key=GEMINI_API_KEY)
blob_service = BlobServiceClient.from_connection_string(STORAGE_CONNECTION)
blob_container = blob_service.get_container_client(RESULTS_CONTAINER)


def analyze_with_gemini(title: str, content: str, source: str) -> dict:
    """Extract problem using Gemini."""
    if not content or len(content) < 50:
        return {"problem": None}

    prompt = f"""Analyze this {source} post about Roblox development.

TITLE: {title}
CONTENT: {content[:4000]}

Extract the SPECIFIC technical problem. Return JSON:
{{
    "problem": "Specific searchable problem statement",
    "category": "Scripting|3D Modeling|UI|Animation|Physics|Networking|DataStore|Audio|Monetization|Other",
    "solution": "Solution if provided, null otherwise",
    "solved": true/false,
    "difficulty": "beginner|intermediate|advanced",
    "apis_mentioned": ["API1"],
    "error_messages": ["errors if any"],
    "automatable": true/false,
    "automation_hint": "How a coding agent could help"
}}"""

    try:
        response = client.models.generate_content(
            model="gemini-3-flash-preview",
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        return json.loads(response.text)
    except Exception as e:
        return {"problem": None, "error": str(e)[:100]}


def save_result(source: str, source_id: str, data: dict):
    """Save result to Azure Blob Storage."""
    blob_name = f"problems/{source}/{source_id}.json"
    blob_client = blob_container.get_blob_client(blob_name)
    blob_client.upload_blob(json.dumps(data, default=str), overwrite=True)


# ============ DEVFORUM SCRAPER ============

DEVFORUM_CATEGORIES = [
    {"slug": "scripting-support", "id": 55},
    {"slug": "building-support", "id": 56},
    {"slug": "art-design-support", "id": 57},
    {"slug": "game-design-support", "id": 83},
]


def scrape_devforum():
    """Scrape DevForum."""
    print(f"[{WORKER_ID}] Starting DevForum scraper ({PAGES} pages per category)")

    total = 0
    automatable = 0

    for cat in DEVFORUM_CATEGORIES:
        print(f"[{WORKER_ID}] Category: {cat['slug']}")

        for page in range(PAGES):
            url = f"https://devforum.roblox.com/c/{cat['slug']}/{cat['id']}.json?page={page}"
            try:
                resp = requests.get(url, timeout=30)
                topics = resp.json().get("topic_list", {}).get("topics", [])
                if not topics:
                    break

                for topic in topics:
                    topic_id = topic["id"]
                    title = topic.get("title", "")

                    # Get full content
                    try:
                        resp = requests.get(f"https://devforum.roblox.com/t/{topic_id}.json", timeout=30)
                        data = resp.json()
                        posts = data.get("post_stream", {}).get("posts", [])
                        if not posts:
                            continue

                        content = re.sub(r'<[^>]+>', ' ', posts[0].get("cooked", ""))
                        replies = [re.sub(r'<[^>]+>', ' ', p.get("cooked", "")) for p in posts[1:3]]
                        full_content = content + "\n\nReplies:\n" + "\n---\n".join(replies)

                        analysis = analyze_with_gemini(title, full_content, "DevForum")
                        time.sleep(0.3)

                        result = {
                            "source": "devforum",
                            "source_id": str(topic_id),
                            "title": title,
                            "url": f"https://devforum.roblox.com/t/{topic_id}",
                            "category": cat["slug"],
                            "views": topic.get("views", 0),
                            "replies": topic.get("posts_count", 0),
                            "created_at": topic.get("created_at"),
                            "analysis": analysis,
                            "scraped_at": datetime.utcnow().isoformat(),
                            "worker_id": WORKER_ID
                        }

                        save_result("devforum", str(topic_id), result)
                        total += 1

                        if analysis.get("automatable"):
                            automatable += 1
                            print(f"[{WORKER_ID}] [{total}] {analysis.get('problem', '')[:50]}")

                    except Exception as e:
                        print(f"[{WORKER_ID}] Error {topic_id}: {e}")

                time.sleep(0.5)

            except Exception as e:
                print(f"[{WORKER_ID}] Page error: {e}")
                break

    print(f"[{WORKER_ID}] Done. Total: {total}, Automatable: {automatable}")


# ============ REDDIT SCRAPER ============

REDDIT_SUBREDDITS = ["robloxgamedev", "roblox", "RobloxDevelopers"]


def scrape_reddit():
    """Scrape Reddit."""
    print(f"[{WORKER_ID}] Starting Reddit scraper")

    headers = {"User-Agent": "RobloxSEOBot/1.0"}
    total = 0
    automatable = 0

    for subreddit in REDDIT_SUBREDDITS:
        print(f"[{WORKER_ID}] Subreddit: r/{subreddit}")

        after = None
        posts_scraped = 0

        while posts_scraped < PAGES * 25:  # ~25 posts per page
            url = f"https://www.reddit.com/r/{subreddit}/search.json?q=script+OR+lua+OR+help&restrict_sr=1&limit=100&sort=relevance"
            if after:
                url += f"&after={after}"

            try:
                resp = requests.get(url, headers=headers, timeout=30)
                if resp.status_code != 200:
                    break
                data = resp.json().get("data", {})
                posts = data.get("children", [])
                if not posts:
                    break

                for post_data in posts:
                    post = post_data["data"]
                    post_id = post.get("id")
                    title = post.get("title", "")
                    content = post.get("selftext", "")

                    if not content or len(content) < 50:
                        continue

                    analysis = analyze_with_gemini(title, content, "Reddit")
                    time.sleep(0.3)

                    result = {
                        "source": "reddit",
                        "source_id": post_id,
                        "title": title,
                        "url": f"https://reddit.com{post.get('permalink', '')}",
                        "subreddit": subreddit,
                        "upvotes": post.get("score", 0),
                        "comments": post.get("num_comments", 0),
                        "created_at": datetime.fromtimestamp(post.get("created_utc", 0)).isoformat(),
                        "analysis": analysis,
                        "scraped_at": datetime.utcnow().isoformat(),
                        "worker_id": WORKER_ID
                    }

                    save_result("reddit", post_id, result)
                    total += 1
                    posts_scraped += 1

                    if analysis.get("automatable"):
                        automatable += 1
                        print(f"[{WORKER_ID}] [{total}] {analysis.get('problem', '')[:50]}")

                after = data.get("after")
                if not after:
                    break
                time.sleep(1)  # Reddit rate limit

            except Exception as e:
                print(f"[{WORKER_ID}] Error: {e}")
                break

    print(f"[{WORKER_ID}] Done. Total: {total}, Automatable: {automatable}")


def main():
    if SOURCE == "devforum":
        scrape_devforum()
    elif SOURCE == "reddit":
        scrape_reddit()
    else:
        print(f"Unknown source: {SOURCE}")


if __name__ == "__main__":
    main()
