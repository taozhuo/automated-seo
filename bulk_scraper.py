#!/usr/bin/env python3
"""
Unified bulk scraper for YouTube, DevForum, and Reddit.
Extracts technical problems using Gemini Flash 3.
"""

import json
import os
import re
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

warnings.filterwarnings("ignore")

import requests
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from google import genai

# Configuration
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DB_NAME = "seo_data"
DB_USER = os.environ.get("USER")

# Initialize
client = genai.Client(api_key=GEMINI_API_KEY)
db_pool = ThreadedConnectionPool(1, 10, dbname=DB_NAME, user=DB_USER)


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


def save_problem(source: str, source_id: str, title: str, url: str,
                 content: str, analysis: dict, metadata: dict):
    """Save to unified problems table."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO problems
            (source, source_id, title, url, category, problem, solution, solved,
             difficulty, apis_mentioned, error_messages, automatable, automation_hint,
             views, upvotes, comments, created_at, raw_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            source,
            source_id,
            title,
            url,
            analysis.get("category"),
            analysis.get("problem"),
            analysis.get("solution"),
            analysis.get("solved"),
            analysis.get("difficulty"),
            analysis.get("apis_mentioned"),
            analysis.get("error_messages"),
            analysis.get("automatable"),
            analysis.get("automation_hint"),
            metadata.get("views", 0),
            metadata.get("upvotes", 0),
            metadata.get("comments", 0),
            metadata.get("created_at"),
            json.dumps({"content": content[:2000], **metadata})
        ))
        conn.commit()
    finally:
        db_pool.putconn(conn)


# ============ DEVFORUM SCRAPER ============

DEVFORUM_CATEGORIES = [
    {"slug": "scripting-support", "id": 55},
    {"slug": "building-support", "id": 56},
    {"slug": "art-design-support", "id": 57},
    {"slug": "game-design-support", "id": 83},
]


def scrape_devforum_topic(topic: dict, category: str) -> dict:
    """Scrape a single DevForum topic."""
    topic_id = topic["id"]
    title = topic.get("title", "")

    try:
        resp = requests.get(f"https://devforum.roblox.com/t/{topic_id}.json", timeout=30)
        if resp.status_code != 200:
            return None

        data = resp.json()
        posts = data.get("post_stream", {}).get("posts", [])
        if not posts:
            return None

        content = re.sub(r'<[^>]+>', ' ', posts[0].get("cooked", ""))
        replies = [re.sub(r'<[^>]+>', ' ', p.get("cooked", "")) for p in posts[1:3]]
        full_content = content + "\n\nReplies:\n" + "\n---\n".join(replies)

        analysis = analyze_with_gemini(title, full_content, "DevForum")
        time.sleep(0.3)

        save_problem(
            source="devforum",
            source_id=str(topic_id),
            title=title,
            url=f"https://devforum.roblox.com/t/{topic_id}",
            content=content,
            analysis=analysis,
            metadata={
                "views": topic.get("views", 0),
                "upvotes": topic.get("like_count", 0),
                "comments": topic.get("posts_count", 0),
                "created_at": topic.get("created_at"),
                "category": category
            }
        )

        return analysis

    except Exception as e:
        print(f"  Error {topic_id}: {e}", flush=True)
        return None


def scrape_devforum(pages_per_category: int = 10):
    """Scrape DevForum at scale."""
    print("\n=== DEVFORUM SCRAPER ===", flush=True)

    for cat in DEVFORUM_CATEGORIES:
        print(f"\n{cat['slug']}:", flush=True)

        all_topics = []
        for page in range(pages_per_category):
            url = f"https://devforum.roblox.com/c/{cat['slug']}/{cat['id']}.json?page={page}"
            try:
                resp = requests.get(url, timeout=30)
                topics = resp.json().get("topic_list", {}).get("topics", [])
                all_topics.extend(topics)
                time.sleep(0.5)
            except:
                break

        print(f"  Found {len(all_topics)} topics", flush=True)

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(scrape_devforum_topic, t, cat['slug']): t
                for t in all_topics
            }
            done = 0
            for future in as_completed(futures):
                done += 1
                result = future.result()
                if result and result.get("automatable"):
                    print(f"  [{done}/{len(all_topics)}] {result.get('problem', '')[:50]}", flush=True)


# ============ REDDIT SCRAPER ============

REDDIT_SUBREDDITS = ["robloxgamedev", "roblox", "RobloxDevelopers"]


def scrape_reddit_post(post: dict) -> dict:
    """Scrape a single Reddit post."""
    try:
        title = post.get("title", "")
        content = post.get("selftext", "")
        post_id = post.get("id")

        if not content or len(content) < 50:
            return None

        # Get comments
        comments_url = f"https://www.reddit.com/r/{post['subreddit']}/comments/{post_id}.json"
        headers = {"User-Agent": "RobloxSEOBot/1.0"}

        try:
            resp = requests.get(comments_url, headers=headers, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                if len(data) > 1:
                    comments = data[1].get("data", {}).get("children", [])[:5]
                    comment_texts = [
                        c.get("data", {}).get("body", "")
                        for c in comments if c.get("kind") == "t1"
                    ]
                    content += "\n\nComments:\n" + "\n---\n".join(comment_texts)
        except:
            pass

        analysis = analyze_with_gemini(title, content, "Reddit")
        time.sleep(0.3)

        save_problem(
            source="reddit",
            source_id=post_id,
            title=title,
            url=f"https://reddit.com{post.get('permalink', '')}",
            content=content,
            analysis=analysis,
            metadata={
                "views": 0,
                "upvotes": post.get("score", 0),
                "comments": post.get("num_comments", 0),
                "created_at": datetime.fromtimestamp(post.get("created_utc", 0)).isoformat(),
                "subreddit": post.get("subreddit")
            }
        )

        return analysis

    except Exception as e:
        print(f"  Error: {e}", flush=True)
        return None


def scrape_reddit(posts_per_subreddit: int = 500):
    """Scrape Reddit at scale."""
    print("\n=== REDDIT SCRAPER ===", flush=True)

    headers = {"User-Agent": "RobloxSEOBot/1.0"}

    for subreddit in REDDIT_SUBREDDITS:
        print(f"\nr/{subreddit}:", flush=True)

        all_posts = []
        after = None

        # Paginate through posts
        while len(all_posts) < posts_per_subreddit:
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

                all_posts.extend([p["data"] for p in posts])
                after = data.get("after")
                time.sleep(1)  # Reddit rate limit

                if not after:
                    break
            except Exception as e:
                print(f"  Error fetching: {e}", flush=True)
                break

        print(f"  Found {len(all_posts)} posts", flush=True)

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(scrape_reddit_post, p): p for p in all_posts}
            done = 0
            for future in as_completed(futures):
                done += 1
                result = future.result()
                if result and result.get("automatable"):
                    print(f"  [{done}/{len(all_posts)}] {result.get('problem', '')[:50]}", flush=True)


# ============ MAIN ============

def get_stats():
    """Print database stats."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT source, COUNT(*) as total,
                   SUM(CASE WHEN automatable THEN 1 ELSE 0 END) as automatable,
                   SUM(CASE WHEN solved THEN 1 ELSE 0 END) as solved
            FROM problems
            GROUP BY source
        """)
        print("\n=== DATABASE STATS ===", flush=True)
        for row in cursor.fetchall():
            print(f"{row[0]}: {row[1]} total, {row[2]} automatable, {row[3]} solved", flush=True)

        cursor.execute("""
            SELECT category, COUNT(*) FROM problems
            WHERE automatable = true
            GROUP BY category ORDER BY COUNT(*) DESC LIMIT 10
        """)
        print("\nTop automatable categories:", flush=True)
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]}", flush=True)
    finally:
        db_pool.putconn(conn)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--devforum", type=int, default=0, help="Pages per DevForum category")
    parser.add_argument("--reddit", type=int, default=0, help="Posts per subreddit")
    parser.add_argument("--all", action="store_true", help="Scrape all sources")
    args = parser.parse_args()

    if args.all:
        args.devforum = 10
        args.reddit = 500

    if args.devforum:
        scrape_devforum(args.devforum)

    if args.reddit:
        scrape_reddit(args.reddit)

    get_stats()

    # Export to JSON
    conn = db_pool.getconn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM problems WHERE automatable = true")
    columns = [desc[0] for desc in cursor.description]
    problems = [dict(zip(columns, row)) for row in cursor.fetchall()]
    db_pool.putconn(conn)

    with open("data/all_problems.json", "w") as f:
        json.dump(problems, f, indent=2, default=str)

    print(f"\nExported {len(problems)} automatable problems to data/all_problems.json", flush=True)


if __name__ == "__main__":
    main()
