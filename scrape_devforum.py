#!/usr/bin/env python3
"""
Scrape Roblox DevForum and extract technical problems using Gemini Flash 3.
"""

import json
import os
import time
import warnings
warnings.filterwarnings("ignore")

import requests
import psycopg2
from google import genai

# Configuration
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
BASE_URL = "https://devforum.roblox.com"
DB_NAME = "seo_data"
DB_USER = os.environ.get("USER")

client = genai.Client(api_key=GEMINI_API_KEY)

# Categories to scrape (pain point focused)
CATEGORIES = [
    {"slug": "scripting-support", "id": 55, "name": "Scripting Support"},
    {"slug": "building-support", "id": 56, "name": "Building Support"},
    {"slug": "art-design-support", "id": 57, "name": "Art Design Support"},
    {"slug": "game-design-support", "id": 83, "name": "Game Design Support"},
]


def get_topics(category_slug: str, category_id: int, page: int = 0) -> list:
    """Fetch topics from a category."""
    url = f"{BASE_URL}/c/{category_slug}/{category_id}.json?page={page}"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("topic_list", {}).get("topics", [])
    except Exception as e:
        print(f"Error fetching topics: {e}", flush=True)
    return []


def get_topic_content(topic_id: int) -> dict:
    """Fetch full topic content including posts."""
    url = f"{BASE_URL}/t/{topic_id}.json"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"Error fetching topic {topic_id}: {e}", flush=True)
    return {}


def extract_problem(title: str, content: str, replies: list) -> dict:
    """Use Gemini to extract the technical problem from forum post."""
    if not content or len(content) < 50:
        return {"problem": None}

    # Combine original post with top replies
    replies_text = "\n---\n".join(replies[:3]) if replies else ""

    prompt = f"""You are analyzing a Roblox Developer Forum post to extract the SPECIFIC TECHNICAL PROBLEM.

POST TITLE: {title}

ORIGINAL POST:
{content[:3000]}

TOP REPLIES:
{replies_text[:2000]}

Extract:
1. What specific technical problem is the developer facing?
2. What have they tried that didn't work?
3. Was the problem solved in the replies?
4. What category does this fall into?

Return JSON ONLY:
{{
    "problem": "Specific searchable problem statement",
    "category": "Scripting" | "3D Modeling" | "UI" | "Animation" | "Physics" | "Networking" | "DataStore" | "Audio" | "Monetization" | "Other",
    "what_they_tried": "Brief description of attempted solutions",
    "solved": true/false,
    "solution_summary": "Brief solution if solved, null if not",
    "difficulty": "beginner" | "intermediate" | "advanced",
    "apis_mentioned": ["API1", "API2"],
    "error_messages": ["Any error messages mentioned"],
    "automatable": true/false,
    "automation_hint": "How a coding agent could help"
}}

Focus on SPECIFIC problems. Good: "RemoteEvent not firing from client to server"
Bad: "Need help with scripting" """

    try:
        response = client.models.generate_content(
            model="gemini-3-flash-preview",
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        return json.loads(response.text)
    except Exception as e:
        print(f"  Gemini error: {e}", flush=True)
        return {"problem": None, "error": str(e)[:100]}


def main():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER)
    cursor = conn.cursor()

    # Create/update table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS devforum_posts (
            id INT PRIMARY KEY,
            title TEXT,
            category VARCHAR(100),
            views INT,
            reply_count INT,
            like_count INT,
            created_at TIMESTAMP,
            content TEXT,
            problem JSONB,
            solved BOOLEAN,
            scraped_at TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_devforum_category ON devforum_posts(category);
        CREATE INDEX IF NOT EXISTS idx_devforum_solved ON devforum_posts(solved);
    """)
    conn.commit()

    all_problems = []

    for cat in CATEGORIES:
        print(f"\n=== {cat['name']} ===", flush=True)

        for page in range(3):  # First 3 pages per category
            print(f"  Page {page + 1}...", flush=True)
            topics = get_topics(cat["slug"], cat["id"], page)

            if not topics:
                break

            for topic in topics[:20]:  # 20 topics per page
                topic_id = topic.get("id")
                title = topic.get("title", "")

                # Skip if already scraped
                cursor.execute("SELECT id FROM devforum_posts WHERE id = %s", (topic_id,))
                if cursor.fetchone():
                    continue

                print(f"    [{topic_id}] {title[:50]}...", flush=True)

                # Get full content
                full_topic = get_topic_content(topic_id)
                time.sleep(0.5)  # Rate limit

                if not full_topic:
                    continue

                posts = full_topic.get("post_stream", {}).get("posts", [])
                if not posts:
                    continue

                # Original post content
                original = posts[0].get("cooked", "")  # HTML content
                # Strip HTML tags roughly
                import re
                original_text = re.sub(r'<[^>]+>', ' ', original)

                # Get reply contents
                replies = [
                    re.sub(r'<[^>]+>', ' ', p.get("cooked", ""))
                    for p in posts[1:4]
                ]

                # Extract problem with Gemini
                problem = extract_problem(title, original_text, replies)
                time.sleep(0.5)  # Rate limit Gemini

                # Save to database
                cursor.execute("""
                    INSERT INTO devforum_posts
                    (id, title, category, views, reply_count, like_count, created_at, content, problem, solved)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET problem = EXCLUDED.problem
                """, (
                    topic_id,
                    title,
                    cat["name"],
                    topic.get("views", 0),
                    topic.get("posts_count", 0),
                    topic.get("like_count", 0),
                    topic.get("created_at"),
                    original_text[:5000],
                    json.dumps(problem),
                    problem.get("solved", False)
                ))
                conn.commit()

                if problem.get("problem"):
                    all_problems.append({
                        "topic_id": topic_id,
                        "title": title,
                        "category": cat["name"],
                        **problem
                    })
                    if problem.get("automatable"):
                        print(f"      -> {problem.get('problem', '')[:60]}", flush=True)

            time.sleep(1)  # Rate limit between pages

    # Summary
    print(f"\n\n=== Extracted {len(all_problems)} Problems ===", flush=True)

    # By category
    by_cat = {}
    for p in all_problems:
        cat = p.get("category", "Other")
        by_cat.setdefault(cat, []).append(p)

    for cat, probs in sorted(by_cat.items(), key=lambda x: -len(x[1])):
        print(f"\n{cat} ({len(probs)}):", flush=True)
        unsolved = [p for p in probs if not p.get("solved")]
        print(f"  Unsolved: {len(unsolved)}", flush=True)
        for p in unsolved[:3]:
            print(f"  - {p.get('problem', '')[:70]}", flush=True)

    # Save to file
    with open("data/devforum_problems.json", "w") as f:
        json.dump(all_problems, f, indent=2)

    print(f"\nSaved to data/devforum_problems.json", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
