#!/usr/bin/env python3
"""
Load scraped data into PostgreSQL with Gemini Flash 3 SEO analysis.
"""

import json
import os
import time
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values
from google import genai

# Configuration
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DB_NAME = "seo_data"
DB_USER = os.environ.get("USER", "postgres")

# Initialize Gemini client
client = genai.Client(api_key=GEMINI_API_KEY)
MODEL_ID = "gemini-3-flash-preview"


def analyze_seo_value(title: str, transcript: str) -> dict:
    """Use Gemini Flash 3 to analyze SEO value of content."""
    if not transcript or len(transcript) < 100:
        return {"has_value": False, "summary": "No transcript", "keywords": []}

    # Truncate very long transcripts
    transcript_sample = transcript[:4000] if len(transcript) > 4000 else transcript

    prompt = f"""You are a Senior Roblox Engineer acting as a Content Filter.
Analyze this video content to see if it is a valid candidate for our 'Game Dev Agent' SEO documentation.

CRITERIA FOR PASSING:
1. Must address a specific technical topic (scripting, building, physics, networking).
2. Must NOT be exploit/cheat related content.
3. Must provide educational value for developers.

VIDEO TITLE: {title}
TRANSCRIPT SNIPPET: {transcript_sample}

Return JSON ONLY:
{{
    "status": "PASS" or "FAIL",
    "confidence": 0.0-1.0,
    "category": "Scripting" | "Building" | "Networking" | "Physics" | "UI/UX" | "Other",
    "summary": "1-2 sentence summary of the content's value",
    "keywords": ["keyword1", "keyword2", "keyword3"]
}}"""

    try:
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        result = json.loads(response.text)
        return {
            "has_value": result.get("status") == "PASS",
            "summary": result.get("summary", ""),
            "keywords": result.get("keywords", []),
            "category": result.get("category", "Other"),
            "confidence": result.get("confidence", 0)
        }
    except Exception as e:
        print(f"  Gemini error: {e}")
        return {"has_value": None, "summary": str(e)[:200], "keywords": []}


def load_youtube_data(data_dir: Path, conn):
    """Load YouTube video data into PostgreSQL."""
    print("Loading YouTube data...")

    cursor = conn.cursor()
    videos_dir = data_dir / "youtube_bulk" / "videos"

    if not videos_dir.exists():
        print(f"  Directory not found: {videos_dir}")
        return

    files = list(videos_dir.rglob("*.json"))
    print(f"  Found {len(files)} video files")

    loaded = 0
    analyzed = 0

    for i, file_path in enumerate(files):
        try:
            with open(file_path) as f:
                data = json.load(f)

            video_id = data.get("id")
            title = data.get("title", "")
            transcript = data.get("transcript", "")

            # Analyze SEO value with Gemini
            print(f"  [{i+1}/{len(files)}] Analyzing: {title[:50]}...")
            seo = analyze_seo_value(title, transcript)
            analyzed += 1

            # Rate limit Gemini calls
            time.sleep(0.5)

            # Insert into database
            cursor.execute("""
                INSERT INTO youtube_videos
                (id, title, channel, views, duration, url, query, transcript,
                 transcript_length, has_seo_value, seo_summary, seo_keywords,
                 seo_category, seo_confidence)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    has_seo_value = EXCLUDED.has_seo_value,
                    seo_summary = EXCLUDED.seo_summary,
                    seo_keywords = EXCLUDED.seo_keywords,
                    seo_category = EXCLUDED.seo_category,
                    seo_confidence = EXCLUDED.seo_confidence
            """, (
                video_id,
                title,
                data.get("channel", ""),
                data.get("views", 0),
                data.get("duration", ""),
                data.get("url", ""),
                data.get("query", ""),
                transcript,
                len(transcript),
                seo["has_value"],
                seo["summary"],
                seo["keywords"],
                seo.get("category"),
                seo.get("confidence")
            ))

            loaded += 1

            if seo["has_value"]:
                print(f"    SEO: {seo['summary'][:60]}...")

        except Exception as e:
            print(f"  Error loading {file_path}: {e}")

    conn.commit()
    print(f"  Loaded {loaded} videos, analyzed {analyzed} with Gemini")


def load_devforum_data(data_dir: Path, conn):
    """Load DevForum topic data into PostgreSQL."""
    print("Loading DevForum data...")

    cursor = conn.cursor()
    topics_dir = data_dir / "raw" / "topics"

    if not topics_dir.exists():
        print(f"  Directory not found: {topics_dir}")
        return

    files = list(topics_dir.glob("*.json"))
    print(f"  Found {len(files)} topic files")

    loaded = 0

    for file_path in files:
        try:
            with open(file_path) as f:
                data = json.load(f)

            # Combine all post content
            content = "\n\n".join([
                p.get("content_raw", "") for p in data.get("posts", [])
            ])

            cursor.execute("""
                INSERT INTO devforum_topics
                (id, title, slug, category_name, views, reply_count, like_count,
                 created_at, content)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                data.get("id"),
                data.get("title", ""),
                data.get("slug", ""),
                data.get("category_name", ""),
                data.get("views", 0),
                data.get("reply_count", 0),
                data.get("like_count", 0),
                data.get("created_at"),
                content
            ))

            loaded += 1

        except Exception as e:
            print(f"  Error loading {file_path}: {e}")

    conn.commit()
    print(f"  Loaded {loaded} topics")


def get_stats(conn):
    """Print database statistics."""
    cursor = conn.cursor()

    print("\n=== Database Stats ===")

    cursor.execute("SELECT COUNT(*) FROM youtube_videos")
    print(f"YouTube videos: {cursor.fetchone()[0]}")

    cursor.execute("SELECT COUNT(*) FROM youtube_videos WHERE has_seo_value = true")
    print(f"  With SEO value: {cursor.fetchone()[0]}")

    cursor.execute("SELECT SUM(views) FROM youtube_videos")
    views = cursor.fetchone()[0] or 0
    print(f"  Total views: {views:,}")

    cursor.execute("SELECT COUNT(*) FROM devforum_topics")
    print(f"DevForum topics: {cursor.fetchone()[0]}")

    # Top SEO keywords
    cursor.execute("""
        SELECT unnest(seo_keywords) as kw, COUNT(*) as cnt
        FROM youtube_videos
        WHERE has_seo_value = true
        GROUP BY kw ORDER BY cnt DESC LIMIT 10
    """)
    print("\nTop SEO keywords:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]}")


def main():
    # Connect to database
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER)

    data_dir = Path("data")

    # Load data
    load_youtube_data(data_dir, conn)
    load_devforum_data(data_dir, conn)

    # Show stats
    get_stats(conn)

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
