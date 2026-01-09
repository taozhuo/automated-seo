#!/usr/bin/env python3
"""
Scrape YouTube comments to find developer pain points and validate problems.
Comments often contain real questions and struggles that your coding agent can solve.
"""

import json
import os
import subprocess
import time
import warnings
warnings.filterwarnings("ignore")

import psycopg2
from google import genai

# Configuration
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DB_NAME = "seo_data"
DB_USER = os.environ.get("USER", "postgres")

client = genai.Client(api_key=GEMINI_API_KEY)
MODEL_ID = "gemini-3-flash-preview"


def get_video_comments(video_id: str, max_comments: int = 50) -> list:
    """Fetch comments using yt-dlp."""
    try:
        cmd = [
            "yt-dlp",
            f"https://www.youtube.com/watch?v={video_id}",
            "--write-comments",
            "--skip-download",
            "--no-write-thumbnail",
            "-o", f"/tmp/yt_{video_id}",
            "--quiet"
        ]
        subprocess.run(cmd, capture_output=True, timeout=60)

        # Read comments file
        comments_file = f"/tmp/yt_{video_id}.info.json"
        if os.path.exists(comments_file):
            with open(comments_file) as f:
                data = json.load(f)
            comments = data.get("comments", [])[:max_comments]
            os.remove(comments_file)
            return [c.get("text", "") for c in comments if c.get("text")]
    except Exception as e:
        print(f"  Error fetching comments: {e}")
    return []


def analyze_comments_for_pain_points(title: str, comments: list) -> dict:
    """Use Gemini to extract pain points from comments."""
    if not comments:
        return {"pain_points": [], "unsolved_questions": []}

    comments_text = "\n---\n".join(comments[:30])

    prompt = f"""You are analyzing YouTube comments on a Roblox tutorial video.
Find REAL DEVELOPER PAIN POINTS - specific problems people are struggling with.

VIDEO TITLE: {title}
COMMENTS:
{comments_text}

Extract:
1. Questions people are asking (problems they still have)
2. Specific errors or issues mentioned
3. Features they wish existed
4. Things the tutorial didn't cover that they need

Return JSON ONLY:
{{
    "pain_points": [
        {{
            "problem": "Specific problem statement",
            "frequency": "how many comments mention similar issue",
            "urgency": "low" | "medium" | "high",
            "can_automate": true/false,
            "automation_approach": "How a coding agent could solve this"
        }}
    ],
    "unsolved_questions": [
        "Direct questions from comments that weren't answered"
    ],
    "common_errors": [
        "Error messages or bugs mentioned"
    ],
    "sentiment": "positive" | "mixed" | "frustrated"
}}

Focus on problems a CODING AGENT can automate. Ignore off-topic comments."""

    try:
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        return json.loads(response.text)
    except Exception as e:
        print(f"  Gemini error: {e}")
        return {"pain_points": [], "error": str(e)[:200]}


def main():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER)
    cursor = conn.cursor()

    # Add columns
    cursor.execute("""
        ALTER TABLE youtube_videos ADD COLUMN IF NOT EXISTS comments_analyzed BOOLEAN DEFAULT false;
        ALTER TABLE youtube_videos ADD COLUMN IF NOT EXISTS pain_points JSONB;
        ALTER TABLE youtube_videos ADD COLUMN IF NOT EXISTS unsolved_questions TEXT[];
    """)
    conn.commit()

    # Get top videos by views (where comments matter most)
    cursor.execute("""
        SELECT id, title FROM youtube_videos
        WHERE has_seo_value = true
        AND (comments_analyzed IS NULL OR comments_analyzed = false)
        ORDER BY views DESC
        LIMIT 30
    """)
    videos = cursor.fetchall()

    print(f"Analyzing comments from {len(videos)} videos...")
    all_pain_points = []

    for i, (video_id, title) in enumerate(videos):
        print(f"\n[{i+1}/{len(videos)}] {title[:50]}...")

        # Fetch comments
        print("  Fetching comments...")
        comments = get_video_comments(video_id)
        print(f"  Got {len(comments)} comments")

        if not comments:
            cursor.execute("""
                UPDATE youtube_videos SET comments_analyzed = true WHERE id = %s
            """, (video_id,))
            conn.commit()
            continue

        # Analyze with Gemini
        print("  Analyzing pain points...")
        result = analyze_comments_for_pain_points(title, comments)
        time.sleep(1)  # Rate limit

        # Update database
        cursor.execute("""
            UPDATE youtube_videos
            SET comments_analyzed = true,
                pain_points = %s,
                unsolved_questions = %s
            WHERE id = %s
        """, (
            json.dumps(result.get("pain_points", [])),
            result.get("unsolved_questions", []),
            video_id
        ))
        conn.commit()

        # Collect automatable pain points
        for p in result.get("pain_points", []):
            if p.get("can_automate"):
                all_pain_points.append({
                    "video_id": video_id,
                    "title": title,
                    **p
                })
                print(f"    AUTOMATABLE: {p.get('problem', 'N/A')[:60]}")

        for q in result.get("unsolved_questions", [])[:2]:
            print(f"    Q: {q[:60]}")

    # Summary
    print(f"\n\n=== Found {len(all_pain_points)} Automatable Pain Points ===\n")

    # Group by urgency
    by_urgency = {"high": [], "medium": [], "low": []}
    for p in all_pain_points:
        urgency = p.get("urgency", "low")
        by_urgency.get(urgency, by_urgency["low"]).append(p)

    for urgency in ["high", "medium", "low"]:
        problems = by_urgency[urgency]
        if problems:
            print(f"\n{urgency.upper()} URGENCY ({len(problems)}):")
            for p in problems[:5]:
                print(f"  Problem: {p.get('problem', 'N/A')}")
                print(f"  Automate: {p.get('automation_approach', 'N/A')[:70]}")
                print()

    # Save to file
    with open("data/automatable_pain_points.json", "w") as f:
        json.dump(all_pain_points, f, indent=2)

    print(f"\nSaved to data/automatable_pain_points.json")
    conn.close()


if __name__ == "__main__":
    main()
