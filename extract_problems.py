#!/usr/bin/env python3
"""
Extract specific technical problems from YouTube transcripts using Gemini Flash 3.
"""

import json
import os
import time
import warnings
warnings.filterwarnings("ignore")

import psycopg2
from google import genai

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

def extract_problems(title: str, transcript: str) -> dict:
    if not transcript or len(transcript) < 100:
        return {"problems": []}

    transcript_sample = transcript[:5000]

    prompt = f"""You are a Roblox Engineer. Extract SPECIFIC technical problems from this tutorial.

TITLE: {title}
TRANSCRIPT: {transcript_sample}

CATEGORIES: Scripting, 3D Modeling, Terrain, Animation, UI, Monetization, Networking, Physics, Audio, Lighting, DataStore

Return JSON:
{{
    "problems": [
        {{
            "problem": "Specific searchable problem (e.g., 'Make door open when button clicked')",
            "category": "Category from list above",
            "apis": ["Roblox API/Service used"],
            "difficulty": "beginner/intermediate/advanced",
            "automatable": true/false
        }}
    ],
    "main_topic": "Primary concept"
}}"""

    try:
        response = client.models.generate_content(
            model="gemini-3-flash-preview",
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        return json.loads(response.text)
    except Exception as e:
        print(f"  Error: {e}", flush=True)
        return {"problems": []}


def main():
    conn = psycopg2.connect(dbname="seo_data", user=os.environ.get("USER"))
    cursor = conn.cursor()

    # Ensure columns exist
    cursor.execute("""
        ALTER TABLE youtube_videos ADD COLUMN IF NOT EXISTS technical_problems JSONB;
        ALTER TABLE youtube_videos ADD COLUMN IF NOT EXISTS main_topic VARCHAR(255);
    """)
    conn.commit()

    # Get videos
    cursor.execute("""
        SELECT id, title, transcript FROM youtube_videos
        WHERE has_seo_value = true AND transcript_length > 100
        ORDER BY views DESC
    """)
    videos = cursor.fetchall()
    print(f"Processing {len(videos)} videos...", flush=True)

    all_problems = []

    for i, (video_id, title, transcript) in enumerate(videos):
        print(f"[{i+1}/{len(videos)}] {title[:50]}...", flush=True)

        result = extract_problems(title, transcript)
        time.sleep(0.5)

        problems = result.get("problems", [])
        main_topic = result.get("main_topic", "")

        cursor.execute("""
            UPDATE youtube_videos
            SET technical_problems = %s, main_topic = %s
            WHERE id = %s
        """, (json.dumps(problems), main_topic, video_id))
        conn.commit()

        for p in problems:
            all_problems.append({"video": title, **p})
            if p.get("automatable"):
                print(f"  -> [{p.get('category')}] {p.get('problem', '')[:60]}", flush=True)

    # Summary
    print(f"\n=== Extracted {len(all_problems)} Problems ===", flush=True)

    # By category
    by_cat = {}
    for p in all_problems:
        cat = p.get("category", "Other")
        by_cat.setdefault(cat, []).append(p)

    for cat, probs in sorted(by_cat.items(), key=lambda x: -len(x[1])):
        print(f"\n{cat} ({len(probs)}):", flush=True)
        for p in probs[:3]:
            print(f"  - {p.get('problem', '')[:70]}", flush=True)

    # Save
    with open("data/technical_problems.json", "w") as f:
        json.dump(all_problems, f, indent=2)
    print(f"\nSaved to data/technical_problems.json", flush=True)

    conn.close()


if __name__ == "__main__":
    main()
