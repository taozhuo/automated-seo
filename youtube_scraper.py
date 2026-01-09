#!/usr/bin/env python3
"""
YouTube Tutorial Scraper for Roblox development content.
Extracts video metadata and transcripts for SEO research.
"""

import json
import re
import subprocess
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound


@dataclass
class YouTubeVideo:
    id: str
    title: str
    channel: str
    views: int
    duration: str
    upload_date: str
    description: str
    url: str
    transcript: str = ""
    transcript_segments: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)


class YouTubeScraper:
    def __init__(self, data_dir: str = "data/youtube"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def search_videos(
        self,
        query: str,
        max_results: int = 20,
        min_views: int = 10000,
    ) -> list[dict]:
        """Search YouTube for videos using yt-dlp."""
        print(f"Searching YouTube for: {query}")

        cmd = [
            "yt-dlp",
            f"ytsearch{max_results * 2}:{query}",  # Get extra to filter by views
            "--dump-json",
            "--flat-playlist",
            "--no-download",
            "--ignore-errors",
        ]

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=60
            )
            videos = []
            for line in result.stdout.strip().split("\n"):
                if line:
                    try:
                        data = json.loads(line)
                        if data.get("view_count", 0) >= min_views:
                            videos.append(data)
                    except json.JSONDecodeError:
                        continue

            # Sort by views and limit
            videos.sort(key=lambda x: x.get("view_count", 0), reverse=True)
            return videos[:max_results]

        except subprocess.TimeoutExpired:
            print("Search timed out")
            return []
        except Exception as e:
            print(f"Search error: {e}")
            return []

    def get_transcript(self, video_id: str) -> tuple[str, list[dict]]:
        """Get transcript for a video."""
        try:
            api = YouTubeTranscriptApi()
            transcript_list = api.fetch(video_id, languages=["en", "en-US", "en-GB"])

            # Convert to list of dicts
            segments = [{"text": t.text, "start": t.start, "duration": t.duration} for t in transcript_list]

            # Combine into full text
            full_text = " ".join([t.text for t in transcript_list])

            # Clean up transcript
            full_text = re.sub(r"\[.*?\]", "", full_text)  # Remove [Music] etc
            full_text = re.sub(r"\s+", " ", full_text).strip()

            return full_text, segments

        except (TranscriptsDisabled, NoTranscriptFound):
            return "", []
        except Exception as e:
            print(f"Transcript error for {video_id}: {e}")
            return "", []

    def scrape_video(self, video_data: dict) -> Optional[YouTubeVideo]:
        """Scrape a single video's metadata and transcript."""
        video_id = video_data.get("id", "")
        if not video_id:
            return None

        print(f"  Getting transcript for: {video_data.get('title', '')[:50]}...")

        transcript, segments = self.get_transcript(video_id)

        description = video_data.get("description") or ""
        video = YouTubeVideo(
            id=video_id,
            title=video_data.get("title", ""),
            channel=video_data.get("channel", video_data.get("uploader", "")),
            views=video_data.get("view_count", 0),
            duration=video_data.get("duration_string", ""),
            upload_date=video_data.get("upload_date", ""),
            description=description[:500],
            url=f"https://www.youtube.com/watch?v={video_id}",
            transcript=transcript,
            transcript_segments=segments,
        )

        return video

    def save_video(self, video: YouTubeVideo, category: str = "tutorials") -> Path:
        """Save video data to JSON."""
        category_dir = self.data_dir / category
        category_dir.mkdir(parents=True, exist_ok=True)
        filepath = category_dir / f"{video.id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(video.to_json())
        return filepath

    def scrape(
        self,
        queries: list[str],
        max_per_query: int = 10,
        min_views: int = 10000,
    ) -> list[YouTubeVideo]:
        """Scrape videos for multiple search queries."""
        all_videos = []
        seen_ids = set()

        for query in queries:
            print(f"\n--- Searching: {query} ---")
            results = self.search_videos(query, max_per_query, min_views)
            print(f"Found {len(results)} videos with {min_views}+ views")

            for video_data in results:
                video_id = video_data.get("id", "")
                if video_id in seen_ids:
                    continue
                seen_ids.add(video_id)

                video = self.scrape_video(video_data)
                if video and video.transcript:
                    self.save_video(video)
                    all_videos.append(video)
                    print(f"    ✓ {video.views:,} views, {len(video.transcript)} chars transcript")
                elif video:
                    print(f"    ✗ No transcript available")

        return all_videos

    def export_analysis(self, videos: list[YouTubeVideo], output_file: str = "data/youtube_analysis.json"):
        """Export analysis of scraped videos."""
        # Extract keywords from transcripts
        from collections import Counter

        roblox_terms = [
            "script", "local script", "module script", "server script",
            "remote event", "remote function", "bindable",
            "datastore", "data store", "save data",
            "tween", "animation", "animate",
            "gui", "screen gui", "surface gui", "billboard",
            "part", "mesh", "union", "model",
            "player", "character", "humanoid", "tool",
            "workspace", "replicated storage", "server storage",
            "raycast", "collision", "physics",
            "touched", "click", "input",
            "loop", "while", "for loop", "function",
            "table", "array", "dictionary",
            "pathfinding", "ai", "npc",
            "chat", "filter",
            "gamepass", "dev product", "robux", "monetize",
            "obby", "simulator", "tycoon", "fps", "rpg",
        ]

        keyword_counts = Counter()
        for video in videos:
            text = video.transcript.lower()
            for term in roblox_terms:
                count = text.count(term)
                if count > 0:
                    keyword_counts[term] += count

        output = {
            "total_videos": len(videos),
            "total_views": sum(v.views for v in videos),
            "top_keywords": keyword_counts.most_common(30),
            "videos": [
                {
                    "title": v.title,
                    "channel": v.channel,
                    "views": v.views,
                    "url": v.url,
                    "transcript_preview": v.transcript[:500] if v.transcript else "",
                }
                for v in sorted(videos, key=lambda x: x.views, reverse=True)
            ],
        }

        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(output, f, indent=2)

        return output


# Default search queries for Roblox development
ROBLOX_TUTORIAL_QUERIES = [
    "roblox scripting tutorial",
    "roblox studio tutorial beginner",
    "roblox lua tutorial",
    "how to script in roblox",
    "roblox building tutorial",
    "roblox game development",
    "roblox datastore tutorial",
    "roblox animation tutorial",
    "roblox gui tutorial",
    "roblox pathfinding tutorial",
]


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Scrape YouTube Roblox tutorials")
    parser.add_argument(
        "--queries", "-q", nargs="+",
        default=ROBLOX_TUTORIAL_QUERIES[:3],
        help="Search queries"
    )
    parser.add_argument(
        "--max", "-m", type=int, default=5,
        help="Max videos per query"
    )
    parser.add_argument(
        "--min-views", "-v", type=int, default=10000,
        help="Minimum view count"
    )

    args = parser.parse_args()

    scraper = YouTubeScraper()
    videos = scraper.scrape(args.queries, args.max, args.min_views)

    if videos:
        analysis = scraper.export_analysis(videos)
        print(f"\n=== Results ===")
        print(f"Scraped {len(videos)} videos with transcripts")
        print(f"Total views: {analysis['total_views']:,}")
        print(f"\nTop keywords from transcripts:")
        for kw, count in analysis["top_keywords"][:15]:
            print(f"  {kw}: {count}")
        print(f"\nData saved to data/youtube/")
    else:
        print("No videos with transcripts found")


if __name__ == "__main__":
    main()
