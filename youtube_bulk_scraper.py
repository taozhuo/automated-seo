#!/usr/bin/env python3
"""
Bulk YouTube Scraper - Designed for 100k+ videos
Features:
- Parallel workers with rate limiting
- Resumable scraping with checkpoints
- Rotating search queries
- Error handling and retries
- Progress tracking
"""

import json
import os
import re
import subprocess
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Optional
from queue import Queue
import hashlib

from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound
from tqdm import tqdm


# Configuration
CONFIG = {
    "data_dir": "data/youtube_bulk",
    "state_file": "data/youtube_bulk/scraper_state.json",
    "max_workers": 5,  # Parallel transcript fetchers
    "rate_limit": 0.5,  # Seconds between requests per worker
    "videos_per_query": 50,  # yt-dlp results per query
    "min_views": 1000,  # Minimum views filter
    "checkpoint_every": 100,  # Save state every N videos
    "max_retries": 3,
}

# Expanded search queries for comprehensive coverage
ROBLOX_QUERIES = [
    # Scripting fundamentals
    "roblox scripting tutorial",
    "roblox lua tutorial beginner",
    "how to script roblox",
    "roblox studio scripting",
    "learn roblox scripting",
    "roblox coding tutorial",

    # Specific scripting topics
    "roblox datastore tutorial",
    "roblox remote event tutorial",
    "roblox module script tutorial",
    "roblox oop tutorial",
    "roblox tween service tutorial",
    "roblox gui scripting",
    "roblox animation script",
    "roblox pathfinding tutorial",
    "roblox raycast tutorial",
    "roblox physics tutorial",
    "roblox collision tutorial",
    "roblox touched event",
    "roblox player data save",
    "roblox inventory system",
    "roblox combat system tutorial",
    "roblox round system tutorial",
    "roblox leaderboard tutorial",
    "roblox shop system tutorial",

    # Building
    "roblox building tutorial",
    "roblox studio tutorial beginner",
    "roblox terrain tutorial",
    "roblox lighting tutorial",
    "roblox blender to roblox",
    "roblox mesh tutorial",
    "roblox union tutorial",
    "roblox particle effects",
    "roblox ui design tutorial",

    # Game genres
    "how to make roblox obby",
    "how to make roblox simulator",
    "how to make roblox tycoon",
    "roblox fps game tutorial",
    "roblox rpg tutorial",
    "roblox horror game tutorial",
    "roblox racing game tutorial",
    "roblox fighting game tutorial",

    # Monetization
    "roblox gamepass tutorial",
    "roblox dev product tutorial",
    "how to make robux from games",
    "roblox game monetization",

    # Advanced
    "roblox optimization tutorial",
    "roblox anti cheat tutorial",
    "roblox server script tutorial",
    "roblox filtering enabled",
    "roblox replication tutorial",

    # Common problems (pain points!)
    "roblox script not working",
    "roblox datastore not saving",
    "roblox remote event not working",
    "roblox animation not playing",
    "roblox gui not showing",
    "roblox lag fix",
    "roblox studio crash fix",
]


@dataclass
class VideoData:
    id: str
    title: str
    channel: str
    views: int
    duration: str
    url: str
    query: str
    transcript: str = ""
    scraped_at: str = ""
    error: str = ""


class BulkYouTubeScraper:
    def __init__(self):
        self.data_dir = Path(CONFIG["data_dir"])
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = Path(CONFIG["state_file"])
        self.state = self._load_state()
        self.lock = Lock()
        self.request_times = {}  # Track per-worker rate limiting

    def _load_state(self) -> dict:
        if self.state_file.exists():
            with open(self.state_file) as f:
                return json.load(f)
        return {
            "scraped_ids": [],
            "failed_ids": [],
            "queries_completed": [],
            "total_scraped": 0,
            "total_with_transcript": 0,
            "started_at": datetime.now().isoformat(),
        }

    def _save_state(self):
        with self.lock:
            with open(self.state_file, "w") as f:
                json.dump(self.state, f, indent=2)

    def search_videos(self, query: str, max_results: int = 50) -> list[dict]:
        """Search YouTube using yt-dlp."""
        cmd = [
            "yt-dlp",
            f"ytsearch{max_results}:{query}",
            "--dump-json",
            "--flat-playlist",
            "--no-download",
            "--ignore-errors",
            "--quiet",
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            videos = []
            for line in result.stdout.strip().split("\n"):
                if line:
                    try:
                        data = json.loads(line)
                        if data.get("view_count", 0) >= CONFIG["min_views"]:
                            data["query"] = query
                            videos.append(data)
                    except json.JSONDecodeError:
                        continue
            return videos
        except Exception as e:
            print(f"Search error for '{query}': {e}")
            return []

    def get_transcript(self, video_id: str) -> tuple[str, str]:
        """Get transcript with error handling."""
        try:
            api = YouTubeTranscriptApi()
            transcript_list = api.fetch(video_id, languages=["en", "en-US", "en-GB"])
            full_text = " ".join([t.text for t in transcript_list])
            full_text = re.sub(r"\[.*?\]", "", full_text)
            full_text = re.sub(r"\s+", " ", full_text).strip()
            return full_text, ""
        except (TranscriptsDisabled, NoTranscriptFound):
            return "", "no_transcript"
        except Exception as e:
            return "", str(e)[:100]

    def process_video(self, video_data: dict, worker_id: int) -> Optional[VideoData]:
        """Process a single video - fetch transcript and save."""
        video_id = video_data.get("id", "")

        # Skip if already scraped
        with self.lock:
            if video_id in self.state["scraped_ids"]:
                return None

        # Rate limiting per worker
        worker_key = f"worker_{worker_id}"
        if worker_key in self.request_times:
            elapsed = time.time() - self.request_times[worker_key]
            if elapsed < CONFIG["rate_limit"]:
                time.sleep(CONFIG["rate_limit"] - elapsed)

        self.request_times[worker_key] = time.time()

        # Get transcript
        transcript, error = self.get_transcript(video_id)

        video = VideoData(
            id=video_id,
            title=video_data.get("title", ""),
            channel=video_data.get("channel", video_data.get("uploader", "")),
            views=video_data.get("view_count", 0),
            duration=video_data.get("duration_string", ""),
            url=f"https://www.youtube.com/watch?v={video_id}",
            query=video_data.get("query", ""),
            transcript=transcript,
            scraped_at=datetime.now().isoformat(),
            error=error,
        )

        # Save individual video
        self._save_video(video)

        # Update state
        with self.lock:
            self.state["scraped_ids"].append(video_id)
            self.state["total_scraped"] += 1
            if transcript:
                self.state["total_with_transcript"] += 1

            if self.state["total_scraped"] % CONFIG["checkpoint_every"] == 0:
                self._save_state()

        return video

    def _save_video(self, video: VideoData):
        """Save video to JSON file."""
        # Organize by first 2 chars of ID for better file distribution
        subdir = self.data_dir / "videos" / video.id[:2]
        subdir.mkdir(parents=True, exist_ok=True)
        filepath = subdir / f"{video.id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(asdict(video), f, ensure_ascii=False)

    def collect_video_urls(self, queries: list[str], target_count: int) -> list[dict]:
        """Collect video URLs from multiple queries."""
        all_videos = []
        seen_ids = set(self.state["scraped_ids"])

        print(f"Collecting video URLs (target: {target_count})...")

        for query in tqdm(queries, desc="Searching queries"):
            if query in self.state["queries_completed"]:
                continue

            videos = self.search_videos(query, CONFIG["videos_per_query"])

            for v in videos:
                vid = v.get("id", "")
                if vid and vid not in seen_ids:
                    all_videos.append(v)
                    seen_ids.add(vid)

            self.state["queries_completed"].append(query)

            if len(all_videos) >= target_count:
                break

            # Small delay between searches
            time.sleep(0.5)

        print(f"Collected {len(all_videos)} unique video URLs")
        return all_videos[:target_count]

    def scrape(self, target_count: int = 1000, queries: Optional[list[str]] = None):
        """Main scraping function with parallel workers."""
        queries = queries or ROBLOX_QUERIES

        # Expand queries if needed for large targets
        if target_count > len(queries) * CONFIG["videos_per_query"]:
            # Add variations
            expanded = []
            for q in queries:
                expanded.append(q)
                expanded.append(f"{q} 2024")
                expanded.append(f"{q} 2025")
                expanded.append(f"{q} beginner")
                expanded.append(f"{q} advanced")
            queries = expanded

        # Collect video URLs first
        videos_to_process = self.collect_video_urls(queries, target_count)

        if not videos_to_process:
            print("No new videos to process")
            return

        print(f"\nProcessing {len(videos_to_process)} videos with {CONFIG['max_workers']} workers...")

        success_count = 0
        transcript_count = 0

        with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor:
            futures = {
                executor.submit(self.process_video, video, i % CONFIG["max_workers"]): video
                for i, video in enumerate(videos_to_process)
            }

            with tqdm(total=len(futures), desc="Scraping") as pbar:
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            success_count += 1
                            if result.transcript:
                                transcript_count += 1
                    except Exception as e:
                        pass  # Already handled in process_video
                    pbar.update(1)
                    pbar.set_postfix({
                        "success": success_count,
                        "transcripts": transcript_count
                    })

        self._save_state()

        print(f"\n=== Scraping Complete ===")
        print(f"Total processed: {success_count}")
        print(f"With transcripts: {transcript_count}")
        print(f"Overall total: {self.state['total_scraped']}")
        print(f"Data saved to: {self.data_dir}")

    def export_analysis(self, output_file: str = "data/youtube_bulk_analysis.json"):
        """Analyze all scraped videos."""
        from collections import Counter

        print("Analyzing scraped videos...")

        videos_dir = self.data_dir / "videos"
        if not videos_dir.exists():
            print("No videos found")
            return

        all_transcripts = []
        video_data = []

        for subdir in videos_dir.iterdir():
            if subdir.is_dir():
                for f in subdir.glob("*.json"):
                    try:
                        with open(f) as fp:
                            v = json.load(fp)
                            if v.get("transcript"):
                                all_transcripts.append(v["transcript"].lower())
                                video_data.append({
                                    "title": v["title"],
                                    "views": v["views"],
                                    "url": v["url"],
                                    "query": v["query"],
                                })
                    except:
                        continue

        # Keyword extraction
        roblox_terms = [
            "script", "local script", "module script", "server script",
            "remote event", "remote function", "bindable",
            "datastore", "data store", "save data", "profileservice",
            "tween", "animation", "animate",
            "gui", "screen gui", "ui", "button", "textlabel", "frame",
            "part", "mesh", "union", "model", "cframe",
            "player", "character", "humanoid", "tool",
            "workspace", "replicated storage", "server storage",
            "raycast", "collision", "physics", "touched",
            "loop", "while", "for loop", "function", "return",
            "table", "array", "dictionary", "metatables",
            "pathfinding", "ai", "npc",
            "gamepass", "dev product", "robux", "monetize",
            "obby", "simulator", "tycoon", "fps", "rpg",
            "error", "bug", "fix", "not working", "problem",
            "beginner", "advanced", "tutorial", "how to",
        ]

        keyword_counts = Counter()
        combined_text = " ".join(all_transcripts)
        for term in roblox_terms:
            count = combined_text.count(term)
            if count > 0:
                keyword_counts[term] = count

        # Sort videos by views
        video_data.sort(key=lambda x: x["views"], reverse=True)

        output = {
            "total_videos": len(video_data),
            "total_transcript_chars": len(combined_text),
            "top_keywords": keyword_counts.most_common(50),
            "top_videos_by_views": video_data[:100],
            "state": self.state,
        }

        with open(output_file, "w") as f:
            json.dump(output, f, indent=2)

        print(f"Analysis saved to {output_file}")
        print(f"Total videos with transcripts: {len(video_data)}")
        print(f"\nTop 20 keywords:")
        for kw, count in keyword_counts.most_common(20):
            print(f"  {kw}: {count:,}")

        return output


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Bulk YouTube Roblox Tutorial Scraper")
    parser.add_argument("--target", "-t", type=int, default=1000, help="Target number of videos")
    parser.add_argument("--workers", "-w", type=int, default=5, help="Number of parallel workers")
    parser.add_argument("--analyze", "-a", action="store_true", help="Run analysis only")
    parser.add_argument("--min-views", "-v", type=int, default=1000, help="Minimum view count")

    args = parser.parse_args()

    CONFIG["max_workers"] = args.workers
    CONFIG["min_views"] = args.min_views

    scraper = BulkYouTubeScraper()

    if args.analyze:
        scraper.export_analysis()
    else:
        scraper.scrape(target_count=args.target)
        scraper.export_analysis()


if __name__ == "__main__":
    main()
