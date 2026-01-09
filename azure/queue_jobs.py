#!/usr/bin/env python3
"""
Queue Jobs - Searches YouTube and queues video IDs for processing.
Run this locally to populate the Azure Queue before deploying workers.
"""

import argparse
import json
import os
import subprocess
import time
from pathlib import Path

from azure.storage.queue import QueueClient
from tqdm import tqdm


# Search queries for comprehensive Roblox coverage
QUERIES = [
    # Scripting fundamentals
    "roblox scripting tutorial",
    "roblox lua tutorial",
    "how to script roblox",
    "roblox studio scripting beginner",
    "learn roblox coding",
    "roblox programming tutorial",

    # Specific topics
    "roblox datastore tutorial",
    "roblox remote event tutorial",
    "roblox module script tutorial",
    "roblox tween service tutorial",
    "roblox gui tutorial",
    "roblox animation script tutorial",
    "roblox pathfinding tutorial",
    "roblox raycast tutorial",
    "roblox touched event tutorial",
    "roblox player data save",
    "roblox inventory system tutorial",
    "roblox combat system tutorial",
    "roblox round system tutorial",
    "roblox leaderboard tutorial",
    "roblox shop system tutorial",
    "roblox npc tutorial",
    "roblox ai tutorial",
    "roblox oop tutorial",
    "roblox metatables tutorial",

    # Building
    "roblox building tutorial",
    "roblox studio tutorial",
    "roblox terrain tutorial",
    "roblox lighting tutorial",
    "roblox blender tutorial",
    "roblox mesh tutorial",
    "roblox ui design tutorial",
    "roblox particle effects tutorial",

    # Game genres
    "how to make roblox obby",
    "how to make roblox simulator",
    "how to make roblox tycoon",
    "roblox fps game tutorial",
    "roblox rpg tutorial",
    "roblox horror game tutorial",
    "roblox fighting game tutorial",

    # Monetization
    "roblox gamepass tutorial",
    "roblox dev product tutorial",
    "how to make robux",
    "roblox monetization",

    # Problems/Pain points
    "roblox script not working fix",
    "roblox datastore not saving fix",
    "roblox remote event not working",
    "roblox animation not playing fix",
    "roblox lag fix optimization",
    "roblox studio crash fix",
    "roblox filtering enabled explained",

    # Years for freshness
    "roblox tutorial 2024",
    "roblox tutorial 2025",
    "roblox scripting 2024",
    "roblox scripting 2025",
]


def search_youtube(query: str, max_results: int = 100) -> list[dict]:
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
                    videos.append({
                        "video_id": data.get("id"),
                        "title": data.get("title"),
                        "views": data.get("view_count", 0),
                        "query": query,
                    })
                except json.JSONDecodeError:
                    continue
        return videos
    except Exception as e:
        print(f"Error searching '{query}': {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Queue YouTube scraping jobs")
    parser.add_argument("--count", "-c", type=int, default=10000, help="Target video count")
    parser.add_argument("--min-views", "-v", type=int, default=1000, help="Minimum views")
    parser.add_argument("--per-query", "-p", type=int, default=100, help="Results per query")
    parser.add_argument("--connection", "-s", type=str, help="Storage connection string")

    args = parser.parse_args()

    # Get connection string
    connection = args.connection or os.environ.get("STORAGE_CONNECTION")
    if not connection:
        # Try to load from config file
        config_file = Path("azure/config.env")
        if config_file.exists():
            for line in config_file.read_text().splitlines():
                if line.startswith("STORAGE_CONNECTION="):
                    connection = line.split("=", 1)[1]
                    break

    if not connection:
        print("Error: No storage connection string provided")
        print("Use --connection or set STORAGE_CONNECTION env var")
        return

    queue_client = QueueClient.from_connection_string(connection, "scraper-jobs")

    print(f"Target: {args.count} videos with {args.min_views}+ views")
    print(f"Queries: {len(QUERIES)}")
    print()

    # Expand queries for more coverage
    all_queries = QUERIES.copy()
    if args.count > len(QUERIES) * args.per_query:
        for q in QUERIES[:20]:
            all_queries.append(f"{q} beginner")
            all_queries.append(f"{q} advanced")
            all_queries.append(f"{q} full guide")

    seen_ids = set()
    queued = 0

    for query in tqdm(all_queries, desc="Searching"):
        if queued >= args.count:
            break

        videos = search_youtube(query, args.per_query)

        for video in videos:
            if queued >= args.count:
                break

            vid = video["video_id"]
            views = video.get("views", 0) or 0

            if vid and vid not in seen_ids and views >= args.min_views:
                # Add to queue
                queue_client.send_message(json.dumps({
                    "video_id": vid,
                    "query": query,
                }))
                seen_ids.add(vid)
                queued += 1

        time.sleep(0.3)  # Rate limit searches

    print(f"\n=== Queued {queued} videos ===")
    print(f"Queue: scraper-jobs")
    print(f"Now deploy workers with: ./azure/deploy_workers.sh --count 20")


if __name__ == "__main__":
    main()
