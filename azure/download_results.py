#!/usr/bin/env python3
"""
Download and analyze results from Azure Blob Storage.
"""

import argparse
import json
import os
from collections import Counter
from pathlib import Path

from azure.storage.blob import BlobServiceClient
from tqdm import tqdm


def main():
    parser = argparse.ArgumentParser(description="Download scraping results from Azure")
    parser.add_argument("--connection", "-s", type=str, help="Storage connection string")
    parser.add_argument("--output", "-o", type=str, default="data/azure_results", help="Output directory")
    parser.add_argument("--analyze-only", "-a", action="store_true", help="Only analyze, don't download")

    args = parser.parse_args()

    # Get connection string
    connection = args.connection or os.environ.get("STORAGE_CONNECTION")
    if not connection:
        config_file = Path("azure/config.env")
        if config_file.exists():
            for line in config_file.read_text().splitlines():
                if line.startswith("STORAGE_CONNECTION="):
                    connection = line.split("=", 1)[1]
                    break

    if not connection:
        print("Error: No storage connection string")
        return

    blob_service = BlobServiceClient.from_connection_string(connection)
    container = blob_service.get_container_client("scraper-results")

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not args.analyze_only:
        print("Downloading results from Azure...")
        blobs = list(container.list_blobs(name_starts_with="videos/"))
        print(f"Found {len(blobs)} videos")

        for blob in tqdm(blobs, desc="Downloading"):
            blob_client = container.get_blob_client(blob.name)
            local_path = output_dir / blob.name
            local_path.parent.mkdir(parents=True, exist_ok=True)

            with open(local_path, "wb") as f:
                f.write(blob_client.download_blob().readall())

    # Analyze results
    print("\nAnalyzing results...")

    videos_dir = output_dir / "videos"
    if not videos_dir.exists():
        print("No videos found")
        return

    all_data = []
    keyword_counter = Counter()

    roblox_terms = [
        "script", "local script", "module script", "server script",
        "remote event", "remote function", "datastore", "data store",
        "tween", "animation", "gui", "screen gui",
        "part", "mesh", "model", "cframe",
        "player", "character", "humanoid", "tool",
        "workspace", "replicated storage",
        "raycast", "collision", "touched",
        "loop", "while", "for loop", "function",
        "table", "array", "metatables",
        "pathfinding", "ai", "npc",
        "gamepass", "robux", "monetize",
        "obby", "simulator", "tycoon",
        "error", "bug", "fix", "not working",
    ]

    for subdir in videos_dir.iterdir():
        if subdir.is_dir():
            for f in subdir.glob("*.json"):
                try:
                    with open(f) as fp:
                        data = json.load(fp)
                        all_data.append(data)

                        if data.get("transcript"):
                            text = data["transcript"].lower()
                            for term in roblox_terms:
                                count = text.count(term)
                                if count > 0:
                                    keyword_counter[term] += count
                except:
                    continue

    # Stats
    with_transcript = [d for d in all_data if d.get("transcript")]
    total_views = sum(d.get("views", 0) for d in all_data)

    print(f"\n=== Results ===")
    print(f"Total videos: {len(all_data)}")
    print(f"With transcripts: {len(with_transcript)}")
    print(f"Total views: {total_views:,}")

    print(f"\nTop 30 keywords from transcripts:")
    for kw, count in keyword_counter.most_common(30):
        print(f"  {kw}: {count:,}")

    # Export analysis
    analysis = {
        "total_videos": len(all_data),
        "with_transcripts": len(with_transcript),
        "total_views": total_views,
        "top_keywords": keyword_counter.most_common(50),
        "top_videos": sorted(
            [{"title": d["title"], "views": d["views"], "url": d["url"]}
             for d in with_transcript],
            key=lambda x: x["views"],
            reverse=True
        )[:100]
    }

    with open(output_dir / "analysis.json", "w") as f:
        json.dump(analysis, f, indent=2)

    print(f"\nAnalysis saved to {output_dir}/analysis.json")


if __name__ == "__main__":
    main()
