#!/usr/bin/env python3
"""
Azure Worker - Pulls jobs from queue, scrapes transcripts, uploads to blob storage.
Runs in Azure Container Instance.
"""

import json
import os
import re
import subprocess
import time
import signal
from datetime import datetime
from typing import Optional

from azure.storage.queue import QueueClient
from azure.storage.blob import BlobServiceClient
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound


VISIBILITY_TIMEOUT = 300  # 5 minutes to process


class ScraperWorker:
    def __init__(self):
        # Read config from environment
        self.storage_connection = os.environ.get("STORAGE_CONNECTION")
        if not self.storage_connection:
            raise ValueError("STORAGE_CONNECTION environment variable required")

        self.queue_name = os.environ.get("QUEUE_NAME", "scraper-jobs")
        self.results_container = os.environ.get("RESULTS_CONTAINER", "scraper-results")
        self.worker_id = os.environ.get("WORKER_ID", "worker-0")
        self.batch_size = int(os.environ.get("BATCH_SIZE", "32"))

        # Initialize clients
        self.queue_client = QueueClient.from_connection_string(
            self.storage_connection, self.queue_name
        )
        self.blob_service = BlobServiceClient.from_connection_string(
            self.storage_connection
        )
        self.blob_container = self.blob_service.get_container_client(
            self.results_container
        )

        self.running = True
        self.processed = 0
        self.errors = 0

        # Graceful shutdown
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        print(f"[{self.worker_id}] Shutting down gracefully...")
        self.running = False

    def get_transcript(self, video_id: str) -> tuple[str, str]:
        """Fetch transcript for a video."""
        try:
            api = YouTubeTranscriptApi()
            transcript = api.fetch(video_id, languages=["en", "en-US", "en-GB"])
            text = " ".join([t.text for t in transcript])
            text = re.sub(r"\[.*?\]", "", text)
            text = re.sub(r"\s+", " ", text).strip()
            return text, ""
        except (TranscriptsDisabled, NoTranscriptFound):
            return "", "no_transcript"
        except Exception as e:
            return "", str(e)[:200]

    def get_video_metadata(self, video_id: str) -> Optional[dict]:
        """Get video metadata using yt-dlp."""
        try:
            cmd = [
                "yt-dlp",
                f"https://www.youtube.com/watch?v={video_id}",
                "--dump-json",
                "--no-download",
                "--quiet",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                return json.loads(result.stdout)
        except Exception:
            pass
        return None

    def process_job(self, job: dict) -> dict:
        """Process a single video job."""
        video_id = job["video_id"]
        query = job.get("query", "")

        # Get metadata
        metadata = self.get_video_metadata(video_id)

        # Get transcript
        transcript, error = self.get_transcript(video_id)

        result = {
            "video_id": video_id,
            "title": metadata.get("title", "") if metadata else "",
            "channel": metadata.get("channel", metadata.get("uploader", "")) if metadata else "",
            "views": metadata.get("view_count", 0) if metadata else 0,
            "duration": metadata.get("duration_string", "") if metadata else "",
            "upload_date": metadata.get("upload_date", "") if metadata else "",
            "description": (metadata.get("description", "") or "")[:500] if metadata else "",
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "query": query,
            "transcript": transcript,
            "transcript_length": len(transcript),
            "error": error,
            "worker_id": self.worker_id,
            "processed_at": datetime.utcnow().isoformat(),
        }

        return result

    def upload_result(self, result: dict):
        """Upload result to blob storage."""
        video_id = result["video_id"]
        blob_name = f"videos/{video_id[:2]}/{video_id}.json"

        blob_client = self.blob_container.get_blob_client(blob_name)
        blob_client.upload_blob(
            json.dumps(result, ensure_ascii=False),
            overwrite=True
        )

    def run(self):
        """Main worker loop - pull jobs from queue and process."""
        print(f"[{self.worker_id}] Starting worker...")
        print(f"[{self.worker_id}] Queue: {self.queue_name}, Container: {self.results_container}")

        while self.running:
            try:
                # Get batch of messages
                messages = self.queue_client.receive_messages(
                    messages_per_page=self.batch_size,
                    visibility_timeout=VISIBILITY_TIMEOUT
                )

                batch_count = 0
                for message in messages:
                    if not self.running:
                        break

                    try:
                        job = json.loads(message.content)
                        result = self.process_job(job)

                        # Upload result
                        self.upload_result(result)

                        # Delete message from queue (job completed)
                        self.queue_client.delete_message(message)

                        self.processed += 1
                        batch_count += 1

                        if result["transcript"]:
                            print(f"[{self.worker_id}] OK {result['video_id']} ({result['views']:,} views, {result['transcript_length']} chars)")
                        else:
                            print(f"[{self.worker_id}] NO {result['video_id']} ({result['error']})")

                    except Exception as e:
                        self.errors += 1
                        print(f"[{self.worker_id}] Error: {e}")

                    # Small delay between videos
                    time.sleep(0.5)

                if batch_count == 0:
                    # No messages, wait before polling again
                    print(f"[{self.worker_id}] Queue empty, waiting... (processed: {self.processed})")
                    time.sleep(10)

            except Exception as e:
                print(f"[{self.worker_id}] Queue error: {e}")
                time.sleep(5)

        print(f"[{self.worker_id}] Stopped. Processed: {self.processed}, Errors: {self.errors}")


def main():
    worker = ScraperWorker()
    worker.run()


if __name__ == "__main__":
    main()
