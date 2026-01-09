#!/usr/bin/env python3
"""Download forum scraper results from Azure and load into PostgreSQL."""

import json
import os
import psycopg2
from azure.storage.blob import BlobServiceClient

# Load config
config = {}
with open("azure/config.env") as f:
    for line in f:
        if "=" in line:
            key, value = line.strip().split("=", 1)
            config[key] = value

STORAGE_CONNECTION = config["STORAGE_CONNECTION"]
RESULTS_CONTAINER = config["RESULTS_CONTAINER"]
DB_NAME = "seo_data"
DB_USER = os.environ.get("USER")


def main():
    # Connect to Azure
    blob_service = BlobServiceClient.from_connection_string(STORAGE_CONNECTION)
    container = blob_service.get_container_client(RESULTS_CONTAINER)

    # Connect to PostgreSQL
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER)
    cursor = conn.cursor()

    # Download all problem files
    print("Downloading results from Azure...")
    downloaded = 0
    loaded = 0

    for blob in container.list_blobs(name_starts_with="problems/"):
        try:
            blob_client = container.get_blob_client(blob.name)
            data = json.loads(blob_client.download_blob().readall())

            analysis = data.get("analysis", {})

            cursor.execute("""
                INSERT INTO problems
                (source, source_id, title, url, category, problem, solution, solved,
                 difficulty, apis_mentioned, error_messages, automatable, automation_hint,
                 views, upvotes, comments, created_at, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                data.get("source"),
                data.get("source_id"),
                data.get("title"),
                data.get("url"),
                analysis.get("category"),
                analysis.get("problem"),
                analysis.get("solution"),
                analysis.get("solved"),
                analysis.get("difficulty"),
                analysis.get("apis_mentioned"),
                analysis.get("error_messages"),
                analysis.get("automatable"),
                analysis.get("automation_hint"),
                data.get("views", 0),
                data.get("upvotes", 0),
                data.get("comments", data.get("replies", 0)),
                data.get("created_at"),
                json.dumps(data)
            ))
            loaded += 1
            downloaded += 1

        except Exception as e:
            print(f"Error processing {blob.name}: {e}")

    conn.commit()

    # Stats
    cursor.execute("""
        SELECT source, COUNT(*), SUM(CASE WHEN automatable THEN 1 ELSE 0 END)
        FROM problems GROUP BY source
    """)
    print(f"\n=== Downloaded {downloaded} results ===\n")
    print("Database stats:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} total, {row[2]} automatable")

    conn.close()


if __name__ == "__main__":
    main()
