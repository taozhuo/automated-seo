# Dockerfile for YouTube Scraper Worker
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements-azure.txt .
RUN pip install --no-cache-dir -r requirements-azure.txt

# Copy scraper code
COPY worker.py .

# Run worker
CMD ["python", "worker.py"]
