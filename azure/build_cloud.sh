#!/bin/bash
# Build Docker image in Azure (no local Docker needed)

set -e
source azure/config.env

echo "=== Building image in Azure Cloud ==="

# Build in ACR directly
az acr build \
    --registry $CONTAINER_REGISTRY \
    --image youtube-scraper:latest \
    --file azure/Dockerfile \
    azure/

echo "=== Image built successfully ==="
echo "Image: $CONTAINER_REGISTRY.azurecr.io/youtube-scraper:latest"
