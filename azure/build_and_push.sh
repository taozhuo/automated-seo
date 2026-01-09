#!/bin/bash
# Build and push Docker image to Azure Container Registry

set -e

# Load config
source azure/config.env

echo "=== Building and pushing scraper image ==="

# Login to ACR
echo "Logging into ACR: $CONTAINER_REGISTRY"
az acr login --name $CONTAINER_REGISTRY

# Build image
echo "Building Docker image..."
docker build -t $CONTAINER_REGISTRY.azurecr.io/youtube-scraper:latest -f azure/Dockerfile azure/

# Push to ACR
echo "Pushing to ACR..."
docker push $CONTAINER_REGISTRY.azurecr.io/youtube-scraper:latest

echo ""
echo "=== Image pushed successfully ==="
echo "Image: $CONTAINER_REGISTRY.azurecr.io/youtube-scraper:latest"
