#!/bin/bash
# Deploy DevForum and Reddit scrapers to Azure Container Instances

set -e
source azure/config.env

echo "=== Building cloud scraper image ==="

# Create Dockerfile for forum scraper
cat > azure/Dockerfile.forum << 'EOF'
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    requests \
    azure-storage-blob \
    google-genai

COPY cloud_scraper.py .

CMD ["python", "cloud_scraper.py"]
EOF

# Build in ACR
az acr build \
    --registry $CONTAINER_REGISTRY \
    --image forum-scraper:latest \
    --file azure/Dockerfile.forum \
    azure/

echo "=== Deploying scrapers ==="

# Get ACR credentials
ACR_PASSWORD=$(az acr credential show --name $CONTAINER_REGISTRY --query "passwords[0].value" -o tsv)

# Deploy DevForum scraper (2 workers)
for i in 1 2; do
    az container create \
        --resource-group $RESOURCE_GROUP \
        --name forum-scraper-devforum-$i \
        --image $CONTAINER_REGISTRY.azurecr.io/forum-scraper:latest \
        --registry-login-server $CONTAINER_REGISTRY.azurecr.io \
        --registry-username $CONTAINER_REGISTRY \
        --registry-password "$ACR_PASSWORD" \
        --os-type Linux \
        --cpu 1 \
        --memory 1 \
        --restart-policy Never \
        --secure-environment-variables \
            GEMINI_API_KEY="$GEMINI_API_KEY" \
            STORAGE_CONNECTION="$STORAGE_CONNECTION" \
        --environment-variables \
            RESULTS_CONTAINER="$RESULTS_CONTAINER" \
            SOURCE="devforum" \
            PAGES="100" \
            WORKER_ID="devforum-$i" \
        --output none &
    sleep 2
done

# Deploy Reddit scraper (2 workers)
for i in 1 2; do
    az container create \
        --resource-group $RESOURCE_GROUP \
        --name forum-scraper-reddit-$i \
        --image $CONTAINER_REGISTRY.azurecr.io/forum-scraper:latest \
        --registry-login-server $CONTAINER_REGISTRY.azurecr.io \
        --registry-username $CONTAINER_REGISTRY \
        --registry-password "$ACR_PASSWORD" \
        --os-type Linux \
        --cpu 1 \
        --memory 1 \
        --restart-policy Never \
        --secure-environment-variables \
            GEMINI_API_KEY="$GEMINI_API_KEY" \
            STORAGE_CONNECTION="$STORAGE_CONNECTION" \
        --environment-variables \
            RESULTS_CONTAINER="$RESULTS_CONTAINER" \
            SOURCE="reddit" \
            PAGES="200" \
            WORKER_ID="reddit-$i" \
        --output none &
    sleep 2
done

wait

echo ""
echo "=== Deployed 4 scrapers ==="
echo ""
echo "Monitor with:"
echo "  az container list --resource-group $RESOURCE_GROUP -o table"
echo "  az container logs --resource-group $RESOURCE_GROUP --name forum-scraper-devforum-1"
echo ""
echo "Download results:"
echo "  python azure/download_forum_results.py"
