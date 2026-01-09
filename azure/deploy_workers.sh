#!/bin/bash
# Deploy multiple worker containers to Azure Container Instances

set -e

# Load config
source azure/config.env

# Parse arguments
WORKER_COUNT=${1:-10}
CPU_CORES=${2:-1}
MEMORY_GB=${3:-1}

echo "=== Deploying $WORKER_COUNT scraper workers ==="
echo "CPU: $CPU_CORES cores, Memory: $MEMORY_GB GB each"

# Get ACR credentials
ACR_USERNAME=$CONTAINER_REGISTRY
ACR_PASSWORD=$(az acr credential show --name $CONTAINER_REGISTRY --query "passwords[0].value" -o tsv)

# Deploy workers
for i in $(seq 1 $WORKER_COUNT); do
    WORKER_NAME="scraper-worker-$i"
    echo "Deploying $WORKER_NAME..."

    az container create \
        --resource-group $RESOURCE_GROUP \
        --name $WORKER_NAME \
        --image $CONTAINER_REGISTRY.azurecr.io/youtube-scraper:latest \
        --registry-login-server $CONTAINER_REGISTRY.azurecr.io \
        --registry-username $ACR_USERNAME \
        --registry-password $ACR_PASSWORD \
        --os-type Linux \
        --cpu $CPU_CORES \
        --memory $MEMORY_GB \
        --restart-policy OnFailure \
        --environment-variables \
            STORAGE_CONNECTION="$STORAGE_CONNECTION" \
            QUEUE_NAME="$QUEUE_NAME" \
            RESULTS_CONTAINER="$RESULTS_CONTAINER" \
            WORKER_ID="$WORKER_NAME" \
        --output none &

    # Stagger deployments
    sleep 2
done

# Wait for all deployments
wait

echo ""
echo "=== Deployed $WORKER_COUNT workers ==="
echo ""
echo "Monitor with:"
echo "  az container list --resource-group $RESOURCE_GROUP -o table"
echo "  az container logs --resource-group $RESOURCE_GROUP --name scraper-worker-1"
echo ""
echo "Stop all workers:"
echo "  ./azure/stop_workers.sh"
