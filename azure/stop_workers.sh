#!/bin/bash
# Stop and delete all worker containers

set -e

source azure/config.env

echo "=== Stopping all scraper workers ==="

# Get all container names
CONTAINERS=$(az container list --resource-group $RESOURCE_GROUP --query "[?starts_with(name, 'scraper-worker')].name" -o tsv)

for CONTAINER in $CONTAINERS; do
    echo "Deleting $CONTAINER..."
    az container delete --resource-group $RESOURCE_GROUP --name $CONTAINER --yes --output none &
done

wait

echo "=== All workers stopped ==="
