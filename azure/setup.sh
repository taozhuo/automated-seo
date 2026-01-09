#!/bin/bash
# Azure Infrastructure Setup for YouTube Bulk Scraper
# This creates all necessary Azure resources for distributed scraping

set -e

# Configuration - modify these
RESOURCE_GROUP="roblox-seo-scraper"
LOCATION="eastus"
STORAGE_ACCOUNT="robloxseostorage$RANDOM"
CONTAINER_REGISTRY="robloxseoacr$RANDOM"
QUEUE_NAME="scraper-jobs"
RESULTS_CONTAINER="scraper-results"

echo "=== Setting up Azure infrastructure for bulk scraping ==="

# Login check
echo "Checking Azure login..."
az account show > /dev/null 2>&1 || { echo "Please run 'az login' first"; exit 1; }

# Create resource group
echo "Creating resource group: $RESOURCE_GROUP"
az group create \
    --name $RESOURCE_GROUP \
    --location $LOCATION \
    --output none

# Create storage account
echo "Creating storage account: $STORAGE_ACCOUNT"
az storage account create \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku Standard_LRS \
    --output none

# Get storage connection string
STORAGE_CONNECTION=$(az storage account show-connection-string \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --query connectionString -o tsv)

# Create queue for jobs
echo "Creating job queue: $QUEUE_NAME"
az storage queue create \
    --name $QUEUE_NAME \
    --connection-string "$STORAGE_CONNECTION" \
    --output none

# Create blob container for results
echo "Creating results container: $RESULTS_CONTAINER"
az storage container create \
    --name $RESULTS_CONTAINER \
    --connection-string "$STORAGE_CONNECTION" \
    --output none

# Create container registry
echo "Creating container registry: $CONTAINER_REGISTRY"
az acr create \
    --name $CONTAINER_REGISTRY \
    --resource-group $RESOURCE_GROUP \
    --sku Basic \
    --admin-enabled true \
    --output none

# Get ACR credentials
ACR_PASSWORD=$(az acr credential show \
    --name $CONTAINER_REGISTRY \
    --query "passwords[0].value" -o tsv)

# Save configuration
cat > azure/config.env << EOF
RESOURCE_GROUP=$RESOURCE_GROUP
LOCATION=$LOCATION
STORAGE_ACCOUNT=$STORAGE_ACCOUNT
STORAGE_CONNECTION=$STORAGE_CONNECTION
CONTAINER_REGISTRY=$CONTAINER_REGISTRY
ACR_PASSWORD=$ACR_PASSWORD
QUEUE_NAME=$QUEUE_NAME
RESULTS_CONTAINER=$RESULTS_CONTAINER
EOF

echo ""
echo "=== Azure infrastructure created successfully ==="
echo "Configuration saved to azure/config.env"
echo ""
echo "Storage Account: $STORAGE_ACCOUNT"
echo "Container Registry: $CONTAINER_REGISTRY.azurecr.io"
echo "Job Queue: $QUEUE_NAME"
echo ""
echo "Next steps:"
echo "  1. Run: ./azure/build_and_push.sh"
echo "  2. Run: python azure/queue_jobs.py --count 100000"
echo "  3. Run: ./azure/deploy_workers.sh --count 50"
