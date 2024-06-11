#!/bin/bash

# Set environment variables
PROJECT_ID="tfmv-371720"
SERVICE_NAME="addressmatchpro"
REGION="us-central1"
IMAGE_NAME="gcr.io/$PROJECT_ID/$SERVICE_NAME"

# Submit the build to Google Cloud Build
echo "Submitting the build to Google Cloud Build..."
gcloud builds submit --tag $IMAGE_NAME . || { echo "Error: Failed to submit build"; exit 1; }

# Deploy to Cloud Run
echo "Deploying the service to Cloud Run..."
gcloud run deploy $SERVICE_NAME --image $IMAGE_NAME --platform managed --region $REGION --allow-unauthenticated || { echo "Error: Failed to deploy service"; exit 1; }

# Confirm deployment
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format 'value(status.url)')

if [ -z "$SERVICE_URL" ]; then
  echo "Error: Failed to get service URL"
  exit 1
else
  echo "Service deployed successfully!"
  echo "Service URL: $SERVICE_URL"
fi

