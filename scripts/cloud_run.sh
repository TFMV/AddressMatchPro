#!/bin/bash

# Set variables
PROJECT_ID="your-gcp-project-id"
REGION="us-central1" # or any other region where you want to deploy
SERVICE_NAME="fuzzymatchfinder"
IMAGE_NAME="gcr.io/$PROJECT_ID/$SERVICE_NAME"
CLOUD_BUILD_CONFIG="cloudbuild.yaml"

# Create cloudbuild.yaml file
cat <<EOF > $CLOUD_BUILD_CONFIG
steps:
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build', '-t', '$IMAGE_NAME', '.']
  - name: 'gcr.io/cloud-builders/docker'
    args: ['push', '$IMAGE_NAME']
  - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'
    entrypoint: 'gcloud'
    args: ['run', 'deploy', '$SERVICE_NAME', '--image', '$IMAGE_NAME', '--region', '$REGION', '--platform', 'managed', '--allow-unauthenticated', '--set-env-vars', 'CONFIG_PATH=/app/config.yaml,SCRIPT_PATH=/app/python-ml/generate_embeddings.py']
images:
  - '$IMAGE_NAME'
EOF

# Submit the build to Google Cloud Build
echo "Submitting the build to Google Cloud Build..."
gcloud builds submit --config $CLOUD_BUILD_CONFIG --project $PROJECT_ID

# Get the service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region $REGION --format "value(status.url)")
echo "Service deployed to $SERVICE_URL"

echo "Deployment completed successfully!"
