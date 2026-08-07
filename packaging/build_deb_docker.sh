#!/bin/bash
set -e

echo "Starting Docker build process for OpenBack .deb package..."

# Build the docker image which compiles and packages OpenBack
docker build -t openback-deb-builder -f packaging/Dockerfile.deb .

# Create a temporary container
echo "Extracting .deb package from container..."
CONTAINER_ID=$(docker create openback-deb-builder)

# Copy the .deb file to the host
docker cp $CONTAINER_ID:/tmp/deb/openback_0.1.0_amd64.deb ./openback_0.1.0_amd64.deb

# Clean up
docker rm $CONTAINER_ID

echo "Success! The Debian package is ready at ./openback_0.1.0_amd64.deb"
