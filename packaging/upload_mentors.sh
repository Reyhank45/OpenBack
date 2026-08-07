#!/bin/bash
set -e

echo "Building Docker image for Debian Mentors upload..."
docker build --network host -t openback-mentors-uploader -f packaging/Dockerfile.mentors .

echo "Running upload process inside container..."
docker run --network host --rm openback-mentors-uploader
