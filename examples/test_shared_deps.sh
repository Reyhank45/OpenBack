#!/bin/bash
set -e

echo "==========================================="
echo "OpenBack Multi-App Shared Dependency Test"
echo "==========================================="

echo -e "\n[1] Starting web-api..."
sudo ./target/debug/openback run openback-web.json

echo -e "\n[2] Starting worker-service..."
sudo ./target/debug/openback run openback-worker.json

echo -e "\n[3] Waiting for apps to boot (3s)..."
sleep 3

echo -e "\n[4] Checking active processes (openback ps):"
sudo ./target/debug/openback ps

echo -e "\n[5] Inspecting logs for web-api (Verifying shared dependency):"
sudo ./target/debug/openback logs web-api | head -n 8

echo -e "\n[6] Inspecting logs for worker-service (Verifying shared dependency):"
sudo ./target/debug/openback logs worker-service | head -n 8

echo -e "\n[7] Stopping web-api..."
sudo ./target/debug/openback stop web-api

echo -e "\n[8] Verifying worker-service is unaffected:"
sudo ./target/debug/openback ps

echo -e "\n[9] Cleaning up worker-service..."
sudo ./target/debug/openback stop worker-service

echo -e "\nTest Completed Successfully!"
