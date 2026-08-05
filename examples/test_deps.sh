#!/bin/bash
set -e

echo "Creating dummy dependency..."
sudo mkdir -p /tmp/openback/store/deps/dummy/1.0
sudo touch /tmp/openback/store/deps/dummy/1.0/dummy.txt

echo "Starting apps..."
sudo ./target/debug/openback run openback-web.json
sudo ./target/debug/openback run openback-worker.json

echo ""
echo "Running: openback deps list"
./target/debug/openback deps list

echo ""
echo "Running: openback deps prune"
./target/debug/openback deps prune

echo ""
echo "Running: openback deps list (after prune)"
./target/debug/openback deps list
