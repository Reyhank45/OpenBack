#!/bin/bash
set -e

echo "Applying deployment.yaml..."
./target/debug/backcli apply -f deployment.yaml

echo ""
echo "Running: backcli get apps"
./target/debug/backcli get apps

echo ""
echo "Running: backcli describe app python-demoapp"
./target/debug/backcli describe app python-demoapp

echo ""
echo "Running: backcli scale app python-demoapp --replicas 4"
./target/debug/backcli scale app python-demoapp --replicas 4

echo ""
echo "Waiting a second for scaling..."
sleep 1

echo ""
echo "Running: backcli get apps (after scaling)"
./target/debug/backcli get apps

echo ""
echo "Running: backcli get nodes"
./target/debug/backcli get nodes

echo ""
echo "Running: backcli delete app python-demoapp"
./target/debug/backcli delete app python-demoapp

echo ""
echo "Running: backcli get apps (after delete)"
./target/debug/backcli get apps
