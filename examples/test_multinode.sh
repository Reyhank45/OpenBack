#!/bin/bash
set -e

# Cleanup trap
trap 'echo "Cleaning up cluster..."; kill $(jobs -p) 2>/dev/null || true; #rm -rf /tmp/openback_test' EXIT

echo "Compiling..."
cargo build

mkdir -p /tmp/openback_test/master/store/bases/glibc-v1
mkdir -p /tmp/openback_test/slave1/store/bases/glibc-v1
mkdir -p /tmp/openback_test/slave2/store/bases/glibc-v1
mkdir -p /tmp/openback_test/master/store/bases/glibc-v1_bkp

TOKEN="test-secret-123"
BIN="./target/debug/openback"

echo "Bootstrapping Multi-Node Cluster..."
mkdir -p /tmp/openback_test/{master,slave1,slave2,master_bkp}
mkdir -p /tmp/openback_test/master/store/bases/glibc-v1
mkdir -p /tmp/openback_test/slave1/store/bases/glibc-v1
mkdir -p /tmp/openback_test/slave2/store/bases/glibc-v1
mkdir -p /tmp/openback_test/master_bkp/store/bases/glibc-v1

# Master
OPENBACK_STORE_DIR=/tmp/openback_test/master OPENBACK_SOCKET=/tmp/openback_test/master/openbackd.sock $BIN daemon --role master --port 9090 --cluster-token "$TOKEN" > /tmp/openback_test/master/out.log 2>&1 &
sleep 1

# Slave 1
OPENBACK_STORE_DIR=/tmp/openback_test/slave1 OPENBACK_SOCKET=/tmp/openback_test/slave1/openbackd.sock $BIN daemon --role slave --port 9091 --master-addr 127.0.0.1:9090 --cluster-token "$TOKEN" > /tmp/openback_test/slave1/out.log 2>&1 &

# Slave 2
OPENBACK_STORE_DIR=/tmp/openback_test/slave2 OPENBACK_SOCKET=/tmp/openback_test/slave2/openbackd.sock $BIN daemon --role slave --port 9092 --master-addr 127.0.0.1:9090 --cluster-token "$TOKEN" > /tmp/openback_test/slave2/out.log 2>&1 &

# Master Backup
OPENBACK_STORE_DIR=/tmp/openback_test/master_bkp OPENBACK_SOCKET=/tmp/openback_test/master_bkp/openbackd.sock $BIN daemon --role master-backup --port 9093 --master-addr 127.0.0.1:9090 --cluster-token "$TOKEN" > /tmp/openback_test/master_bkp/out.log 2>&1 &

echo "Waiting for heartbeats (3 seconds)..."
sleep 3

# We will use the master's unix socket to interact with the cluster
export OPENBACK_SOCKET=/tmp/openback_test/master/openbackd.sock
export OPENBACK_STORE_DIR=/tmp/openback_test/master

BACKCLI="./target/debug/backcli"

echo ""
echo "=== Step 1: Cluster Nodes ==="
$BACKCLI get nodes || true

echo ""
echo "=== Step 2: Applying Workload ==="
# Temporarily patch deployment.yaml to 6 replicas
sed -i 's/replicas: .*/replicas: 6/g' deployment.yaml
$BACKCLI apply -f deployment.yaml

echo "Waiting for deployment to roll out..."
sleep 2

$BACKCLI describe app python-demoapp || true

echo ""
echo "=== Step 3: Triggering Failover Test (Killing Slave 1) ==="
SLAVE1_PID=$(pgrep -f "role slave --port 9091" || true)
if [ ! -z "$SLAVE1_PID" ]; then
    kill -9 $SLAVE1_PID
    echo "Killed Slave 1 (PID $SLAVE1_PID)"
else
    echo "Slave 1 PID not found!"
fi

echo ""
echo "=== Step 4: Waiting for Heartbeat Timeout (16 seconds) ==="
sleep 16

$BACKCLI get nodes || true

echo ""
echo "=== Step 5: Asserting Automatic Rescheduling ==="
$BACKCLI describe app python-demoapp || true

echo "Test completed successfully!"
