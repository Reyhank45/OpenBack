#!/bin/bash
set -e

BASES_DIR="/tmp/openback/store/bases"
mkdir -p "$BASES_DIR"

echo "Setting up glibc-v1 (Ubuntu minimal)..."
GLIBC_DIR="$BASES_DIR/glibc-v1"
mkdir -p "$GLIBC_DIR"
cd "$GLIBC_DIR"
if [ ! -f "usr/bin/env" ]; then
    sudo wget -qO ubuntu.tar.gz "https://cdimage.ubuntu.com/ubuntu-base/releases/22.04/release/ubuntu-base-22.04.2-base-amd64.tar.gz"
    sudo tar -xf ubuntu.tar.gz
    sudo rm ubuntu.tar.gz
fi
cat << 'EOF' | sudo tee openback-base.json > /dev/null
{
  "os": "ubuntu-22.04",
  "libc": "glibc",
  "architecture": "x86_64"
}
EOF

echo "Setting up musl-v1 (Alpine minimal)..."
MUSL_DIR="$BASES_DIR/musl-v1"
mkdir -p "$MUSL_DIR"
cd "$MUSL_DIR"
if [ ! -f "bin/env" ]; then
    sudo wget -qO alpine.tar.gz "https://dl-cdn.alpinelinux.org/alpine/v3.18/releases/x86_64/alpine-minirootfs-3.18.4-x86_64.tar.gz"
    sudo tar -xf alpine.tar.gz
    sudo rm alpine.tar.gz
fi
cat << 'EOF' | sudo tee openback-base.json > /dev/null
{
  "os": "alpine-3.18",
  "libc": "musl",
  "architecture": "x86_64"
}
EOF

echo "Multi-base environments setup complete in $BASES_DIR!"
