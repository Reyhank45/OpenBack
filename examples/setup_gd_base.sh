#!/bin/bash
set -e

GD_NAME="openback-gd-v1"
GD_PATH="/tmp/openback/store/gd/$GD_NAME"

echo "[Setup] Preparing OpenBack General Distribution Layer: $GD_NAME"
echo "[Setup] Target Path: $GD_PATH"

if [ -d "$GD_PATH" ]; then
    echo "[Setup] GD path already exists. Removing to start fresh..."
    sudo rm -rf "$GD_PATH"
fi

sudo mkdir -p "$GD_PATH"

# For the sake of the PoC, we will download a minimal Ubuntu Base tarball
# and extract it to form the base filesystem root.
TARBALL_URL="https://cdimage.ubuntu.com/ubuntu-base/releases/22.04/release/ubuntu-base-22.04.4-base-amd64.tar.gz"
TARBALL_DEST="/tmp/ubuntu-base-22.04.4-base-amd64.tar.gz"

if [ ! -f "$TARBALL_DEST" ]; then
    echo "[Setup] Downloading Ubuntu Base tarball (this might take a minute)..."
    wget -qO "$TARBALL_DEST" "$TARBALL_URL"
else
    echo "[Setup] Tarball already exists at $TARBALL_DEST"
fi

echo "[Setup] Extracting Ubuntu Base to $GD_PATH..."
sudo tar -xzf "$TARBALL_DEST" -C "$GD_PATH"

echo "[Setup] Creating OpenBack-specific directories in the base image..."
# /deps is the directory where the tmpfs will be mounted, and dependencies will be mapped
sudo mkdir -p "$GD_PATH/deps"
# /oldroot is the directory needed for pivot_root to put the old host root
sudo mkdir -p "$GD_PATH/oldroot"
# /app is where the application's source code will be bind-mounted
sudo mkdir -p "$GD_PATH/app"
# /run is where the IPC sockets will be bind-mounted (standard FHS)
sudo mkdir -p "$GD_PATH/run"

echo "[Setup] OpenBack GD Base Layer ($GD_NAME) is ready!"
