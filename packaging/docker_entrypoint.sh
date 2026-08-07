#!/bin/bash
set -e

# Import the GPG key
gpg --import /usr/src/openback/packaging/openback-privkey.asc

# Build the source package and sign it with GPG
# -S: source package only
# -sa: include original source
# -k: sign with specific key
echo "Generating upstream orig tarball for 3.0 (quilt)..."
cd /usr/src/openback
tar --exclude=debian --exclude=.git --exclude=target -cJf ../openback_0.1.3.orig.tar.xz .

echo "Building source package..."
dpkg-buildpackage -d -S -sa -k"wiratamareyhan85@gmail.com"

echo "Checking the generated files..."
cd ..
ls -la openback_0.1.3-1*

# Setup dput config for Mentors

# Upload to mentors
echo "Uploading to mentors.debian.net..."
dput mentors openback_0.1.3-1_source.changes
echo "Upload complete!"
