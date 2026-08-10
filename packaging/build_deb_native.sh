#!/bin/bash
set -e

echo "Starting native build process for OpenBack .deb package..."

# 1. Build release binaries
echo "Compiling Rust binary..."
cargo build --release

# 2. Set up Debian package structure in a temp directory
DEB_DIR=$(mktemp -d)
mkdir -p $DEB_DIR/openback_0.2.0_amd64/DEBIAN \
         $DEB_DIR/openback_0.2.0_amd64/usr/bin \
         $DEB_DIR/openback_0.2.0_amd64/lib/systemd/system \
         $DEB_DIR/openback_0.2.0_amd64/etc/default

# Copy metadata
cp packaging/DEBIAN/control $DEB_DIR/openback_0.2.0_amd64/DEBIAN/
cp packaging/DEBIAN/postinst $DEB_DIR/openback_0.2.0_amd64/DEBIAN/
cp packaging/DEBIAN/prerm $DEB_DIR/openback_0.2.0_amd64/DEBIAN/
cp packaging/DEBIAN/postrm $DEB_DIR/openback_0.2.0_amd64/DEBIAN/
chmod 0755 $DEB_DIR/openback_0.2.0_amd64/DEBIAN/postinst \
           $DEB_DIR/openback_0.2.0_amd64/DEBIAN/prerm \
           $DEB_DIR/openback_0.2.0_amd64/DEBIAN/postrm

# Copy binaries
cp target/release/openback target/release/backctl target/release/backlet target/release/backadm $DEB_DIR/openback_0.2.0_amd64/usr/bin/

# Copy systemd
cp packaging/openbackd.service $DEB_DIR/openback_0.2.0_amd64/lib/systemd/system/
cp packaging/openbackd.default $DEB_DIR/openback_0.2.0_amd64/etc/default/openbackd
cp packaging/backlet.service $DEB_DIR/openback_0.2.0_amd64/lib/systemd/system/
cp packaging/backlet.default $DEB_DIR/openback_0.2.0_amd64/etc/default/backlet

# 3. Create the data.tar.gz
echo "Creating data.tar.gz..."
cd $DEB_DIR/openback_0.2.0_amd64
# Data tarball contains everything EXCEPT the DEBIAN directory
tar -czf ../data.tar.gz --exclude=DEBIAN *

# 4. Create the control.tar.gz
echo "Creating control.tar.gz..."
cd DEBIAN
tar -czf ../../control.tar.gz *

# 5. Create debian-binary
echo "2.0" > ../../debian-binary

# 6. Archive with ar
cd ../../
ar rc openback_0.2.0_amd64.deb debian-binary control.tar.gz data.tar.gz

# 7. Copy output
cp openback_0.2.0_amd64.deb /home/reyhank45/Documents/OpenBack/openback_0.2.0_amd64.deb
echo "Success! The Debian package is ready at ./openback_0.2.0_amd64.deb"
