#!/bin/bash
set -e

echo "Ensuring rpm-build is installed..."
if ! command -v rpmbuild &> /dev/null; then
    echo "rpmbuild not found! Please install it by running: sudo dnf install -y rpm-build systemd-rpm-macros"
    exit 1
fi

echo "Setting up RPM build environment..."
mkdir -p ~/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}

echo "Creating source tarball..."
# The spec file expects openback-0.2.0.tar.gz where the top-level directory is openback-0.2.0
cd ..
tar --transform 's/^OpenBack/openback-0.2.0/' --exclude='OpenBack/target' --exclude='OpenBack/.git' --exclude='OpenBack/openback-*.tar.gz' -czf openback-0.2.0.tar.gz OpenBack
mv openback-0.2.0.tar.gz ~/rpmbuild/SOURCES/
cd OpenBack

echo "Copying spec file..."
cp packaging/openback.spec ~/rpmbuild/SPECS/

echo "Building SRPM (Source RPM) and RPM (Binary)..."
rpmbuild -ba --nodeps ~/rpmbuild/SPECS/openback.spec

echo "Copying packages to project root..."
cp ~/rpmbuild/SRPMS/openback-0.2.0-1*.src.rpm .
cp ~/rpmbuild/RPMS/x86_64/openback-0.2.0-1*.rpm . || echo "Check RPMS dir for the generated binary package if architecture differs."

echo "RPM Build Complete!"
ls -la openback-0.2.0-1*
