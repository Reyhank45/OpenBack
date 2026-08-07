# OpenBack

OpenBack is a high-performance, enterprise-grade Linux process orchestrator written in Rust. It securely isolates applications into full-blown POSIX environments by leveraging bare-metal namespace virtualization, strict Linux capability filtering, and an automated package manager overlay cache system.

Designed as a modern alternative to bloated container runtimes, OpenBack enforces absolute security by stripping all host privileges and hardware access, while dynamically bridging exactly what is requested in a declarative, Kubernetes-style YAML manifest.

## 📖 What is this?
OpenBack is a container engine and orchestration daemon. It consists of two primary components:
1. **`openbackd` (Daemon):** The root-level daemon responsible for building OverlayFS caches, managing namespaces, exposing dual-mode networking gateways, and monitoring process health.
2. **`backcli` (Orchestrator CLI):** The command-line tool used by developers to communicate with the daemon, apply manifests, scale replicas, and inspect logs.

Unlike standard runtimes that rely on massive pre-baked Dockerfile images, OpenBack uses a deterministic package overlay engine. It takes standard minimal Linux distribution bases (e.g., `ubuntu:24.04` or `alpine:3.20`), intercepts the boot sequence, and ephemerally runs native package managers (`apt`, `apk`, `dnf`) inside isolated namespaces. It caches these packages as reusable OverlayFS layers, allowing instantaneous scaling of replicas.

## 🏗️ Design and Topology

When `openbackd` boots an application replica, it constructs a 3-tier OverlayFS stack inside the container's private mount namespace (`CLONE_NEWNS`):

```text
+---------------------------------------------------------------------------------+
| LAYER 3: Application UpperDir (Read-Write / Ephemeral)                          |
|          • Temporary container writes, /tmp, /run, and Application Code         |
+---------------------------------------------------------------------------------+
| LAYER 2: Dependency Package Overlay LowerDir (Read-Only)                        |
|          • Installed via distro package manager (/usr/bin, /usr/lib, etc.)      |
+---------------------------------------------------------------------------------+
| LAYER 1: Standard Distro Base Image LowerDir (Read-Only)                        |
|          • Base RootFS (e.g., /var/lib/openback/store/images/ubuntu-24.04)      |
+---------------------------------------------------------------------------------+
```

### Advanced Features
- **Asynchronous Single-Flight Caching:** If 10 replicas scale simultaneously, only the first replica performs the `apt-get` download. The other 9 replicas are placed in a non-blocking `Building` state and instantly resume boot once the single `SHA256` cache is ready.
- **Dual-Mode Networking Gateway:** OpenBack denies raw IP network access (`lo` only). All ingress traffic passes through the Daemon's TCP proxy gateway securely into the container's isolated Unix Domain Socket (`/run/app.sock`).
- **Scoped Permission Bridge:** Host access is strictly prohibited by default. A sterile `tmpfs` is mounted over `/dev`, populating only essential pseudo-devices. Capabilities and hardware (e.g., `/dev/net/tun`) are bridged only if requested in the manifest.

## ⚙️ How to Build

OpenBack is written in Rust. You will need the latest stable Rust toolchain and `cargo`. Because OpenBack utilizes kernel namespaces and OverlayFS, it only supports **Linux** environments.

```bash
# Clone the repository
git clone https://github.com/reyhank45/OpenBack.git
cd OpenBack

# Build the project
cargo build --release

# The compiled binaries will be located at:
# target/release/openbackd (The Daemon)
# target/release/backcli   (The CLI)
```

### Packaging for Linux Distributions

OpenBack includes native packaging scripts to build `.deb` and `.rpm` packages for streamlined systemd integration and production deployments.

**Building a DEB package (Debian/Ubuntu):**
```bash
./packaging/build_deb_native.sh
# The output .deb file will be generated in the parent directory (../).
```

**Building an RPM package (Fedora/RHEL/CentOS):**
```bash
./packaging/build_rpm.sh
# The output .rpm file will be generated in ~/rpmbuild/RPMS/x86_64/.
```

## 🚀 Quickstart

1. **Start the OpenBack Daemon:**
   The daemon must run as root to unshare kernel namespaces and assemble OverlayFS mounts.
   ```bash
   sudo ./target/release/openbackd
   ```

2. **Deploy an Application:**
   In a separate terminal, deploy a simple workload using the CLI. We have included multiple examples in the root directory.
   ```bash
   ./target/release/backcli apply -f hello-world-ubuntu-python.yaml
   ```

3. **Check the Status:**
   ```bash
   ./target/release/backcli ps
   ```

## 📝 How to Use

OpenBack uses a Kubernetes-style YAML manifest to declare workloads. Here is an example of an application that relies on an automated Ubuntu base image overlay:

```yaml
apiVersion: openback.io/v1
kind: Application
metadata:
  name: hello-world-ubuntu-python
spec:
  baseImage: ubuntu-24.04
  packages:
    apt:
      - python3
  workDir: /app
  replicas: 1
  entrypoint:
    - python3
    - -c
    - "import http.server, socketserver; print('Ubuntu Python Hello World serving on port 8080'); socketserver.TCPServer(('', 8080), http.server.SimpleHTTPRequestHandler).serve_forever()"
  networking:
    ipc_socket: /run/app.sock
    ports:
      - host_port: 8080
        container_socket: /run/app.sock
```

### CLI Commands

- `backcli apply -f manifest.yaml`: Deploy or update an application.
- `backcli ps`: List all running and building applications.
- `backcli describe <app_name>`: Inspect detailed runtime topology for an application.
- `backcli logs <app_name>`: Stream logs for an application replica.
- `backcli stop <app_name>`: Tear down an application and all of its replicas.

## 📜 License
This project is licensed under the **GNU General Public License v3.0 (GPLv3)**. See the `LICENSE` file for details.
