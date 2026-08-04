# OpenBack

OpenBack is a high-performance, enterprise-grade Linux process orchestrator written in Rust. It securely isolates applications into full-blown POSIX environments by leveraging bare-metal namespace virtualization, strict Linux capability filtering, and a powerful dual-mode networking gateway.

Designed as a modern alternative to bloated container runtimes, OpenBack enforces absolute security by stripping all host privileges and hardware access, while dynamically bridging exactly what is requested in the application manifest.

## 🚀 Enterprise Features

### 1. Bare-Metal Namespace Isolation
OpenBack utilizes `CLONE_NEWNS`, `CLONE_NEWPID`, and `CLONE_NEWNET` to completely sever an application from the host operating system. It uses `pivot_root` to jail the payload into a completely sterile **OpenBack General Distribution (GD) Base Layer**—an ultra-minimal, immutable Ubuntu userland.

### 2. Standalone Dependency Pool
To avoid the security risks and bloat of dynamic `apt-get` packages inside the base layer, OpenBack utilizes a **Shared Dependency Pool**. Applications declare dependencies via string-based identifiers (e.g., `python-runtime@3.11`). OpenBack dynamically bind-mounts pristine, statically-compiled, standalone distributions (like Python 3.11) directly into the isolated rootfs at runtime.

### 3. Dual-Mode Networking Gateway
OpenBack aggressively isolates applications by denying them raw IP network access (`lo` only). All ingress and egress traffic must pass through the OpenBack Daemon's TCP proxy gateway:
- **Exposed Mode:** The daemon binds to a host TCP port (e.g., `8080`) and securely proxies traffic into the container's isolated Unix Domain Socket (`/run/app.sock`).
- **Internal Mode:** Processes communicate exclusively via ultra-fast, kernel-level IPC sockets, completely bypassing the TCP/IP stack.

### 4. Scoped Permission Bridge
Containers run without `--privileged` mode. Host access is strictly prohibited by default. OpenBack features a programmable bridge to allow scoped access:
- **Hardware Devices:** A sterile `tmpfs` is mounted over `/dev`, populating only essential pseudo-devices (`/dev/null`, `/dev/urandom`, etc.). Additional hardware (e.g., `/dev/net/tun`) is dynamically bind-mounted *only* if requested in the manifest.
- **Linux Capabilities:** Using the `caps` crate, OpenBack scans the active capability Bounding set before payload execution and aggressively drops all kernel privileges except those explicitly requested (e.g., `CAP_NET_ADMIN`).

## 🛠️ Usage

OpenBack uses a standardized JSON manifest (`openback.json`) to declare workloads.

```json
{
  "app_name": "python-demoapp",
  "target_gd": "openback-gd-v1",
  "dependencies": [
    "python-runtime@3.11"
  ],
  "env": {
    "HOME": "/tmp"
  },
  "networking": {
    "ipc_socket": "/run/app.sock",
    "ports": [
      {
        "host_port": 8080,
        "container_socket": "/run/app.sock"
      }
    ]
  },
  "permissions": {
    "devices": [
      "/dev/net/tun"
    ],
    "capabilities": [
      "CAP_NET_ADMIN"
    ]
  },
  "entrypoint": [
    "/deps/python-runtime/3.11/bin/python3",
    "/deps/python-runtime/3.11/bin/gunicorn",
    "--bind", "unix:/run/app.sock",
    "--chdir", "/app/src",
    "run:app"
  ]
}
```

### Starting the Daemon
```bash
sudo ./target/debug/openback daemon
```

### Launching an Application
```bash
sudo ./target/debug/openback run openback.json
```

## 📜 License
This project is licensed under the **GNU General Public License v3.0 (GPLv3)**. See the `LICENSE` file for details.
