# Security Policy

## Supported Versions

We actively maintain and issue security patches for the following versions of OpenBack:

| Version | Supported          |
| ------- | ------------------ |
| 0.2.x   | :white_check_mark: |
| 0.1.x   | :x:                |

---

## Reporting a Vulnerability

We take the security of **OpenBack** (`openbackd` runtime engine and `backcli` orchestrator) very seriously. If you discover a security vulnerability, please do **NOT** open a public GitHub issue.

### Private Reporting Channel
* **Private Vulnerability Reporting:** Use the [GitHub Private Vulnerability Reporting](https://github.com/reyhank45/OpenBack/security/advisories/new) feature on this repository.
* **Email:** Alternatively, email us directly at `reyhank45@fedora` or your designated security contact.

### What to Include in Your Report
Please include as much detail as possible to help us reproduce and fix the issue:
- Type of issue (e.g., namespace breakout, privilege escalation, unauthorized RPC access, token bypass).
- A minimal Proof of Concept (PoC) script or `backcli` manifest demonstrating the flaw.
- Affected components (`openbackd`, `backcli`, `openback-control`).
- Step-by-step instructions to reproduce.

### Response Timeline
- **Initial Response:** Within 48 hours of receiving the report.
- **Status Update:** Within 7 days with an estimated fix release date.
- **Public Disclosure:** Coordinated after a patch is merged and released to protect production users.

Thank you for helping keep OpenBack and the open-source container ecosystem secure!