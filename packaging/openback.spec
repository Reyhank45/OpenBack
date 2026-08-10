Name:           openback
Version:        0.2.0
Release:        1%{?dist}
Summary:        OpenBack Orchestrator and CLI

License:        GPL-3.0-or-later
URL:            https://github.com/reyhank45/openback
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  cargo
BuildRequires:  rust
BuildRequires:  systemd-rpm-macros
Requires:       etcd

%description
A multi-node distributed container orchestrator.
Includes the openbackd daemon, backlet orchestrator, and backctl tool.

%prep
%setup -q

%build
cargo build --release

%install
rm -rf %{buildroot}
install -D -m 0755 target/release/openback %{buildroot}/usr/bin/openback
install -D -m 0755 target/release/backctl %{buildroot}/usr/bin/backctl
install -D -m 0755 target/release/backlet %{buildroot}/usr/bin/backlet
install -D -m 0755 target/release/backadm %{buildroot}/usr/bin/backadm
install -D -m 0644 packaging/openbackd.service %{buildroot}/usr/lib/systemd/system/openbackd.service
install -D -m 0644 packaging/openbackd.default %{buildroot}/etc/default/openbackd
install -D -m 0644 packaging/backlet.service %{buildroot}/usr/lib/systemd/system/backlet.service
install -D -m 0644 packaging/backlet.default %{buildroot}/etc/default/backlet

%post
%systemd_post openbackd.service
%systemd_post backlet.service

%preun
%systemd_preun openbackd.service
%systemd_preun backlet.service

%postun
%systemd_postun_with_restart openbackd.service
%systemd_postun_with_restart backlet.service

%files
%license LICENSE
/usr/bin/openback
/usr/bin/backctl
/usr/bin/backlet
/usr/bin/backadm
/usr/lib/systemd/system/openbackd.service
/usr/lib/systemd/system/backlet.service
%config(noreplace) /etc/default/openbackd
%config(noreplace) /etc/default/backlet

%changelog
* Mon Aug 10 2026 OpenBack Maintainer <wiratamareyhan85@gmail.com> - 0.2.0-1
- Etcd integration and daemon refactoring.
- Added backlet orchestrator and renamed backcli to backctl.

* Fri Aug 07 2026 OpenBack Maintainer <wiratamareyhan85@gmail.com> - 0.1.0-1
- Initial Fedora packaging release.
