Name:           openback
Version:        0.1.1
Release:        1%{?dist}
Summary:        OpenBack Orchestrator and CLI

License:        GPL-3.0-or-later
URL:            https://github.com/reyhank45/openback
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  cargo
BuildRequires:  rust
BuildRequires:  systemd-rpm-macros

%description
A multi-node distributed container orchestrator.
Includes the openbackd daemon and backcli tool.

%prep
%setup -q

%build
cargo build --release

%install
rm -rf %{buildroot}
install -D -m 0755 target/release/openback %{buildroot}/usr/bin/openback
install -D -m 0755 target/release/backcli %{buildroot}/usr/bin/backcli
install -D -m 0644 packaging/openbackd.service %{buildroot}/usr/lib/systemd/system/openbackd.service
install -D -m 0644 packaging/openbackd.default %{buildroot}/etc/default/openbackd

%post
%systemd_post openbackd.service

%preun
%systemd_preun openbackd.service

%postun
%systemd_postun_with_restart openbackd.service

%files
%license LICENSE
/usr/bin/openback
/usr/bin/backcli
/usr/lib/systemd/system/openbackd.service
%config(noreplace) /etc/default/openbackd

%changelog
* Fri Aug 07 2026 OpenBack Maintainer <wiratamareyhan85@gmail.com> - 0.1.0-1
- Initial Fedora packaging release.
