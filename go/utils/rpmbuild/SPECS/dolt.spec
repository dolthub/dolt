Name: dolt
Version: %{DOLT_VERSION}
Release: 1
Summary: Dolt Binary
License: Apache-2.0
URL: https://github.com/dolthub/dolt
Source: https://github.com/dolthub/dolt/releases/download/v%{version}/dolt-linux-%{DOLT_ARCH}.tar.gz

%description
Dolt is a MySQL-compatible SQL database with Git-like version control features.
It supports things like branch, merge, diff, clone, push and pull on the SQL
database itself.

%prep

%build
tar zxvf %{SOURCE0}

%install
mkdir -p %{buildroot}%{_bindir} %{buildroot}/%{_defaultlicensedir}/%{name}-%{version}/
install -m 0755 dolt-linux-%{DOLT_ARCH}/bin/dolt %{buildroot}/%{_bindir}/%{name}
cp dolt-linux-%{DOLT_ARCH}/LICENSES %{_buildrootdir}/

%files
%{_bindir}/%{name}
%license LICENSES
