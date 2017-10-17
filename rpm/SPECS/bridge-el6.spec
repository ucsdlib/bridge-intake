%define __jar_repack {%nil}
%define _prefix %{_usr}/local/chronopolis/intake/dc
%define jar bridge-intake.jar
%define yaml application.yml
%define initsh /etc/init.d/bridge-intake
%define build_date %(date +"%Y%m%d")

Name: bridge-intake
Version: %{ver}
Release: %{build_date}.el6
Source: bridge-intake.jar
Source1: bridge-intake.sh
Source2: application.yml
Summary: Chronopolis Intake Client for the Duracloud Bridge
License: UMD
URL: https://gitlab.umiacs.umd.edu/chronopolis
Group: System Environment/Daemons
autoprov: yes
autoreq: yes
BuildArch: noarch
BuildRoot: ${_tmppath}/build-%{name}-%{version}

%description
The Bridge Intake Client monitors for snapshot requests from Duracloud
and prepares them for ingestion into DPN/Chronopolis

%install

%__install -D -m0644 "%{SOURCE0}" "%{buildroot}%{_prefix}/%{jar}"
%__install -D -m0644 "%{SOURCE0}" "%{buildroot}%{_prefix}/%{yaml}"
%__install -D -m0644 "%{SOURCE0}" "%{buildroot}%{initsh}"

%__install -d "%{buildroot}/var/log/chronopolis"

%files

%defattr(-,root,root)
%dir %{_prefix}
%{_prefix}/%{jar}
%config(noreplace) %{_prefix}/%{yaml}
%{initsh}

%dir %attr(0755,-,-) /var/log/chronopolis

%post

chkconfig --add bridge-intake

%preun

chkconfig --del bridge-intake

%changelog

* Tue Oct 17 2017 Mike Ritter <shake@umiacs.umd.edu> 1.6.0-20171017
- Clean up spec and include post/preun/changelog
- Update install location to /usr/local/chronopolis
