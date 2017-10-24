%define __jar_repack {%nil}
%define _prefix %{_usr}/local/chronopolis/intake/dc
%define jar bridge-intake.jar
%define yaml application.yml
%define service /usr/lib/systemd/system/bridge-intake.service
%define build_date %(date +"%Y%m%d")

Name: bridge-intake
Version: %{ver}
Release: %{build_date}.el7
Source: bridge-intake.service
Source1: bridge-intake.jar
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
and prepares them for ingestion into DPN and/or Chronopolis

%preun

systemctl disable bridge-intake

%install

%__install -D -m0644 "%{SOURCE0}" "%{buildroot}%{service}"
%__install -D -m0644 "%{SOURCE1}" "%{buildroot}%{_prefix}/%{jar}"
%__install -D -m0644 "%{SOURCE2}" "%{buildroot}%{_prefix}/%{yaml}"

%__install -d "%{buildroot}/var/log/chronopolis"

%files

%defattr(-,root,root)
%dir %{_prefix}
%dir %attr(0755,-,-) /var/log/chronopolis

%{service}
%{_prefix}/%{jar}
%config(noreplace) %{_prefix}/%{yaml}

%changelog

* Tue Oct 17 2017 Mike Ritter <shake@umiacs.umd.edu> 1.6.0-20171017
- update spec to be consistent with other chronopolis rpm builds
