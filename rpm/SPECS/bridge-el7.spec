%define __jar_repack {%nil}
%define _prefix %{_usr}/local/chronopolis/intake/dc
%define jar bridge-intake.jar
%define yaml application.yml
%define prep bridge-prepare
%define service /usr/lib/systemd/system/bridge-intake.service
%define build_date %(date +"%Y%m%d")

Name: bridge-intake
Version: %{ver}
Release: %{build_date}.el7
Source: bridge-intake.service
Source1: bridge-intake.jar
Source2: application.yml
Source3: bridge-prepare.sh
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
%__install -D -m0644 "%{SOURCE3}" "%{buildroot}%{_prefix}/%{prep}"

%files

%defattr(-,root,root)
%dir %{_prefix}

%{service}
%{_prefix}/%{jar}
%{_prefix}/%{prep}
%config(noreplace) %{_prefix}/%{yaml}

%changelog

* Thu Nov 09 2017 Mike Ritter <shake@umiacs.umd.edu> 2.0.2-20171109
- Remove /var/log/chronopolis directory creation
- Add bridge-prepare script to create logging directory if it does not exist

* Tue Oct 17 2017 Mike Ritter <shake@umiacs.umd.edu> 1.6.0-20171017
- Update spec to be consistent with other chronopolis rpm builds
