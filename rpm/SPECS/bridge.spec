%define __jar_repack {%nil}
# Compatibility with ncar
%define _source_payload w0.gzdio
%define _binary_payload w0.gzdio
%define _binary_filedigest_algorithm 1

# For use below
%define _prefix %{_usr}/lib/chronopolis
%define _confdir /etc/chronopolis
%define service bridge-client 
%define build_date %(date +"%Y%m%d")

Name: bridge-client
Version: %{ver}
Release: %{build_date}%{?dist}
Source: bridge-client.jar
Source1: bridge-client.sh
Source2: application.properties
Summary: Duracloud Bridge Client
License: UMD
URL: https://gitlab.umiacs.umd.edu/chronopolis
Group: System Environment/Daemons
Requires: /usr/sbin/groupadd /usr/sbin/useradd
autoprov: yes
autoreq: yes
BuildArch: noarch
BuildRoot: ${_tmppath}/build-%{name}-%{version}

%description
The Bridge Client monitors for snapshot requests from Duracloud
and prepares them for ingestion into DPN/Chronopolis

%install

rm -rf "%{buildroot}"
%__install -D -m0644 "%{SOURCE0}" "%{buildroot}%{_prefix}/%{service}.jar"

%__install -d "%{buildroot}/var/log/chronopolis"
%__install -d "%{buildroot}/etc/chronopolis"

%__install -D -m0755 "%{SOURCE1}" "%{buildroot}/etc/init.d/%{service}"
%__install -D -m0600 "%{SOURCE2}" "%{buildroot}%{_confdir}/application.properties"


%files

%defattr(-,root,root)
# conf
%dir %{_confdir}
%config %attr(0644,-,-) %{_confdir}/application.properties
# jar
%dir %attr(0755,chronopolis,chronopolis) %{_prefix}
%{_prefix}/%{service}.jar
# init/log
%config(noreplace) /etc/init.d/%{service}
%dir %attr(0755,chronopolis,chronopolis) /var/log/chronopolis

%pre
/usr/sbin/groupadd -r chronopolis > /dev/null 2>&1 || :
/usr/sbin/useradd -r -g chronopolis -c "Chronopolis Service User" \
        -s /bin/bash -d /usr/lib/chronopolis/ chronopolis > /dev/null 2>&1 || :
