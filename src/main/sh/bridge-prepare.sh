#!/usr/bin/env bash

LOG_DIR="/var/log/chronopolis/"
DATA_DIR="/var/lib/chronopolis/data/"
PIDFILE="/var/run/bridge-intake.pid"
user=`systemctl show -p User bridge-intake | sed 's/User=//'`
group=`systemctl show -p Group bridge-intake | sed 's/Group=//'`

function check_dir(){
    dir=$1

    # directory exists
    if [ ! -d "$dir" ]; then
        echo "Creating $dir"
        mkdir -p "$dir"
    fi

    # permissions for dir
    uname="$(stat --format '%U' "$dir")"
    if [ "x${uname}" != "x${user}" ]; then
        echo "Updating permissions for $dir"
        chown "$user":"$group" "$dir"
    fi

    return 0
}

check_dir ${LOG_DIR}
check_dir ${DATA_DIR}

# Just want the pid file to exist so we don't throw any exceptions
touch ${PIDFILE}
chown ${user}:${group} ${PIDFILE}

exit 0
