#!/usr/bin/env bash

logdir="/var/log/chronopolis/"
user=`systemctl show -p User bridge-intake | sed 's/User=//'`
group=`systemctl show -p Group bridge-intake | sed 's/Group=//'`

# log directory exists
if [ ! -d "$logdir" ]; then
    echo "Creating $logdir"
    mkdir "$logdir"
fi

# permissions for logging
uname="$(stat --format '%U' "$logdir")"
if [ "x${uname}" != "x${user}" ]; then
    echo "Updating permissions for $logdir"
    chown "$user":"$group" "$logdir"
fi

exit 0
