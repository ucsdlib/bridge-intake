#!/bin/sh

### BEGIN INIT INFO
# Provides:      replication
# Default-Start: 3 5
# Default-Stop:  0 1 2 6
# Description:   Start the Chronopolis replication service
### END INIT INFO

# User to execute as
CHRON_USER="chronopolis"

BRIDGE_JAR="/usr/lib/chronopolis/bridge-client.jar"
BRIDGE_PID_FILE="/var/run/bridge-client.pid"

# Set the location which holds our application.properties
# SPRING_CONFIG_NAME="settings.conf"
SPRING_CONFIG_LOCATION="/etc/chronopolis/"

JAVA_BIN=/usr/bin/java
JAVA_CMD="$JAVA_BIN -jar $BRIDGE_JAR"
PARAMS="--spring.config.location=$SPRING_CONFIG_LOCATION &"

. /etc/init.d/functions

RETVAL=0
COUNTDOWN=1

case "$1" in
    start)
    echo "Starting the bridge client"
    daemon --user "$CHRON_USER" --pidfile "$BRIDGE_PID_FILE" $JAVA_CMD $PARAMS > /dev/null 2>&1
    RETVAL=$?

    echo "Waiting for startup to complete..."
    while [ $COUNTDOWN -gt 0 ]; do
        sleep 8 # This seems to be the minimum amount of time to get consistent results
        let COUNTDOWN=0
    done

    # This from the jenkins init script... slightly modified for our use
    if [ $RETVAL -eq 0 ]; then
        # Create a pipe to read from so we can still alter $RUNNING
        if [ ! -p check_pipe ]; then
            mkfifo check_pipe
        fi

        /bin/ps hww -u "$CHRON_USER" -o sess,ppid,pid,cmd > check_pipe &
        while read sess ppid pid cmd; do
            [ $ppid -eq 1 ] || continue
            echo "$cmd" | grep $BRIDGE_JAR > /dev/null
            [ $? -eq 0 ] || continue
            echo $pid > $BRIDGE_PID_FILE
            let RUNNING=0
            # echo $RUNNING
        done < check_pipe
        rm check_pipe
    fi

    if [ $RUNNING -eq 0 ]; then
        success
    else
        failure
    fi

    RETVAL=$RUNNING
    ;;
    stop)
    echo "Stopping the bridge client"
    killproc -p $BRIDGE_PID_FILE bridge-client 
    ;;
    restart)
    $0 stop
    $0 start
    ;;
    status)
        status bridge-client 
        RETVAL=$?
    ;;
esac

exit $RETVAL
