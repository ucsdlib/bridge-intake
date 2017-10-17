#!/bin/sh
#
# bridge-intake  This script starts the bridge intake client
#
# chkconfig: - 64 36
# description: Start the Chronopolis Intake Service for the Duracloud Bridge
# processname: bridge-intake.jar
# config: /usr/local/chronopolis/intake/dc/application.yml
#
### BEGIN INIT INFO
# Provides:      bridge-intake
# Required-Start: $network
# Required-Stop:
# Short-Description:   Start the Chronopolis Intake Service
# Description:   Start the Chronopolis Intake Service for the Duracloud Bridge
### END INIT INFO

# User to execute as
CHRON_USER="chronopolis"

INTAKE_DIR="/usr/local/chronopolis/intake/dc"
INTAKE_JAR="bridge-client.jar"

JAVA_BIN=/usr/bin/java
JAVA_CMD="$JAVA_BIN -jar $INTAKE_DIR/$INTAKE_JAR &"

. /etc/init.d/functions

prog="bridge-intake"
pidfile="/var/run/bridge-intake.pid"
lockfile="/var/lock/subsys/bridge-intake"

# env vars for spring
export SPRING_CONFIG_LOCATION="/etc/chronopolis/"
export SPRING_PID_FILE="$pidfile"

start(){
    # check user exists
    if ! getent passwd $CHRON_USER > /dev/null 2>&1; then
        echo "User $CHRON_USER does not exist; unable to start bridge-intake service"
        action $"Starting $prog: " /bin/false
        return 2
    fi

    # check already running
    RUNNING=0
    if [ -f "$pidfile" ]; then
        PID=`cat "$pidfile" 2>/dev/null`
        # PID + dir exists == still running
        if [ -n "$PID" ] && [ -d "/proc/$PID" ]; then
            RUNNING=1
        fi
    else
        touch "$pidfile"
        chown "$CHRON_USER":"$CHRON_USER" "$pidfile"
    fi

    # If we're running return early
    if [ $RUNNING = 1 ]; then
        action $"Starting $prog: " /bin/true
        return 0
    fi

    daemon --user "$CHRON_USER" --pidfile "$pidfile" $JAVA_CMD
    RC=$?

    # Should clean this up a bit, but sleep an arbitrary amount before checking the state
    sleep 12

    if [ -f "$pidfile" ]; then
        PID=`cat "$pidfile" 2>/dev/null`
        if [ -n "$PID" ] && [ -d "/proc/$PID" ]; then
            RC=0
            action $"Starting $prog: " /bin/true
        else
            action $"Starting $prog: " /bin/false
        fi
    else
        action $"Starting $prog: " /bin/false
    fi

    return $RC
}

stop(){
    RC=0

    # check if the pidfile exists
    if [ ! -f "$pidfile" ]; then
        action $"Stopping $prog: " /bin/true
        return $RC
    fi

    # get the pid and attempt to kill
    PID=`cat "$pidfile" 2>/dev/null`
    if [ -n "$PID" ]; then
        /bin/kill "$PID" > /dev/null 2>&1 || break
        if [ $? -eq 0 ]; then
            action $"Stopping $prog: " /bin/true
            rm -f $lockfile
            rm -f "$pidfile"
        else
            action $"Stopping $prog: " /bin/false
            RC=4
        fi
    else
        action $"Stopping $prog: " /bin/false
        RC=4
    fi

    return $RC
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        $0 stop
        $0 start
        ;;
    reload)
        return 3
        ;;
    status)
        status -p "$pidfile" bridge-intake
        ;;
esac

exit $?
