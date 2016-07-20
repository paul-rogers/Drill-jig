#!/usr/bin/env bash

usage="Usage: jig-server.sh [--config|--site <site-dir>]\
 (start|stop|status|restart|run) [args]"

jigbin=`dirname "${BASH_SOURCE-$0}"`
jigbin=`cd "$jigbin">/dev/null; pwd`

base=`basename "${BASH_SOURCE-$0}"`
command=${base/.*/}

# Infer Drill home. Must be given, or we assume this script
# is in:
#
# $DRILL_HOME/jig/bin

if [ -n "$DRILL_HOME" ]; then
  export DRILL_HOME=`cd "$jigbin/../..">/dev/null; pwd`
fi

# Setup environment. This parses, and removes, the
# options --config conf-dir parameters.

. "$DRILL_HOME/bin/drill-config.sh"

# if no args specified, show usage
if [ ${#args[@]} = 0 ]; then
  echo $usage
  exit 1
fi

# Adjust environment as needed by the Jig server. Might want to refactor
# drill-config.sh to make this easier.

# Prepare log file prefix and the main Drillbit log file.

export DRILL_LOG_PREFIX="$DRILL_LOG_DIR/jig-server"
export DRILLBIT_LOG_PATH="${DRILL_LOG_PREFIX}.log"

#--------------------------------------------------------------------
# The following comes directly from drillbit.sh. Share the code
# at some point. The differences can all be parameterized.

# Get command. all other args are JVM args, typically properties.
action="${args[0]}"
args[0]=''
export args

# Set default scheduling priority
DRILL_NICENESS=${DRILL_NICENESS:-0}

waitForProcessEnd()
{
  pidKilled=$1
  commandName=$2
  processedAt=`date +%s`
  origcnt=${DRILL_STOP_TIMEOUT:-120}
  while kill -0 $pidKilled > /dev/null 2>&1;
   do
     echo -n "."
     sleep 1;
     # if process persists more than $DRILL_STOP_TIMEOUT (default 120 sec) no mercy
     if [ $(( `date +%s` - $processedAt )) -gt $origcnt ]; then
       break;
     fi
  done
  echo
  # process still there : kill -9
  if kill -0 $pidKilled > /dev/null 2>&1; then
    echo "$commandName did not complete after $origcnt seconds, killing with kill -9 $pidKilled"
    $JAVA_HOME/bin/jstack -l $pidKilled > "$logout" 2>&1
    kill -9 $pidKilled > /dev/null 2>&1
  fi
}

check_before_start()
{
  #check that the process is not running
  mkdir -p "$DRILL_PID_DIR"
  if [ -f $pid ]; then
    if kill -0 `cat $pid` > /dev/null 2>&1; then
      echo "$command is already running as process `cat $pid`.  Stop it first."
      exit 1
    fi
  fi
}

wait_until_done ()
{
  p=$1
  cnt=${DRILLBIT_TIMEOUT:-300}
  origcnt=$cnt
  while kill -0 $p > /dev/null 2>&1; do
    if [ $cnt -gt 1 ]; then
      cnt=`expr $cnt - 1`
      sleep 1
    else
      echo "Process did not complete after $origcnt seconds, killing."
      kill -9 $p
      exit 1
    fi
  done
  return 0
}

start_jig ( )
{
  check_before_start
  echo "Starting $command, logging to $logout"
  echo "`date` Starting $command on `hostname`" >> "$DRILLBIT_LOG_PATH"
  echo "`ulimit -a`" >> "$DRILLBIT_LOG_PATH" 2>&1
  nohup nice -n $DRILL_NICENESS "$DRILL_HOME/bin/runjig.sh" exec ${args[@]} >> "$logout" 2>&1 &
  echo $! > $pid
  sleep 1
}

stop_bit ( )
{
  if [ -f $pid ]; then
    pidToKill=`cat $pid`
    # kill -0 == see if the PID exists
    if kill -0 $pidToKill > /dev/null 2>&1; then
      echo "Stopping $command"
      echo "`date` Terminating $command pid $pidToKill" >> "$DRILLBIT_LOG_PATH"
      kill $pidToKill > /dev/null 2>&1
      waitForProcessEnd $pidToKill $command
      retval=0
    else
      retval=$?
      echo "No $command to stop because kill -0 of pid $pidToKill failed with status $retval"
    fi
    rm $pid > /dev/null 2>&1
  else
    echo "No $command to stop because no pid file $pid"
    retval=1
  fi
  return $retval
}

pid=$DRILL_PID_DIR/jig-server.pid
logout="${DRILL_LOG_PREFIX}.out"

thiscmd=$0

case $action in

(start)
  start_jig
  ;;

(run)
  # Launch Jig as a child process. Does not redirect stderr or stdout.
  # Does not capture the Jig server pid.
  # Use this when launching Jig from your own script that manages the
  # process, such as (roll-your-own) YARN, Mesos, supervisord, etc.

  echo "`date` Starting $command on `hostname`"
  echo "`ulimit -a`"
  $DRILL_HOME/bin/runjig.sh exec ${args[@]}
  ;;

(stop)
  stop_bit
  exit $?
  ;;

(restart)
  # stop the command
  stop_bit
  # wait a user-specified sleep period
  sp=${DRILL_RESTART_SLEEP:-3}
  if [ $sp -gt 0 ]; then
    sleep $sp
  fi
  # start the command
  start_bit
  ;;

(status)
  if [ -f $pid ]; then
    TARGET_PID=`cat $pid`
    if kill -0 $TARGET_PID > /dev/null 2>&1; then
      echo "$command is running."
    else
      echo "$pid file is present but $command is not running."
      exit 1
    fi
  else
    echo "$command is not running."
    exit 1
  fi
  ;;

(debug)
  # Undocumented command to print out environment and Drillbit
  # command line after all adjustments.

  echo "command: $command"
  echo "args: ${args[@]}"
  echo "cwd:" `pwd`
  # Print Drill command line
  "$DRILL_HOME/bin/runjig.sh" debug ${args[@]}
  ;;

(*)
  echo $usage
  exit 1
  ;;
esac
