#!/bin/bash

cmd=$1
shift

LOG_CONF="-Dlogback.configurationFile=drillpress-log.xml"
LOG_OPTS="-Djig.log.path=$DRILLBIT_LOG_PATH $LOG_CONF"
if [ -n "$DRILL_JAVA_LIB_PATH" ]; then
  DRILL_JAVA_OPTS="$DRILL_JAVA_OPTS -Djava.library.path=$DRILL_JAVA_LIB_PATH"
fi
DRILLPRESS_HEAP=${DRILLPRESS_HEAP:-"1G"}
DRILLPRESS_MAX_DIRECT_MEMORY=${DRILLPRESS_MAX_DIRECT_MEMORY:-"1G"}
DRILLPRESS_JAVA_OPTS=${DRILLPRESS_JAVA_OPTS:-""}
DRILLPRESS_OPTS="-Xms$DRILLPRESS_HEAP -Xmx$DRILLPRESS_HEAP -XX:MaxDirectMemorySize=$DRILLPRESS_MAX_DIRECT_MEMORY"
DRILL_ALL_JAVA_OPTS="$DRILLPRESS_OPTS $DRILLPRESS_JAVA_OPTS $SERVER_GC_OPTS $@ $LOG_OPTS"
JIGCMD="$JAVA $DRILL_ALL_JAVA_OPTS -cp $CP org.apache.drill.jig.drillpress.DrillPress"

# The wrapper is purely for unit testing.

if [ -n "$_DRILL_WRAPPER_" ]; then
  JIGCMD="$_DRILL_WRAPPER_ $JIGCMD"
fi

# Run the command (exec) or just print it (debug).
# Two options: run as a child (exec) or
# just print the command (debug).

case $cmd in
(debug)
  echo "----------------- Environment ------------------"
  env
  echo "------------------------------------------------"
  echo "Launch command:"
  echo $JIGCMD
  ;;
(*)
  exec $JIGCMD
  ;;
esac
