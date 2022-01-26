#!/bin/sh

#typeset -A ARGS
#ARGS["debug"]=" -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=n "
#ARGS["jmx"]=" -Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false "

cd `dirname $0`
BIN_DIR=`pwd`
cd ..
DEPLOY_DIR=`pwd`
CONF_DIR=$DEPLOY_DIR/conf

LOGS_DIR=$DEPLOY_DIR/logs
if [ ! -d $LOGS_DIR ]; then
    mkdir $LOGS_DIR
fi
STDOUT_FILE=$LOGS_DIR/stdout.log

LIB_DIR=$DEPLOY_DIR/lib
LIB_JARS=$LIB_DIR/classes:$LIB_DIR/*

JAVA_OPTS=" -server -Xmx4g -Xms4g -Xss256k "

echo -e "Starting the $SERVER_NAME ...\c"
nohup java $JAVA_OPTS -classpath $CONF_DIR:$LIB_JARS io.github.zhangjig.transfer.benchmark.ConnectorMain 2>&1 >$STDOUT_FILE &

COUNT=0
while [ $COUNT -lt 1 ]; do
    echo -e ".\c"
    sleep 1
    if [ -n "$SERVER_PORT" ]; then
        COUNT=`netstat -an | grep $SERVER_PORT | wc -l`
    else
    	COUNT=`ps -f | grep java | grep "$DEPLOY_DIR" | awk '{print $2}' | wc -l`
    fi
    if [ $COUNT -gt 0 ]; then
        break
    fi
done

echo "OK!"
PIDS=`ps -f | grep java | grep "$DEPLOY_DIR" | awk '{print $2}'`
echo "PID: $PIDS"
echo "STDOUT: $STDOUT_FILE"
