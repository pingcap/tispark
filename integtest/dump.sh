#!/usr/bin/env bash

BASEDIR=$(cd `dirname $0`; pwd)
CLASS="com.pingcap.spark.TestFramework"

cp ${BASEDIR}/conf/tispark_config_dump.properties.template ${BASEDIR}/conf/tispark_config.properties
cp ${BASEDIR}/conf/tispark_config_dump.properties.template ${SPARK_HOME}/conf/tispark_config.properties
java -Dtest.mode=Dump -Dtest.dumpDB.databases="$@" -cp ${BASEDIR}/conf:${BASEDIR}/lib/* ${CLASS}
