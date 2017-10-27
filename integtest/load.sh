#!/usr/bin/env bash

BASEDIR=$(cd `dirname $0`; pwd)
CLASS="com.pingcap.spark.TestFramework"

cp ${BASEDIR}/conf/tispark_config_load.properties.template ${BASEDIR}/conf/tispark_config.properties
java -Dtest.mode=Load -cp ${BASEDIR}/conf:${BASEDIR}/lib/* ${CLASS}