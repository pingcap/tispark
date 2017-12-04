#!/usr/bin/env bash
set -ue

source _env.sh

CLASS="com.pingcap.spark.TestFramework"

cp ${PATH_TO_CONF}/tispark_config_load.properties.template ${PATH_TO_CONF}/tispark_config.properties
cp ${PATH_TO_CONF}/tispark_config_load.properties.template ${TISPARK_CONF}
java -Dtest.mode=Load -cp ${PATH_TO_CONF}:${BASEDIR}/lib/* ${CLASS}