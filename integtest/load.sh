#!/usr/bin/env bash
set -ue

source _env.sh

CLASS="com.pingcap.spark.TestFramework"

create_conf_load

java -Dtest.mode=Load -cp ${PATH_TO_CONF}:${BASEDIR}/lib/* ${CLASS}