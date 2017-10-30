#!/usr/bin/env bash

BASEDIR=$(cd `dirname $0`; pwd)
CLASS="com.pingcap.spark.TestFramework"

java -Dtest.mode=Dump -Dtest.dumpDB.databases="$@" -cp ${BASEDIR}/conf:${BASEDIR}/lib/* ${CLASS}
