#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
CLASS="com.pingcap.spark.TestFramework"

java -Dtest.mode=Load -cp ${BASEDIR}/conf:${BASEDIR}/lib/* $CLASS 

echo "If you have not load tpch data yet, please execute following command"
echo "cd tpch/scripts"
echo "bash genandloadalldata.sh"
