#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
CLASS="com.pingcap.spark.TestFramework"

java -Dtest.mode=Dump -Dtest.dumpDB.databases="$@" -cp ${BASEDIR}/conf:${BASEDIR}/lib/* $CLASS 
