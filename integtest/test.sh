#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
CLASS="com.pingcap.spark.TestFramework"

cp ${BASEDIR}/conf/tispark_config.properties "${SPARK_HOME}"/conf/tispark_config.properties

if [[ "$@" = *--debug ]] || [[ "$@" = *-d ]]; then
  "${SPARK_HOME}"/bin/spark-submit --driver-java-options "-agentlib:jdwp=transport=dt_socket,server=y,address=5005,suspend=y -Dtest.mode=Test" --class $CLASS ${BASEDIR}/lib/*
else
  "${SPARK_HOME}"/bin/spark-submit --driver-java-options "-Dtest.mode=Test" --class $CLASS ${BASEDIR}/lib/*
fi

