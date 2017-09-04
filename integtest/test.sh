#!/usr/bin/env bash
set -ue

clear_last_diff_files() {
    if [ -z "ls *.spark" ]; then
        rm *.spark
    fi
    if [ -z "ls *.tidb" ]; then
        rm *.tidb
    fi
}

clear_last_diff_files

BASEDIR=$(dirname "$0")
CLASS="com.pingcap.spark.TestFramework"

cp ${BASEDIR}/conf/tispark_config.properties "${SPARK_HOME}"/conf/tispark_config.properties
spark_debug_opt="-agentlib:jdwp=transport=dt_socket,server=y,address=5005,suspend=y -Dtest.mode=Test"
spark_test_opt="-Dtest.mode=Test"
# --driver-java-options
spark_cmd="${SPARK_HOME}/bin/spark-submit --class $CLASS ${BASEDIR}/lib/* --driver-java-options"
if [[ "$@" = *--debug ]] || [[ "$@" = *-d ]]; then
    echo "debuging..."
    $spark_cmd $spark_debug_opt | grep "*.sql result"
else
    echo "testing...."
    $spark_cmd $spark_test_opt 2>&1 | grep "result:"
fi

