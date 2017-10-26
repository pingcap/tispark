#!/usr/bin/env bash
set -ue

BASEDIR=$(cd `dirname $0`; pwd)
echo ${BASEDIR}
echo "usage: <bin> [-d | --debug]"

clear_last_diff_files() {
    for f in ./*.spark; do
        [ -e "$f" ] && rm *.spark
        break
    done
    for f in ./*.tidb; do
        [ -e "$f" ] && rm *.tidb
        break
    done
}

clear_last_diff_files

CLASS="com.pingcap.spark.TestFramework"

./load_index.sh

cp ${BASEDIR}/conf/tispark_config_testindex.properties.template ${SPARK_HOME}/conf/tispark_config.properties
spark_debug_opt="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=5005,suspend=y"
spark_test_opt=""

spark_cmd="${SPARK_HOME}/bin/spark-submit --class ${CLASS} ${BASEDIR}/lib/* --driver-java-options"
if [[ "$@" = *--debug ]] || [[ "$@" = *-d ]]; then
    echo "debuging..."
    $spark_cmd $spark_debug_opt
else
    echo "testing...."
    $spark_cmd $spark_test_opt 2>&1 | grep "result:"
fi
