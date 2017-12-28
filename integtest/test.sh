#!/usr/bin/env bash
set -ue

source _env.sh

echo ${BASEDIR}
echo "usage: <bin> [-d | --debug]"

build_init_properties
clear_last_diff_files
check_tpch_dir_is_present
check_tpch_data_is_loaded

CLASS="com.pingcap.spark.TestFramework"

create_conf

spark_debug_opt="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=5005,suspend=y"
spark_test_opt=""

spark_cmd="${SPARK_HOME}/bin/spark-submit --class ${CLASS} ${BASEDIR}/lib/* --driver-java-options"
if [[ "$@" = *--debug ]] || [[ "$@" = *-d ]]; then
    echo "debugging..."
    ${spark_cmd} ${spark_debug_opt}
else
    echo "testing...."
    ${spark_cmd} ${spark_test_opt} 2>&1 | grep "result:\|time:"
fi
