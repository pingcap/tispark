#!/usr/bin/env bash
set -ue

BASE_DIR=$(pwd)
TEST_DIR="${BASE_DIR}/testcases"

echo "usage: <bin> [-d | --debug]"

clear_last_diff_files() {
    if [ -f "*.spark" ]; then
        rm *.spark
    fi
    if [ -f "*.tidb" ]; then
        rm *.tidb
    fi
}

check_tpch_dir_is_present() {
    if [ ! -d "${BASE_DIR}/tpch" ]; then
        echo "tpch is not present. You have to clone it to your local machine."
        echo "this script is required to generate and load tpch database to TiDB cluster."
        echo "git clone https://github.com/zhexuany/tispark_tpch tpch"
        exit
    fi
}

check_tpch_data_is_loaded() {
    if [ hash mysql 2>/dev/null ]; then
        echo "please install mysql first."
        exit
    fi
    res=`mysql -h 127.0.0.1 -P 4000 -u root -e "show databases" | grep "tpch" > /dev/null; echo "$?"`
    if [ ! "$res" -eq 0 ]; then
        echo "please load tpch data to tidb cluster first."
        exit
    fi
}

clear_last_diff_files
check_tpch_dir_is_present
check_tpch_data_is_loaded

CLASS="com.pingcap.spark.TestFramework"

cp ${BASE_DIR}/conf/tispark_config.properties "${SPARK_HOME}"/conf/tispark_config.properties
spark_debug_opt="-agentlib:jdwp=transport=dt_socket,server=y,address=5005,suspend=y -Dtest.mode=Test"
spark_test_opt="-Dtest.mode=Test"

spark_cmd="${SPARK_HOME}/bin/spark-submit --class ${CLASS} ${BASE_DIR}/lib/* --driver-java-options"
if [[ "$@" = *--debug ]] || [[ "$@" = *-d ]]; then
    echo "debuging..."
    $spark_cmd $spark_debug_opt | grep "*.sql result"
else
    echo "testing...."
    $spark_cmd $spark_test_opt 2>&1 | grep "result:"
fi