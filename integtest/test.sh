#!/usr/bin/env bash
set -ue

BASE_DIR=$(cd `dirname $0`; pwd)
echo ${BASE_DIR}
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
    res=`mysql -h 127.0.0.1 -P 4000 -u root -e "show databases" | grep "tpch_test" > /dev/null; echo "$?"`
    if [ ! "$res" -eq 0 ]; then
        echo "please load tpch data to tidb cluster first."
        exit
    fi
}

clear_last_diff_files
check_tpch_dir_is_present
check_tpch_data_is_loaded

CLASS="com.pingcap.spark.TestFramework"

cp ${BASE_DIR}/conf/tispark_config.properties.template ${SPARK_HOME}/conf/tispark_config.properties
spark_debug_opt="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=5005,suspend=y"
spark_test_opt=""

spark_cmd="${SPARK_HOME}/bin/spark-submit --class ${CLASS} ${BASE_DIR}/lib/* --driver-java-options"
if [[ "$@" = *--debug ]] || [[ "$@" = *-d ]]; then
    echo "debugging..."
    ${spark_cmd} ${spark_debug_opt}
else
    echo "testing...."
    $spark_cmd ${spark_test_opt} 2>&1 | grep "result:\|time:"
fi
