#!/usr/bin/env bash
set -ue

BASEDIR=$(cd `dirname $0`; pwd)
echo ${BASEDIR}
echo "usage: <bin> [-a | -d | -h | -s | -i]"

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

isdebug=false

while getopts ":dias" arg
do
    case $arg in
        d)
            isdebug=true
            ;;
        a)   
            cd ../tikv-client-lib-java/
            mvn clean install
            cd ../
            mvn clean install
            cd integtest/
            mvn clean install
            ;;
        s)
            cd ../
            mvn clean install
            cd integtest/
            mvn clean install
            ;;
        i)
            mvn clean install
            ;;
        h)
            echo ">help -a make all projects"
            echo ">help -s make tispark and integtest"
            echo ">help -i make integtest only"
            echo ">help -d debug mode"
            echo ">help -h show help"
            exit 1
            ;;
        ?)
            echo "unknown argument"
            exit 1
            ;;
    esac
done

CLASS="com.pingcap.spark.TestFramework"

cp ${BASEDIR}/conf/tispark_config_testindex.properties.template ${SPARK_HOME}/conf/tispark_config.properties
spark_debug_opt="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=5005,suspend=y"
spark_test_opt=""

spark_cmd="${SPARK_HOME}/bin/spark-submit --class ${CLASS} ${BASEDIR}/lib/* --driver-java-options"
if [ $isdebug = true ]; then
    echo "debuging..."
    ${spark_cmd} ${spark_debug_opt}
else
    echo "testing...."
    ${spark_cmd} ${spark_test_opt} 2>&1 | grep "result:\|Elapsed time:\|query on spark\|query on TiDB"
fi
