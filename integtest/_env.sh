#!/usr/bin/env bash
set -ue

BASEDIR=$(cd `dirname $0`; pwd)
PATH_TO_JDBC="~/Downloads/mysql-connector-java-5.1.44/mysql-connector-java-5.1.44-bin.jar"
PATH_TO_CONF="$BASEDIR/conf"

if [ -z "${SPARK_HOME}" ]; then
    echo "SPARK_HOME is not set, please check"
    exit -1
else
    SPARK_HOME=${SPARK_HOME}
fi

CLASS="com.pingcap.spark.TestFramework"

spark_cmd="${SPARK_HOME}/bin/spark-submit --class ${CLASS} ${BASEDIR}/lib/* --driver-java-options"
spark_debug_opt="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=5005,suspend=y"
spark_test_opt=""

TISPARK_CONF="${SPARK_HOME}/conf/tispark_config.properties"
BASE_CONF="${BASEDIR}/conf/tispark_config.properties"

mysql_addr="localhost"
mysql_user="root"
mysql_password="root"
use_raw_mysql=false

addMysqlInfo() {
    use_raw_mysql=true
    echo "spark.use_raw_mysql=true" >> ${TISPARK_CONF}
    echo "mysql.addr=$mysql_addr" >> ${TISPARK_CONF}
    echo "mysql.user=$mysql_user" >> ${TISPARK_CONF}
    echo "mysql.password=$mysql_password" >> ${TISPARK_CONF}

    echo "spark.use_raw_mysql=true" >> ${BASE_CONF}
    echo "mysql.addr=$mysql_addr" >> ${BASE_CONF}
    echo "mysql.user=$mysql_user" >> ${BASE_CONF}
    echo "mysql.password=$mysql_password" >> ${BASE_CONF}
}

clear_last_diff_files() {
    for f in ./*.spark; do
        [ -e "$f" ] && rm *.spark
        break
    done
    for f in ./*.tidb; do
        [ -e "$f" ] && rm *.tidb
        break
    done
    for f in ./*.jdbc; do
        [ -e "$f" ] && rm *.jdbc
        break
    done
}

check_tpch_dir_is_present() {
    if [ ! -d "$BASEDIR/tpch" ]; then
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

load_DAG_Table() {
    mysql -h 127.0.0.1 -P 4000 -u root < ./testcases/tispark_test/TisparkTest.sql
}