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