#!/usr/bin/env bash
set -ue

BASEDIR=$(cd `dirname $0`; pwd)
echo "Base directory in: $BASEDIR"
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
INIT_CONF="${BASEDIR}/conf/tidb_config.properties"

mysql_addr="localhost"
mysql_user="root"
mysql_password="root"
use_raw_mysql=false

tidb_addr=
tidb_port=
tidb_user=

build_if_not_exists_init_properties() {
    file=${BASE_CONF}
    init=${INIT_CONF}

    if ! [ -f "$file" ]
    then
        echo "$file not found. "
        echo "Building initial config file."
        if ! [ -f "$init" ]; then
            echo "$init not found. Please set this file manually according to README."
            exit -1
        fi
        cp ${init} ${file}
    fi
}

read_properties() {
    file=${BASE_CONF}

    if [ -f "$file" ]
    then
      echo "$file found."

      while IFS='=' read -r key value
      do
        key=$(echo ${key} | tr '.' '_')
        if ! [ -z "${key}" ]; then
            eval "${key}='${value}'"
        fi
      done < "$file"

      echo "Address   = " ${tidb_addr}
      echo "User port = " ${tidb_port}
      echo "User name = " ${tidb_user}
    else
      echo "$file not found."
    fi
}

build_if_not_exists_init_properties
read_properties

create_conf_db_options() {
    echo "tidb.addr=$tidb_addr" >  ${BASE_CONF}
    echo "tidb.port=$tidb_port" >> ${BASE_CONF}
    echo "tidb.user=$tidb_user" >> ${BASE_CONF}
    echo "" >> ${BASE_CONF}
    echo "# Options below will be refreshed each time. You don't have to change them" >> ${BASE_CONF}
    echo "" >> ${BASE_CONF}
}

create_conf() {
    echo "create default conf..."
    create_conf_db_options
    echo "test.mode=Test"    >> ${BASE_CONF}
    echo "test.ignore=tpch" >> ${BASE_CONF}

    cp ${BASE_CONF} ${TISPARK_CONF}
}

create_conf_no_tpch() {
    echo "create conf for custom tests..."
    create_conf_db_options
    echo "test.mode=TestAlone"    >> ${BASE_CONF}
    echo "test.ignore=tpch,tpch_test,tispark_test" >> ${BASE_CONF}

    cp ${BASE_CONF} ${TISPARK_CONF}
}

create_conf_dag() {
    echo "create conf for dag tests..."
    create_conf_db_options
    echo "test.mode=TestDAG"    >> ${BASE_CONF}
    echo "test.db=tispark_test" >> ${BASE_CONF}

    cp ${BASE_CONF} ${TISPARK_CONF}
}

create_conf_tpch() {
    echo "create conf for TPCH tests..."
    create_conf_db_options
    echo "test.mode=Test"    >> ${BASE_CONF}
    echo "test.db=tpch_test" >> ${BASE_CONF}

    cp ${BASE_CONF} ${TISPARK_CONF}
}

create_conf_load() {
    echo "create conf for loading data..."
    create_conf_db_options
    echo "test.mode=Load"    >> ${BASE_CONF}
    echo "test.ignore=tpch,tpch_TEST" >> ${BASE_CONF}

    cp ${BASE_CONF} ${TISPARK_CONF}
}

create_conf_dump() {
    echo "create conf for dumping data..."
    create_conf_db_options
    echo "test.mode=Dump"    >> ${BASE_CONF}
    echo "test.ignore=tpch" >> ${BASE_CONF}

    cp ${BASE_CONF} ${TISPARK_CONF}
}

#add_MySQL_info() {
#    use_raw_mysql=true
#    echo "spark.use_raw_mysql=true" >> ${TISPARK_CONF}
#    echo "mysql.addr=$mysql_addr" >> ${TISPARK_CONF}
#    echo "mysql.user=$mysql_user" >> ${TISPARK_CONF}
#    echo "mysql.password=$mysql_password" >> ${TISPARK_CONF}
#
#    echo "spark.use_raw_mysql=true" >> ${BASE_CONF}
#    echo "mysql.addr=$mysql_addr" >> ${BASE_CONF}
#    echo "mysql.user=$mysql_user" >> ${BASE_CONF}
#    echo "mysql.password=$mysql_password" >> ${BASE_CONF}
#}

clear_all_diff_files() {
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

clear_last_diff_files() {
    for f in ./inlineTest*.spark; do
        [ -e "$f" ] && rm inlineTest*.spark
        break
    done
    for f in ./inlineTest*.tidb; do
        [ -e "$f" ] && rm inlineTest*.tidb
        break
    done
    for f in ./inlineTest*.jdbc; do
        [ -e "$f" ] && rm inlineTest*.jdbc
        break
    done
}

clear_last_diff_files_DAG() {
    clear_last_diff_files
    for f in ./TestDAG*.spark; do
        [ -e "$f" ] && rm TestDAG*.spark
        break
    done
    for f in ./TestDAG*.tidb; do
        [ -e "$f" ] && rm TestDAG*.tidb
        break
    done
    for f in ./TestDAG*.jdbc; do
        [ -e "$f" ] && rm TestDAG*.jdbc
        break
    done
}

clear_last_diff_files_tpch() {
    for f in ./tpch_test*.spark; do
        [ -e "$f" ] && rm tpch_test*.spark
        break
    done
    for f in ./tpch_test*.tidb; do
        [ -e "$f" ] && rm tpch_test*.tidb
        break
    done
    for f in ./tpch_test*.jdbc; do
        [ -e "$f" ] && rm tpch_test*.jdbc
        break
    done
    for f in ./TestTPCH*.spark; do
        [ -e "$f" ] && rm TestTPCH*.spark
        break
    done
    for f in ./TestTPCH*.tidb; do
        [ -e "$f" ] && rm TestTPCH*.tidb
        break
    done
    for f in ./TestTPCH*.jdbc; do
        [ -e "$f" ] && rm TestTPCH*.jdbc
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
    res=`mysql -h ${tidb_addr} -P ${tidb_port} -u "${tidb_user}" -e "show databases" | grep "tpch_test" > /dev/null; echo "$?"`
    if [ ! "$res" -eq 0 ]; then
        echo "please load tpch data to tidb cluster first."
        exit
    fi
}

load_DAG_Table() {
    mysql -h ${tidb_addr} -P ${tidb_port} -u "${tidb_user}" < ./testcases/tispark_test/TisparkTest.sql
}

load_Index_Table() {
    mysql -h ${tidb_addr} -P ${tidb_port} -u "${tidb_user}" < ./testcases/test_index/testIndex.sql
}

rename_result_files_no_tpch() {
    for f in ./*.jdbc; do
        [ -e "$f" ] && mv "$f" "${f/inlineTest/TestNoTPCH}"
        # break
    done
    for f in ./*.spark; do
        [ -e "$f" ] && mv "$f" "${f/inlineTest/TestNoTPCH}"
        # break
    done
    for f in ./*.tidb; do
        [ -e "$f" ] && mv "$f" "${f/inlineTest/TestNoTPCH}"
        # break
    done
    echo "renamed files to TestNoTPCH"
}

rename_result_files_dag() {
    for f in ./inlineTest*.jdbc; do
        [ -e "$f" ] && mv "$f" "${f/inlineTest/TestDAG}"
        # break
    done
    for f in ./inlineTest*.spark; do
        [ -e "$f" ] && mv "$f" "${f/inlineTest/TestDAG}"
        # break
    done
    for f in ./inlineTest*.tidb; do
        [ -e "$f" ] && mv "$f" "${f/inlineTest/TestDAG}"
        # break
    done
    echo "renamed files to TestDAG"
}

rename_result_files_tpch() {
    for f in ./tpch_test*.jdbc; do
        [ -e "$f" ] && mv "$f" "${f/tpch_test/TestTPCH}"
        # break
    done
    for f in ./tpch_test*.spark; do
        [ -e "$f" ] && mv "$f" "${f/tpch_test/TestTPCH}"
        # break
    done
    for f in ./tpch_test*.tidb; do
        [ -e "$f" ] && mv "$f" "${f/tpch_test/TestTPCH}"
        # break
    done
    echo "renamed files to TestTPCH"
}