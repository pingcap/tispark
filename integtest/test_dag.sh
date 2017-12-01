#!/usr/bin/env bash
set -ue

BASEDIR=$(cd `dirname $0`; pwd)
echo "Base directory in: ${BASEDIR}"
echo "Usage: <bin> [-a | -d | -h | -s | -i | -r]"

clear_last_diff_files() {
    for f in ./*.spark; do
        [ -e "$f" ] && rm *.spark
        break
    done
    for f in ./*.tidb; do
        [ -e "$f" ] && rm *.tidb
        break
    done
    for f in ./*.testSql; do
        [ -e "$f" ] && rm *.testSql
        break
    done
}

clear_last_diff_files

isDebug=false
showResultOnly=false

while getopts ":radish" arg
do
    case ${arg} in
        d)
            isDebug=true
            ;;
		r)
			showResultOnly=true
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
            echo "Options"
            echo "  -a    make all projects"
            echo "  -s    make tispark and integtest"
            echo "  -i    make integtest only"
			echo "	-r	  show result only"
            echo "  -d    debug mode"
            echo "  -h    show help"
            exit 1
            ;;
        ?)
            echo "Fatal: Unknown argument"
            echo "exiting..."
            exit 1
            ;;
    esac
done

CLASS="com.pingcap.spark.TestFramework"

cp ${BASEDIR}/conf/tispark_config_dag.properties.template ${SPARK_HOME}/conf/tispark_config.properties
spark_debug_opt="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=5005,suspend=y"
spark_test_opt=""

spark_cmd="${SPARK_HOME}/bin/spark-submit --class ${CLASS} ${BASEDIR}/lib/* --driver-java-options"
if [ ${isDebug} = true ]; then
    echo "debugging..."
    ${spark_cmd} ${spark_debug_opt}
else
    echo "testing...."
	if [ ${showResultOnly} = true ]; then
		${spark_cmd} ${spark_test_opt} 2>&1 | grep -F "***************"
	else
		${spark_cmd} ${spark_test_opt} 2>&1 | grep "hint:\|output:\|result:\|Elapsed time:\|query on spark\|query on TiDB"
	fi
fi
