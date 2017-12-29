#!/usr/bin/env bash
set -ue

source _env.sh

echo "Usage: <bin> [-h | -g | -a | -d | -s | -i | -r | -t <sql> | -b <db>]"
echo "Note: <sql> must be quoted. e.g., \"select * from t\""
echo "You may use sql-only like this:"
echo "./test_no_tpch.sh -t \"select * from t\" -b \"test\""

clear_last_diff_files

isDebug=false
showResultStats=false
showFailedOnly=false
mode="Integration"
sql=
db=

while getopts "t:b:dhrag" arg
do
    case ${arg} in
        d)
            isDebug=true
            ;;
		r)
			showResultStats=true
			;;
		g)
		    showFailedOnly=true
		    ;;
        a)
            cd ../
            mvn clean install
            cd integtest/
            ;;
        t)
            sql=$OPTARG
            echo "sql=$sql"
            mode="QueryOnly"
            ;;
        b)
            db=$OPTARG
            echo "db=$db"
            ;;
        h)
            echo "Options"
            echo "  -a        build all projects"
            echo "  -r        show result stats (SQL, outputs, time consumed, etc.)"
            echo "  -g        show failed only"
            echo "  -t <sql>  run sql statement <sql> (with quotes) only on TiSpark with debug mode (must assign a database)"
            echo "  -b <db>   use database <db> (with quotes) implicitly"
            echo "  -d        debug mode"
            echo "  -h        show help"
            exit 1
            ;;
        ?)
            echo "Fatal: Unknown argument"
            echo "exiting..."
            exit 1
            ;;
    esac
done

load_Index_Table

if [ "${mode}" == "Integration" ]; then
    filter=""
    create_conf_no_tpch
    if ! [ -z "${db}" ]; then
        echo "test.db=$db" >> ${TISPARK_CONF}
        echo "test.db=$db" >> ${BASE_CONF}
    fi
    if [ ${isDebug} = true ]; then
        echo "debugging..."
        ${spark_cmd} ${spark_debug_opt}
    else
        echo "testing...."
        if [ ${showResultStats} = true ]; then
            if [ ${showFailedOnly} = true ]; then
                filter="hint:\|output:\|Result:\|Elapsed time:\|query on\|FAILED.\|file error"
            else
                filter="hint:\|output:\|Result:\|Elapsed time:\|query on\|FAILED.\|PASSED.\|SKIPPED.\|exception caught"
            fi
        else
            if [ ${showFailedOnly} = true ]; then
                filter="Tests result:\|Result:\|exception caught.\|FAILED.\|file error"
            else
                filter="Tests result:\|Result:\|FAILED.\|PASSED.\|SKIPPED."
            fi
        fi
        ${spark_cmd} ${spark_test_opt} 2>&1 | grep "${filter}"
    fi
elif [ "${mode}" == "QueryOnly" ]; then
    create_conf
    if [ -z "${sql}" ]; then
        echo "sql can not be empty. Aborting..."
        exit -1
    else
        echo "test.sql=$sql" >> ${TISPARK_CONF}
        echo "test.sql=$sql" >> ${BASE_CONF}
    fi
    if [ -z "${db}" ]; then
        echo "DB name not specified. Aborting..."
        exit -1
    else
        echo "test.db=$db" >> ${TISPARK_CONF}
        echo "test.db=$db" >> ${BASE_CONF}
    fi
    echo "Running statement $sql"
    if [ ${isDebug} = true ]; then
        echo "debugging..."
        ${spark_cmd} ${spark_debug_opt}
    else
        echo "testing..."
        ${spark_cmd} ${spark_test_opt} 2>&1 | grep "hint:\|output:\|Result:\|Elapsed time:\|query on\|exception caught"
    fi
else
    echo "UnKnown test mode: $mode. Aborting..."
fi