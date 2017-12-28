#!/usr/bin/env bash

source _env.sh

echo "Usage: <bin> [-a | -s | -i | -d | -h]"

build_init_properties
isDebug=false

while getopts ":shida" arg
do
    case ${arg} in
        d)
            isDebug=true
            ;;
        a)
            cd ../tikv-client/
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
            echo "  -a        make all projects"
            echo "  -s        make tiSpark and integration test projects"
            echo "  -i        make integration test only"
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

if [ ${isDebug} = true ]; then
    clear_all_diff_files
    ./test_no_tpch.sh -d
    rename_result_files_no_tpch
    ./test_dag.sh -d
    rename_result_files_dag
    ./test_tpch.sh -d
    rename_result_files_tpch
else
    clear_all_diff_files
    ./test_no_tpch.sh
    rename_result_files_no_tpch
    ./test_dag.sh
    rename_result_files_dag
    ./test_tpch.sh
    rename_result_files_tpch
fi

