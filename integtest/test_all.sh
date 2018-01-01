#!/usr/bin/env bash

source _env.sh

echo "Usage: <bin> [-a | -d | -h]"

isDebug=false

while getopts ":hda" arg
do
    case ${arg} in
        d)
            isDebug=true
            ;;
        a)
            cd ../
            mvn clean install
            cd integtest/
            ;;
        h)
            echo "Options"
            echo "  -a        build all projects"
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

