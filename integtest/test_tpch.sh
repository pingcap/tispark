#!/usr/bin/env bash
set -ue

source _env.sh

echo ${BASEDIR}
echo "usage: <bin> [-d | --debug]"

clear_last_diff_files_tpch
check_tpch_dir_is_present
check_tpch_data_is_loaded

create_conf_tpch

if [[ "$@" = *--debug ]] || [[ "$@" = *-d ]]; then
    echo "debugging..."
    ${spark_cmd} ${spark_debug_opt}
else
    echo "testing...."
    ${spark_cmd} ${spark_test_opt} 2>&1 | grep "result:\|time:"
fi
