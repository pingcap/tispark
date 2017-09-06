table="$1"

set -eu

source _load.sh

dir=$DBGEN_RESULT_DIR_PREFIX

if [ ! -z "$table" ]; then
    if [ -f "$dir/$table.tbl" ]; then
        load_table_from_file $table $dir/$table.tbl
    elif [ -f "$dir/$table.tbl.1" ]; then
        load_table_from_files $table
    fi
fi
