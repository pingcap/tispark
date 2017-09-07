source _env.sh
set -ue

check_dbgen_binary() {
    local dbgen_dir=$CURRENT_DIR/../dbgen
    local dbgen_bin=$dbgen_dir/dbgen
    if [ ! -f $dbgen_bin ]; then
        echo "dbgen is not present, please go to dbgen directory and make dbgen executable binary file."
        exit
    fi
}

check_dbgen_binary
cat $META_DIR/tables | awk -F ' ' '{print$3}' | while read table; do
    echo "creating database and tables $table"
    # bash create.sh
    # echo "generating tables $table"
    # bash gen.sh "$table"
    # echo "loading tables $table"
    # bash load.sh "$table"
    done
