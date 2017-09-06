source _env.sh
set -ue

cat $META_DIR/tables | awk -F ' ' '{print$3}' | while read table; do
    echo "creating database and tables $table"
    bash create.sh
    echo "generating tables $table"
    bash gen.sh "$table"
    echo "loading tables $table"
    bash load.sh "$table"
    done
