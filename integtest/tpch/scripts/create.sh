table="$1"

set -eu

source _env.sh
source _create.sh

ensure_database
if [ -z "$table" ]; then
	  ls $META_DIR/schema/ | awk -F '.' '{print $1}' | while read table; do
		create_table $table
	  done
else
	  create_table $table
fi
