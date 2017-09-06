source _create.sh

load_table_from_file()
{
	  local table="$1"
	  local file="$2"

	  if [ -z "$file" ]; then
		    file="$DBGEN_RESULT_DIR_PREFIX"1"/$table.tbl"
	  fi
	  if [ ! -f "$file" ]; then
		    echo "$file: not found" >&2
		    return 1
	  fi

	  create_table "$table"

	  local sql="LOAD DATA LOCAL INFILE '$file' INTO TABLE $table FIELDS TERMINATED BY '|'"
	  $MYSQL_CMD --local-infile=1 -e "$sql"
}
export -f load_table_from_file

load_table_from_files()
{
	  local table="$1"

    for ((i = 0; i < $TPCH_BLOCKS; i+=$TPCH_STEP)); do
        for ((j=i; j < i + $TPCH_STEP; j++)); do
            let "n = $j + 1"
		        local file="$DBGEN_RESULT_DIR_PREFIX/$table.tbl.$n"
		        if [ ! -f "$file" ]; then
			          echo "$file: not found" >&2
			          return 1
		        fi
		        load_table_from_file "$table" "$file" &
        done
	      wait_sub_procs
    done
}
export -f load_table_from_files
