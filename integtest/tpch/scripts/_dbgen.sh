source _meta.sh

dbgen_bin()
{
	  $DBGEN_DIR/dbgen -b $DBGEN_DIR/dists.dss $@  2>/dev/null
}
export -f dbgen_bin

get_dbgen_flag()
{
	local table="$1"
	cat "$META_DIR/tables" | grep "$table$" | awk -F '\t' '{print $1}'
}
export -f get_dbgen_flag

dbgen_all_in_one()
{
	local dir="$DBGEN_RESULT_DIR_PREFIX"1

	if [ -d "$dir" ]; then
		echo "$dir: exists, exit" >&2
		return 1
	fi

	mkdir -p "$dir"
	cd "$dir"
	dbgen_bin -s "$tpch_scale"
}
export -f dbgen_all_in_one

dbgen_table_in_one()
{
	local table="$1"
	local dir="$DBGEN_RESULT_DIR_PREFIX"1
	local flag=`get_dbgen_flag $table`

	if [ -f "$dir/$table.tbl" ]; then
		echo "$dir/$table.tbl: exists, exit" >&2
		return 1
	fi
	if [ -z "$flag" ]; then
		echo "unknown table name: $table" >&2
		return 1
	fi

	mkdir -p "$dir"
	cd "$dir"
	dbgen_bin -s "$TPCH_SCALE" $flag
}
export -f dbgen_table_in_one

dbgen_table_blocks()
{
  local step="$1"
	local blocks="$2"
	local table="$3"
	local dir="$DBGEN_RESULT_DIR_PREFIX"

	if [ -f "$dir/$table.tbl" ]; then
		echo "$dir/$table.tbl: exists, skip" >&2
		return 1
	fi
	if [ -f "$dir/$table.tbl.1" ]; then
		echo "$dir/$table.tbl.*: exists, skip" >&2
		return 1
	fi

	local flag=`get_dbgen_flag $table`
	if [ -z "$flag" ]; then
		echo "unknown table name: $table" >&2
		return 1
	fi

	mkdir -p "$dir"
	cd "$dir"

  if [ "$table" == "nation" ] || [ "$table" == "region" ]; then
		  dbgen_bin -s "$TPCH_SCALE" $flag &
  else
      for ((i = 0; i < $blocks; i+=$step)); do
          for((j = i; j < i + $step && j < $blocks; j++)); do
              let "n = $j + 1"
		          dbgen_bin -s "$TPCH_SCALE" -C "$blocks" -S "$n" $flag &
          done
	    done
  fi

	wait_sub_procs
}
export -f dbgen_table_blocks

dbgen_tables_blocks()
{
  local step="$1"
	local blocks="$2"
	get_table_names | while read table; do
		  dbgen_table_blocks "$step" "$blocks" "$table" 
	done
}
export -f dbgen_tables_blocks

dbgen()
{

  local step=$TPCH_STEP
	local blocks=$TPCH_BLOCKS
	local table="$1"
	if [ "$blocks" == "1" ]; then
		if [ -z "$table" ]; then
			dbgen_all_in_one
		else
			dbgen_table_in_one $table
		fi
	else
		if [ -z "$table" ]; then
			dbgen_tables_blocks "$step" "$blocks" 
		else
		  dbgen_table_blocks "$step" "$blocks" "$table" 
		fi
	fi
}
export -f dbgen
