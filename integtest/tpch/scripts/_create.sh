#!/bin/bash
source _msq.sh

ensure_database()
{
	  $MYSQL_BIN -e "CREATE DATABASE IF NOT EXISTS $DATABASE"
}
export -f ensure_database

create_table()
{
	  local table="$1"
	  ensure_database
    local sql_file=$META_DIR/schema/$table.sql
    if [ -f "$sql_file" ]; then
	      $MYSQL_CMD --local-infile < $sql_file
    else
        "$sql_file is not existed."
    fi
}
export -f create_table
