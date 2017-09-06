#!/bin/bash
source _env.sh

get_table_names()
{
	  cat "$meta_dir/tables" | awk -F '\t' '{print $2}'
}
export -f get_table_names
