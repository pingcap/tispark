#!/bin/bash
export POSTFIX=scripts
export PAR_DIR=${PWD%/$POSTFIX}
export DBGEN_DIR=$PAR_DIR/dbgen
export DBGEN_BIN=$DBGEN_DIR/dbgen

wait_sub_procs()
{
	if [ "$is_mac" == "yes" ]; then
		wait
	else
		local failed=0
		jobs -p | while read job; do
			wait $job || let "$failed+=1"
		done
		if [ "$failed" != 0 ]; then
			echo "$failed jobs failed"
			return 1
		fi
	fi
}
export -f wait_sub_procs

# check os is mac or not
_get_is_mac(){
    mac="no"
    if [ "$(uname)" == "Darwin" ]; then
        mac="yes"
    fi
    echo $mac
}
export is_mac=`_get_is_mac`

# check dbgen binary is existed or not. 
_check_dbgen_bin()
{
    if [ ! -f "$DBGEN_BIN" ]; then
        echo "dbgen binary is not present yet. Creating dbgen binary now..."
        cd $DBGEN_DIR; make >/dev/null 2>1&
        wait_sub_procs
        echo "dbgen binary is finished."
    fi
}
export -f _check_dbgen_bin
