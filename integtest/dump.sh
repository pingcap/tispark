#!/usr/bin/env bash
set -ue

source _env.sh

create_conf_dump

java -Dtest.mode=Dump -Dtest.dumpDB.databases="$@" -cp ${PATH_TO_CONF}:${BASEDIR}/lib/* ${CLASS}
