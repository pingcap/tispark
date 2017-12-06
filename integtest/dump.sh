#!/usr/bin/env bash
set -ue

source _env.sh

cp ${PATH_TO_CONF}/tispark_config_dump.properties.template ${PATH_TO_CONF}/tispark_config.properties
cp ${PATH_TO_CONF}/tispark_config_dump.properties.template ${TISPARK_CONF}
java -Dtest.mode=Dump -Dtest.dumpDB.databases="$@" -cp ${PATH_TO_CONF}:${BASEDIR}/lib/* ${CLASS}
