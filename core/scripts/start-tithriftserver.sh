#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Shell script for starting the Spark SQL Thrift server

# Enter posix mode for bash
set -o posix

TISPARK_JAR=tispark-core-1.0-jar-with-dependencies.jar

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

if [ -z "$SPARK_SCALA_VERSION" ]; then

  ASSEMBLY_DIR2="${SPARK_HOME}/assembly/target/scala-2.11"
  ASSEMBLY_DIR1="${SPARK_HOME}/assembly/target/scala-2.10"

  if [[ -d "$ASSEMBLY_DIR2" && -d "$ASSEMBLY_DIR1" ]]; then
    echo -e "Presence of build for both scala versions(SCALA 2.10 and SCALA 2.11) detected." 1>&2
    echo -e 'Either clean one of them or, export SPARK_SCALA_VERSION=2.11 in spark-env.sh.' 1>&2
    exit 1
  fi

  if [ -d "$ASSEMBLY_DIR2" ]; then
    export SPARK_SCALA_VERSION="2.11"
  else
    export SPARK_SCALA_VERSION="2.10"
  fi
fi

if [ -d "${SPARK_HOME}/jars" ]; then
  SPARK_JARS_DIR="${SPARK_HOME}/jars"
else
  SPARK_JARS_DIR="${SPARK_HOME}/assembly/target/scala-$SPARK_SCALA_VERSION/jars"
fi


# NOTE: This exact class name is matched downstream by SparkSubmit.
# Any changes need to be reflected there.
CLASS="org.apache.spark.sql.hive.thriftserver.TiHiveThriftServer2"

function usage {
  echo "Usage: ./sbin/start-thriftserver [options] [thrift server options]"
  pattern="usage"
  pattern+="\|Spark assembly has been built with Hive"
  pattern+="\|NOTE: SPARK_PREPEND_CLASSES is set"
  pattern+="\|Spark Command: "
  pattern+="\|======="
  pattern+="\|--help"

  "${SPARK_HOME}"/bin/spark-submit --help 2>&1 | grep -v Usage 1>&2
  echo
  echo "Thrift server options:"
  "${SPARK_HOME}"/bin/spark-class $CLASS --help 2>&1 | grep -v "$pattern" 1>&2
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  usage
  exit 0
fi

export SUBMIT_USAGE_FUNCTION=usage

exec "${SPARK_HOME}"/sbin/spark-daemon.sh submit $CLASS 1 --name "Thrift JDBC/ODBC Server" "${SPARK_JARS_DIR}"/"${TISPARK_JAR}"
