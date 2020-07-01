#!/bin/sh
#
#   Copyright 2019 PingCAP, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

set -e

VALID_VERSIONS=( 2.11 2.12 )

usage() {
  echo "Usage: $(basename $0) [-h|--help] <version>
where :
  -h| --help Display this help text
  valid version values : ${VALID_VERSIONS[*]}
" 1>&2
  exit 1
}

if [[ ($# -ne 1) || ( $1 == "--help") ||  $1 == "-h" ]]; then
  usage
fi

TO_VERSION=$1

check_scala_version() {
  for i in ${VALID_VERSIONS[*]}; do [ $i = "$1" ] && return 0; done
  echo "Invalid Scala version: $1. Valid versions: ${VALID_VERSIONS[*]}" 1>&2
  exit 1
}

check_scala_version "$TO_VERSION"

if [ $TO_VERSION = "2.11" ]; then
  FROM_VERSION="2.12"
else
  FROM_VERSION="2.11"
fi

sed_i() {
  sed -e "$1" "$2" > "$2.tmp" && mv "$2.tmp" "$2"
}

export -f sed_i

BASEDIR=$(dirname $0)/..

cd $BASEDIR
mvn clean -DskipTests

find "$BASEDIR" -name 'pom.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(artifactId.*\)_'$FROM_VERSION'/\1_'$TO_VERSION'/g' {}" \;

# Also update <scala.binary.version> in assembly.xml
ASSEMBLY="$BASEDIR/assembly/src/main/assembly/assembly.xml"
echo "$ASSEMBLY"
sed_i 's/\(spark-wrapper-spark-.*\)_'$FROM_VERSION'/\1_'$TO_VERSION'/g' "$ASSEMBLY"
sed_i 's/\(tispark-core-internal.*\)_'$FROM_VERSION'/\1_'$TO_VERSION'/g' "$ASSEMBLY"
sed_i 's/\(tikv-client\)_'$FROM_VERSION'/\1_'$TO_VERSION'/g' "$ASSEMBLY"

# Also update <scala.binary.version> in scalafmt
sed_i 's/\(mvn-scalafmt\)_'$FROM_VERSION'/\1_'$TO_VERSION'/g' "$BASEDIR/dev/scalafmt"