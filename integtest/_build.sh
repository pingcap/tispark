#!/usr/bin/env bash
set -ue

source _env.sh

cd ../tikv-client/
mvn clean install
cd ..
mvn clean install
cd integtest
mvn clean install