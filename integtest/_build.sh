#!/usr/bin/env bash
set -ue

source _env.sh

cd ..
mvn clean install
cd integtest