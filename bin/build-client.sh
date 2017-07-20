#!/bin/bash

BASEDIR=$(dirname "$0")/../

git submodule update --init --recursive --remote --merge

cd ${BASEDIR}/tikv-client-lib-java
mvn clean install

