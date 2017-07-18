#!/bin/bash

BASEDIR=$(dirname "$0")/../

git submodule update --init --recursive

cd ${BASEDIR}/tikv-client-lib-java
mvn clean install

