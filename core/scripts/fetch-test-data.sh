#!/usr/bin/env bash

CURRENT_DIR=`pwd`
TISPARK_HOME="$(cd "`dirname "$0"`"/../..; pwd)"
cd $TISPARK_HOME/core
git submodule update --init --recursive
cd $CURRENT_DIR