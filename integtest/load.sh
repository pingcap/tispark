#!/usr/bin/env bash

CLASS="com.pingcap.spark.TestFramework"

java -Dtest.mode=Load -cp ./conf:./lib/* $CLASS 