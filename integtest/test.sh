#!/usr/bin/env bash

CLASS="com.pingcap.spark.TestFramework"

"${SPARK_HOME}"/bin/spark-submit --driver-java-options -Dtest.mode=Test --class $CLASS ./lib/*