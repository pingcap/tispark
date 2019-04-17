#!/bin/sh
#
#   Copyright 2017 PingCAP, Inc.
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


TiSparkReleaseVersion=2.1-SNAPSHOT
TiSparkBuildTS=`date -u '+%Y-%m-%d %I:%M:%S'`
TiSparkGitHash=`git rev-parse HEAD`
TiSparkGitBranch=`git rev-parse --abbrev-ref HEAD`
echo '
package com.pingcap.tispark

object TiSparkVersion { val version: String = "Release Version: '${TiSparkReleaseVersion}'\\nSupported Spark Version: " + System.getProperty("sparkVersion", "spark-2.3") + "\\nGit Commit Hash: '${TiSparkGitHash}'\\nGit Branch: '${TiSparkGitBranch}'\\nUTC Build Time: '${TiSparkBuildTS}'" }' > src/main/scala/com/pingcap/tispark/TiSparkVersion.scala
