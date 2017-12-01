#!/bin/sh

SHA1=`git describe --tags`
echo '
package com.pingcap.tispark

object TiSparkVersion { val CommitVersion: String = "'${SHA1}'" }' > src/main/scala/com/pingcap/tispark/TiSparkVersion.scala
