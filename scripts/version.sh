#!/bin/sh

SHA1=`git describe --tags`
echo '
package com.pingcap.tispark

import com.pingcap.tikv.TiVersion

object TiSparkVersion { val version: String = "'${SHA1}'-Client:" + TiVersion.CommitVersion }' > src/main/scala/com/pingcap/tispark/TiSparkVersion.scala
