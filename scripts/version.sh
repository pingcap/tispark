#!/bin/sh

SHA1=`git describe --tags`
echo '
package com.pingcap.tispark

import com.pingcap.tikv.TiVersion

object TiSparkVersion { val version: String = "'${SHA1}'-CLI-" + TiVersion.CommitVersion }' > src/main/scala/com/pingcap/tispark/TiSparkVersion.scala
