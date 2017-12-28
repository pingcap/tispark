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

SHA1=`git describe --tags`
echo '
package com.pingcap.tikv;
public final class TiVersion { public final static String CommitVersion = "'${SHA1}'"; }' > src/main/java/com/pingcap/tikv/TiVersion.java
