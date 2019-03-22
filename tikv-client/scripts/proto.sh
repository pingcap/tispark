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

if [ -d "kvproto" ]; then
	cd kvproto; git pull origin master; cd ..
else
	git clone https://github.com/pingcap/kvproto
fi

if [ -d "raft-rs" ]; then
	cd raft-rs; git pull origin master; cd ..
else
	git clone https://github.com/pingcap/raft-rs
fi

if [ -d "tipb" ]; then
	cd tipb; git pull origin master; cd ..
else
	git clone https://github.com/pingcap/tipb
fi
