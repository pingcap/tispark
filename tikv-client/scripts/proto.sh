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

CURRENT_DIR=`pwd`
TISPARK_HOME="$(cd "`dirname "$0"`"/../..; pwd)"
cd $TISPARK_HOME/tikv-client

kvproto_hash=e6d6090277c921c3291c48c5bc8fb38907c852d0

raft_rs_hash=b9891b673573fad77ebcf9bbe0969cf945841926

tipb_hash=45e60c77588fefe421d0f6f29426a36b5b15171d

if [ -d "kvproto" ]; then
	cd kvproto; git fetch -p; git checkout ${kvproto_hash}; cd ..
else
	git clone https://github.com/pingcap/kvproto; cd kvproto; git checkout ${kvproto_hash}; cd ..
fi

if [ -d "raft-rs" ]; then
	cd raft-rs; git fetch -p; git checkout ${raft_rs_hash}; cd ..
else
	git clone https://github.com/pingcap/raft-rs; cd raft-rs; git checkout ${raft_rs_hash}; cd ..
fi

if [ -d "tipb" ]; then
	cd tipb; git fetch -p; git checkout ${tipb_hash}; cd ..
else
	git clone https://github.com/pingcap/tipb; cd tipb; git checkout ${tipb_hash}; cd ..
fi

cd $CURRENT_DIR
