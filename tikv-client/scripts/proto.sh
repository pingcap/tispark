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

kvproto_hash=a0f02f7c718c2e64946f442c9c06980e2fba6e83

raft_rs_hash=4fd768dc24bf9ac370f4ac2f2c3eeafef8214c66

tipb_hash=c0b8f1a8c8395c319049600dc0efd278f1e26a0d

if [ -d "kvproto" ]; then
	cd kvproto; git remote add birdstorm https://github.com/birdstorm/kvproto.git -t refresh-ttl; git fetch -p birdstorm; git checkout ${kvproto_hash}; cd ..
else
	git clone https://github.com/pingcap/kvproto; cd kvproto; git remote add birdstorm https://github.com/birdstorm/kvproto.git -t refresh-ttl; git fetch -p birdstorm; git checkout ${kvproto_hash}; cd ..
fi

if [[ -d "raft-rs" ]]; then
	cd raft-rs; git fetch -p; git checkout ${raft_rs_hash}; cd ..
else
	git clone https://github.com/pingcap/raft-rs; cd raft-rs; git checkout ${raft_rs_hash}; cd ..
fi

if [[ -d "tipb" ]]; then
	cd tipb; git fetch -p; git checkout ${tipb_hash}; cd ..
else
	git clone https://github.com/pingcap/tipb; cd tipb; git checkout ${tipb_hash}; cd ..
fi
