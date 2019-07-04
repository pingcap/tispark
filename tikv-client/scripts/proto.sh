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

kvproto_hash=762fbf48a7e77a60d0eb623629842e9b06979743

raft_rs_hash=3799cfa482e288a0fc4a3a6c365c6681afd85b3a

tipb_hash=a2a2b2ab2ee4b8cd4349418ac1c1cde289798485

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