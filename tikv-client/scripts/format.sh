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

if hash google-java-format 2>/dev/null; then
	files=$(find src -name "*.java")
	for file in "${files[@]}";do
		echo "formatting..."
		#google-java-format -r ("${file}")
		google-java-format -r ${file}
	done
else
	echo "google-java-format is not installed, please install it first"
	echo "If you are using macos, type 'brew install google-java-format'"
	echo "Otherwise, go to google-java-format github page:
	https://github.com/google/google-java-format.git
   	and download it."
fi
