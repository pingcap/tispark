#
# Copyright 2017 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.
#
#

# Title     : TiSparkR
# Objective : TiSpark entry for R
# Created by: novemser
# Created on: 17-11-1

# Function:createTiContext
# Create a new TiContext via the spark session passed in
#
# @return A new TiContext created on session
# @param session A Spark Session for TiContext creation
createTiContext <- function(session) {
  sparkR.newJObject("org.apache.spark.sql.TiContext", session)
}

# Function:tidbMapDatabase
# Mapping TiContext designated database to `dbName`.
#
# @param tiContext TiSpark context
# @param dbName Database name to map
# @param isPrefix Whether to use dbName As Prefix
# @param loadStatistics Whether to use statistics information from TiDB
tidbMapDatabase <- function(tiContext, dbName, isPrefix=FALSE, loadStatistics=TRUE) {
  sparkR.callJMethod(tiContext, "tidbMapDatabase", dbName, isPrefix, loadStatistics)
  paste("Mapping to database:", dbName)
}
