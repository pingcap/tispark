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

from py4j.java_gateway import java_import
from pyspark.context import SparkContext


class TiContext:
    # create a new TiContext
    def __init__(self, sparkSession):
        SparkContext._ensure_initialized()
        gw = SparkContext._gateway
        java_import(gw.jvm, "org.apache.spark.sql.TiContext")
        self.ti = gw.jvm.TiContext(sparkSession._jsparkSession)

    def getContext(self):
        return self.ti

    def tidbMapDatabase(self, dbName, isPrefix):
        self.ti.tidbMapDatabase(dbName, isPrefix)