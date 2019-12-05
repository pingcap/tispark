/*
 * Copyright 2019 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

class MultipleSparkSessionTestSuite extends BaseTiSparkTest {
  test("Test multiple Spark Session register udf") {
    val sparkSession1 = spark
    assert(sparkSession1.sql("select ti_version()").count() === 1)
    val sparkSession2 = sparkSession1.newSession()
    assert(sparkSession2.sql("select ti_version()").count() === 1)
  }

  test("Test multiple Spark Session with different catalog") {
    val sparkSession1 = spark
    val sparkSession2 = sparkSession1.newSession()
    sparkSession1.sql(s"use ${dbPrefix}tispark_test")
    sparkSession2.sql(s"use ${dbPrefix}prefix_index")
    sparkSession1.sql("show tables").show()
    sparkSession2.sql("show tables").show()
  }
}
