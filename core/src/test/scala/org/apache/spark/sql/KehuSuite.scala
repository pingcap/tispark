/*
 * Copyright 2017 PingCAP, Inc.
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

class KehuSuite extends BaseTiSparkTest {
  /*
  use test;

  create table ttt (
`id` bigint(20) NOT NULL AUTO_INCREMENT,
`collection_code` int(4) ,
PRIMARY KEY (`id`),
KEY `idx_collection_code` (`collection_code`)
);

insert into ttt values(1, 1);
insert into ttt values(2, 1);
insert into ttt values(3, 2);
insert into ttt values(4, 3);
   */

  test("kehu") {
    spark.sql("use tidb_test")
    spark
      .sql("SELECT collection_code,count(1) as a from ttt group by collection_code")
      .explain(true)
    spark.sql("SELECT collection_code,count(1) as a from ttt group by collection_code").show(false)
  }

  test("kehu2") {
    spark.sql("use tidb_test")
    spark
      .sql("SELECT id, collection_code,count(1) as a from ttt group by collection_code, id")
      .explain(true)
    spark
      .sql("SELECT id, collection_code,count(1) as a from ttt group by collection_code, id")
      .show(false)
  }
}
