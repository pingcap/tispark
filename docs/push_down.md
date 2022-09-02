# TiSpark push down


## Overview
TiSpark can push down some operators to TiKV
- predicate
- aggregate
- sort
- limit
- topN

Here is the detail table:

| 数据类型 | sum | count | avg | min | max | predicate & order by & group by |
| ---------- | --- | ----- | --- | --- | --- | ------------------------------- |
| BIT        | ❌ | ✅   | ❌ | ✅ | ✅ | ❌                             |
| BOOLEAN    | ✅ | ✅   | ✅ | ✅ | ✅ | ✅                             |
| TINYINT    | ✅ | ✅   | ✅ | ✅ | ✅ | ✅                             |
| SMALLINT   | ✅ | ✅   | ✅ | ✅ | ✅ | ✅                             |
| MEDIUMINT  | ✅ | ✅   | ✅ | ✅ | ✅ | ✅                             |
| INTEGER    | ✅ | ✅   | ✅ | ✅ | ✅ | ✅                             |
| BIGINT     | ✅ | ✅   | ✅ | ✅ | ✅ | ✅                             |
| FLOAT      | ❌ | ✅   | ✅ | ✅ | ✅ | ✅                             |
| DOUBLE     | ✅ | ✅   | ✅ | ✅ | ✅ | ✅                             |
| DECIMAL    | ✅ | ✅   | ✅ | ✅ | ✅ | ✅                             |
| DATE       | ❌ | ✅   | ❌ | ✅ | ✅ | ✅                             |
| DATETIME   | ❌ | ✅   | ❌ | ✅ | ✅ | ✅                             |
| TIMESTAMP  | ❌ | ✅   | ❌ | ✅ | ✅ | ✅                             |
| TIME       | ❌ | ✅   | ❌ | ？ | ？ | ✅                             |
| YEAR       | ✅ | ✅   | ✅ | ✅ | ✅ | ✅                             |
| CHAR       | ❌ | ✅   | ❌ | ✅ | ✅ | ✅                             |
| VARCHAR    | ❌ | ✅   | ❌ | ✅ | ✅ | ✅                             |
| TINYTEXT   | ❌ | ✅   | ❌ | ✅ | ✅ | ✅                             |
| TEXT       | ❌ | ✅   | ❌ | ✅ | ✅ | ✅                             |
| MEDIUMTEXT | ❌ | ✅   | ❌ | ✅ | ✅ | ✅                             |
| LONGTEXT   | ❌ | ✅   | ❌ | ✅ | ✅ | ✅                             |
| BINARY     | ❌ | ✅   | ❌ | ✅ | ✅ | ✅                             |
| VARBINARY  | ❌ | ✅   | ❌ | ✅ | ✅ | ✅                             |
| TINYBLOB   | ❌ | ✅   | ❌ | ✅ | ✅ | ❌                             |
| BLOB       | ❌ | ✅   | ❌ | ✅ | ✅ | ❌                             |
| MEDIUMBLOB | ❌ | ✅   | ❌ | ✅ | ✅ | ❌                             |
| LONGBLOB   | ❌ | ✅   | ❌ | ✅ | ✅ | ❌                             |
| ENUM       | ❌ | ✅   | ❌ | ✅ | ✅ | ❌                             |
| SET        | ❌ | ❌   | ❌ | ？ | ？ | ❌                             |


- min/max(time) will get wrong data now
- min/max(set) may cause TiKV panic now

## Aggregate

Aggregate push down is supported After TiSpark 2.5.0(include)

Here are some limits:
- Sum(float) can't be pushed down because Spark will cast float to double, but `cast` can't be pushed down in TiSpark.
- Count(set) can't be pushed down because TiKV does not support set type.
- Count(col1,col2,...) can't be pushed down because TiDB/TiKV does not support Count(col1,col2,...).
- Avg can be pushed down when both the sum and count can be pushed down.