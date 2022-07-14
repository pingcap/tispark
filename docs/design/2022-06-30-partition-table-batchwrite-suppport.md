# Support bypass-TiDB write partition table

- Author(s): [yangxin](http://github.com/xuanyu66)
- Tracking Issue: https://github.com/pingcap/tispark/issues/2437

## Introduction

This design doc is about how to support bypass-TiDB write partition table with DataSource API.

The semantics to be supported in this feature are:

- INSERT semantics with Data Source API
- REPLACE semantics with Data Source API
- DELETE semantics with Spark SQL

And the associated supported partition types to be supported in this feature are:

- [Range partitioning](https://docs.pingcap.com/tidb/dev/partitioned-table#range-partitioning)
- [Hash partitioning](https://docs.pingcap.com/tidb/dev/partitioned-table#hash-partitioning)

And the parititon expression must be under contions as bellow:
+ column expression
+ `YEAR` (expression) where the expression is a column and its type is datetime or string literal
that can be parsed as datetime.

## Motivation and background

Partitioning is a way in which a database splits its actual data down into separate tables, but
still gets treated as a single table by the SQL layer.
The main goal is to reduce the amount of data read for particular SQL operations so that overall
response time is reduced while it also can be used to accomplish a number of various objectives.

### The feature-level implementation of partitioning in TiDB

Currently, TiDB supports Range partitioning, List partitioning, List COLUMNS partitioning, and Hash
partitioning.
For a table partitioned by [RANGE COLUMNS](https://dev.mysql.com/doc/refman/5.7/en/partitioning-columns-range.html),
currently TiDB only supports using a single partitioning column.
You can read the [documentation](https://docs.pingcap.com/tidb/dev/partitioned-table#compatibility-with-mysql) for more
details.

### The code-level implementation of partitioning in TiDB

If a table is partitioned,
the [`TableInfo`](https://github.com/pingcap/tidb/blob/v6.1.0/parser/model/model.go#L370-L447) holds
the id of the single table by the SQL layer, which can be called logical tableId,
while the n(n is the number of partitions)
[`PartitionDefinition`](https://github.com/pingcap/tidb/blob/v6.1.0/parser/model/model.go#L1076-L1084)
each holds the id of the table stored in TiKV, which can be called physical tableId.

It's typical that the logical tableId should be used to allocate id or gain meta information, while
the physical tableId should be used to generate encoded key or any scans which we operate on the
physical table.

## Goals

Since TiSpark supports INSERT, REPLACE semantics with DataSource API and DELETE statement with Spark SQL, 
it's natural to support the partitioning feature in these semantics.

List partitioning, List COLUMNS partitioning are generally available in V6.1 recently and also more
complicated. Considering the workload of this feature, we have decide to support them in the future.
In this feature, we only focus on Range partitioning and Hash partitioning.

Currently TiSpark only support YEAR function, so the parititon expression must be under contions as bellow:
+ column expression
+ `YEAR` (expression) where the expression is a column and its type is datetime or string literal
that can be parsed as datetime.

## API changes

There is no API change in this feature. Users use the DataSource API to execute INSERT, REPLACE and
use Spark SQL to DELETE as normal.

## Detailed design

Since TiSpark supports INSERT, REPLACE and DELETE semantics with the physical table, the
responsibility is simplified to transform the `TableInfo` of the partition table to the physical
table so that we can reuse the existing API rather than bring more complexity into the code.

### Generate the expression for partitioning

The original type of expression is `String`, we need to use `TiParser` to parse the expression in
order to evaluate the partitioning expression for the latter steps.
For the `Range` and `Range Column`  partitioning, it also needs to extract the bound description
'partition p0 less than xx ... partition p1 less than ...' and then parse to bound's expression.
```
[ 
   [year(birthday@DATE) LESS_THAN 1995], 
   [[year(birthday@DATE) GREATER_EQUAL 1995] AND [year(birthday@DATE) LESS_THAN 1997]], 
   [[year(birthday@DATE) GREATER_EQUAL 1997] AND 1] 
]
```

### Locate the partition for each row

In this step, we need to get the physical tableId for each row.

For the `Range` partitioning, the expression can be a column or indeed a function.

- If the expression is a column, we can use the column's value to compare with the bound's expression
  to locate the partition.
- If the expression is a function, we can evaluate the function's value and then compare with the
  less than parts to locate the partition

For the `Range Column` partitioning, the expression can only be a column, so we choose the first if
branch above.

For the `Hash` partitioning, the result of the expression must be an integer, it can be a column or
indeed a function. We take the result of the expression and mod it with the number of partitions, then use the
remainder as the array index of the partition.

### Group the rows by partition and invoke write API

Group the rows by partition to generate `TiBatchWriteTable` and then invoke write API to write the
rows in on transactions.

There is one problem that `TiBatchWriteTable` use the `TableInfo` as a physical table which is not
appropriate for the partitioning scenario.
We need to use a new type such as `PhysicalTable` to replace the `TableInfo` in the write-associated
interface.

- For the partition table, the `TableInfo` generates n `PhysicalTable`s, each of which is a physical
  table for a partition.
- For the common table, the `TableInfo` generates a single `PhysicalTable` which is the physical
  table for the whole table.

## Test Design

### Functional test

- insert into Range partition table
- insert into Range Column partition table
- insert into Hash partition table
- delete from Range partition table
- delete from Range Column partition table
- delete from Hash partition table
- replace into Range partition table
- replace into Range Column partition table
- replace into Hash partition table

### Scenario tests

- Range partitioning with YEAR() function
- Range partitioning with unsupported functions
- Range Column partitioning with DATE type
