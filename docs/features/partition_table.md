# TiSpark partition table

## Read from partition table

TiSpark supports reads the range, hash and list partition table from TiDB.

TiSpark doesn't support a MySQL/TiDB partition table syntax `select col_name from table_name partition(partition_name)`, but you can still use `where` condition to filter the partitions.

## Partition pruning in Reading

TiSpark decides whether to apply partition pruning according to the partition type and the partition expression associated with the table. If partition pruning is not applied, TiSpark's reading is equivalent to doing a table scan over all partitions.

TiSpark only supports partition pruning with the following partition expression in **range** partition:

+ column expression
+ `YEAR(col)` and its type is datetime/string/date literal that can be parsed as datetime.
+ `TO_DAYS(col)` and its type is datetime/string/date literal that can be parsed as datetime.

### Limitations

- TiSpark does not support partition pruning in hash and list partition.
- TiSpark can not apply partition pruning for some special characters in partition definition. For example, Partition definition with `""` can not be pruned. such as `partition p0 values less than ('"string"')`.

## Write into partition table

Currently, TiSpark only supports writing into the range and hash partition table under the following conditions:
+ the partition expression is column expression
+ the partition expression is `YEAR($argument)` where the argument is a column and its type is datetime or string literal
  that can be parsed as datetime.

There are two ways to write into partition table:
1. Use datasource API to write into partition table which supports replace and append semantics.
2. Use delete statement with Spark SQL.

> [!NOTE]
> Because different character sets and collations have different sort orders, the character sets and
> collations in use may affect which partition of a table partitioned by RANGE COLUMNS a given row
> is stored in when using string columns as partitioning columns.
> For supported character sets and collations, see [Limitations](../README.md#limitations)