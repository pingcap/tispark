# TiSpark Design Documents

- Author(s): [qidi1](https://github.com/qidi1)
- Tracking Issue: https://github.com/pingcap/tispark/issues/2385

## Table of Contents

- [Introduction](#introduction)
- [Background](#background)
- [Detailed Design](#detailed-design)
- [Test Design](#test-design)

## Introduction

The physical plan is the physical execution plan in TiSpark. When we use to explain in Spark which runs with TiSpark, the process of the physical plan will be displayed in the terminal. But now this display about how the plan executes has some problems.

- appear a filter that should not appear.
- unclear representation of the execution process.
- expression(s) that constitutes the scanning range is hard to know.
- confusing naming of the operator.

The display of the physical plan needs to be improved.

## Background

### Now the way to build explaination

If we call Spark's `explain`, we may see the following output.

```text
== Physical Plan ==
*(1) ColumnarToRow
// many other node.
+- ...
	+- TiKV CoprocessorRDD{[table: t1] TableScan, Columns: a@UNSIGNED LONG, Residual Filter: [a@UNSIGNED LONG GREATER_THAN 1], PushDown Filter: [a@UNSIGNED LONG GREATER_THAN 1], KeyRange: [([t\200\000\000\000\000\000\004W_r\000\000\000\000\000\000\000\000], [t\200\000\000\000\000\000\004W_s\000\000\000\000\000\000\000\000])], Aggregates: , startTs: 433780573880188929}
// many other node.
+- ...
```

What we are interested in here is the content in `CoprocessorRDD`. This node is the output of TiSpark, other nodes are the output of Spark. The content in the `CoprocessRDD` is the output of the `toStringInternal` method called by `TiDAGRequest`.

First of all, let's introduce a few member variables in `TiDAGRequest`.

* `indexInfo` is the index to be scanned by the physical plan, if it is empty, it means scanning the table.
* `isDoubleRead` is `true` means scanning table after scanning the index, `false` means only scanning the index or scanning the table.
* `filters` is the selection expression in the physical plan in normal execution.
* `downgradeFilters` is the selection condition of the physical plan after the downgrade is triggered (what is downgrade will be described later).
* `pushdownFilters` is only used in `toStringInternal`. When `indexInfo==null` or `isDoubleRead==false` or all the columns involved in the filter are in the index, the content of `pushDownFilters` is equal to filters, otherwise, `pushDownFilters` is empty.
* `keyRange` is the scan range of the table or index.

The main code of `toStringInternal` is shown below.

```java
private String toStringInternal(){
	StringBuilder sb=new StringBuilder();
    // init some data in TiDAGRequest.
	init();
    switch (getIndexScanType()) {
      case INDEX_SCAN:
        sb.append("IndexScan");
        isIndexScan=true;
        break;
      case COVERING_INDEX_SCAN:
        sb.append("CoveringIndexScan");
        break;
      case TABLE_SCAN:
        sb.append("TableScan");
    }
    if (isIndexScan && !getDowngradeFilters().isEmpty()) {
       sb.append(", Downgrade Filter: ");
       Joiner.on(", ").skipNulls().appendTo(sb, request.getDowngradeFilters());
    }
    if (!isIndexScan && !getFilters().isEmpty()) {
        sb.append(", Residual Filter: ");
        Joiner.on(", ").skipNulls().appendTo(sb, request.getFilters());
    }
    if (!getPushDownFilters().isEmpty()) {
        sb.append(", PushDown Filter: ");
        Joiner.on(", ").skipNulls().appendTo(sb, request.getPushDownFilters());
    }
    // add keyRange and other push down expression to sb.
    ...
    return sb.toString();
}
```

From the code we can see that the execution plan is divided into three kinds of `IndexScan`, `CoveringIndexScan`, and `TableScan`; `TiDAGRequest` has three kinds `Filter` —— `Downgrade Filter`, ` Resdiual Filter`, `PushDown Filter`. There is zero or more `Filter` for an execution plan. We will describe the meaning of each execution plan and `Filter` in detail below.

### Meaning of execution plan

#### `IndexScan`

In TiSpark, an `IndexScan` requires two scans. The first scan is scanning in index data and the second scan is scanning in table data to get the data we need from the results returned in the first scan.

#### `CoveringIndexScan`

`CoveringIndexScan` is a special case of `IndexScan`. If the column(s) in `Filter` and the column(s) in `Projection` are both inside the Index, then we only need to scan the index once, no need to scan for the table. That is, it only scans for the index, such a scan is called `CoveringIndexScan`.

#### `TableScan`

`TableScan` is different from `IndexScan` and `CoveringIndexScan`. `TableScan` only scans table data.

### Meaning of `Filter`

#### `Downgrade Filter`

If the second scan in `IndexScan` does too many queries on COP([TiKV Coprocessor](https://docs.pingcap.com/tidb/stable/tikv-overview#tikv-coprocessor))/TiKV, it can cause performance problems in COP/TiKV. To solve this problem a downgrading mechanism is introduced. `Downgrade Filter` is used after `IndexScan` downgrade.

The first scan of `IndexScan` will return the data that meets the conditions, and then TiSpark will sort and aggregate the returned data to obtain the `regionTask` that needs to be done in the `IndexScan` second scan. If the number of `regionTask` is bigger than `downgradeThreshold`, a downgrade will be triggered. When a downgrade is triggered, the range of the second scan table will be changed from the returned value of the first scan index to all values in the middle of the minimum to maximum value of the first scan index, and the `filters` of the second scan will change to `downgradeFilters`(`downgradeFilters` is the same as if the execution plan is `TableScan`'s `filters`).

> **`RegionTask`**
>
> For all returned data, all consecutive data in a region will be treated as a `regionTask`.
>
> For example like this the data returned in the first stage are 1, 3, 4, 5 and 1, 3, 4 are in the same region and 5 is in another region. Since 1 and 3, 4 are not contiguous, 1 is a `regionTask`, and since 3, 4 and 5 are not in a region, 3, 4 is a `regionTask` and 5 is a `regionTask`. The `regionTask` number is three.

#### `Residual Filter`

In the original design, the `Residual Filter` represents operators that cannot be pushed down to COP/TiKV. However, in the current implementation, before the construction of `TiDAGRequest`, it will judge whether the operators can be pushed down, and only the operators that can be pushed down will participate in the construction of `TiDAGRequest`. This means that all operators in `TiDAGRequest` can be pushed down to COP/TiKV. The `Residual Filter` loses its original meaning because there are no operators in `TiDAGRequest` that cannot be pushed down.

#### `PushDown Filter`

The expression passed to COP/TiKV as a selection expression without triggering a downgrade.

### The Problem of DAG Explain

1. appear a filter that should not appear

   As stated before, the `Residual Filter` loses its meaning and should not be present in the physical plan explanation. But `Residual Filter` still appears in `TableScan` and `CoveringIndexScan`.

   ```sql
   CREATE TABLE `t1` (
     `a` BIGINT(20) UNSIGNED  NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
   ```

   ```sql
   SELECT * FROM t2 where a>1
   ```

   ```Text
   == Physical Plan ==
   *(1) ColumnarToRow
   +- TiKV CoprocessorRDD{[table: t1] TableScan, Columns: a@UNSIGNED LONG, Residual Filter: [a@UNSIGNED LONG GREATER_THAN 1], PushDown Filter: [a@UNSIGNED LONG GREATER_THAN 1], KeyRange: [([t\200\000\000\000\000\000\004W_r\000\000\000\000\000\000\000\000], [t\200\000\000\000\000\000\004W_s\000\000\000\000\000\000\000\000])], Aggregates: , startTs: 433780573880188929}
   ```

2. unclear representation of the execution process

   1. unable to know the execution steps

      As stated before, the execution process for `IndexScan` is divided into two phases, the first phase for index data scanning, and the second phase for table data scanning. However, we cannot see this execution process in the physical plan explanation.

   2. the displayed `Filter` is not the one that would be executed under normal execution

      The `Downgrade Filter` in `RegionTaskExec` is designed to indicate that the `Downgrade Filter` will be used after the downgrade(There is some problem with the content displayed in `RegionTaskExec`, it should show the content of `downgradeFilters` in `TiDAGRequest`, but it displays the content of `filters` in `TiDAGRequest`. We will fix this bug here.). However, the `Downgrade Filter` is displayed again in the `FetchHandleRDD` and the `PushDown Filter`, which is executed under normal circumstances, is not displayed.

      > **`RegionTaskExec`**
      >
      > `RegionTaskExec` is the node that determines whether to downgrade.
      >
      >
      >**`FetchHandleRDD`**
      >
      >When the `isDouble` variable of `TiDAGRequest` is true, the `CoprocessorRDD` is called `FetchHandleRDD`.

   3. information about the used index is hard to know

      In the physical plan explanation, only the name of the index we use is shown, not the columns that make up the index.

   ```SQL
   CREATE TABLE `t2` (
     `a` BIGINT(20) UNSIGNED  NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL,
     PRIMARY KEY (`a`,`b`)
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   ```

   ```SQL
   SELECT * FROM t2 where a<1 or b>'bb'"
   ```

   ```text
   == Physical Plan ==
   *(1) ColumnarToRow
   +- TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[[[a@UNSIGNED LONG LESS_THAN 1] OR [b@VARCHAR(255) GREATER_THAN "bb"]]]
      +- RowToColumnar
         +- TiKV FetchHandleRDD{[table: t2] IndexScan[Index: primary] , Columns: a@UNSIGNED LONG, b@VARCHAR(255), c@VARCHAR(255), Downgrade Filter: [[a@UNSIGNED LONG LESS_THAN 1] OR [b@VARCHAR(255) GREATER_THAN "bb"]], PushDown Filter: [[a@UNSIGNED LONG LESS_THAN 1] OR [b@VARCHAR(255) GREATER_THAN "bb"]], KeyRange: [([t\200\000\000\000\000\000\003\374_i\200\000\000\000\000\000\000\001\000], [t\200\000\000\000\000\000\003\374_i\200\000\000\000\000\000\000\001\372])], Aggregates: , startTs: 433735624236728323}
   ```

3. expression(s) that constitutes the scanning range is hard to know

   The expression(s) for building ranges are not shown explicitly, which makes it difficult to know which expression(s) are used to build range. As shown below, for the query expression `a>0` is pushed down, but the pushed-down information is only implicitly given inside the `KeyRange`, which is not convenient for users to understand.

   ```sql
   CREATE TABLE `t3` (
     `a` BIGINT(20) NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL,
     PRIMARY KEY (`a`) clustered
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   ```

   ```sql
   SELECT a FROM t3 where a>0
   ```

   ```text
   == Physical Plan ==
   *(1) ColumnarToRow
   +- TiKV CoprocessorRDD{[table: t3] TableScan, Columns: a@LONG, KeyRange: [([t\200\000\000\000\000\000\005/_r\200\000\000\000\000\000\000\001], [t\200\000\000\000\000\000\005/_s\000\000\000\000\000\000\000\000])], startTs: 434063568626778113}
   ```

4. confusing naming of the operator

   In TiDB, a process named `Scan` means scanning a piece of data, not aggregating and computing the result of the scan. But in TiSpark, a process named scan means aggregating and computing the results of the scan. This may confuse users who switch from TiDB to TiSpark.

   The selection expression pushed down to TiKV/COP in TiDB is called `Selection`, while the selection expression is called `Pushdown Filter` in TiSpark. This may also confuse users who switch from TiDB to TiSpark.

## Detailed Design

### appear a filter that should not appear

To solve the problem of appearing a filter that shouldn't appear, we removed the `Residual Filter`.

### unclear representation of the execution process

1. unable to know the execution steps

   To solve the problem of the execution process for `IndexScan` is divided into two phases is not show, we divide the scan of `IndexScan` into two parts `IndexRangeScan`, `TableRowIDScan`. The scan of `TableScan` is named `TableRangeScan`. The scan of `CoveringIndexScan` is named `IndexRangeScan`. The meanings of these scans are shown below.

   * **`TableRangeScan`**: Table scans with the specified range. We consider full table scan as a special case of `TableRangeScan`, so full table scan is also called `TableRangeScan`.
   * **`TableRowIDScan`**: Scans the table data based on the `RowID`. Usually follows an index read operation to retrieve the matching data rows.
   * **`IndexRangeScan`**: Index scans with the specified range. We consider full index scan as a special case of `IndexRangeScan`, so full index scan is also called `IndexRangeScan`.

2. the displayed `Filter` is not the one that would be executed under normal execution

   To solve the problem of the displayed `Filter` is not the one that would be executed under normal execution, we delete the `Downgrade Filter` and add the `Selection` (`Selection` will be introduced later) in `FetchHandleRDD`.

3. information about the used index is hard to know

   To solve the problem of information about the used index is hard to know, we add the columns that make up the index after the index name.

### expression(s) that constitutes the scanning range is hard to know

To solve the problem of expression(s) that constitutes the scanning range is hard to know, we added a `RangeFilter` to the `KeyRange` to indicate the expression(s) used to construct the scan range.

- **`RangeFilter`**: `RangeFilter` indicates which expression(s) the range is made up of. If `RangeFilter` is empty, it indicates a full table scan or full index scan. `RangeFilter` generally appears when the query involves an index range, when query the expressions in the `RangeFilter` form the scanned range from left to right.

### confusing naming of the operator

To solve the problem of confusing the naming of the operator, we change the operator to the same name as TiDB. `IndexScan` has changed to `IndexLookUp`; `CoveringIndexScan` has changed to `IndexReader`; `TableScan` has changed to `TableReader`. The `PushDown Filter` has changed to `Selection`.

- **`TableReader`**: Aggregates the data obtained by the underlying operator `TableRangeScan` in TiKV.
- **`IndexReader`**: Aggregates the data obtained by the underlying operator `IndexRangeScan` in TiKV.
- **`IndexLookUp`**: First aggregates the RowIDs (in TiKV) scanned by the first scan in the index. Then at the second scan in the table, accurately reads the data from TiKV based on these RowIDs. At the first scan in the index, there is  `IndexRangeScan` operator; at the second scan in the table, there is the `TableRowIDScan` operator.
- **`Selection`**: The expression passed to COP/TiKV as selection expression without triggering a downgrade.

### Code Design

![image-20220628112756535](./imgs/phyical_plan_explain/code_flow_chart.png)

## Test Design

1. Table without cluster index

   ```SQL
   CREATE TABLE `t1` (
     `a` BIGINT(20) NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   ```

   - `TableScan` with selection and without `RangeFilter`

     ```sql
     SELECT * FROM t1 where a>0 and b > 'aa'
     ```

2. Table with cluster index

   TiDB version smaller than 5.0

   ```sql
   CREATE TABLE `t1` (
     `a` BIGINT(20) NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL,
     PRIMARY KEY (`a`) 
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   ```

   TiDB version bigger than 5.0

   ```sql
   CREATE TABLE `t1` (
     `a` BIGINT(20) NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL,
     PRIMARY KEY (`a`) clustered
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   ```

   - `TableScan` with selection and with `RangeFilter`

     ```sql
     SELECT * FROM t1 where a>0 and b > 'aa'
     ```

   - `TableScan` without selection and with `RangeFilter`

     ```sql
     SELECT * FROM t1 where a>0
     ```

3. Table with cluster index and partition

   ```sql
   CREATE TABLE `t1` (
     `a` BIGINT(20) NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL,
     PRIMARY KEY (a)
   )PARTITION BY RANGE (a) (
       PARTITION p0 VALUES LESS THAN (6),
       PARTITION p1 VALUES LESS THAN (11),
       PARTITION p2 VALUES LESS THAN (16),
       PARTITION p3 VALUES LESS THAN MAXVALUE
     )
   ```

   - `TableScan` with Selection and with `RangeFilter` with partition

     ```sql
     SELECT a,b FROM t1 where a>0 and b>'aa'
     ```

4. Table with secondary index

   ```sql
   CREATE TABLE `t1` (
   `a` BIGINT(20)  NOT NULL,
   `b` varchar(255) NOT NULL,
   `c` varchar(255) DEFAULT NULL,
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   CREATE INDEX `testIndex` ON `t1` (`a`,`b`);
   ```

   - `IndexScan` with Selection and with `RangeFilter`

     ```sql
     SELECT * FROM t1 where a>0 and b > 'aa'
     ```

   - `IndexScan` without Selection and with `RangeFilter`

     ```sql
     SELECT * FROM t1 where a=0 and b > 'aa'
     ```

   - `CoveringIndex` with Selection and with `RangeFilter`

     ```sql
     SELECT a,b FROM t1 where a>0 and b > 'aa'
     ```

5. Table with secondary prefix index

   ```sql
   CREATE TABLE `t1` (
   `a` BIGINT(20)   NOT NULL,
   `b` varchar(255) NOT NULL,
   `c` varchar(255) DEFAULT NULL,
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   CREATE INDEX `testIndex` ON `t1` (`b`(4),a);
   ```

   - `IndexScan` with `RangeFilter` and with `Selection`

     ```sql
     SELECT * FROM t1 where a>0 and b > 'aa'
     ```

   - `IndexScan` with `RangeFilter` and without `Selection`

     ```sql
     SELECT * FROM t1 where b > 'aa'
     ```

   - `CoveringIndexScan` with `RangeFilter` and with `Selection`

     ```sql
     SELECT * FROM t1 where a>0 and b > 'aa'
     ```

   - `CoveringIndexScan` with `RangeFilter` and without `Selection`

     ```sql
     SELECT * FROM t1 where b > 'aa'
     ```

6. Table with secondary index and partition

   ```sql
   CREATE TABLE `t1` (
     `a` BIGINT(20) NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL,
   )PARTITION BY RANGE (a) (
       PARTITION p0 VALUES LESS THAN (6),
       PARTITION p1 VALUES LESS THAN (11),
       PARTITION p2 VALUES LESS THAN (16),
       PARTITION p3 VALUES LESS THAN MAXVALUE
     )
   CREATE INDEX `testIndex` ON `t1` (`a`,`b`);
   ```

   - `IndexScan` with `Selectio`n and with `RangeFilter` with partition

     ```sql
     SELECT * FROM t1 where a>0 and b > 'aa'
     ```

   - `CoveringIndexScan` with `Selection` and with `RangeFilter` with partition

     ```sql
     SELECT a,b FROM t1 where a>0 and b > 'aa'
     ```

   - `CoveringIndexScan` with complicated sql

     ```sql
     SELECT sum(a) FROM t1 where a > 0 or b < 'bb' group by a order by(a)
     ```

7. Table with secondary prefix index and partition

   ```sql
   CREATE TABLE `t1` (
     `a` BIGINT(20) NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL,
      PRIMARY KEY (a)
   )PARTITION BY RANGE (a) (
       PARTITION p0 VALUES LESS THAN (6),
       PARTITION p1 VALUES LESS THAN (11),
       PARTITION p2 VALUES LESS THAN (16),
       PARTITION p3 VALUES LESS THAN MAXVALUE
     )
   CREATE INDEX `testIndex` ON `t1` (`b`(4));
   ```

   - `IndexScan` with `Selection` and with `RangeFilter` with partition

     ```sql
     SELECT * FROM t1 where a>0 and b > 'aa'
     ```

   - `IndexScan` with complicated sql

     ```sql
     SELECT b FROM t1 where b > 'aa' group by b order by(b) limit(10) 
     ```

   - `TableScan` with complicated sql

     ```sql
     SELECT max(c) FROM t1 where c > 'cc' or c < 'bb' group by c order by(c)
     ```
