# TiSpark Design Documents

- Author(s): [qidi1](https://github.com/qidi1)
- Tracking Issue: https://github.com/pingcap/tispark/issues/2385

## Table of Contents

- [Introduction](#introduction)
- [Background](#background)
- [Detailed Design](#detailed-design)
- [Test Design](#test-design)

## Introduction

The physical plan is the physical execution plan in TiSpark. When we use explain in Spark which runs with TiSpark, the process of the physical plan will be displayed in the terminal. But now this display about how the plan executes has some problems.

- `Residual Filter`  should not appear in physical plan explanation.
- unclear representation of the execution process.
- expression(s) that constitutes the scanning range is hard to know.
- confusing naming of the operator.

The display of the physical plan needs to be improved.

## Background

### The current way to build explanations

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

The main code is in [`toStringInternal`](https://github.com/pingcap/tispark/blob/v3.0.1/tikv-client/src/main/java/com/pingcap/tikv/meta/TiDAGRequest.java#L972-L1012).

From the code we can see that the execution plan is divided into three kinds of `IndexScan`, `CoveringIndexScan`, and `TableScan`; `TiDAGRequest` has three kinds `Filter` —— `Downgrade Filter`, ` Resdiual Filter`, `PushDown Filter`. There is zero or more `Filter` for an execution plan. We will describe the meaning of each execution plan and `Filter` in detail below.

### Meaning of execution plan

#### `IndexScan`

In TiSpark, an `IndexScan` requires two steps. The first step is scanning in index data and the second step is scanning in table data to get the data we need from the results returned in the first scan.

#### `CoveringIndexScan`

`CoveringIndexScan` is a special case of `IndexScan`. If the column(s) in `Filter` and the column(s) in `Projection` are both inside the Index, then we only need to scan the index once, no need to scan for the table. That is, it only scans for the index, such a scan is called `CoveringIndexScan`.

#### `TableScan`

`TableScan` is different from `IndexScan` and `CoveringIndexScan`. `TableScan` only scans table data.

### Meaning of `Filter`

#### `Downgrade Filter`

If the second scan in `IndexScan` does too many queries on COP([TiKV Coprocessor](https://docs.pingcap.com/tidb/stable/tikv-overview#tikv-coprocessor))/TiKV, it can cause performance problems in COP/TiKV. To solve this problem a downgrading mechanism is introduced. `Downgrade Filter` is used after `IndexScan` downgrade.

The first scan of `IndexScan` will return the data that meets the conditions, and then TiSpark will sort and aggregate the returned data to obtain the `regionTask` that needs to be done in the `IndexScan` second scan. If the number of `regionTask` is bigger than `downgradeThreshold`, a downgrade will be triggered. When a downgrade is triggered, the range of the second scan table will be changed from the returned value of the first scan index to all values between the minimum and maximum value of the first scan index, and the `filters` of the second scan will change to `downgradeFilters`(`downgradeFilters` is the same as if the execution plan is `TableScan`'s `filters`).

> **`RegionTask`**
>
> For all returned data, all consecutive data in a region will be treated as a `regionTask`.
>
> For example like this the data returned in the first stage are 1, 3, 4, 5 and 1, 3, 4 are in the same region and 5 is in another region. Since 1 and 3, 4 are not contiguous, 1 is a `regionTask`, and since 3, 4 and 5 are not in a region, 3, 4 is a `regionTask` and 5 is a `regionTask`. The number of `regionTask`  is three.

#### `Residual Filter`

In the original design, the `Residual Filter` represents operators that cannot be pushed down to COP/TiKV. However, in the current implementation, before the construction of `TiDAGRequest`, it will judge whether the operators can be pushed down, and only the operators that can be pushed down will participate in the construction of `TiDAGRequest`. This means that all operators in `TiDAGRequest` can be pushed down to COP/TiKV. The `Residual Filter` loses its original meaning because there are no operators in `TiDAGRequest` that cannot be pushed down.

#### `PushDown Filter`

The expression passed to COP/TiKV as a selection expression without triggering a downgrade.

### The Problem of DAG Explain

1. `Residual Filter`  should not appear in physical plan explanation.

   As stated before, the `Residual Filter` loses its meaning and should not be present in the physical plan explanation. But `Residual Filter` still appears in `TableScan` and `CoveringIndexScan`.

   ```sql
   CREATE TABLE `t1` (
     `a` BIGINT(20) NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   ```

   ```sql
   select * from t1 where a>0 and b>'aa'
   ```

   ```Text
   == Physical Plan ==
   *(1) ColumnarToRow
   +- TiKV CoprocessorRDD{[table: t1] TableScan, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255), Residual Filter: [a@LONG GREATER_THAN 0], [b@VARCHAR(255) GREATER_THAN "aa"], PushDown Filter: [a@LONG GREATER_THAN 0], [b@VARCHAR(255) GREATER_THAN "aa"], KeyRange: [([t\200\000\000\000\000\000\rE_r\000\000\000\000\000\000\000\000], [t\200\000\000\000\000\000\rE_s\000\000\000\000\000\000\000\000])], startTs: 434247263670239233}
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
   CREATE TABLE `t1` (
   `a` BIGINT(20)  NOT NULL,
   `b` varchar(255) NOT NULL,
   `c` varchar(255) DEFAULT NULL
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   CREATE INDEX `testIndex` ON `t1` (`a`,`b`)
   ```

   ```SQL
   SELECT * FROM t1 where a>0 and b > 'aa'
   ```

   ```text
   == Physical Plan ==
   *(1) ColumnarToRow
   +- TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[[b@VARCHAR(255) GREATER_THAN "aa"]]
      +- RowToColumnar
         +- TiKV FetchHandleRDD{[table: t1] IndexScan[Index: testindex] , Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255), Downgrade Filter: [b@VARCHAR(255) GREATER_THAN "aa"], [a@LONG GREATER_THAN 0], KeyRange: [([t\200\000\000\000\000\000\rA_i\200\000\000\000\000\000\000\001\003\200\000\000\000\000\000\000\001], [t\200\000\000\000\000\000\rA_i\200\000\000\000\000\000\000\001\372])], startTs: 434247241322725377}
   ```

3. expression(s) that constitutes the scanning range is hard to know

   The expression(s) for building ranges are not shown explicitly, which makes it difficult to know which expression(s) are used to build range. As shown below, for the query expression `a>0` is pushed down, but the pushed-down information is only implicitly given inside the `KeyRange`, which is not convenient for users to understand.

   ```sql
   CREATE TABLE `t1` (
   `a` BIGINT(20)  NOT NULL,
   `b` varchar(255) NOT NULL,
   `c` varchar(255) DEFAULT NULL
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   CREATE INDEX `testIndex` ON `t1` (`a`,`b`)
   ```

   ```sql
   SELECT a FROM t1 where a>0 
   ```

   ```text
   == Physical Plan ==
   *(1) ColumnarToRow
   +- TiKV CoprocessorRDD{[table: t1] CoveringIndexScan[Index: testindex] , Columns: a@LONG, KeyRange: [([t\200\000\000\000\000\000\r=_i\200\000\000\000\000\000\000\001\003\200\000\000\000\000\000\000\001], [t\200\000\000\000\000\000\r=_i\200\000\000\000\000\000\000\001\372])], startTs: 434247223260741633}
   ```

4. confusing naming of the operator

   In TiDB, a process named `Scan` means scanning a piece of data, not aggregating and computing the result of the scan. But in TiSpark, a process named scan means aggregating and computing the results of the scan. This may confuse users who switch from TiDB to TiSpark.

   The selection expression pushed down to TiKV/COP in TiDB is called `Selection`, while the selection expression is called `Pushdown Filter` in TiSpark. This may also confuse users who switch from TiDB to TiSpark.

## Detailed Design

### Remove the `Residual Filter`

To solve the problem of appearing a filter that shouldn't appear, we removed the `Residual Filter`.

As shown below, in the case of the same table and query as in [The Problem of DAG Explain](#the-problem-of-dag-explain) section, the `Residual Filter` has been removed from the `explain` output.

```text
== Physical Plan ==
*(1) ColumnarToRow
+- TiKV CoprocessorRDD{[table: t1] TableReader, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): { TableRangeScan: { RangeFilter: [], Range: [([t\200\000\000\000\000\000\r\'_r\000\000\000\000\000\000\000\000], [t\200\000\000\000\000\000\r\'_s\000\000\000\000\000\000\000\000])] }, Selection: [[a@LONG GREATER_THAN 1]] }, startTs: 434245944457035777}
```

### Added more description to the execution process

1. unable to know the execution steps

   To solve the problem of the execution process for `IndexScan` is divided into two phases is not show, we divide the scan of `IndexScan` into two parts `IndexRangeScan`, `TableRowIDScan`. The scan of `TableScan` is named `TableRangeScan`. The scan of `CoveringIndexScan` is named `IndexRangeScan`. The meanings of these scans are shown below.

   * **`TableRangeScan`**: Table scans with the specified range. We consider full table scan as a special case of `TableRangeScan`, so full table scan is also called `TableRangeScan`.
   * **`TableRowIDScan`**: Scans the table data based on the `RowID`. Usually follows an index read operation to retrieve the matching data rows.
   * **`IndexRangeScan`**: Index scans with the specified range. We consider full index scan as a special case of `IndexRangeScan`, so full index scan is also called `IndexRangeScan`.

2. the displayed `Filter` is not the one that would be executed under normal execution

   To solve the problem of the displayed `Filter` is not the one that would be executed under normal execution, we delete the `Downgrade Filter` and add the `Selection` (`Selection` will be introduced later) in `FetchHandleRDD`.

3. information about the used index is hard to know

   To solve the problem of information about the used index is hard to know, we add the columns that make up the index after the index name.

As shown below, in the case of the same table and query as in [The Problem of DAG Explain](#the-problem-of-dag-explain) section, add `IndexRangeScan` and `TableRowIDScan`, delete the `Downgrade Filter` and add the `Selection` in `FetchHandleRDD` , add the columns that make up the index after the index name to the output.

```text
== Physical Plan ==
*(1) ColumnarToRow
+- TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[[b@VARCHAR(255) GREATER_THAN "aa"], [a@LONG GREATER_THAN 0]]
   +- RowToColumnar
      +- TiKV FetchHandleRDD{[table: t1] IndexLookUp, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): { {IndexRangeScan(Index:testindex(a,b)): { RangeFilter: [[a@LONG GREATER_THAN 0]], Range: [([t\200\000\000\000\000\000\r-_i\200\000\000\000\000\000\000\001\003\200\000\000\000\000\000\000\001], [t\200\000\000\000\000\000\r-_i\200\000\000\000\000\000\000\001\372])] }, Selection: [[b@VARCHAR(255) GREATER_THAN "aa"]]}; {TableRowIDScan, Selection: [[b@VARCHAR(255) GREATER_THAN "aa"]]} }, startTs: 434247058345951234}
```

### Define the `RangeFilter` to make the scanning range more explicit

To solve the problem of expression(s) that constitutes the scanning range is hard to know, we added a `RangeFilter` to the `KeyRange` to indicate the expression(s) used to construct the scan range.

- **`RangeFilter`**: `RangeFilter` indicates which expression(s) the range is made up of. If `RangeFilter` is empty, it indicates a full table scan or full index scan. `RangeFilter` generally appears when the query involves an index range, when query the expressions in the `RangeFilter` form the scanned range from left to right.

As shown below, in the case of the same table and query as in [The Problem of DAG Explain](#the-problem-of-dag-explain) section,  add `RangeFilter` to the output.

```text
== Physical Plan ==
*(1) ColumnarToRow
+- TiKV CoprocessorRDD{[table: t1] IndexReader, Columns: a@LONG: { IndexRangeScan(Index:testindex(a,b)): { RangeFilter: [[a@LONG GREATER_THAN 0]], Range: [([t\200\000\000\000\000\000\rH_i\200\000\000\000\000\000\000\001\003\200\000\000\000\000\000\000\001], [t\200\000\000\000\000\000\rH_i\200\000\000\000\000\000\000\001\372])] } }, startTs: 434247289531006977}
```

### Use the same operator naming as TiDB

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

   - `TableScan` with `Selection` and without `RangeFilter`

     ```sql
     SELECT * FROM t1 where a>0 and b > 'aa'
     ```

   - `TableScan` with complex sql statements

     ```sql
     select * from t1 where a>0 or b > 'aa' or c<'cc' and c>'aa' order by(c) limit(10)
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

   - `TableScan` with `Selection` and with `RangeFilter`

     ```sql
     SELECT * FROM t1 where a>0 and b > 'aa'
     ```

   - `TableScan` without `Selection` and with `RangeFilter`

     ```sql
     SELECT * FROM t1 where a>0
     ```

   - `TableScan` with `Selection` and with `RangeFilter`

     ```sql
     SELECT * FROM t1 where b>'aa'
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

   - `TableScan` with `Selection` and with `RangeFilter` with partition

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

   - `IndexScan` with `Selection` and with `RangeFilter`

     ```sql
     SELECT * FROM t1 where a>0 and b > 'aa'
     ```

   - `IndexScan` without `Selection`and with `RangeFilter`

     ```sql
     SELECT * FROM t1 where a=0 and b > 'aa'
     ```

   - `CoveringIndex` with `Selection` and with `RangeFilter`

     ```sql
     SELECT a,b FROM t1 where a>0 and b > 'aa'
     ```

   - `IndexScan` with complex sql statements

     ```sql
     SELECT max(c) FROM t1 where a>0 and c > 'cc' and c < 'bb' group by c order by(c)
     ```

   - `CoveringIndexScan` with complex sql statements

     ```sql
     select sum(a) from t1 where a>0 and b > 'aa' or b<'cc' and a>0
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

   - `IndexScan` with complex sql statements

     ```sql
     select a from t1 where a>0 and b > 'aa' or c<'cc' and c>'aa' order by(c) limit(10) group by a
     ```
