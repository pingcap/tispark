# TiSpark Design Documents

- Author(s): [qidi1](https://github.com/qidi1)
- Tracking Issue: https://github.com/pingcap/tispark/issues/2385

## Table of Contents

- [Introduction](#introduction)
- [Motivation or Background](#motivation-or-background)
- [Detailed Design](#detailed-design)
- [Test Design](#test-design)

## Introduction

The physical plan is the physical execution plan in TiSpark. When we use `explain` in Spark which runs with TiSpark, The process of the physical plan will be displayed in the terminal. but now this display about how the plan executes has some problems.

- outputting multiple filters which not should show together at the same time.

- unclear representation of the execution process.

- obscure push-down conditions.

The display of the  physical plan needs to be improved.

## Background

### The Process of Scan

#### IndexScan

In TiSpark, An IndexScan requires two scans. One is scanning in index data to get the RowID and the other one is scanning in table data to get the real data. The first scan in code is called IndexScan, and the second scan in code called TableScan, but to facilitate the distinction, in the latter part of the text, the first scan is called IndexRangeScan, and the second scan is called TableRowIDScan.

#### CoveringIndexScan

CoveringIndexScan is a special case of IndexScan. If the column in Filter and the column in Projection are both inside the Index in one visit, then we only need to scan the Index once, no need to scan for the Table. that is, it only scans for the Index, such a Scan is called CoveringIndexScan.

#### TableScan

TableScan is different from IndexScan and CoveringIndexScan. TableScan only scans table data. such a scan we called TableRangeScan in the following.

### Filter Concept

#### PushDown Filter

The expression passed to COP/TiKV as Selection without triggering a downgrade.

#### Downgrade Filter

Downgrade Filter is used after IndexScan downgrade.

The first stage of IndexScan which we called IndexRangeScan will return the RowIDs that meet the conditions, and then TiSpark will sort and aggregate the returned Row IDs to obtain the region that needs to be scanned in the second stage of IndexScan——TableRowIDScan. After sorting and aggregating we will get the regionTask number(The number of RangeTask is equal to the number of ranges to be scanned by TableRowIDScan. If the RowIDs returned in the first stage are 1,3,4,5 and 1,3,4 are in the same region and 5 is in another region. Since 1 and 3,4 are not contiguous, 1 is RegionTask, and since 3,4 and 5 are not in a region, 3,4 is a RegionTask and 5 is another RegionTask. ). If the RegionTask number is bigger than `downgradeThreshold`, The TableRowIDScan in the second will be TableScan(Startkey will be first Row that returns and Endkey will be last Row that returns).

#### Residual Filter

In the original design, the Residual Filter represents operators that cannot be down-pushed to COP/TiKV. However, in the current implementation, before the construction of DAGRequest, it will judge whether the operators can be downscaled, and only the operators that can be downscaled will participate in the construction of DAGRequest, which means that all the operators in DAGRequest can be downscaled to COP/TiKV. The Residual Filter loses its original meaning because there are no operators in DAGRequest that cannot be downgraded.

### The Problem of DAG Explain

1. outputting multiple filters which not should show together at the same time

   As shown below, both Residual Filter and PushDown Filter appear, while for a TableScan there should be only PushDown Filter.

   ```SQL
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

   For an IndexScan, normally the IndexRangeScan should be executed first, then the TableRowIDScan. Only in the case of triggering a downgrade will the Downgrade Filter be executed, but in the physical execution plan, only the Downgrade Filter is shown.

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

3. obscure push-down conditions

   As shown below, for the query condition a>0 is pushed down, but the pushed-down information is only implicitly given inside the KeyRange, which is not convenient for users to understand.

   ```SQL
   CREATE TABLE `t3` (
     `a` BIGINT(20) NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL,
     PRIMARY KEY (`a`) clustered
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   ```

   ```SQL
   SELECT a FROM t3 where a>0
   ```

   ```text
   == Physical Plan ==
   *(1) ColumnarToRow
   +- TiKV CoprocessorRDD{[table: t2] CoveringIndexScan[Index: primary(a,b)] , Columns: a@UNSIGNED LONG, b@VARCHAR(255), Residual Filter: [b@VARCHAR(255) GREATER_THAN "aa"], PushDown Filter: [b@VARCHAR(255) GREATER_THAN "aa"], KeyRange: [([t\200\000\000\000\000\000\005z_i\200\000\000\000\000\000\000\001\000], [t\200\000\000\000\000\000\005z_i\200\000\000\000\000\000\000\001\372])], Aggregates: , startTs: 433789855560368130}
   ```

## Detailed Design

### Design Overview

- **TableRangeScan**: Table scans with the specified range, We consider full table scan as a special case of TableRangeScan, so full table scan is also called TableRangeScan

- **TableRowIDScan**: Scans the table data based on the RowID. Usually follows an index read operation to retrieve the matching data rows.

- **IndexRangeScan**: Index scans with the specified range. We consider full index scan as a special case of IndexRangeScan, so full index scan is also called IndexRangeScan

- **RangeFilter**: RangeFilter indicates which conditions the range is made up of. RangeFilter generally appears when the query involves an index. If RangeFilter is empty, it indicates a full table scan or full index scan.RangeFilter generally appears when the query involves an index range, When query The expressions in the RangeFilter form the scanned range from left to right.

  For Example

  ```sql
  CREATE TABLE `t3` (
    `a` BIGINT(20) NOT NULL,
    `b` varchar(255) NOT NULL,
    `c` varchar(255) DEFAULT NULL,
    PRIMARY KEY (a,b) clustered
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  ```

  ```sql
  SELECT * FROM t3 where a=0,b>'aa'
  ```

  The RangeFilter will be `{a=0,b>'aa'}`,Range will be `{t_0 _aa ,t__0_inf}`.

### Output we expect

#### Table Scan

- Add TableRangeScan.
- Remove  Residual Filter.
- Renamed PushDown Filter to Selection.
- Add RangeFliter indicating which conditions are used to build Range.

```sql
CREATE TABLE `t1` (
  `a` BIGINT(20) NOT NULL,
  `b` varchar(255) NOT NULL,
  `c` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
```

```sql
SELECT * FROM t1 where a>0
```

**Before:**

```text
== Physical Plan ==
*(1) ColumnarToRow
+- TiKV CoprocessorRDD{[table: t1] TableScan, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255), Residual Filter: [a@LONG GREATER_THAN 0], PushDown Filter: [a@LONG GREATER_THAN 0], KeyRange: [([t\200\000\000\000\000\000\002\376_r\000\000\000\000\000\000\000\000], [t\200\000\000\000\000\000\002\376_s\000\000\000\000\000\000\000\000])], startTs: 433995142104612867}
```

**After:**

```text
= Physical Plan ==
*(1) ColumnarToRow
+- TiKV CoprocessorRDD{[table: t1] TableScan, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): { TableRangeScan: { KeyRange: { RangeFliter: {}, Range: [([t\200\000\000\000\000\000\003A_r\000\000\000\000\000\000\000\000], [t\200\000\000\000\000\000\003A_s\000\000\000\000\000\000\000\000])], Selection: [a@LONG GREATER_THAN 0] } }, startTs: 433995460281892865}
```

```sql
CREATE TABLE `t3` (
  `a` BIGINT(20) NOT NULL,
  `b` varchar(255) NOT NULL,
  `c` varchar(255) DEFAULT NULL,
  PRIMARY KEY (a) clustered
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
```

```sql
SELECT * FROM t3 where a>0
```

**Before:**

```text
== Physical Plan ==
*(1) ColumnarToRow
+- TiKV CoprocessorRDD{[table: t3] TableScan, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255), KeyRange: [([t\200\000\000\000\000\000\003\022_r\200\000\000\000\000\000\000\001], [t\200\000\000\000\000\000\003\022_s\000\000\000\000\000\000\000\000])], startTs: 433995273490923521}
```

**After:**

```text
== Physical Plan ==
*(1) ColumnarToRow
+- TiKV CoprocessorRDD{[table: t3] TableScan, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): { TableRangeScan: { KeyRange: { RangeFliter: {[a@LONG GREATER_THAN 0]}, Range: [([t\200\000\000\000\000\000\003U_r\200\000\000\000\000\000\000\001], [t\200\000\000\000\000\000\003U_s\000\000\000\000\000\000\000\000])] } }, startTs: 433995476849393665}
```

#### IndexScan

- The Downgrade Filter on RegionTaskExec is retained.
- The output IndexScan of FetchHandleRDD is further refined to IndexRangeScan and TableRowIDScan, indicating that after the IndexScan there is a TableScan for the RowID is scanned after IndexScan.
- Delete the original Downgrade Fliter content and add Selection to indicate the Selection condition executed in the normal execution process.
- Add the description information of  the index which used in scan.
- Add RangeFliter in Range, indicating which conditions are used to build Range.

``` sql
CREATE TABLE `t2` (
  `a` BIGINT(20) NOT NULL,
  `b` varchar(255) NOT NULL,
  `c` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`a`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
```

```SQL
SELECT * FROM t2 where a>0
```

**Before:**

```text
== Physical Plan ==
*(1) ColumnarToRow
+- TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[]
   +- RowToColumnar
      +- TiKV FetchHandleRDD{[table: t2] IndexScan[Index: primary] , Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255), Downgrade Filter: [a@LONG GREATER_THAN 0], KeyRange: [([t\200\000\000\000\000\000\003 _i\200\000\000\000\000\000\000\001\003\200\000\000\000\000\000\000\001], [t\200\000\000\000\000\000\003 _i\200\000\000\000\000\000\000\001\372])], startTs: 433995306940497921}
```

**After:**

```text
== Physical Plan ==
*(1) ColumnarToRow
+- TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[]
   +- RowToColumnar
      +- TiKV FetchHandleRDD{[table: t2] IndexScan, Columns: a@LONG, b@VARCHAR(255), c@VARCHAR(255): { IndexRangeScan: [Index: primary (a)]: { KeyRange: { RangeFliter: {[a@LONG GREATER_THAN 0]}, Range: [([t\200\000\000\000\000\000\003s_i\200\000\000\000\000\000\000\001\003\200\000\000\000\000\000\000\001], [t\200\000\000\000\000\000\003s_i\200\000\000\000\000\000\000\001\372])] }, TableRowIDScan:{  } }, startTs: 433995556016619522}
```

#### CoveringIndexScan

- Remove Residual Filter.
- Rename PushDown Filter to Selection.
- Add the description information of  the index which used in scan.
- Add RangeFliter to indicate which conditions are used to build Range.

```sql
CREATE TABLE `t2` (
  `a` BIGINT(20) UNSIGNED  NOT NULL,
  `b` varchar(255) NOT NULL,
  `c` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`a`,`b`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
```

```sql
SELECT a,b FROM t2 where a>0 and b>'aa'
```

**Before:**

```text
== Physical Plan ==
*(1) ColumnarToRow
+- TiKV CoprocessorRDD{[table: t2] CoveringIndexScan[Index: primary] , Columns: a@LONG, b@VARCHAR(255), Residual Filter: [b@VARCHAR(255) GREATER_THAN "aa"], PushDown Filter: [b@VARCHAR(255) GREATER_THAN "aa"], KeyRange: [([t\200\000\000\000\000\000\0033_i\200\000\000\000\000\000\000\001\003\200\000\000\000\000\000\000\001], [t\200\000\000\000\000\000\0033_i\200\000\000\000\000\000\000\001\372])], startTs: 433995356695429121}
```

**After:**

```text
== Physical Plan ==
*(1) ColumnarToRow
+- TiKV CoprocessorRDD{[table: t2] CoveringIndexScan, Columns: a@LONG, b@VARCHAR(255): { IndexRangeScan: [Index: primary (a,b)]: { KeyRange: { RangeFliter: {[a@LONG GREATER_THAN 0]}, Range: [([t\200\000\000\000\000\000\003\223_i\200\000\000\000\000\000\000\001\003\200\000\000\000\000\000\000\001], [t\200\000\000\000\000\000\003\223_i\200\000\000\000\000\000\000\001\372])], Selection: [b@VARCHAR(255) GREATER_THAN "aa"] } }, startTs: 433995598326923265}
```

### Code Design

1. We first determine the type of Scan, and here we will classify the Scan into three types

   - Scan on Index first, then on Table, called IndexScan.

   - Scan only for Index, called CoveringIndexScan.

   - Scan for Table only, called TableScan.

   For Scan type, CoveringIndexScan and IndexScan set `isIndexScan` to true.

   ```java
    switch (getIndexScanType()) {
      case INDEX_SCAN:
        sb.append("IndexScan");
        isIndexScan = true;
        break;
      case COVERING_INDEX_SCAN:
        sb.append("CoveringIndexScan");
        isIndexScan = true;
        break;
      case TABLE_SCAN:
        sb.append("TableScan");
    }
   ```

2. Get the columns in projection and predicates.

   ```java
    if (!getFields().isEmpty()) {
         sb.append(", Columns: ");
         Joiner.on(", ").skipNulls().appendTo(sb, getFields());
       }
   ```

3. Handled separately according to whether `isIndexScan` is true or not.

   ```java
      if (isIndexScan) {
        sb.append(toIndexScanPhysicalPlan());
      } else {
        sb.append(toTableScanPhysicalPlan());
      }
   ```

   - `toTableScanPhysicalPlan:`

     Call `buildTableScan`, then call `toPhysicalPlan`.

     ```java
        sb.append("TableRangeScan");
        sb.append(": {");
        TiDAGRequest tableRangeScan = this.copy();
        tableRangeScan.buildTableScan();
        sb.append(tableRangeScan.toPhysicalPlan());
     ```

   - `toIndexScanPhysicalPlan:`

      - First process `IndexRangeScan`, add the information of index scanned by `IndexRangeScan`, call `buildIndexScan`, then call `toPhysicalPlan`.
      - if it is`DoubleRead`, add `TableRowIDScan`. first call `buildTableScan`, then call `toPhysicalPlan`.

     ```java
         sb.append("IndexRangeScan: ");
         sb.append(index.colNames);
         ...
         TiDAGRequest indexRangeScan = this.copy();
         indexRangeScan.buildIndexScan();
         sb.append(indexRangeScan.toPhysicalPlan());
         if (isDoubleRead()) {
           sb.append(", TableRowIDScan:");
           TiDAGRequest tableRowIDScan = this.copy();
           tableRowIDScan.resetRanges();
           tableRowIDScan.buildTableScan();
           sb.append(tableRowIDScan.toPhysicalPlan());
         }
     ```

   - `toPhysicalPlan`

     Return `Range`, `Filters`, `Aggregates`, `GroupBy`, `OrderBy`,  `Limit`  to String.

     ```java
     if (!getRangesMaps().isEmpty()) {
          sb.append(getRangeFliter())
          sb.append(getRangesMaps())
     }
     if (!getPushDownFilters().isEmpty()) {
         sb.append(getPushDownFilters())
     }
     if(!getPushDownAggregates().isEmpty()){
         sb.append(getPushDownAggregates())
     }
     if (!getGroupByItems().isEmpty()) {
          sb.append(getGroupByItems())
     }
     if (!getOrderByItems().isEmpty()) {
         sb.append(getOrderByItems())
     }
     if (getLimit() != 0) {
         sb.append(getLimit())
     }
     ```

## Test Design

1. Table without cluster index

   ```SQL
   CREATE TABLE `t1` (
     `a` BIGINT(20) NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   ```

   ```sql
   SELECT * FROM t1 where a>0 and b > 'aa'
   ```

2. Table with cluster index

   ```sql
   CREATE TABLE `t1` (
     `a` BIGINT(20) NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL,
     PRIMARY KEY (`a`) clustered
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   ```

   ```sql
   SELECT * FROM t1 where a>0 and b > 'aa'
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
   CREATE INDEX `testIndex` ON `t1` (`a`);
   ```

   ```sql
   SELECT * FROM t1 where a>0 and b > 'aa'
   ```

   ```sql
   CREATE TABLE `t1` (
   `a` BIGINT(20)  NOT NULL,
   `b` varchar(255) NOT NULL,
   `c` varchar(255) DEFAULT NULL,
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   CREATE INDEX `testIndex` ON `t1` (`a`,`b`);
   ```

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
   CREATE INDEX `testIndex` ON `t1` (`b`(4));
   ```

   ```sql
   SELECT * FROM t1 where a>0 and b > 'aa'
   ```

      ```sql
   CREATE TABLE `t1` (
   `a` BIGINT(20)   NOT NULL,
   `b` varchar(255) NOT NULL,
   `c` varchar(255) DEFAULT NULL,
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   CREATE INDEX `testIndex` ON `t1` (`b`(4),a);
      ```

   ```sql
   SELECT * FROM t1 where a>0 and b > 'aa'
   ```

6. Table with secondary index and partition

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
   CREATE INDEX `testIndex` ON `t1` (`b`);
   ```

   ```sql
   SELECT * FROM t1 where a>0 and b > 'aa'
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

   ```sql
   SELECT * FROM t1 where a>0 and b > 'aa'
   ```
