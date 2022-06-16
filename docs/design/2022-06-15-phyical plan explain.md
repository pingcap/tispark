# TiSpark Design Documents

- Author(s): [Author Name](http://github.com/your-github-id), [Co-Author Name](http://github.com/your-github-id), ...
- Tracking Issue: https://github.com/pingcap/tidb/issues/XXX

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)

## Introduction

The physical plan is the physical execution plan in TiSpark. When we use explain in Spark which run with TiSpark, The process of physical plan will be displayed in the terminal if we use `explain` in Spark which run with TiSpark. The current physical plan which is  displayed in the terminal has problems such as outputting multiple filters  which not should show together in the same time,unclear representation of the execution process and obscure push-down conditions. The displayed of physical plan need to be improved. 

## Motivation or Background

### The Process of Scan

#### IndexScan

In TiSpark, An IndexScan requires two scans. One is scanning in index data to get the RowID and the other one is scanning in table data to get the real data. The first scan in code called IndexScan, and the second scan in code called TableScan,but in order to facilitate the distinction,  in the latter part of the text, the first scan is called IndexRangeScan, the second scan is called TableRowIDScan.

#### CoveringIndexScan

CoveringIndexScan is a special case of IndexScan. If the column in Fliter and the column in Projection are both inside the Index in one visit, then we only need to scan the Index once, no need to scan for the Table. that is, it only scans for the Index, such a Scan is called CoveringIndexScan.

#### TableScan

TableScan is different from IndexScan and CoveringIndexScan. TableScan only scan table data. such a scan we called TableRangeScan in the following.

###   Fliter Concept

#### PushDown Filter

The expression passed to COP/TiKV as Selection without triggering a downgrade.

#### Downgrade Filter

Downgrade Filter is used after IndexScan downgrade.

In the first stage of IndexScan which we called IndexRangeScan will return the RowIDs that meet the conditions,   and then TiSpark will sort and aggregate  the returned Row IDs to obtain the Region that need to be scanned in the second stage of IndexScan——TableRowIDScan.After sort and aggregate we will get region task number(The number of RangeTask is equal to the number of ranges to be scanned by TableRowIDScan. If the RowIDs returned in the first stage are 1,3,4,5 and 1,3,4 are in same region and 5 is in another region. the 1 will be used as a range, 3,4 will be used as a range,5 will be used as a range,the regionTask number will be 3.). If region task number is bigger than `downgradeThreshold`, The TableRowIDScan in second will be TableScan(start key will be first Row that returns and end key will be last key that returns).

#### Residual Filter

In the original design, Residual Filter represents operators that cannot be down-pushed to COP/TiKV. However, in the current implementation, before the construction of DAGRequest, it will judge whether the operators can be downscaled, and only the operators that can be downscaled will participate in the construction of DAGRequest, which means that all the operators in DAGRequest can be downscaled to COP/TiKV. The Residual Filter loses its original meaning because there are no operators in DAGRequest that cannot be downgraded.

### The Problem of DAG Explain

1. Filter recurrence

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

2. Filter execution process is not well described

   As shown below, for an IndexScan, normally the IndexRangeScan should be executed first, then the TableRowIDScan. only in the case of triggering a downgrade will the Downgrade Fliter be executed, but in the physical execution plan only the Downgrade Filter is shown.

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

   ```
   == Physical Plan ==
   *(1) ColumnarToRow
   +- TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[[[a@UNSIGNED LONG LESS_THAN 1] OR [b@VARCHAR(255) GREATER_THAN "bb"]]]
      +- RowToColumnar
         +- TiKV FetchHandleRDD{[table: t2] IndexScan[Index: primary] , Columns: a@UNSIGNED LONG, b@VARCHAR(255), c@VARCHAR(255), Downgrade Filter: [[a@UNSIGNED LONG LESS_THAN 1] OR [b@VARCHAR(255) GREATER_THAN "bb"]], PushDown Filter: [[a@UNSIGNED LONG LESS_THAN 1] OR [b@VARCHAR(255) GREATER_THAN "bb"]], KeyRange: [([t\200\000\000\000\000\000\003\374_i\200\000\000\000\000\000\000\001\000], [t\200\000\000\000\000\000\003\374_i\200\000\000\000\000\000\000\001\372])], Aggregates: , startTs: 433735624236728323}
   ```

3. downgrade filter not show distinctly.

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

   ```
   == Physical Plan ==
   *(1) ColumnarToRow
   +- TiKV CoprocessorRDD{[table: t2] CoveringIndexScan[Index: primary(a,b)] , Columns: a@UNSIGNED LONG, b@VARCHAR(255), Residual Filter: [b@VARCHAR(255) GREATER_THAN "aa"], PushDown Filter: [b@VARCHAR(255) GREATER_THAN "aa"], KeyRange: [([t\200\000\000\000\000\000\005z_i\200\000\000\000\000\000\000\001\000], [t\200\000\000\000\000\000\005z_i\200\000\000\000\000\000\000\001\372])], Aggregates: , startTs: 433789855560368130}
   ```


## Detailed Design

### Table Scan:

* Add TableRangeScan;
* Remove  Residual Filter 
* Renamed PushDown Filter to Selection;
* Add RangeFliter indicating which conditions are used to build Range.If RangeFilter is empty, it means the scan range is all, otherwise the scan range consists of the conditions in RangeFilter.

```SQL
CREATE TABLE `t1` (
  `a` BIGINT(20) UNSIGNED  NOT NULL,
  `b` varchar(255) NOT NULL,
  `c` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
```

```sql
SELECT * FROM t1 where a>9223372036854775807
```

**Before:**



**After:**

```
== Physical Plan ==
*(1) ColumnarToRow
+- TiKV CoprocessorRDD{[table: t1] TableScan, Columns: a@UNSIGNED LONG, b@VARCHAR(255), c@VARCHAR(255): { TableRangeScan: { KeyRange: { RangeFliter: {}, Range: [([t\200\000\000\000\000\000\000\200_r\000\000\000\000\000\000\000\000], [t\200\000\000\000\000\000\000\200_s\000\000\000\000\000\000\000\000])], Selection: [a@UNSIGNED LONG GREATER_THAN 9223372036854775807] } }, startTs: 433926806617194497}
```

```sql
CREATE TABLE `t3` (
  `a` BIGINT(20) UNSIGNED NOT NULL,
  `b` varchar(255) NOT NULL,
  `c` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`a`) clustered
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
```

```sql
SELECT * FROM t3 where a>9223372036854775807
```

**Before:**



**After:**

```
= Physical Plan ==
*(1) ColumnarToRow
+- TiKV CoprocessorRDD{[table: t3] TableScan, Columns: a@UNSIGNED LONG, b@VARCHAR(255), c@VARCHAR(255): { TableRangeScan: { KeyRange: { RangeFliter: {[a@UNSIGNED LONG GREATER_THAN 9223372036854775807], [a@UNSIGNED LONG GREATER_THAN 9223372036854775807]}, Range: [([t\200\000\000\000\000\000\000\237_r\000\000\000\000\000\000\000\000], [t\200\000\000\000\000\000\000\237_r\200\000\000\000\000\000\000\000])] } }, startTs: 433927358731517954}
```

### IndexScan:

* The Downgrade Filter on RegionTaskExec is retained.
* The output IndexScan of FetchHandleRDD is further refined to IndexRangeScan and TableRowIDScan, indicating that after the IndexScan there is a TableScan for the RowID is scanned after IndexScan.
* Delete the original Downgrade Fliter content and add Selection to indicate the Selection condition executed in the normal execution process. 
* Add the description information of Index.
* Add RangeFliter in Range, indicating which conditions are used to build Range.If RangeFilter is empty, it means the scan range is all, otherwise the scan range consists of the conditions in RangeFilter.

``` sql
CREATE TABLE `t2` (
  `a` BIGINT(20) UNSIGNED  NOT NULL,
  `b` varchar(255) NOT NULL,
  `c` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`a`,`b`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
```

```SQL
SELECT * FROM t2 where a>9223372036854775807 and b>'aa'
```

**Before:**



**After:**

```
== Physical Plan ==
*(1) ColumnarToRow
+- TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[[b@VARCHAR(255) GREATER_THAN "aa"], [a@UNSIGNED LONG GREATER_THAN 9223372036854775807]]
   +- RowToColumnar
      +- TiKV FetchHandleRDD{[table: t2] IndexScan, Columns: a@UNSIGNED LONG, b@VARCHAR(255), c@VARCHAR(255): { IndexRangeScan: [Index: primary (a,b)]: { KeyRange: { RangeFliter: {[a@UNSIGNED LONG GREATER_THAN 9223372036854775807]}, Range: [([t\200\000\000\000\000\000\000\213_i\200\000\000\000\000\000\000\001\004\200\000\000\000\000\000\000\000], [t\200\000\000\000\000\000\000\213_i\200\000\000\000\000\000\000\001\372])], Selection: [b@VARCHAR(255) GREATER_THAN "aa"] }, TableRowIDScan:{ Selection: [b@VARCHAR(255) GREATER_THAN "aa"] } }, startTs: 433926839385194497}
```

### CoveringIndexScan

* Remove Residual Filter
* Rename PushDown Filter to Selection.
* Add the description information of  Index.
* Add RangeFliter to indicate which conditions are used to build Range.If RangeFilter is empty, it means the scan range is all, otherwise the scan range consists of the conditions in RangeFilter.

```SQL
CREATE TABLE `t2` (
  `a` BIGINT(20) UNSIGNED  NOT NULL,
  `b` varchar(255) NOT NULL,
  `c` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`a`,`b`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
```

```SQL 
SELECT a FROM t2 where a>9223372036854775807 and b>'aa'
```

**Before:**



**After:**

```
== Physical Plan ==
*(1) Project [a#163]
+- *(1) ColumnarToRow
   +- TiKV CoprocessorRDD{[table: t2] CoveringIndexScan, Columns: a@UNSIGNED LONG, b@VARCHAR(255): { IndexRangeScan: [Index: primary (a,b)]: { KeyRange: { RangeFliter: {[a@UNSIGNED LONG GREATER_THAN 9223372036854775807], [a@UNSIGNED LONG GREATER_THAN 9223372036854775807]}, Range: [([t\200\000\000\000\000\000\000\224_i\200\000\000\000\000\000\000\001\004\200\000\000\000\000\000\000\000], [t\200\000\000\000\000\000\000\224_i\200\000\000\000\000\000\000\001\372])], Selection: [b@VARCHAR(255) GREATER_THAN "aa"] } }, startTs: 433926993158864898}
```

## Test Design

### TableScan

1. Full Table Scan with Selection

   ```SQL
   CREATE TABLE `t1` (
     `a` BIGINT(20) UNSIGNED  NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   ```

   ```SQL
   SELECT * FROM t1 where a>9223372036854775807
   ```

2. Range Table Scan with Selection

   ```SQL
   CREATE TABLE `t1` (
     `a` BIGINT(20) UNSIGNED NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL,
     PRIMARY KEY (`a`) clustered
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   ```

   ```SQL
   SELECT * FROM t1 where a>9223372036854775807 and b > 'aa'
   ```
   
3. prefix index scan
   
   ```sql
   CREATE TABLE `t3` (
     `a` BIGINT(20) UNSIGNED NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL,
     PRIMARY KEY (`a`(3)) clustered
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   
   ```
   
   ```sql
   SELECT * FROM t3 where a>9223372036854775807
   ```
   
   Exception:
   
   ```
   == Physical Plan ==
   *(1) ColumnarToRow
   +- TiKV CoprocessorRDD{[table: t3] TableScan, Columns: a@UNSIGNED LONG, b@VARCHAR(255), c@VARCHAR(255): { TableRangeScan: { KeyRange: { RangeFliter: {[a@UNSIGNED LONG GREATER_THAN 9223372036854775807]}, Range: [([t\200\000\000\000\000\000\001\233_r\000\000\000\000\000\000\000\000], [t\200\000\000\000\000\000\001\233_r\200\000\000\000\000\000\000\000])] } }, startTs: 433943624491728897}
   ```
   
   ```sql
   CREATE TABLE `t3` (
     `a` BIGINT(20) UNSIGNED NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL,
     PRIMARY KEY (`b`(4)) clustered
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   ```
   
   ```sql
   SELECT * FROM t3 where b>'aa'
   ```
   
   Exception:
   
   ```
   == Physical Plan ==
   *(1) ColumnarToRow
   +- TiKV CoprocessorRDD{[table: t3] TableScan, Columns: a@UNSIGNED LONG, b@VARCHAR(255), c@VARCHAR(255): { TableRangeScan: { KeyRange: { RangeFliter: {}, Range: [([t\200\000\000\000\000\000\001\343_r\000\000\000\000\000\000\000\000], [t\200\000\000\000\000\000\001\343_s\000\000\000\000\000\000\000\000])], Selection: [b@VARCHAR(255) GREATER_THAN "aa"] } }, startTs: 433943812829085697}
   ```
   

### IndexScan

1. range index scan with selection

     ```sql
     CREATE TABLE `t1` (
     `a` BIGINT(20) UNSIGNED  NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL,
     PRIMARY KEY (`a`,`b`)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
     ```

     ```sql
     SELECT * FROM t1 where a>9223372036854775807 and b>'aa'        
     ```

     Exception:

     ```
     == Physical Plan ==
     *(1) ColumnarToRow
     +- TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[[b@VARCHAR(255) GREATER_THAN "aa"], [a@UNSIGNED LONG GREATER_THAN 9223372036854775807]]
        +- RowToColumnar
           +- TiKV FetchHandleRDD{[table: t2] IndexScan, Columns: a@UNSIGNED LONG, b@VARCHAR(255), c@VARCHAR(255): { IndexRangeScan: [Index: primary (a,b)]: { KeyRange: { RangeFliter: {[a@UNSIGNED LONG GREATER_THAN 9223372036854775807]}, Range: [([t\200\000\000\000\000\000\001\220_i\200\000\000\000\000\000\000\001\004\200\000\000\000\000\000\000\000], [t\200\000\000\000\000\000\001\220_i\200\000\000\000\000\000\000\001\372])], Selection: [b@VARCHAR(255) GREATER_THAN "aa"] }, TableRowIDScan:{ Selection: [b@VARCHAR(255) GREATER_THAN "aa"] } }, startTs: 433943524392304641}
     ```

2. prefix index scan with selection

   ```sql
   CREATE TABLE `t2` (
     `a` BIGINT(20) UNSIGNED  NOT NULL,
     `b` varchar(255) NOT NULL,
     `c` varchar(255) DEFAULT NULL,
     PRIMARY KEY (`b`(1))
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   ```

   ```sql
   SELECT * FROM t2 where b>'aa' and a>1
   ```
   
   Exception:
   
   ```
   == Physical Plan ==
   *(1) ColumnarToRow
   +- TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[[b@VARCHAR(255) GREATER_THAN "aa"], [a@UNSIGNED LONG GREATER_THAN 1]]
      +- RowToColumnar
         +- TiKV FetchHandleRDD{[table: t2] IndexScan, Columns: a@UNSIGNED LONG, b@VARCHAR(255), c@VARCHAR(255): { IndexRangeScan: [Index: primary (b)]: { KeyRange: { RangeFliter: {[b@VARCHAR(255) GREATER_THAN "aa"]}, Range: [([t\200\000\000\000\000\000\001~_i\200\000\000\000\000\000\000\001\001a\000\000\000\000\000\000\000\370], [t\200\000\000\000\000\000\001~_i\200\000\000\000\000\000\000\001\372])] }, TableRowIDScan:{ Selection: [b@VARCHAR(255) GREATER_THAN "aa"], [a@UNSIGNED LONG GREATER_THAN 1] } }, startTs: 433943452984016897}
   ```

### CoveringIndexScan

```sql
CREATE TABLE `t2` (
  `a` BIGINT(20) UNSIGNED  NOT NULL,
  `b` varchar(255) NOT NULL,
  `c` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`a`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
```

```sql
SELECT a FROM t2 where a=1
```

Exception:

```
== Physical Plan ==
*(1) ColumnarToRow
+- TiKV CoprocessorRDD{[table: t2] CoveringIndexScan, Columns: a@UNSIGNED LONG: { IndexRangeScan: [Index: primary (a)]: { KeyRange: { RangeFliter: {[a@UNSIGNED LONG EQUAL 1]}, Range: [([t\200\000\000\000\000\000\002\032_i\200\000\000\000\000\000\000\001\004\000\000\000\000\000\000\000\001], [t\200\000\000\000\000\000\002\032_i\200\000\000\000\000\000\000\001\004\000\000\000\000\000\000\000\002])] } }, startTs: 433944223621840897}
```



## References

List the reference materials here.