#  Query Execution Plan in TiSpark

If we call Spark's `explain` that runs with TiSpark,  We might see output like this.

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [max(c)#163]
   +- Sort [c#161 ASC NULLS FIRST], true, 0
      +- Exchange rangepartitioning(c#161 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [id=#55]
         +- SortAggregate(key=[c#161], functions=[max(max(c#161)#165)])
            +- Sort [c#161 ASC NULLS FIRST], false, 0
               +- Exchange hashpartitioning(c#161, 200), ENSURE_REQUIREMENTS, [id=#51]
                  +- SortAggregate(key=[c#161], functions=[partial_max(max(c#161)#165)])
                     +- Sort [c#161 ASC NULLS FIRST], false, 0
                        +- TiKV CoprocessorRDD{[table: t1] TableReader, Columns: c@VARCHAR(255), a@LONG: { TableRangeScan: { RangeFilter: [[a@LONG GREATER_THAN 0]], Range: [([t\200\000\000\000\000\000\000f_r\200\000\000\000\000\000\000\001], [t\200\000\000\000\000\000\000f_s\000\000\000\000\000\000\000\000])] }, Selection: [[c@VARCHAR(255) GREATER_THAN "cc"], [c@VARCHAR(255) LESS_THAN "bb"], Not(IsNull(c@VARCHAR(255)))], Aggregates: Max(c@VARCHAR(255)), First(c@VARCHAR(255)), Group By: [c@VARCHAR(255) ASC] }, startTs: 434352901827854337}
```

Or output like this.

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [max(c)#166]
   +- Sort [c#164 ASC NULLS FIRST], true, 0
      +- Exchange rangepartitioning(c#164 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [id=#69]
         +- SortAggregate(key=[c#164], functions=[max(max(c#164)#168)])
            +- Sort [c#164 ASC NULLS FIRST], false, 0
               +- Exchange hashpartitioning(c#164, 200), ENSURE_REQUIREMENTS, [id=#65]
                  +- SortAggregate(key=[c#164], functions=[partial_max(max(c#164)#168)])
                     +- Sort [c#164 ASC NULLS FIRST], false, 0
                        +- TiSpark RegionTaskExec{downgradeThreshold=1000000000,downgradeFilter=[[c@VARCHAR(255) GREATER_THAN "cc"], Not(IsNull(c@VARCHAR(255))), [a@LONG GREATER_THAN 0], [c@VARCHAR(255) LESS_THAN "bb"]]
                           +- TiKV FetchHandleRDD{[table: t1] IndexLookUp, Columns: c@VARCHAR(255), a@LONG: { {IndexRangeScan(Index:testindex(a,b)): { RangeFilter: [[a@LONG GREATER_THAN 0]], Range: [([t\200\000\000\000\000\000\000__i\200\000\000\000\000\000\000\001\003\200\000\000\000\000\000\000\001], [t\200\000\000\000\000\000\000__i\200\000\000\000\000\000\000\001\372])] }}; {TableRowIDScan, Selection: [[c@VARCHAR(255) GREATER_THAN "cc"], Not(IsNull(c@VARCHAR(255))), [c@VARCHAR(255) LESS_THAN "bb"]], Aggregates: Max(c@VARCHAR(255)), First(c@VARCHAR(255)), Group By: [c@VARCHAR(255) ASC]} }, startTs: 434352842229415937}
```

Only the node `RegionTaskExec` and the child node `CoprocessorRDD`(`FetchHandleRDD`)  are the output of TiSpark. The rest nodes are the output of Spark. So here we mainly explain the `RegionTaskExec` and `CoprocessorRDD`(`FetchHandleRDD`).

## Understand EXPLAIN output in `CoprocessorRDD`(`FetchHandleRDD`)

### Operator that perform table scans

An operator is a particular step that is executed as part of returning query results. The operators that perform table scans (of the disk or the TiKV Block Cache) are listed as follows:

* **`TableRangeScan`**: Table scans with the specified range. We consider full table scan as a special case of `TableRangeScan`, so full table scan is also called `TableRangeScan`.
* **`TableRowIDScan`**: Scans the table data based on the `RowID`. Usually follows an index read operation to retrieve the matching data rows.
* **`IndexRangeScan`**: Index scans with the specified range. We consider full index scan as a special case of `IndexRangeScan`, so full index scan is also called `IndexRangeScan`.

### Operator that aggregates the data from TiKV/TiFlash

TiSpark aggregates the data or calculation results scanned from TiKV/TiFlash. The data aggregation operators can be divided into the following categories:

- **`TableReader`**: Aggregates the data obtained by the underlying operator `TableRangeScan` in TiKV/TiFlash.
- **`IndexReader`**: Aggregates the data obtained by the underlying operator `IndexRangeScan` in TiKV/TiFlash.
- **`IndexLookUp`**: First aggregates the RowIDs (in TiKV/TiFlash) scanned by the first scan in the index. Then at the second scan in the table, accurately reads the data from TiKV/TiFlash based on these RowIDs. At the first scan in the index, there is `IndexRangeScan` operator; at the second scan in the table, there is the `TableRowIDScan` operator.

### `Range`&`RangeFilter`

In the `WHERE`/`HAVING`/`ON` conditions, the TiSpark optimizer analyzes the result returned by the primary key query or the index key query. For example, these conditions might include comparison operators of the numeric and date type, such as `>`, `<`, `=`, `>=`, `<=`, and the character type such as `LIKE`.

The `Range` in `CoprocessorRDD`(`FetchHandleRDD`) represents the range of scanning. `RangeFilter` indicates which expression(s) the range is made up of. If `RangeFilter` is empty, it indicates a full table scan or full index scan. `RangeFilter` generally appears when the query involves an index range, when query the expressions in the `RangeFilter` form the scanned range from left to right.

### Selection

The expression passed to COP/TiKV as selection expression without triggering a downgrade.

## Understand EXPLAIN output in `RegionTaskExec`

From the previous article, we know that `IndexLookUp` will perform two scanning operations, the first scan is `IndexRangeScan` and the second scan is `TableRowIDScan`. If the `TableRowIDScan` in `IndexLookUp` does too many queries on COP([TiKV Coprocessor](https://docs.pingcap.com/tidb/stable/tikv-overview#tikv-coprocessor))/TiKV, it can cause performance problems in COP/TiKV. To solve this problem a downgrading mechanism is introduced.

The `IndexRangeScan` of `IndexLookUp` will return the data that meets the conditions, and then TiSpark will sort and aggregate the returned data to obtain the `regionTask` that needs to be done in the `TableRowIDScan`. If the number of `regionTask` is bigger than `downgradeThreshold`, a downgrade will be triggered. When a downgrade is triggered, the range of the second  table  scan will be changed from the returned value of the first scan index to all values between the minimum and maximum value of the first scan index, and the `filters` of the second scan will change to `downgradeFilters`(`downgradeFilters` is the same as if the execution plan is `TableScan`'s `filters`).

> **`RegionTask`**
>
> For all returned data, all consecutive data in a region will be treated as a `regionTask`.
>
> For example like this the data returned in the first stage are 1, 3, 4, 5 and 1, 3, 4 are in the same region and 5 is in another region. Since 1 and 3, 4 are not contiguous, 1 is a `regionTask`, and since 3, 4 and 5 are not in a region, 3, 4 is a `regionTask` and 5 is a `regionTask`. The number of `regionTask`  is three.

### `downgradeThreshold`

The threshold value that triggers a downgrade. The downgrade is triggered when the number of `RegionTask` exceeds the `downgradeThreshold`.

### `downgradeFilter`

The expression passed to COP/TiKV as selection expression when triggering a downgrade.
