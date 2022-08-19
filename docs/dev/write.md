# Write
## Intro
In TiSpark, we support the use of DataSource API and Spark SQL for data writing. 
This feature is achieved by inheriting and implementing the `createRelation`  function of `CreatableRelationProvider` 
and the `newWriteBuilder` function of `SupportsWrite` respectively.

Currently, we still use the interface of Spark DataSource V1 in `newWriteBuilder` because it is difficult to solve 
problems such as region split under the Spark DataSource V2 model. 
That means, whether writing using the DataSource API or Spark SQL, they share the same logic and code currently.

## Preprocess Data
`createRelation`, as the caller of the top layer of writing, calls the function `TiDBWriter.write()`. 
After a series of checks and processing, this function will eventually enter [`TiBatchWrite.doWrite()`](https://github.com/pingcap/tispark/blob/master/core/src/main/scala/com/pingcap/tispark/write/TiBatchWrite.scala#:~:text=private%20def-,doWrite,-()%3A%20Unit), 
the core write logic.

We will first encapsulate the data and its corresponding metadata into [`TiBatchWriteTable`](https://github.com/pingcap/tispark/blob/master/core/src/main/scala/com/pingcap/tispark/write/TiBatchWriteTable.scala#:~:text=def-,preCalculate,-(startTimeStamp%3A)),
which will be used for subsequent operations.
Then we will do a series of checks for it, including support check, authorization check, empty check and schema check.
If all checks are passed, we will move to the data processing phase. It's mainly handled by `TiBatchWriteTable.preCalculate`. 
This function is responsible for processing the data passed in by the user, converting it into a format supported by TiKV and performing other processing according to the options.
1. First of all, it will judge whether this table has a unique key, if not, we will add a column to it and use it as the identity column.
2. Then, if the Reduplicate option is true, the incoming data will be deduplicated and only the unique data will be kept.
3. Finally, if the Replace option is true, we will judge whether there is data with the same key in TiKV now with incoming data. If so, we will delete the original data.
   Finally, the data will be converted into an RDD in the form of <PrimaryKey, Value>. 
## Region Split
Since in the process of writing data, TIKV may generate region split, which will reduce the writing efficiency. 
Therefore, we will pre-split the region according to the current region information and the amount of data written. 
This not only avoids the region splits in the process of continuous writing, but also effectively utilizes Spark's parallel writing capabilities. 
## Write using 2PC
After getting the data, we will use 2PC to write it into TiKV. Our 2PC implement follows Percolator. For more details, you can read [TiKV | Percolator](https://tikv.org/deep-dive/distributed-transaction/percolator/) 
and [Google Percolator](https://research.google/pubs/pub36726/). Here is a brief process introduction.
1. Lock table
2. Prewrite primary key 
3. Prewrite secondary key
4. Commit primary key
5. Unlock table
6. Async commit secondary key

