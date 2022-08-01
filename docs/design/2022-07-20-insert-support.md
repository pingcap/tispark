# Support Insert SQL 

- Author(s): TrafalgarRicardoLu
- Tracking Issue: https://github.com/pingcap/tispark/issues/2462

## Table of Contents
- [Introduction](#introduction)
- [Motivation or Background](#motivation-or-background)
- [Goals](#goals)
- [API](#api)
- [Detailed Design](#detailed-design)
  * [Add new capability](#add-new-capability)
  * [Remove write options](#remove-write-options)
  * [Implement V2 interface using V1 code](#implement-v2-interface-using-v1-code)
  * [Compatible with different Spark versions](#compatible-with-different-spark-versions)
  * [Why not V2 write model](#why-not-v2-write-model)
- [Compatibility](#compatibility)
- [Test Design](#test-design)
  * [Functional Tests](#functional-tests)
  * [Compatibility Tests](#compatibility-tests)
- [References](#references)

## Introduction

New feature: TiSpark Insert SQL Feature

## Motivation or Background

TiSpark is a connector that provides read, insert and delete support.
But it only supports insert using DataSource API, users can't use Spark SQL to write data.

It's common for users to use SQL statements in the big-data workflow. We are going to support TiSpark to
write using Spark SQL.

## Goals
**Must have**
- Support write data by Spark SQL
- Compatible with Spark 3.x

**Nice to have**
- Type convert compatible with the previous version
- User doc

## API
Use spark SQL to insert data in TiSpark.
```
// insert with spark sql
spark.sql("insert into tidb_db.db.table values(x, x...)(y, y...)...")
```

## Detailed Design
Here are the main steps:
- Add new capability
- remove write options
- Implement V2 interface using V1 code
- Compatible with different Spark versions

### Add new capability
Spark will check the [capabilities](https://github.com/apache/spark/blob/0494dc90af48ce7da0625485a4dc6917a244d580/sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/TableCapability.java)
of TiDBTable which signal to Spark how many features TiDBTable supports. 

If we don't add corresponding capability and insert data by SQL, Spark will throw an exception says TiDBTable don't support it.
Since we will implement V2 interface using V1 code so `V1_BATCH_WRITE` is needed.

### Remove write options
Different from writing using DataSource API, Spark SQL can't get required information like tidb address and username.
We use it to create a JDBC client to connect with TiDB, but we only need to use the JDBC client to update statics now.

So we are going to remove the options check before writing and useless JDBC client code.
If users want to enable statics update when using Spark SQL, they can add information by `spark.conf.set()`.

### Implement V2 interface using V1 code
Spark DataSource V2 is evolving and not suitable for TiSpark write model currently.
Spark provides us with an interface that is implemented by DataSource V1 and leverages the DataSource V2 write code path.

We can use this interface to implement INSERT SQL without rewriting all write codes.

### Compatible with different Spark versions
The interface we used to combine DataSource V1 and DataSource V2 is different between
Spark 3.0, 3.1, and 3.2. So we need to write code for each version. Then we can use reflection to get object of
corresponding version。

To be specific, we will use [V1WriteBuilder](https://github.com/apache/spark/blob/branch-3.0/sql/core/src/main/java/org/apache/spark/sql/connector/write/V1WriteBuilder.java) in 3.0 and 3.1, and use [V1Write](https://github.com/apache/spark/blob/1a42aa5bd44e7524bb55463bbd85bea782715834/sql/core/src/main/java/org/apache/spark/sql/connector/write/V1Write.java) in 3.2.

### Why not V2 write model
There is a deduplicate operation in TiSpark that will handle the problem that data have the same unique key.
DataSource V2 will split data into different executors, and we can't control it. This may lead to different
executors writing data with the same unique key and cause data corruption.

Another problem is about region split. Currently, we split regions to enable parallel write, each executor will write data to only one
region. Since we can't control how to split data in DataSource V2, each executor may receive data that
should be written to different regions. If we don't do region split, it may cause performance issue. If we do, region split will be
done in every executor which also causes performance issue.


## Compatibility
- TiDB support: 4.x, 5.x and 6.x
- Spark support: 3.0, 3.1 and 3.2

## Test Design

### Functional Tests
- INSERT SQL write test
- Auth test
- Can't affect writing data by DataSource API

### Compatibility Tests
- Type convert test
- Compatible with different Spark versions.

| Spark version | Interface      |
|---------------|----------------|
| 3.0           | V1WriteBuilder | 
| 3.1           | V1WriteBuilder |
| 3.2           | V1Write        | 


