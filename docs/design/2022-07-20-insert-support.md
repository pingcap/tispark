# TiSpark Design Documents

- Author(s): TrafalgarRicardoLu
- Tracking Issue: https://github.com/pingcap/tispark/issues/2462

## Table of Contents
- [Introduction](#introduction)
- [Motivation or Background](#motivation-or-background)
- [Goals](#goals)
- [API](#api)
- [Detailed Design](#detailed-design)
  * [Add new capability](#add-new-capability)
  * [Get write options](#get-write-options)
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

TiSpark is a connector that provides read, write and delete support.
But it only supports writing data using DataSource API, users can't use Spark SQL to write data.

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
Use spark SQL to write data in TiSpark.
```
// insert with spark sql
spark.sql("insert into tidb_db.db.table values(x, x...)(y, y...)...")
```

## Detailed Design
Here are the main steps:
- Add new capability
- Get write options
- Implement V2 interface using V1 code
- Compatible with different Spark versions

### Add new capability
Spark will check the capabilities of TiDBTable. If we don't add corresponding capability, Spark will throw an exception.

### Get write options
Different from writing using DataSource API, Spark SQL can't get required information like tidb address and username.
Users need to indicate required information before writing data by `spark.conf.set(key, value)`.
Then we can get it.

### Implement V2 interface using V1 code
Spark DataSource V2 is evolving and not suitable for TiSpark write model currently.
Spark provides us with an interface that is implemented by DataSource V1 and leverages the DataSource V2 write code path.

We can use this interface to implement INSERT SQL without rewriting all write codes.

### Compatible with different Spark versions
The interface we used to combine DataSource V1 and DataSource V2 is different between
Spark 3.0, 3.1, and 3.2. So we need to write code for each one.

To be specific, we will use V1WriteBuilder  in 3.0 and 3.1, and use V1Write in 3.2.

### Why not V2 write model
There is a deduplicate operation in TiSpark which will handle the problem that data have same unique key.
DataSource V2 will split data into different executors, and we can't control it. This may lead to different
executors write data with same unique key and cause data corruption.

Other problem is about region split. Currently, we split region to enable parallel write, each executor will write data to only one
region. Since we can't control how to split data in DataSource V2, each executor may receive data which
should be written to different region. If we don't do region split, it may cause performance issue. If we do, region split will be
done in every executor which also causes performance issue.


## Compatibility
- TiDB support: 4.x and 5.x
- Spark support: >= 3.0

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


