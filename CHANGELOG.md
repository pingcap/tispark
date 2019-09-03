# TiSpark Changelog
All notable changes to this project will be documented in this file.

## [TiSpark 2.1.5] 2019-09-02
### Fixes
- Remove useless scala and jackson dependencies [#1079](https://github.com/pingcap/tispark/pull/1079)
- Fix range partition throw UnsupportedSyntaxException error [#1088](https://github.com/pingcap/tispark/pull/1088)
- Make TiSpark reading data from a hash partition table [#1089](https://github.com/pingcap/tispark/pull/1089)

## [TiSpark 2.2.0] 2019-08-30
### New Features
* Natively support writing data to TiKV (ACID) using Spark Data Source API

### Improvements
* Release one TiSpark jar (both support Spark-2.3.x and Spark-2.4.x) instead of two [#933](https://github.com/pingcap/tispark/pull/933)
* Add spark version to TiSpark's udf ti_version [#943](https://github.com/pingcap/tispark/pull/943)
* Bump grpc to 1.17 [#982](https://github.com/pingcap/tispark/pull/982)
* Add retry mechanism for `batchGet` [#986](https://github.com/pingcap/tispark/pull/986)

### Fixes
* Catch UnsupportedSyntaxException when generating partition expressions [#960](https://github.com/pingcap/tispark/pull/960)
* Fix TiSpark cannot read from a hash partition table [#966](https://github.com/pingcap/tispark/pull/966)
* Prohibit extra index data type pushdown when doing index scan to avoid decoding extra column [#995](https://github.com/pingcap/tispark/pull/995)
* Prohibit agg or groupby pushdown on double read [#1004](https://github.com/pingcap/tispark/pull/1004)

## [TiSpark 2.1.4] 2019-08-27
### Fixes
- Fix distinct without alias bug: disable pushdown aggregate with alias [#1055](https://github.com/pingcap/tispark/pull/1055)
- Fix reflection bug: pass in different arguments for different version of same function [#1037](https://github.com/pingcap/tispark/pull/1037)

## [TiSpark 2.1.3] 2019-08-15
### Fixes
- Fix cost model in table scan [#1023](https://github.com/pingcap/tispark/pull/1023)
- Fix index scan bug [#1024](https://github.com/pingcap/tispark/pull/1024)
- Prohibit aggregate or group by pushdown on double read [#1027](https://github.com/pingcap/tispark/pull/1027)
- Fix reflection bug for HDP release [#1017](https://github.com/pingcap/tispark/pull/1017)
- Fix scala compiler version [#1019](https://github.com/pingcap/tispark/pull/1019)

## [TiSpark 2.1.2] 2019-07-29
### Fixes
* Fix improper response with region error [#922](https://github.com/pingcap/tispark/pull/922)
* Fix view parsing problem [#953](https://github.com/pingcap/tispark/pull/953)

## [TiSpark 1.2.1]
### Fixes
* Fix count error, if advanceNextResponse is empty, we should read next region (#899)
* Use fixed version of proto (#898)

## [TiSpark 2.1.1]
### Fixes
* Add TiDB/TiKV/PD version and Spark version supported for each latest major release (#804) (#887)
* Fix incorrect timestamp of tidbMapDatabase (#862) (#885)
* Fix column size estimation (#858) (#884)
* Fix count error, if advanceNextResponse is empty, we should read next region (#878) (#882)
* Use fixed version of proto instead of master branch (#843) (#850)

## [TiSpark 2.1]
### Features
* Support range partition pruning (Beta) (#599)
* Support show columns command (#614)

### Fixes
* Fix build key ranges with xor expression (#576)
* Fix cannot initialize pd if using ipv6 address (#587)
* Fix default value bug (#596)
* Fix possible IndexOutOfBoundException in KeyUtils (#597)
* Fix outputOffset is incorrect when building DAGRequest (#615)
* Fix incorrect implementation of Key.next() (#648)
* Fix partition parser can't parser numerical value 0 (#651)
* Fix prefix length may be larger than the value used. (#668)
* Fix retry logic when scan meet lock (#666)
* Fix inconsistent timestamp (#676)
* Fix tempView may be unresolved when applying timestamp to plan (#690)
* Fix concurrent DAGRequest issue (#714)
* Fix downgrade scan logic (#725)
* Fix integer type default value should be parsed to long (#741)
* Fix index scan on partition table (#735)
* Fix KeyNotInRegion may occur when retrieving rows by handle (#755)
* Fix encode value long max (#761)
* Fix MatchErrorException may occur when Unsigned BigInt contains in group by columns (#780)
* Fix IndexOutOfBoundException when trying to get pd member (#788)

## [TiSpark 2.0]
### Features
* Work with Spark 2.3
* Support use `$database` statement
* Support show databases statement
* Support show tables statement
* No need to use `TiContext.mapTiDBDatabase`, use `$database.$table` to identify a table instead
* Support data type SET and ENUM
* Support data type YEAR
* Support data type TIME
* Support isolation level settings
* Support describe table command
* Support cache tables and uncache tables
* Support read from a TiDB partition table
* Support use TiDB as metastore

### Fixes
* Fix JSON parsing (#491)
* Fix count on empty table (#498)
* Fix ScanIterator unable to read from adjacent empty regions (#519)
* Fix possible NullPointerException when setting show_row_id true (#522)

### Improved
* Make ti version usable without selecting database (#545)

## [TiSpark 1.2]
### Fixes
* Fixes compatibility with PDServer #480

## [TiSpark 1.1]
### Fixes multiple bugs:
* Fix daylight saving time (DST) (#347)
* Fix count(1) result is always 0 if subquery contains limit (#346)
* Fix incorrect totalRowCount calculation (#353)
* Fix request fail with Key not in region after retrying NotLeaderError (#354)
* Fix ScanIterator logic where index may be out of bound (#357)
* Fix tispark-sql dbName (#379)
* Fix StoreNotMatch (#396)
* Fix utf8 prefix index (#400)
* Fix decimal decoding (#401)
* Refactor not leader logic (#412)
* Fix global temp view not visible in thriftserver (#437)

### Adds:
* Allow TiSpark retrieve row id (#367)
* Decode json to string (#417)

### Improvements:
* Improve PD connection issue's error log (#388)
* Add DB prefix option for TiDB tables (#416)

## [TiSpark 1.0.1]
* Fix unsigned index
* Compatible with TiDB before and since 48a42f

## [TiSpark 1.0 GA]
### New Features
TiSpark provides distributed computing of TiDB data using Apache Spark.

* Provide a gRPC communication framework to read data from TiKV
* Provide encoding and decoding of TiKV component data and communication protocol
* Provide calculation pushdown, which includes:
    - Aggregate pushdown
    - Predicate pushdown
    - TopN pushdown
    - Limit pushdown
* Provide index related support
    - Transform predicate into Region key range or secondary index
    - Optimize Index Only queries
    - Adaptive downgrade index scan to table scan per region
* Provide cost-based optimization
    - Support statistics
    - Select index
    - Estimate broadcast table cost
* Provide support for multiple Spark interfaces
    - Support Spark Shell
    - Support ThriftServer/JDBC
    - Support Spark-SQL interaction
    - Support PySpark Shell
    - Support SparkR
