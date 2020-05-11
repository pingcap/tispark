# TiSpark Changelog
All notable changes to this project will be documented in this file.

## [TiSpark 2.1.9] 2020-05-11
### Fixes
- Fix desc temp view [#1328](https://github.com/pingcap/tispark/pull/1328)
- Fix prefix index on blob [#1334](https://github.com/pingcap/tispark/pull/1334)
- Shade io.opencensus to resolve grpc conflict [#1352](https://github.com/pingcap/tispark/pull/1352)
- Fix parition table isn't shown in show command [#1374](https://github.com/pingcap/tispark/pull/1374)
- Fix partition pruning when partition definition contains big integer [#1385](https://github.com/pingcap/tispark/pull/1385)
- Support TiDB-4.0 [#1398] https://github.com/pingcap/tispark/pull/1398

## [TiSpark 2.1.8] 2019-12-12
### Fixes
- Fix UnsupportedOperationException: using stream rather than removeIf [#1303](https://github.com/pingcap/tispark/pull/1303)

## [TiSpark 2.1.7] 2019-12-09
### Fixes
- Add task retry if tikv is down [#1207](https://github.com/pingcap/tispark/pull/1207)
- Fix output offsets: add field type to Constant and ColumnRef when encoding proto [#1231](https://github.com/pingcap/tispark/pull/1231)
- Register udf `ti_version` for every sparksession [#1258](https://github.com/pingcap/tispark/pull/1258)
- Add timezone check [#1275](https://github.com/pingcap/tispark/pull/1275)
- Disable set, enum and bit pushed down [#1242](https://github.com/pingcap/tispark/pull/1242)

## [TiSpark 2.1.6] 2019-11-08
### Fixes
- Fix TopN push down bug [#1185](https://github.com/pingcap/tispark/pull/1185)
- Consider nulls order in TopN pushdown [#1187](https://github.com/pingcap/tispark/pull/1187)
- Fix Stack Overflow Error when reading from partition table [#1179](https://github.com/pingcap/tispark/pull/1179)
- Fix parsing view table's json bug [#1174](https://github.com/pingcap/tispark/pull/1174)
- Fix No Matching column bug [#1162](https://github.com/pingcap/tispark/pull/1162)
- Fix behavior of estimateTableSize [#845](https://github.com/pingcap/tispark/pull/845)
- Fix Bit Type default value bug [#1148](https://github.com/pingcap/tispark/pull/1148)
- Fix fastxml security alert [#1127](https://github.com/pingcap/tispark/pull/1127)
- Fix bug: TiSpark Catalog has 10-20s delay [#1108](https://github.com/pingcap/tispark/pull/1108)
- Fix reading data from TiDB in Spark Structured Streaming [#1104](https://github.com/pingcap/tispark/pull/1104)

## [TiSpark 2.1.5] 2019-09-02
### Fixes
- Remove useless scala and jackson dependencies [#1079](https://github.com/pingcap/tispark/pull/1079)
- Fix range partition throw UnsupportedSyntaxException error [#1088](https://github.com/pingcap/tispark/pull/1088)
- Make TiSpark reading data from a hash partition table [#1089](https://github.com/pingcap/tispark/pull/1089)

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

## [TiSpark 2.2.0]
### New Features
* Natively support writing data to TiKV using Spark Data Source API
* Support select from partition table [#916](https://github.com/pingcap/tispark/pull/916)
* Release one tispark jar (both support Spark-2.3.x and Spark-2.4.x) instead of two [#933](https://github.com/pingcap/tispark/pull/933)
* Add spark version to tispark udf ti_version [#943](https://github.com/pingcap/tispark/pull/943)

## [TiSpark 2.1.2] 2019-07-29
### Fixes
* Fix improper response with region error [#922](https://github.com/pingcap/tispark/pull/922)
* Fix view parseing problem [#953](https://github.com/pingcap/tispark/pull/953)

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
