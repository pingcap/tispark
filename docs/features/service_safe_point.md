# TiSpark service safe point

## Background

TiKV uses [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) to control transaction concurrency. When you update the data, the original data is not deleted immediately but is kept together with the new data, with a timestamp to distinguish the version. [Garbage Collection (GC)](https://docs.pingcap.com/tidb/stable/garbage-collection-overview) controlled by TiDB will clean the obsolete data before gc safe point periodically.

TiSpark guarantee transaction by the distributed reading of the same version. Thus, we need to ensure the version is always behind the GC safe point during the running of the Spark job, or you may get the wrong data.

## Service safe point

GC safe point will use the min service safe points between all services including TiDB. TiSpark can also register service safe points to pause the advancement of GC safe point during the running of the Spark job.

The following version of TiSpark will support service safe point:

- TiSpark > 3.1.2 for 3.1.x
- TiSpark > 3.2.0

## Workaround

If you are using the version of TiSpark that does not support service safe point. Try to use TiDB system variables [`tidb_gc_life_time`](https://docs.pingcap.com/tidb/stable/system-variables#tidb_gc_life_time-new-in-v50) to control the GC safe point. The GC safe point will be less than or equal to ${now-tidb_gc_life_time}.

Set `tidb_gc_life_time` bigger than execute time of TiSpark is a good practice. For example, If your job runs about 1-2 hours:

```
SET GLOBAL tidb_gc_life_time = '2h'
```

Do not forget to recover after the Spark job is finished.

```
SET GLOBAL tidb_gc_life_time = ${original_value}
```

## Configuration

TiSpark will pause the GC safe point no more than `spark.tispark.gc_max_wait_time` (86400s in default).