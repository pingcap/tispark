# TiSpark Follower Read Feature

The Follower Read feature supports reading with follower or learner replica of a region under the premise of strongly consistent reads.

One use case for follower read is to isolate traffic of OLTP and OLAP (queries from TiSpark) 

## Configuration

You can control the follower read with the following configs:

- spark.tispark.replica_read (leader by default): read data from specified role. The optional roles are leader, follower and learner. You can also specify multiple roles, and we will pick the roles you specify in order.
- spark.tispark.replica_read.label (empty by default): only select TiKV store match specified labels. Format: label_x=value_x,label_y=value_y
- spark.tispark.replica_read.address_whitelist (empty by default): only select TiKV store with given ip addresses. Split mutil addresses by `,`
- spark.tispark.replica_read.address_blacklist (empty by default): do not select TiKV store with given ip addresses. Split mutil addresses by `,`

Here are the rules of the configs:
1. Only the TiKV store matches the `spark.tispark.replica_read` can be selected.
2. Only The TiKV store matches the `spark.tispark.replica_read.label` or in the `spark.tispark.replica_read.address_whitelist` can be selected.
3. A TiKV store will be regarded as matched if it matches all the labels of `spark.tispark.replica_read.label`. It can also be regarded as matched if the `spark.tispark.replica_read.label` is empty.
4. The TiKV store in the `spark.tispark.replica_read.address_blacklist` will never be selected (even it in the `spark.tispark.replica_read.address_whitelist`)

Note that these configs are **session level**. It will not be changed in one spark session.

## Example

### Setup

Deploy TiDB with 3 TiKV nodes
```
tiup playground --kv 3
```

Make sure `replication.max-replicas` > 1 , you can set the config by 
```
show config where Name = 'replication.max-replicas'
set config pd `replication.max-replicas` = 3
```

### Follower Read

Add the following configs in your spark-default.conf:
```
spark.tispark.replica_read follower
```

The query will be performed in the follower region:

```
spark.sql("select * from tidb_catalog.db.table").show
```


You can also use the following configs to control the follower read，here is an example:

> Suppose there are three TiKV nodes. The address are 127.0.0.1:201610,127.0.0.1:20161,127.0.0.1:20162. You can control the store labels with [placement rule](https://docs.pingcap.com/tidb/dev/configure-placement-rules)

```
spark.tispark.replica_read.label key1=value1
spark.tispark.replica_read.address_whitelist 127.0.0.1:20161
spark.tispark.replica_read.address_blacklist 127.0.0.1:20160
```
According to the rules described in Configuration section:
- TiSpark can't read from 127.0.0.1:20160
- TiSpark can read from 127.0.0.1:20161 even though the store labels don't match `key1=value1`
- TiSpark can read from 127.0.0.1:20162 once the store labels match `key1=value1`
