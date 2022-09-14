# TiSpark Follower Read Design Documents

- Author(s): shiyuhang0
- Tracking Issue: https://github.com/pingcap/tispark/issues/2534


## Introduction

New feature: TiSpark follower read

## Motivation or Background

The Follower Read feature supports reading with follower or learner replica of a region under the premise of strongly consistent reads.

TiSpark will support follower read after this doc.

## Detailed design

### New configuration
TiSpark supports follower read with the following configurations
- spark.tispark.replica_read：Read data from specified role. The optional roles are leader, follower and learner. You can also specify multiple roles, and we will pick the roles you specify in order. 
- spark.tispark.replica_read.label：Only select TiKV store match specified labels. Format: label_x=value_x,label_y=value_y 
- spark.tispark.replica_read.address_whitelist：Only select TiKV store with given IP addresses. 
- spark.tispark.replica_read.address_blacklist：Do not select TiKV store with given IP addresses.

The replica meet the following condition can be selected:
- matchRoles && (matchLabels || inWhitelist) && notInBlacklist

### Implement ReplicaSelector

TiKV client-java has provided an interface `ReplicaSelector` for select region replica. All you need to do is override the `select` method and return the selected stores (can be follower)

```
public interface ReplicaSelector extends Serializable {
  ReplicaSelector LEADER = new LeaderReplicaSelector();
  ReplicaSelector FOLLOWER = new FollowerReplicaSelector();
  ReplicaSelector LEADER_AND_FOLLOWER = new LeaderFollowerReplicaSelector();

  List<Store> select(Region region);
}
```

The new class `ReplicaReadPolicy` will extends the `ReplicaSelector` and do the following steps in select method
1. Get region's all stores
2. Pass and handle the configurations describe above
3. apply the rule `matchRoles && (matchLabels || inWhitelist) && notInBlacklist` to select the store
4. throw exception when there is no store be selected

### Initialize TiSession with ReplicaSelector

ReplicaReadPolicy will be created and set into TiConfiguration when we convert spark conf to TiConfiguration.

TiConfiguration is used to initialize TiSession. Then we can read TiKV data according to the select rule in ReplicaReadPolicy.

Note that TiSession will survive in a SparkSession. So the follower read is session level.


## Test Design

Follower Read test can be only triggered in GitHub action because it needs some additional configs for TiKV and PD.

TiSpark will not test if it really read from follower, because is hard to judge. It only tests if the read will success.

The tests are as follows:
- Test `spark.tispark.replica_read`: TiSpark should read success with `spark.tispark.replica_read=follower`
- Test `spark.tispark.replica_read.label`:
  - TiSpark should read success when at least one store match the labels
  - TiSpark should read fail when no store match the labels
- Test `spark.tispark.replica_read.address_whitelist`: TiSpark should read success when no store match the labels but at least one store is in the whitelist
- Test `spark.tispark.replica_read.address_blacklist`: TiSpark should fail when all the stores are in the blacklist
