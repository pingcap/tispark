# TiSpark Design Documents

- Author(s): [`ajian`](http://github.com/ajian2002)
- Tracking Issue:[#2232](https://github.com/pingcap/tispark/issues/2232)
  ,[tikv/client-java#514](https://github.com/tikv/client-java/issues/514)

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
    * [Build and Dependences](#build-and-dependences)
    * [Configuration](#configuration)
        * [TiConfiguration](#tiConfiguration)
        * [TIDBOptions](#tidboptions)
    * [ClientSession](#clientsession)
        * [Previous module or method call relationship](#previous-module-or-method-call-relationship)
        * [Current  module or method call relationship](#current-module-or-method-call-relationship)
* [Compatibility](#compatibility)
* [Test Design](#test-design)
    * [Unit tests](#unit-tests)
    * [Integration tests](#integration-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [References](#references)

## Introduction

Replace `tispark's tikv-client` module, use `tikv's client-java` to interact
with [`tikv`](https:///github.com/tikv/tikv).

## Motivation or Background

Until the latest version 3.0.1, the interaction between `tispark` and `tikv` still uses the built-in
client (`tikv-client` module). The upstream (`tikv/client-java`) provides a dedicated Java client to
interact with `tikv`. Due to the duplication of a large number of functions, `tispark` hopes to use
the upstream client to handle the interaction with `tikv`, and migrate its own client module related
functions to the upstream, only maintain some information that `tispark` itself needs to retain to
reduce the complexity of `tispark`.

## Detailed Design

### Build and Dependences

First, I will import the upstream (referring
to [`tikv/client-java`](https://github.com/tikv/client-java)) maven dependency.

1. Considering that the current `tikv-client` module (referring to the internal implementation of
   the current `tispark/tikv-client`) relies on three `protobuf` repositories (`kvproto`,`raft-rs`
   and `tipb`, see `tikv-client/scripts/proto.sh` for details), in addition to the need for your own
   maintained `tipb`, the other two can be handed over to upstream maintenance.
2. Due to the introduction of upstream dependencies, some of the same dependencies on both sides may
   have version conflicts. Use maven's shade plugin and replace plugin to rename conflicting library
   package names to prevent conflicts.

### Configuration

Second, consider that there are differences and a lot of similarities between the upstream
configuration item (`TiConfiguration`) and the current configuration. So keep the current
configuration class, add the `ConverterUpstreamUtils.java` tool class, and convert the current
configuration into the upstream configuration as much as possible. The following is a partial
configuration comparison table. (See also the comments for the `convertTiConfiguration` method in
the `ConverterUpstreamUtils` class). (Not perfect, because some upstream configurations cannot be
modified).

Here are some notable configuration items

#### TiConfiguration

| item                       | tispark    | client-java | description                                     |
|----------------------------|------------|-------------|-------------------------------------------------|
| timeout                    | 10 minutes | 200ms       | convert                                         |
| maxFrameSize               | 2GB        | 512MB       | convert                                         |
| netWorkMappingName         | ""         | ""          | can't be converted,but has a same default value |
| downGradeThreshold         | 1000_0000  | 1000_0000   | can't be convert,but has same default value     |
| maxRequestKeyRangeSize     | 2_0000     | 2_0000      | can't be convert,but has same default value     |
| ~~ScanBatchSize~~ (delete) | 10480      | 10240       | can't be convert,deleted because it is not used |

#### TIDBOptions

| item                                | reasons                              |
|-------------------------------------|--------------------------------------|
| ~~txnPrewriteBatchSize~~   (delete) | client-java does not support setting |
| ~~txnCommitBatchSize~~     (delete) | client-java does not support setting |
| ~~writeBufferSize~~        (delete) | client-java does not support setting |
| ~~writeThreadPerTask~~     (delete) | client-java does not support setting |
| ~~retryCommitSecondaryKey~~(delete) | client-java does not support setting |
| ~~prewriteMaxRetryTimes~~  (delete) | client-java does not support setting |

### ClientSession

The code of the core module takes the `TiSession` class of the client module as the entry, calls
the `createSnapshot`, `getOrCreateSnapShotCatalog`and `getPdclient` methods, and uses the snapshot,
catalog, and `pdclient` classes as the cut points to connect other codes of the entire client
module. So the key is to replace the `tiSession` class and rewrite these methods, so that the core
module can not only enjoy the upstream functions, but also flexibly rewrite and expand the upstream.
Therefore, the `ClientSession` class was added, the `TiSession`, `PDClient` and other classes were
deleted for they will be maintained by the upstream, the Catalog and Snapshot classes were retained, and the `ClientSession` was used as a new
entry for the core module. `ClientSession` holds the upstream `TiSession` and the current original
Catalog and Snapshot.
This code replaces the original call to `TiSession` and creates and calls it via the `ClientSession`
configuration file.

#### Previous module or method call relationship

```mermaid
flowchart 
A[core]
B[other module]
C[tiSession]
D[Snapshot]
E[Catalog]
F[PDClient]
G[....]
H[TiConfiguaration]
subgraph <font color='green'>core</font>
A
end
subgraph <font color='green'>other</font>
B
end
subgraph <font color='green'>tikv-client</font>
C-->D
C-->E
C-->F
C-->H
C-->G
end
A-->C
B-->C

```

#### Current module or method call relationship

```mermaid

flowchart TD
A[core]
B[other module]
subgraph <font color='green'>core</font>
A
end
subgraph <font color='green'>other</font>
B
end
A-->C
B-->C
C[ClientSession]
D[Snapshot]
E[Catalog]
F[TiSession]
L[PDClient]
G[...]
H[tispark/TIConfiguration]
I[tikv/TIConfiguration]
C-->|<font color='red'>ticonfiguration converent</font> |F
subgraph <font color='green'>tispark/tikv-client</font>
C-->D
C-->E
C-->H
end
subgraph  <font color='green'>tikv/client-java</font>
F-->L
F-->I
F-->G
end
```

## Compatibility

- Not forward compatible, but users will not directly use `tispark client` related modules, so it
  does not affect users' use.
- When using `TiSession` afterwards, you need to use `ClientSession` instead. The method provided
  by `ClientSession` is preferred, and if it is not provided, `getTiKVSession()` is required to
  indirectly call the upstream richer method.
- It is recommended to add the `org.tikv.shade` prefix when you need to use the three
  packages (`io.grpc`, `com.google`, `io.netty`).

## Test Design

### Unit tests

Because we use the upstream client to replace the current client, most of the unit tests on the
current client can be deprecated, correctness and tests are guaranteed by upstream, and only some of
the module tests we need to keep are kept.

### Integration tests

**Ignore the case of the integration test `batchGetRetryTest` for now, the test needs to be
modified**.

Other tests can continue to be used, and the refactored code can pass the tests and maintain the
same behavior.

## Impacts & Risks

The upstream client code is copied from the old `tispark's tikv-client` module with some
modifications. After replication, we have made some fixes and enhancements to the current `tispark`
client that differ from upstream. Some new modifications to current client modules, especially bug
fixes, may need to be synced upstream. But some new features and performance improvements upstream
also made `tispark` available.

The refactoring effort reduces coupling and introduces some new challenges.

## Investigation & Alternatives

The modification refers to [`TiBigData#182`](https://github.com/tidb-incubator/TiBigData/pull/182).
The modification of the reference is to remove the parts related to the maintenance of the project
itself from the upstream dependencies of `client-java`, which is contrary to the purpose of this
refactoring. But it has certain reference ideas and meanings.

## References

[Move clustered-index related classes out of client-java into TiBigdata](https://github.com/tidb-incubator/TiBigData/pull/182)

[Incoporate TiSpark TiKV Java client](https://github.com/tikv/client-java/issues/514)
