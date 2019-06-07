/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv;

import static com.pingcap.tikv.operation.iterator.CoprocessIterator.getHandleIterator;
import static com.pingcap.tikv.operation.iterator.CoprocessIterator.getRowIterator;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.operation.iterator.IndexScanIterator;
import com.pingcap.tikv.operation.iterator.ScanIterator;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.txn.TxnKVClient;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.RangeSplitter;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import org.tikv.kvproto.Kvrpcpb.KvPair;

public class Snapshot {
  private final TiTimestamp timestamp;
  private final TiConfiguration conf;

  public Snapshot(@Nonnull TiTimestamp timestamp, TiConfiguration conf) {
    this.timestamp = timestamp;
    this.conf = conf;
  }

  public TiSession getSession() {
    return TiSessionCache.getSession(conf);
  }

  public long getVersion() {
    return timestamp.getVersion();
  }

  public TiTimestamp getTimestamp() {
    return timestamp;
  }

  public byte[] get(byte[] key) {
    ByteString keyString = ByteString.copyFrom(key);
    ByteString value = get(keyString);
    return value.toByteArray();
  }

  public ByteString get(ByteString key) {
    RegionStoreClient client = getSession().getRegionStoreClientBuilder().build(key);
    // TODO: Need to deal with lock error after grpc stable
    return client.get(ConcreteBackOffer.newGetBackOff(), key, timestamp.getVersion());
  }

  public void set(ByteString key, ByteString value) {
    TxnKVClient txnKVClient =
        new TxnKVClient(
            conf, getSession().getRegionStoreClientBuilder(), getSession().getPDClient());
    TwoPhaseCommitter twoPhaseCommitter =
        new TwoPhaseCommitter(txnKVClient, txnKVClient.getTimestamp().getVersion());
    twoPhaseCommitter.prewritePrimaryKey(
        ConcreteBackOffer.newCustomBackOff(BackOffer.PREWRITE_MAX_BACKOFF),
        key.toByteArray(),
        value.toByteArray());
    long commitTs = txnKVClient.getTimestamp().getVersion();
    twoPhaseCommitter.commitPrimaryKey(
        ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_COMMIT_BACKOFF),
        key.toByteArray(),
        commitTs);
  }

  /**
   * Issue a table read request
   *
   * @param dagRequest DAG request for coprocessor
   * @return a Iterator that contains all result from this select request.
   */
  public Iterator<Row> tableRead(TiDAGRequest dagRequest, long physicalId) {
    return tableRead(
        dagRequest,
        RangeSplitter.newSplitter(getSession().getRegionManager())
            .splitRangeByRegion(dagRequest.getRangesByPhysicalId(physicalId)));
  }

  /**
   * Below is lower level API for env like Spark which already did key range split Perform table
   * scan
   *
   * @param dagRequest DAGRequest for coprocessor
   * @param task RegionTask of the coprocessor request to send
   * @return Row iterator to iterate over resulting rows
   */
  public Iterator<Row> tableRead(TiDAGRequest dagRequest, List<RegionTask> task) {
    if (dagRequest.isDoubleRead()) {
      Iterator<Long> iter = getHandleIterator(dagRequest, task, getSession());
      return new IndexScanIterator(this, dagRequest, iter);
    } else {
      return getRowIterator(dagRequest, task, getSession());
    }
  }

  /**
   * Below is lower level API for env like Spark which already did key range split Perform handle
   * scan
   *
   * @param dagRequest DAGRequest for coprocessor
   * @param tasks RegionTask of the coprocessor request to send
   * @return Row iterator to iterate over resulting rows
   */
  public Iterator<Long> indexHandleRead(TiDAGRequest dagRequest, List<RegionTask> tasks) {
    return getHandleIterator(dagRequest, tasks, getSession());
  }

  public Iterator<KvPair> scan(ByteString startKey) {
    return new ScanIterator(startKey, getSession(), timestamp.getVersion());
  }
}
