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

import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.kvproto.Kvrpcpb.KvPair;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.operation.iterator.IndexScanIterator;
import com.pingcap.tikv.operation.iterator.ScanIterator;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;
import com.pingcap.tikv.util.RangeSplitter;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.pingcap.tikv.operation.iterator.CoprocessIterator.getHandleIterator;
import static com.pingcap.tikv.operation.iterator.CoprocessIterator.getRowIterator;
import static com.pingcap.tikv.util.KeyRangeUtils.makeRange;

public class Snapshot {
  private final TiTimestamp timestamp;
  private final TiSession session;
  private final TiConfiguration conf;

  public Snapshot(TiTimestamp timestamp, TiSession session) {
    this.timestamp = timestamp;
    this.session = session;
    this.conf = session.getConf();
  }

  public TiSession getSession() {
    return session;
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
    Pair<TiRegion, Store> pair = session.getRegionManager().getRegionStorePairByKey(key);
    RegionStoreClient client =
        RegionStoreClient.create(pair.first, pair.second, getSession());
    // TODO: Need to deal with lock error after grpc stable
    return client.get(ConcreteBackOffer.newGetBackOff(), key, timestamp.getVersion());
  }

  /**
   * Issue a table read request
   *
   * @param dagRequest DAG request for coprocessor
   * @return a Iterator that contains all result from this select request.
   */
  public Iterator<Row> tableRead(TiDAGRequest dagRequest) {
    if (dagRequest.isIndexScan()) {
      Iterator<Long> iter = getHandleIterator(
          dagRequest,
          RangeSplitter.newSplitter(session.getRegionManager()).splitRangeByRegion(dagRequest.getRanges()),
          session);
      return new IndexScanIterator(this, dagRequest, iter);
    } else {
      return getRowIterator(
          dagRequest,
          RangeSplitter.newSplitter(session.getRegionManager()).splitRangeByRegion(dagRequest.getRanges()),
          session);
    }
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
      Iterator<Long> iter = getHandleIterator(
          dagRequest,
          task,
          session);
      return new IndexScanIterator(this, dagRequest, iter);
    } else {
      return getRowIterator(
          dagRequest,
          task,
          session);
    }
  }

  /**
   * Below is lower level API for env like Spark which already did key range split Perform handle
   * scan
   *
   * @param dagRequest DAGRequest for coprocessor
   * @param tasks      RegionTask of the coprocessor request to send
   * @return Row iterator to iterate over resulting rows
   */
  public Iterator<Long> indexHandleRead(TiDAGRequest dagRequest, List<RegionTask> tasks) {
    return getHandleIterator(
        dagRequest,
        tasks,
        session
    );
  }

  public Iterator<KvPair> scan(ByteString startKey) {
    return new ScanIterator(
        startKey, conf.getScanBatchSize(), null, session, session.getRegionManager(), timestamp.getVersion());
  }

  // TODO: Need faster implementation, say concurrent version
  // Assume keys sorted
  public List<KvPair> batchGet(List<ByteString> keys) {
    TiRegion curRegion = null;
    Range<Key> curKeyRange = null;
    Pair<TiRegion, Store> lastPair;
    List<ByteString> keyBuffer = new ArrayList<>();
    List<KvPair> result = new ArrayList<>(keys.size());
    BackOffer backOffer = ConcreteBackOffer.newBatchGetMaxBackOff();
    for (ByteString key : keys) {
      if (curRegion == null || !curKeyRange.contains(Key.toRawKey(key))) {
        Pair<TiRegion, Store> pair = session.getRegionManager().getRegionStorePairByKey(key);
        lastPair = pair;
        curRegion = pair.first;
        curKeyRange = makeRange(curRegion.getStartKey(), curRegion.getEndKey());

        try (RegionStoreClient client =
                 RegionStoreClient.create(lastPair.first, lastPair.second, getSession())) {
          List<KvPair> partialResult = client.batchGet(backOffer, keyBuffer, timestamp.getVersion());
          // TODO: Add lock check
          result.addAll(partialResult);
        } catch (Exception e) {
          throw new TiClientInternalException("Error Closing Store client.", e);
        }
        keyBuffer = new ArrayList<>();
        keyBuffer.add(key);
      }
    }
    return result;
  }
}
