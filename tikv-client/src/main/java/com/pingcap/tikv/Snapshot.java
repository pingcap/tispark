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

import static com.pingcap.tikv.operation.iterator.CoprocessorIterator.getHandleIterator;
import static com.pingcap.tikv.operation.iterator.CoprocessorIterator.getRowIterator;
import static com.pingcap.tikv.operation.iterator.CoprocessorIterator.getTiChunkIterator;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.columnar.TiChunk;
import com.pingcap.tikv.key.Handle;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.operation.iterator.ConcreteScanIterator;
import com.pingcap.tikv.operation.iterator.IndexScanIterator;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.RangeSplitter;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.tikv.kvproto.Kvrpcpb.KvPair;

public class Snapshot {
  private final TiTimestamp timestamp;
  private final TiSession session;
  private final TiConfiguration conf;

  public Snapshot(@Nonnull TiTimestamp timestamp, TiConfiguration conf) {
    this.timestamp = timestamp;
    this.conf = conf;
    this.session = TiSession.getInstance(conf);
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
    try (KVClient client = new KVClient(session.getConf(), session.getRegionStoreClientBuilder())) {
      return client.get(key, timestamp.getVersion());
    }
  }

  public List<BytePairWrapper> batchGet(int backOffer, List<byte[]> keys) {
    List<ByteString> list = new ArrayList<>();
    for (byte[] key : keys) {
      list.add(ByteString.copyFrom(key));
    }
    try (KVClient client = new KVClient(session.getConf(), session.getRegionStoreClientBuilder())) {
      List<KvPair> kvPairList =
          client.batchGet(
              ConcreteBackOffer.newCustomBackOff(backOffer), list, timestamp.getVersion());
      return kvPairList
          .stream()
          .map(
              kvPair ->
                  new BytePairWrapper(
                      kvPair.getKey().toByteArray(), kvPair.getValue().toByteArray()))
          .collect(Collectors.toList());
    }
  }

  public Iterator<TiChunk> tableReadChunk(
      TiDAGRequest dagRequest, List<RegionTask> tasks, int numOfRows) {
    if (dagRequest.isDoubleRead()) {
      throw new UnsupportedOperationException(
          "double read case should first read handle in row-wise fashion");
    } else {
      return getTiChunkIterator(dagRequest, tasks, getSession(), numOfRows);
    }
  }
  /**
   * Issue a table read request
   *
   * @param dagRequest DAG request for coprocessor
   * @return a Iterator that contains all result from this select request.
   */
  public Iterator<Row> tableReadRow(TiDAGRequest dagRequest, long physicalId) {
    return tableReadRow(
        dagRequest,
        RangeSplitter.newSplitter(session.getRegionManager())
            .splitRangeByRegion(
                dagRequest.getRangesByPhysicalId(physicalId), dagRequest.getStoreType()));
  }

  /**
   * Below is lower level API for env like Spark which already did key range split Perform table
   * scan
   *
   * @param dagRequest DAGRequest for coprocessor
   * @param tasks RegionTasks of the coprocessor request to send
   * @return Row iterator to iterate over resulting rows
   */
  private Iterator<Row> tableReadRow(TiDAGRequest dagRequest, List<RegionTask> tasks) {
    if (dagRequest.isDoubleRead()) {
      Iterator<Handle> iter = getHandleIterator(dagRequest, tasks, getSession());
      return new IndexScanIterator(this, dagRequest, iter);
    } else {
      return getRowIterator(dagRequest, tasks, getSession());
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
  public Iterator<Handle> indexHandleRead(TiDAGRequest dagRequest, List<RegionTask> tasks) {
    return getHandleIterator(dagRequest, tasks, session);
  }

  /**
   * scan all keys after startKey, inclusive
   *
   * @param startKey start of keys
   * @return iterator of kvPair
   */
  public Iterator<KvPair> scan(ByteString startKey) {
    return new ConcreteScanIterator(
        session.getConf(),
        session.getRegionStoreClientBuilder(),
        startKey,
        timestamp.getVersion(),
        Integer.MAX_VALUE);
  }

  /**
   * scan all keys with prefix
   *
   * @param prefix prefix of keys
   * @return iterator of kvPair
   */
  public Iterator<KvPair> scanPrefix(ByteString prefix) {
    ByteString nextPrefix = Key.toRawKey(prefix).nextPrefix().toByteString();
    return new ConcreteScanIterator(
        session.getConf(),
        session.getRegionStoreClientBuilder(),
        prefix,
        nextPrefix,
        timestamp.getVersion());
  }

  public TiConfiguration getConf() {
    return conf;
  }
}
