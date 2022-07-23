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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv;

import static com.pingcap.tikv.operation.iterator.CoprocessorIterator.getHandleIterator;
import static com.pingcap.tikv.operation.iterator.CoprocessorIterator.getRowIterator;
import static com.pingcap.tikv.operation.iterator.CoprocessorIterator.getTiChunkIterator;

import com.pingcap.tikv.columnar.TiChunk;
import com.pingcap.tikv.handle.Handle;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.operation.iterator.ConcreteScanIterator;
import com.pingcap.tikv.operation.iterator.IndexScanIterator;
import com.pingcap.tikv.row.Row;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.RangeSplitter;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.txn.KVClient;

public class Snapshot {
  private final TiTimestamp timestamp;

  public ClientSession getClientSession() {
    return clientSession;
  }

  private final ClientSession clientSession;

  public Snapshot(@Nonnull TiTimestamp timestamp, ClientSession clientSession) {
    this.timestamp = timestamp;
    this.clientSession = clientSession;
  }

  public TiSession getSession() {
    return clientSession.getTikvSession();
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
    try (KVClient client =
        new KVClient(
            getSession().getConf(), getSession().getRegionStoreClientBuilder(), getSession())) {
      return client.get(key, timestamp.getVersion());
    }
  }

  public List<BytePairWrapper> batchGet(int backOffer, List<byte[]> keys) {
    List<ByteString> list = new ArrayList<>();
    for (byte[] key : keys) {
      list.add(ByteString.copyFrom(key));
    }
    try (KVClient client =
        new KVClient(
            getSession().getConf(), getSession().getRegionStoreClientBuilder(), getSession())) {
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
      TiDAGRequest dagRequest, List<RangeSplitter.RegionTask> tasks, int numOfRows) {
    if (dagRequest.isDoubleRead()) {
      throw new UnsupportedOperationException(
          "double read case should first read handle in row-wise fashion");
    } else {
      return getTiChunkIterator(dagRequest, tasks, getClientSession(), numOfRows);
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
        RangeSplitter.newSplitter(getSession().getRegionManager())
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
  private Iterator<Row> tableReadRow(
      TiDAGRequest dagRequest, List<RangeSplitter.RegionTask> tasks) {
    if (dagRequest.isDoubleRead()) {
      Iterator<Handle> iter = getHandleIterator(dagRequest, tasks, getClientSession());
      return new IndexScanIterator(this, dagRequest, iter);
    } else {
      return getRowIterator(dagRequest, tasks, getClientSession());
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
  public Iterator<Handle> indexHandleRead(
      TiDAGRequest dagRequest, List<RangeSplitter.RegionTask> tasks) {
    return getHandleIterator(dagRequest, tasks, getClientSession());
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
        clientSession.getConf(),
        getSession().getRegionStoreClientBuilder(),
        prefix,
        nextPrefix,
        timestamp.getVersion());
  }

  public TiConfiguration getConf() {
    return clientSession.getConf();
  }
}
