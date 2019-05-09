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

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.*;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.Test;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;

public class RegionStoreClientTest extends MockServerTest {

  private RegionStoreClient createClient() {
    Metapb.Store store =
        Metapb.Store.newBuilder()
            .setAddress(LOCAL_ADDR + ":" + port)
            .setId(1)
            .setState(Metapb.StoreState.Up)
            .build();

    RegionStoreClient.RegionStoreClientBuilder builder = session.getRegionStoreClientBuilder();

    return builder.build(region, store);
  }

  @Test
  public void getTest() throws Exception {
    RegionStoreClient client = createClient();
    server.put("key1", "value1");
    ByteString value = client.get(defaultBackOff(), ByteString.copyFromUtf8("key1"), 1);
    assertEquals(ByteString.copyFromUtf8("value1"), value);

    server.putError("error1", KVMockServer.ABORT);
    try {
      client.get(defaultBackOff(), ByteString.copyFromUtf8("error1"), 1);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
    server.clearAllMap();
    client.close();
  }

  @Test
  public void batchGetTest() throws Exception {
    RegionStoreClient client = createClient();

    server.put("key1", "value1");
    server.put("key2", "value2");
    server.put("key4", "value4");
    server.put("key5", "value5");
    List<Kvrpcpb.KvPair> kvs =
        client.batchGet(
            defaultBackOff(),
            ImmutableList.of(ByteString.copyFromUtf8("key1"), ByteString.copyFromUtf8("key2")),
            1);
    assertEquals(2, kvs.size());
    kvs.forEach(
        kv ->
            assertEquals(
                kv.getKey().toStringUtf8().replace("key", "value"), kv.getValue().toStringUtf8()));

    server.putError("error1", KVMockServer.ABORT);
    try {
      client.batchGet(
          defaultBackOff(),
          ImmutableList.of(ByteString.copyFromUtf8("key1"), ByteString.copyFromUtf8("error1")),
          1);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
    server.clearAllMap();
    client.close();
  }

  @Test
  public void scanTest() throws Exception {
    RegionStoreClient client = createClient();

    server.put("key1", "value1");
    server.put("key2", "value2");
    server.put("key4", "value4");
    server.put("key5", "value5");
    List<Kvrpcpb.KvPair> kvs = client.scan(defaultBackOff(), ByteString.copyFromUtf8("key2"), 1);
    assertEquals(3, kvs.size());
    kvs.forEach(
        kv ->
            assertEquals(
                kv.getKey().toStringUtf8().replace("key", "value"), kv.getValue().toStringUtf8()));

    server.putError("error1", KVMockServer.ABORT);
    try {
      client.scan(defaultBackOff(), ByteString.copyFromUtf8("error1"), 1);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
    server.clearAllMap();
    client.close();
  }

  private static KeyRange createByteStringRange(ByteString sKey, ByteString eKey) {
    return KeyRange.newBuilder().setStart(sKey).setEnd(eKey).build();
  }

  @Test
  public void coprocessTest() throws Exception {
    RegionStoreClient client = createClient();

    server.put("key1", "value1");
    server.put("key2", "value2");
    server.put("key4", "value4");
    server.put("key5", "value5");
    server.put("key6", "value6");
    server.put("key7", "value7");
    DAGRequest.Builder builder = DAGRequest.newBuilder();
    builder.setStartTs(1);
    builder.addExecutors(Executor.newBuilder().setTp(ExecType.TypeTableScan).build());
    List<KeyRange> keyRanges =
        ImmutableList.of(
            createByteStringRange(ByteString.copyFromUtf8("key1"), ByteString.copyFromUtf8("key4")),
            createByteStringRange(
                ByteString.copyFromUtf8("key6"), ByteString.copyFromUtf8("key7")));

    SelectResponse resp = coprocess(client, builder.build(), keyRanges);
    assertEquals(5, resp.getChunksCount());
    Set<String> results =
        ImmutableSet.copyOf(
            resp.getChunksList()
                .stream()
                .map(c -> c.getRowsData().toStringUtf8())
                .collect(Collectors.toList()));
    assertTrue(
        results.containsAll(ImmutableList.of("value1", "value2", "value4", "value6", "value7")));

    builder = DAGRequest.newBuilder();
    builder.setStartTs(1);
    keyRanges =
        ImmutableList.of(
            createByteStringRange(
                ByteString.copyFromUtf8("error1"), ByteString.copyFromUtf8("error2")));

    server.putError("error1", KVMockServer.ABORT);
    try {
      coprocess(client, builder.build(), keyRanges);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
    server.clearAllMap();
    client.close();
  }

  private SelectResponse coprocess(
      RegionStoreClient client, DAGRequest request, List<Coprocessor.KeyRange> ranges) {
    BackOffer backOffer = defaultBackOff();
    Queue<SelectResponse> responseQueue = new ArrayDeque<>();

    client.coprocess(backOffer, request, ranges, responseQueue);

    List<Chunk> resultChunk = new ArrayList<>();
    while (!responseQueue.isEmpty()) {
      SelectResponse response = responseQueue.poll();
      if (response != null) {
        resultChunk.addAll(response.getChunksList());
      }
    }

    return SelectResponse.newBuilder().addAllChunks(resultChunk).build();
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(1000);
  }
}
