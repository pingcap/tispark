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

import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.txn.TxnKVClient;
import com.pingcap.tikv.txn.type.BatchKeys;
import com.pingcap.tikv.txn.type.ClientRPCResult;
import com.pingcap.tikv.txn.type.GroupKeyResult;
import com.pingcap.tikv.util.BackOffFunction;
import com.pingcap.tikv.util.BackOffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;

public class TiBatchWrite2PCClient {

  public static class ByteWrapper {
    private byte[] bytes;

    public ByteWrapper(byte[] bytes) {
      this.bytes = bytes;
    }

    public byte[] getBytes() {
      return this.bytes;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ByteWrapper that = (ByteWrapper) o;

      return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }

  public static class BytePairWrapper {
    private byte[] key;
    private byte[] value;

    public BytePairWrapper(byte[] key, byte[] value) {
      this.key = key;
      this.value = value;
    }

    public byte[] getKey() {
      return key;
    }

    public byte[] getValue() {
      return value;
    }
  }

  /** buffer spark rdd iterator data into memory */
  private static final int WRITE_BUFFER_SIZE = 1024 * 1024;

  /**
   * TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's Key+Value size
   * below 16KB.
   */
  private static final int TXN_COMMIT_BATCH_SIZE = 16 * 1024;

  /** unit is second */
  private static final long DEFAULT_BATCH_WRITE_LOCK_TTL = 3000;

  private static final Logger LOG = LoggerFactory.getLogger(TiBatchWrite2PCClient.class);

  private TxnKVClient kvClient;
  private RegionManager regionManager;

  /** start timestamp of transaction which get from PD */
  private long startTs = 0;

  public TiBatchWrite2PCClient(TxnKVClient kvClient, long startTime) {
    this.kvClient = kvClient;
    this.regionManager = kvClient.getRegionManager();
    this.startTs = startTime;
  }

  /**
   * 2pc - prewrite primary key
   *
   * @param backOffer
   * @param primaryKey
   * @param value
   * @return
   */
  public String prewritePrimaryKey(BackOffer backOffer, byte[] primaryKey, byte[] value) {
    return this.doPrewritePrimaryKey(backOffer, primaryKey, value);
  }

  private String doPrewritePrimaryKey(BackOffer backOffer, byte[] key, byte[] value) {
    TiRegion tiRegion = this.regionManager.getRegionByKey(ByteString.copyFrom(key));
    BatchKeys batchKeys = new BatchKeys(tiRegion.getId(), Arrays.asList(value));

    Kvrpcpb.Mutation mutation =
        Kvrpcpb.Mutation.newBuilder()
            .setKey(ByteString.copyFrom(key))
            .setValue(ByteString.copyFrom(value))
            .setOp(Kvrpcpb.Op.Put)
            .build();
    List<Kvrpcpb.Mutation> mutationList = Collections.singletonList(mutation);

    // send rpc request to tikv server
    long regionId = batchKeys.getRegioId();
    long lockTTL = getTxnLockTTL(this.startTs);
    ClientRPCResult prewriteResult =
        this.kvClient.prewrite(backOffer, mutationList, key, lockTTL, this.startTs, regionId);
    if (!prewriteResult.isSuccess() && !prewriteResult.isRetry()) {
      return prewriteResult.getError();
    }
    if (!prewriteResult.isSuccess() && prewriteResult.isRetry()) {
      try {
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss,
            new GrpcException(
                String.format(
                    "Txn prewrite primary key failed, regionId=%s, detail=%s",
                    batchKeys.getRegioId(), prewriteResult.getError())));
        // re-split keys and commit again.
        return this.doPrewritePrimaryKey(backOffer, key, value);
      } catch (GrpcException e) {
        String error =
            String.format(
                "Txn prewrite primary key error, re-split commit failed, regionId=%s, detail=%s",
                batchKeys.getRegioId(), e.getMessage());
        LOG.error(error);
        return error;
      }
    }
    // success return
    return null;
  }

  /**
   * 2pc - commit primary key
   *
   * @param backOffer
   * @param key
   * @return
   */
  public String commitPrimaryKey(BackOffer backOffer, byte[] key, long commitTs) {
    return doCommitPrimaryKey(backOffer, key, commitTs);
  }

  private String doCommitPrimaryKey(BackOffer backOffer, byte[] key, long commitTs) {
    TiRegion tiRegion = this.regionManager.getRegionByKey(ByteString.copyFrom(key));
    long regionId = tiRegion.getId();
    byte[][] keys = new byte[][] {key};

    // send rpc request to tikv server
    ClientRPCResult commitResult =
        this.kvClient.commit(backOffer, keys, this.startTs, commitTs, regionId);
    if (!commitResult.isSuccess() && !commitResult.isRetry()) {
      String error = String.format("Txn commit primary key error, regionId=%s", regionId);
      LOG.error(error);
      return error;
    }
    if (!commitResult.isSuccess() && commitResult.isRetry()) {
      try {
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss,
            new GrpcException(
                String.format("Txn commit primary key failed, regionId=%s", regionId)));
        // re-split keys and commit again.
        String error = this.doCommitPrimaryKey(backOffer, key, commitTs);
        if (error != null) {
          LOG.error(error);
          return error;
        }
      } catch (GrpcException e) {
        String error =
            String.format(
                "Txn commit primary key error, re-split commit failed, regionId=%s", regionId);
        LOG.error(error);
        return error;
      }
    }
    if (!commitResult.isSuccess()) {
      String error =
          String.format(
              "Txn commit primary key error, regionId=%s, detail=%s",
              regionId, commitResult.getError());
      LOG.error(error);
      return error;
    }
    return null;
  }

  /**
   * 2pc - prewrite secondary keys
   *
   * @param backOffer
   * @param primaryKey
   * @param pairs
   * @return
   */
  public String prewriteSecondaryKeys(
      BackOffer backOffer, byte[] primaryKey, Iterator<BytePairWrapper> pairs) {
    return doPrewriteSecondaryKeys(backOffer, primaryKey, pairs);
  }

  private String doPrewriteSecondaryKeys(
      BackOffer backOffer, byte[] primaryKey, Iterator<BytePairWrapper> pairs) {
    while (pairs.hasNext()) {
      byte[][] keyBytes = new byte[WRITE_BUFFER_SIZE][];
      byte[][] valueBytes = new byte[WRITE_BUFFER_SIZE][];
      int size = 0;
      for (int i = 0; i < WRITE_BUFFER_SIZE; i++) {
        if (pairs.hasNext()) {
          BytePairWrapper pair = pairs.next();
          keyBytes[size] = pair.key;
          valueBytes[size] = pair.value;
          size++;
        } else {
          break;
        }
      }
      String error = doPrewriteSecondaryKeys(backOffer, primaryKey, keyBytes, valueBytes, size);
      if (error != null) {
        return error;
      }
    }
    return null;
  }

  private String doPrewriteSecondaryKeys(
      BackOffer backOffer, byte[] primaryKey, byte[][] keys, byte[][] values, int size) {
    if (keys == null || keys.length == 0 || values == null || values.length == 0 || size <= 0) {
      return null;
    }

    Map<ByteWrapper, Kvrpcpb.Mutation> mutations = new LinkedHashMap<>();
    for (int i = 0; i < size; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];

      if (value.length > 0) {
        Kvrpcpb.Mutation mutation =
            Kvrpcpb.Mutation.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .setOp(Kvrpcpb.Op.Put)
                .build();
        mutations.put(new ByteWrapper(key), mutation);
      }
    }

    // groups keys by region
    GroupKeyResult groupResult = this.groupKeysByRegion(backOffer, keys, size);
    if (groupResult.hasError()) {
      return groupResult.getErrorMsg();
    }

    boolean sizeKeyValue = true;
    List<BatchKeys> batchKeyList = new LinkedList<>();
    Map<Long, List<byte[]>> groupKeyMap = groupResult.getGroupsResult();
    for (Long regionId : groupKeyMap.keySet()) {
      this.appendBatchBySize(
          batchKeyList,
          regionId,
          groupKeyMap.get(regionId),
          sizeKeyValue,
          TXN_COMMIT_BATCH_SIZE,
          mutations);
    }

    // For prewrite, stop sending other requests after receiving first error.
    for (BatchKeys batchKeys : batchKeyList) {
      String error = doPrewriteSecondaryKeySingleBatch(backOffer, primaryKey, batchKeys, mutations);
      if (error != null) {
        return error;
      }
    }
    return null;
  }

  private String doPrewriteSecondaryKeySingleBatch(
      BackOffer backOffer,
      byte[] primaryKey,
      BatchKeys batchKeys,
      Map<ByteWrapper, Kvrpcpb.Mutation> mutations) {
    List<byte[]> keyList = batchKeys.getKeys();
    int batchSize = keyList.size();
    byte[][] keys = new byte[batchSize][];
    int index = 0;
    List<Kvrpcpb.Mutation> mutationList = new ArrayList<>(batchSize);
    for (byte[] key : keyList) {
      mutationList.add(mutations.get(new ByteWrapper(key)));
      keys[index++] = key;
    }
    // send rpc request to tikv server
    long regionId = batchKeys.getRegioId();
    int txnSize = batchKeys.getKeys().size();
    long lockTTL = getTxnLockTTL(this.startTs, txnSize);
    ClientRPCResult prewriteResult =
        this.kvClient.prewrite(
            backOffer, mutationList, primaryKey, lockTTL, this.startTs, regionId);
    if (!prewriteResult.isSuccess() && !prewriteResult.isRetry()) {
      return prewriteResult.getError();
    }
    if (!prewriteResult.isSuccess() && prewriteResult.isRetry()) {
      try {
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss,
            new GrpcException(
                String.format(
                    "Txn prewrite secondary key SingleBatch failed, regionId=%s, detail=%s",
                    batchKeys.getRegioId(), prewriteResult.getError())));
        // re-split keys and commit again.
        int size = batchKeys.getKeys().size();
        byte[][] keyBytes = new byte[size][];
        byte[][] valueBytes = new byte[size][];
        int i = 0;
        for (byte[] k : batchKeys.getKeys()) {
          keyBytes[i] = k;
          valueBytes[i] = mutations.get(new String(k)).getValue().toByteArray();
          i++;
        }
        return doPrewriteSecondaryKeys(backOffer, primaryKey, keyBytes, valueBytes, size);
      } catch (GrpcException e) {
        String error =
            String.format(
                "Txn prewrite secondary key SingleBatch error, re-split commit failed, regionId=%s, detail=%s",
                batchKeys.getRegioId(), e.getMessage());
        LOG.error(error);
        return error;
      }
    }
    // success return
    LOG.info("prewrite " + batchKeys.getKeys().size() + "rows successfully");
    return null;
  }

  private void appendBatchBySize(
      List<BatchKeys> batchKeyList,
      Long regionId,
      List<byte[]> keys,
      boolean sizeKeyValue,
      int limit,
      Map<ByteWrapper, Kvrpcpb.Mutation> mutations) {
    int start;
    int end;
    int len = keys.size();
    for (start = 0; start < len; start = end) {
      int size = 0;
      for (end = start; end < len && size < limit; end++) {
        if (sizeKeyValue) {
          size += this.keyValueSize(keys.get(end), mutations);
        } else {
          size += this.keySize(keys.get(end));
        }
      }
      BatchKeys batchKeys = new BatchKeys(regionId, keys.subList(start, end));
      batchKeyList.add(batchKeys);
    }
  }

  private long keyValueSize(byte[] key, Map<ByteWrapper, Kvrpcpb.Mutation> mutations) {
    long size = key.length;
    Kvrpcpb.Mutation mutation = mutations.get(new ByteWrapper(key));
    if (mutation != null) {
      size += mutation.getValue().toByteArray().length;
    }

    return size;
  }

  private long keySize(byte[] key) {
    return key.length;
  }

  /**
   * 2pc - commit secondary keys
   *
   * @param backOffer
   * @param keys
   * @return
   */
  public String commitSecondaryKeys(
      BackOffer backOffer, Iterator<ByteWrapper> keys, long commitTs) {
    return doCommitSecondaryKeys(backOffer, keys, commitTs);
  }

  private String doCommitSecondaryKeys(
      BackOffer backOffer, Iterator<ByteWrapper> keys, long commitTs) {
    while (keys.hasNext()) {
      byte[][] keyBytes = new byte[WRITE_BUFFER_SIZE][];
      int size = 0;
      for (int i = 0; i < WRITE_BUFFER_SIZE; i++) {
        if (keys.hasNext()) {
          keyBytes[size] = keys.next().bytes;
          size++;
        } else {
          break;
        }
      }
      String error = doCommitSecondaryKeys(backOffer, keyBytes, size, commitTs);
      if (error != null) {
        // ignore error
        LOG.warn("commit secondary key error: {}", error);
      }
    }
    return null;
  }

  private String doCommitSecondaryKeys(
      BackOffer backOffer, byte[][] keys, int size, long commitTs) {
    if (keys == null || keys.length == 0 || size <= 0) {
      return null;
    }

    // groups keys by region
    GroupKeyResult groupResult = this.groupKeysByRegion(backOffer, keys, size);
    if (groupResult.hasError()) {
      return groupResult.getErrorMsg();
    }

    boolean sizeKeyValue = false;
    List<BatchKeys> batchKeyList = new LinkedList<>();
    Map<Long, List<byte[]>> groupKeyMap = groupResult.getGroupsResult();
    for (Long regionId : groupKeyMap.keySet()) {
      this.appendBatchBySize(
          batchKeyList,
          regionId,
          groupKeyMap.get(regionId),
          sizeKeyValue,
          TXN_COMMIT_BATCH_SIZE,
          null);
    }

    // For prewrite, stop sending other requests after receiving first error.
    for (BatchKeys batchKeys : batchKeyList) {
      String error = doCommitSecondaryKeySingleBatch(backOffer, batchKeys, commitTs);
      if (error != null) {
        return error;
      }
    }
    return null;
  }

  private String doCommitSecondaryKeySingleBatch(
      BackOffer backOffer, BatchKeys batchKeys, long commitTs) {
    List<byte[]> keysCommit = batchKeys.getKeys();
    byte[][] keys = new byte[keysCommit.size()][];
    keysCommit.toArray(keys);
    // send rpc request to tikv server
    long regionId = batchKeys.getRegioId();
    ClientRPCResult commitResult =
        this.kvClient.commit(backOffer, keys, this.startTs, commitTs, regionId);
    if (!commitResult.isSuccess() && !commitResult.isRetry()) {
      String error =
          String.format("Txn commit secondary key error, regionId=%s", batchKeys.getRegioId());
      LOG.warn(error);
      return error;
    }
    if (!commitResult.isSuccess() && commitResult.isRetry()) {
      try {
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss,
            new GrpcException(
                String.format(
                    "Txn commit secondary key failed, regionId=%s", batchKeys.getRegioId())));
        // re-split keys and commit again.
        int size = batchKeys.getKeys().size();
        byte[][] keyBytes = new byte[size][];
        int i = 0;
        for (byte[] k : batchKeys.getKeys()) {
          keyBytes[i] = k;
          i++;
        }
        String error = this.doCommitSecondaryKeys(backOffer, keyBytes, size, commitTs);
        if (error != null) {
          LOG.warn(error);
          return error;
        }
      } catch (GrpcException e) {
        String error =
            String.format(
                "Txn commit secondary key error, re-split commit failed, regionId=%s",
                batchKeys.getKeys());
        LOG.warn(error);
        return error;
      }
    }
    if (!commitResult.isSuccess()) {
      String error =
          String.format(
              "Txn commit secondary key error, regionId=%s, detail=%s",
              batchKeys.getKeys(), commitResult.getError());
      LOG.warn(error);
      return error;
    }

    LOG.info("commit " + batchKeys.getKeys().size() + "rows successfully");
    return null;
  }

  private GroupKeyResult groupKeysByRegion(BackOffer backOffer, byte[][] keys, int size) {
    Map<Long, List<byte[]>> groups = new HashMap<>();
    long first = 0;
    int index = 0;
    String error = null;
    try {
      for (; index < size; index++) {
        byte[] key = keys[index];
        TiRegion tiRegion = this.regionManager.getRegionByKey(ByteString.copyFrom(key));
        if (tiRegion != null) {
          Long regionId = tiRegion.getId();
          if (index == 0) {
            first = regionId;
          }
          List<byte[]> groupItem = groups.computeIfAbsent(regionId, e -> new LinkedList<>());
          groupItem.add(key);
          groups.put(tiRegion.getId(), groupItem);
        }
      }
    } catch (Exception e) {
      error = String.format("Txn groupKeysByRegion error, %s", e.getMessage());
    }
    GroupKeyResult result = new GroupKeyResult();
    if (error == null) {
      result.setFirstRegion(first);
      result.setGroupsResult(groups);
    }
    result.setErrorMsg(error);
    return result;
  }

  private long getTxnLockTTL(long startTime) {
    // TODO: calculate txn lock ttl
    return DEFAULT_BATCH_WRITE_LOCK_TTL;
  }

  private long getTxnLockTTL(long startTime, int txnSize) {
    // TODO: calculate txn lock ttl
    return DEFAULT_BATCH_WRITE_LOCK_TTL;
  }
}
