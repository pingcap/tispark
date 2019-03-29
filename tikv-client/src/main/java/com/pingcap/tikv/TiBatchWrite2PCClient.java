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
import com.pingcap.tikv.exception.TiBatchWriteException;
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
  private final long startTs;

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
  public void prewritePrimaryKey(BackOffer backOffer, byte[] primaryKey, byte[] value)
      throws TiBatchWriteException {
    this.doPrewritePrimaryKey(backOffer, primaryKey, value);
  }

  private void doPrewritePrimaryKey(BackOffer backOffer, byte[] key, byte[] value)
      throws TiBatchWriteException {
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
      throw new TiBatchWriteException("prewrite primary key error: " + prewriteResult.getError());
    }
    if (prewriteResult.isRetry()) {
      try {
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss,
            new GrpcException(
                String.format(
                    "Txn prewrite primary key failed, regionId=%s, detail=%s",
                    batchKeys.getRegioId(), prewriteResult.getError())));
        // re-split keys and commit again.
        this.doPrewritePrimaryKey(backOffer, key, value);
      } catch (GrpcException e) {
        String error =
            String.format(
                "Txn prewrite primary key error, re-split commit failed, regionId=%s, detail=%s",
                batchKeys.getRegioId(), e.getMessage());
        LOG.error(error);
        throw new TiBatchWriteException("prewrite primary key error", e);
      }
    }
    // success return
    return;
  }

  /**
   * 2pc - commit primary key
   *
   * @param backOffer
   * @param key
   * @return
   */
  public void commitPrimaryKey(BackOffer backOffer, byte[] key, long commitTs)
      throws TiBatchWriteException {
    doCommitPrimaryKey(backOffer, key, commitTs);
  }

  private void doCommitPrimaryKey(BackOffer backOffer, byte[] key, long commitTs)
      throws TiBatchWriteException {
    TiRegion tiRegion = this.regionManager.getRegionByKey(ByteString.copyFrom(key));
    long regionId = tiRegion.getId();
    byte[][] keys = new byte[][] {key};

    // send rpc request to tikv server
    ClientRPCResult commitResult =
        this.kvClient.commit(backOffer, keys, this.startTs, commitTs, regionId);
    if (!commitResult.isSuccess() && !commitResult.isRetry()) {
      throw new TiBatchWriteException("commit primary key error: " + commitResult.getError());
    }
    if (commitResult.isRetry()) {
      try {
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss,
            new GrpcException(
                String.format("Txn commit primary key failed, regionId=%s", regionId)));
        // re-split keys and commit again.
        this.doCommitPrimaryKey(backOffer, key, commitTs);
      } catch (GrpcException e) {
        String error =
            String.format(
                "Txn commit primary key error, re-split commit failed, regionId=%s", regionId);
        LOG.error(error);
        throw new TiBatchWriteException("commit primary key error" + e);
      }
    }
    if (!commitResult.isSuccess()) {
      String error =
          String.format(
              "Txn commit primary key error, regionId=%s, detail=%s",
              regionId, commitResult.getError());
      LOG.error(error);
      throw new TiBatchWriteException("commit primary key error: " + commitResult.getError());
    }

    // return success
    return;
  }

  /**
   * 2pc - prewrite secondary keys
   *
   * @param backOffer
   * @param primaryKey
   * @param pairs
   * @return
   */
  public void prewriteSecondaryKeys(
      BackOffer backOffer, byte[] primaryKey, Iterator<BytePairWrapper> pairs)
      throws TiBatchWriteException {
    doPrewriteSecondaryKeys(backOffer, primaryKey, pairs);
  }

  private void doPrewriteSecondaryKeys(
      BackOffer backOffer, byte[] primaryKey, Iterator<BytePairWrapper> pairs)
      throws TiBatchWriteException {
    while (pairs.hasNext()) {
      byte[][] keyBytes = new byte[WRITE_BUFFER_SIZE][];
      byte[][] valueBytes = new byte[WRITE_BUFFER_SIZE][];
      int size = 0;
      while (size < WRITE_BUFFER_SIZE && pairs.hasNext()) {
        BytePairWrapper pair = pairs.next();
        keyBytes[size] = pair.key;
        valueBytes[size] = pair.value;
        size++;
      }
      doPrewriteSecondaryKeys(backOffer, primaryKey, keyBytes, valueBytes, size);
    }

    // return success
    return;
  }

  private void doPrewriteSecondaryKeys(
      BackOffer backOffer, byte[] primaryKey, byte[][] keys, byte[][] values, int size)
      throws TiBatchWriteException {
    if (keys == null || keys.length == 0 || values == null || values.length == 0 || size <= 0) {
      // return success
      return;
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
      throw new TiBatchWriteException("prewrite secondary key error: " + groupResult.getErrorMsg());
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
      doPrewriteSecondaryKeySingleBatch(backOffer, primaryKey, batchKeys, mutations);
    }

    // return success
    return;
  }

  private void doPrewriteSecondaryKeySingleBatch(
      BackOffer backOffer,
      byte[] primaryKey,
      BatchKeys batchKeys,
      Map<ByteWrapper, Kvrpcpb.Mutation> mutations)
      throws TiBatchWriteException {
    List<byte[]> keyList = batchKeys.getKeys();
    int batchSize = keyList.size();
    List<Kvrpcpb.Mutation> mutationList = new ArrayList<>(batchSize);
    for (byte[] key : keyList) {
      mutationList.add(mutations.get(new ByteWrapper(key)));
    }
    // send rpc request to tikv server
    long regionId = batchKeys.getRegioId();
    int txnSize = batchKeys.getKeys().size();
    long lockTTL = getTxnLockTTL(this.startTs, txnSize);
    ClientRPCResult prewriteResult =
        this.kvClient.prewrite(
            backOffer, mutationList, primaryKey, lockTTL, this.startTs, regionId);
    if (!prewriteResult.isSuccess() && !prewriteResult.isRetry()) {
      throw new TiBatchWriteException("prewrite secondary key error: " + prewriteResult.getError());
    }
    if (prewriteResult.isRetry()) {
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
          valueBytes[i] = mutations.get(new ByteWrapper(k)).getValue().toByteArray();
          i++;
        }
        doPrewriteSecondaryKeys(backOffer, primaryKey, keyBytes, valueBytes, size);
      } catch (GrpcException e) {
        String error =
            String.format(
                "Txn prewrite secondary key SingleBatch error, re-split commit failed, regionId=%s, detail=%s",
                batchKeys.getRegioId(), e.getMessage());
        LOG.error(error);
        throw new TiBatchWriteException("prewrite secondary key error", e);
      }
    }
    // success return
    LOG.info("prewrite " + batchKeys.getKeys().size() + "rows successfully");
    return;
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
  public void commitSecondaryKeys(BackOffer backOffer, Iterator<ByteWrapper> keys, long commitTs)
      throws TiBatchWriteException {
    doCommitSecondaryKeys(backOffer, keys, commitTs);
  }

  private void doCommitSecondaryKeys(BackOffer backOffer, Iterator<ByteWrapper> keys, long commitTs)
      throws TiBatchWriteException {
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
      doCommitSecondaryKeys(backOffer, keyBytes, size, commitTs);
    }
  }

  private void doCommitSecondaryKeys(BackOffer backOffer, byte[][] keys, int size, long commitTs)
      throws TiBatchWriteException {
    if (keys == null || keys.length == 0 || size <= 0) {
      return;
    }

    // groups keys by region
    GroupKeyResult groupResult = this.groupKeysByRegion(backOffer, keys, size);
    if (groupResult.hasError()) {
      LOG.warn("commit secondary key error: " + groupResult.getErrorMsg());
      return;
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
      doCommitSecondaryKeySingleBatch(backOffer, batchKeys, commitTs);
    }
    return;
  }

  private void doCommitSecondaryKeySingleBatch(
      BackOffer backOffer, BatchKeys batchKeys, long commitTs) throws TiBatchWriteException {
    List<byte[]> keysCommit = batchKeys.getKeys();
    byte[][] keys = new byte[keysCommit.size()][];
    keysCommit.toArray(keys);
    // send rpc request to tikv server
    long regionId = batchKeys.getRegioId();
    ClientRPCResult commitResult =
        this.kvClient.commit(backOffer, keys, this.startTs, commitTs, regionId);
    if (!commitResult.isSuccess()) {
      String error =
          String.format("Txn commit secondary key error, regionId=%s", batchKeys.getRegioId());
      LOG.warn(error);
      throw new TiBatchWriteException("commit secondary key error: " + commitResult.getError());
    }

    // return success
    LOG.info("commit " + batchKeys.getKeys().size() + "rows successfully");
    return;
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
          groups.computeIfAbsent(regionId, e -> new LinkedList<>()).add(key);
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
