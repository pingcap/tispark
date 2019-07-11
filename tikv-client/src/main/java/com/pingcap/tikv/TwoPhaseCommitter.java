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
import com.pingcap.tikv.codec.KeyUtils;
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
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.Op;
import org.tikv.kvproto.Metapb;

public class TwoPhaseCommitter {

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
  private static final int WRITE_BUFFER_SIZE = 32 * 1024;

  /**
   * TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's Key+Value size
   * below 768KB.
   */
  private static final int TXN_COMMIT_BATCH_SIZE = 768 * 1024;

  /** unit is millisecond */
  private static final long DEFAULT_BATCH_WRITE_LOCK_TTL = 3600000;

  private static final long MAX_RETRY_TIMES = 3;

  private static final Logger LOG = LoggerFactory.getLogger(TwoPhaseCommitter.class);

  private TxnKVClient kvClient;
  private RegionManager regionManager;

  /** start timestamp of transaction which get from PD */
  private final long startTs;

  /** unit is millisecond */
  private final long lockTTL;

  public TwoPhaseCommitter(TiConfiguration conf, long startTime) {
    this.kvClient = TiSessionCache.getSession(conf).createTxnClient();
    this.regionManager = kvClient.getRegionManager();
    this.startTs = startTime;
    this.lockTTL = DEFAULT_BATCH_WRITE_LOCK_TTL;
  }

  public TwoPhaseCommitter(TiConfiguration conf, long startTime, long lockTTL) {
    this.kvClient = TiSessionCache.getSession(conf).createTxnClient();
    this.regionManager = kvClient.getRegionManager();
    this.startTs = startTime;
    this.lockTTL = lockTTL;
  }

  public void close() throws Exception {}

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
    this.doPrewritePrimaryKeyWithRetry(
        backOffer, ByteString.copyFrom(primaryKey), ByteString.copyFrom(value));
  }

  private void doPrewritePrimaryKeyWithRetry(BackOffer backOffer, ByteString key, ByteString value)
      throws TiBatchWriteException {
    Pair<TiRegion, Metapb.Store> pair = this.regionManager.getRegionStorePairByKey(key);
    TiRegion tiRegion = pair.first;
    Metapb.Store store = pair.second;

    Kvrpcpb.Mutation mutation;
    if (!value.isEmpty()) {
      mutation = Kvrpcpb.Mutation.newBuilder().setKey(key).setValue(value).setOp(Op.Put).build();
    } else {
      mutation = Kvrpcpb.Mutation.newBuilder().setKey(key).setOp(Op.Del).build();
    }
    List<Kvrpcpb.Mutation> mutationList = Collections.singletonList(mutation);

    // send rpc request to tikv server
    long lockTTL = getTxnLockTTL(this.startTs);
    ClientRPCResult prewriteResult =
        this.kvClient.prewrite(
            backOffer, mutationList, key, lockTTL, this.startTs, tiRegion, store);
    if (!prewriteResult.isSuccess() && !prewriteResult.isRetry()) {
      throw new TiBatchWriteException("prewrite primary key error", prewriteResult.getException());
    }
    if (prewriteResult.isRetry()) {
      try {
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss,
            new GrpcException(
                String.format("Txn prewrite primary key failed, regionId=%s", tiRegion.getId()),
                prewriteResult.getException()));
        // re-split keys and commit again.
        this.doPrewritePrimaryKeyWithRetry(backOffer, key, value);
      } catch (GrpcException e) {
        String errorMsg =
            String.format(
                "Txn prewrite primary key error, re-split commit failed, regionId=%s, detail=%s",
                tiRegion.getId(), e.getMessage());
        throw new TiBatchWriteException(errorMsg, e);
      }
    }

    LOG.debug("prewrite primary key {} successfully", KeyUtils.formatBytes(key));
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
    doCommitPrimaryKeyWithRetry(backOffer, ByteString.copyFrom(key), commitTs);
  }

  private void doCommitPrimaryKeyWithRetry(BackOffer backOffer, ByteString key, long commitTs)
      throws TiBatchWriteException {
    Pair<TiRegion, Metapb.Store> pair = this.regionManager.getRegionStorePairByKey(key);
    TiRegion tiRegion = pair.first;
    Metapb.Store store = pair.second;
    ByteString[] keys = new ByteString[] {key};

    // send rpc request to tikv server
    ClientRPCResult commitResult =
        this.kvClient.commit(backOffer, keys, this.startTs, commitTs, tiRegion, store);

    if (!commitResult.isSuccess()) {
      if (!commitResult.isRetry()) {
        throw new TiBatchWriteException("commit primary key error", commitResult.getException());
      } else {
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss,
            new GrpcException(
                String.format("Txn commit primary key failed, regionId=%s", tiRegion.getId()),
                commitResult.getException()));
        // re-split keys and commit again.
        this.doCommitPrimaryKeyWithRetry(backOffer, key, commitTs);
      }
    }

    LOG.debug("commit primary key {} successfully", KeyUtils.formatBytes(key));
  }

  /**
   * 2pc - prewrite secondary keys
   *
   * @param primaryKey
   * @param pairs
   * @return
   */
  public void prewriteSecondaryKeys(byte[] primaryKey, Iterator<BytePairWrapper> pairs)
      throws TiBatchWriteException {
    Iterator<Pair<ByteString, ByteString>> byteStringKeys =
        new Iterator<Pair<ByteString, ByteString>>() {

          @Override
          public boolean hasNext() {
            return pairs.hasNext();
          }

          @Override
          public Pair<ByteString, ByteString> next() {
            BytePairWrapper pair = pairs.next();
            return new Pair<>(
                ByteString.copyFrom(pair.getKey()), ByteString.copyFrom(pair.getValue()));
          }
        };

    doPrewriteSecondaryKeys(ByteString.copyFrom(primaryKey), byteStringKeys);
  }

  private void doPrewriteSecondaryKeys(
      ByteString primaryKey, Iterator<Pair<ByteString, ByteString>> pairs)
      throws TiBatchWriteException {
    int totalSize = 0;
    while (pairs.hasNext()) {
      ByteString[] keyBytes = new ByteString[WRITE_BUFFER_SIZE];
      ByteString[] valueBytes = new ByteString[WRITE_BUFFER_SIZE];
      int size = 0;
      while (size < WRITE_BUFFER_SIZE && pairs.hasNext()) {
        Pair<ByteString, ByteString> pair = pairs.next();
        keyBytes[size] = pair.first;
        valueBytes[size] = pair.second;
        size++;
      }

      BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_PREWRITE_BACKOFF);
      doPrewriteSecondaryKeysInBatchesWithRetry(
          backOffer, primaryKey, keyBytes, valueBytes, size, 0);
      totalSize = totalSize + size;
    }
  }

  private void doPrewriteSecondaryKeysInBatchesWithRetry(
      BackOffer backOffer,
      ByteString primaryKey,
      ByteString[] keys,
      ByteString[] values,
      int size,
      int level)
      throws TiBatchWriteException {
    if (keys == null || keys.length == 0 || values == null || values.length == 0 || size <= 0) {
      // return success
      return;
    }

    Map<ByteString, Kvrpcpb.Mutation> mutations = new LinkedHashMap<>();
    for (int i = 0; i < size; i++) {
      ByteString key = keys[i];
      ByteString value = values[i];

      Kvrpcpb.Mutation mutation;
      if (!value.isEmpty()) {
        mutation =
            Kvrpcpb.Mutation.newBuilder().setKey(key).setValue(value).setOp(Kvrpcpb.Op.Put).build();
      } else {
        // value can be null (table with one primary key integer column, data is encoded in key)
        mutation = Kvrpcpb.Mutation.newBuilder().setKey(key).setOp(Kvrpcpb.Op.Del).build();
      }
      mutations.put(key, mutation);
    }

    // groups keys by region
    GroupKeyResult groupResult = this.groupKeysByRegion(keys, size);
    List<BatchKeys> batchKeyList = new LinkedList<>();
    Map<Pair<TiRegion, Metapb.Store>, List<ByteString>> groupKeyMap = groupResult.getGroupsResult();
    for (Pair<TiRegion, Metapb.Store> pair : groupKeyMap.keySet()) {
      TiRegion tiRegion = pair.first;
      Metapb.Store store = pair.second;
      this.appendBatchBySize(batchKeyList, tiRegion, store, groupKeyMap.get(pair), true, mutations);
    }

    // For prewrite, stop sending other requests after receiving first error.
    for (BatchKeys batchKeys : batchKeyList) {
      TiRegion oldRegion = batchKeys.getRegion();
      TiRegion currentRegion = this.regionManager.getRegionById(oldRegion.getId());
      if (oldRegion.equals(currentRegion)) {
        doPrewriteSecondaryKeySingleBatchWithRetry(backOffer, primaryKey, batchKeys, mutations);
      } else {
        if (level > MAX_RETRY_TIMES) {
          throw new TiBatchWriteException(
              String.format(
                  "> max retry number %s, oldRegion=%s, currentRegion=%s",
                  MAX_RETRY_TIMES, oldRegion, currentRegion));
        }
        LOG.debug(
            String.format(
                "oldRegion=%s != currentRegion=%s, will refetch region info and retry",
                oldRegion, currentRegion));
        retryPrewriteBatch(backOffer, primaryKey, batchKeys, mutations, level <= 0 ? 1 : level + 1);
      }
    }
  }

  private void retryPrewriteBatch(
      BackOffer backOffer,
      ByteString primaryKey,
      BatchKeys batchKeys,
      Map<ByteString, Kvrpcpb.Mutation> mutations,
      int level) {

    int size = batchKeys.getKeys().size();
    ByteString[] keyBytes = new ByteString[size];
    ByteString[] valueBytes = new ByteString[size];
    int i = 0;
    for (ByteString k : batchKeys.getKeys()) {
      keyBytes[i] = k;
      valueBytes[i] = mutations.get(k).getValue();
      i++;
    }
    doPrewriteSecondaryKeysInBatchesWithRetry(
        backOffer, primaryKey, keyBytes, valueBytes, size, level);
  }

  private void doPrewriteSecondaryKeySingleBatchWithRetry(
      BackOffer backOffer,
      ByteString primaryKey,
      BatchKeys batchKeys,
      Map<ByteString, Kvrpcpb.Mutation> mutations)
      throws TiBatchWriteException {
    LOG.debug("start prewrite secondary key, size={}", batchKeys.getKeys().size());

    List<ByteString> keyList = batchKeys.getKeys();
    int batchSize = keyList.size();
    List<Kvrpcpb.Mutation> mutationList = new ArrayList<>(batchSize);
    for (ByteString key : keyList) {
      mutationList.add(mutations.get(key));
    }
    // send rpc request to tikv server
    int txnSize = batchKeys.getKeys().size();
    long lockTTL = getTxnLockTTL(this.startTs, txnSize);
    ClientRPCResult prewriteResult =
        this.kvClient.prewrite(
            backOffer,
            mutationList,
            primaryKey,
            lockTTL,
            this.startTs,
            batchKeys.getRegion(),
            batchKeys.getStore());
    if (!prewriteResult.isSuccess() && !prewriteResult.isRetry()) {
      throw new TiBatchWriteException(
          "prewrite secondary key error", prewriteResult.getException());
    }
    if (prewriteResult.isRetry()) {
      LOG.debug("prewrite secondary key fail, will backoff and retry");
      try {
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss,
            new GrpcException(
                String.format(
                    "Txn prewrite secondary key SingleBatch failed, regionId=%s",
                    batchKeys.getRegion().getId()),
                prewriteResult.getException()));
        // re-split keys and commit again.
        retryPrewriteBatch(backOffer, primaryKey, batchKeys, mutations, 0);
      } catch (GrpcException e) {
        String errorMsg =
            String.format(
                "Txn prewrite secondary key SingleBatch error, re-split commit failed, regionId=%s, detail=%s",
                batchKeys.getRegion().getId(), e.getMessage());
        throw new TiBatchWriteException(errorMsg, e);
      }
    }
    LOG.debug("prewrite secondary key successfully, size={}", batchKeys.getKeys().size());
  }

  private void appendBatchBySize(
      List<BatchKeys> batchKeyList,
      TiRegion tiRegion,
      Metapb.Store store,
      List<ByteString> keys,
      boolean sizeIncludeValue,
      Map<ByteString, Kvrpcpb.Mutation> mutations) {
    int start;
    int end;
    int len = keys.size();
    for (start = 0; start < len; start = end) {
      int size = 0;
      for (end = start; end < len && size < TXN_COMMIT_BATCH_SIZE; end++) {
        if (sizeIncludeValue) {
          size += this.keyValueSize(keys.get(end), mutations);
        } else {
          size += this.keySize(keys.get(end));
        }
      }
      BatchKeys batchKeys = new BatchKeys(tiRegion, store, keys.subList(start, end));
      batchKeyList.add(batchKeys);
    }
  }

  private long keyValueSize(ByteString key, Map<ByteString, Kvrpcpb.Mutation> mutations) {
    long size = key.size();
    Kvrpcpb.Mutation mutation = mutations.get(key);
    if (mutation != null) {
      size += mutation.getValue().toByteArray().length;
    }

    return size;
  }

  private long keySize(ByteString key) {
    return key.size();
  }

  /**
   * 2pc - commit secondary keys
   *
   * @param keys
   * @param commitTs
   * @return
   */
  public void commitSecondaryKeys(Iterator<ByteWrapper> keys, long commitTs)
      throws TiBatchWriteException {

    Iterator<ByteString> byteStringKeys =
        new Iterator<ByteString>() {

          @Override
          public boolean hasNext() {
            return keys.hasNext();
          }

          @Override
          public ByteString next() {
            return ByteString.copyFrom(keys.next().bytes);
          }
        };

    doCommitSecondaryKeys(byteStringKeys, commitTs);
  }

  private void doCommitSecondaryKeys(Iterator<ByteString> keys, long commitTs)
      throws TiBatchWriteException {
    LOG.debug("start commit secondary key");

    int totalSize = 0;
    while (keys.hasNext()) {
      ByteString[] keyBytes = new ByteString[WRITE_BUFFER_SIZE];
      int size = 0;
      for (int i = 0; i < WRITE_BUFFER_SIZE; i++) {
        if (keys.hasNext()) {
          keyBytes[size] = keys.next();
          size++;
        } else {
          break;
        }
      }
      totalSize = totalSize + size;

      BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_COMMIT_BACKOFF);
      doCommitSecondaryKeys(backOffer, keyBytes, size, commitTs);
    }

    LOG.debug("commit secondary key successfully, total size={}", totalSize);
  }

  private void doCommitSecondaryKeys(
      BackOffer backOffer, ByteString[] keys, int size, long commitTs)
      throws TiBatchWriteException {
    if (keys == null || keys.length == 0 || size <= 0) {
      return;
    }

    // groups keys by region
    GroupKeyResult groupResult = this.groupKeysByRegion(keys, size);
    List<BatchKeys> batchKeyList = new LinkedList<>();
    Map<Pair<TiRegion, Metapb.Store>, List<ByteString>> groupKeyMap = groupResult.getGroupsResult();

    for (Pair<TiRegion, Metapb.Store> pair : groupKeyMap.keySet()) {
      TiRegion tiRegion = pair.first;
      Metapb.Store store = pair.second;
      this.appendBatchBySize(batchKeyList, tiRegion, store, groupKeyMap.get(pair), false, null);
    }

    // For prewrite, stop sending other requests after receiving first error.
    for (BatchKeys batchKeys : batchKeyList) {
      doCommitSecondaryKeySingleBatch(backOffer, batchKeys, commitTs);
    }
  }

  private void doCommitSecondaryKeySingleBatch(
      BackOffer backOffer, BatchKeys batchKeys, long commitTs) throws TiBatchWriteException {
    List<ByteString> keysCommit = batchKeys.getKeys();
    ByteString[] keys = new ByteString[keysCommit.size()];
    keysCommit.toArray(keys);
    // send rpc request to tikv server
    ClientRPCResult commitResult =
        this.kvClient.commit(
            backOffer, keys, this.startTs, commitTs, batchKeys.getRegion(), batchKeys.getStore());
    if (!commitResult.isSuccess()) {
      String error =
          String.format("Txn commit secondary key error, regionId=%s", batchKeys.getRegion());
      LOG.warn(error);
      throw new TiBatchWriteException("commit secondary key error", commitResult.getException());
    }
    LOG.debug("commit {} rows successfully", batchKeys.getKeys().size());
  }

  private GroupKeyResult groupKeysByRegion(ByteString[] keys, int size)
      throws TiBatchWriteException {
    Map<Pair<TiRegion, Metapb.Store>, List<ByteString>> groups = new HashMap<>();
    int index = 0;
    try {
      for (; index < size; index++) {
        ByteString key = keys[index];
        Pair<TiRegion, Metapb.Store> pair = this.regionManager.getRegionStorePairByKey(key);
        if (pair != null) {
          groups.computeIfAbsent(pair, e -> new LinkedList<>()).add(key);
        }
      }
    } catch (Exception e) {
      throw new TiBatchWriteException("Txn groupKeysByRegion error", e);
    }
    GroupKeyResult result = new GroupKeyResult();
    result.setGroupsResult(groups);
    return result;
  }

  private long getTxnLockTTL(long startTime) {
    // TODO: calculate txn lock ttl
    return this.lockTTL;
  }

  private long getTxnLockTTL(long startTime, int txnSize) {
    // TODO: calculate txn lock ttl
    return this.lockTTL;
  }
}
