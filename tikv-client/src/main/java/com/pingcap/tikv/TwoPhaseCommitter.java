/*
 * Copyright 2019 PingCAP, Inc.
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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import com.pingcap.tikv.util.LogDesensitization;
import com.pingcap.tikv.util.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.Op;
import org.tikv.kvproto.Metapb;

public class TwoPhaseCommitter {

  /** buffer spark rdd iterator data into memory */
  private static final int WRITE_BUFFER_SIZE = 32 * 1024;

  /**
   * TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's Key+Value size
   * below 768KB.
   */
  private static final int TXN_COMMIT_BATCH_SIZE = 768 * 1024;

  /** unit is millisecond */
  private static final long DEFAULT_BATCH_WRITE_LOCK_TTL = 3600000;

  private static final Logger LOG = LoggerFactory.getLogger(TwoPhaseCommitter.class);
  /** start timestamp of transaction which get from PD */
  private final long startTs;
  /** unit is millisecond */
  private final long lockTTL;

  private final boolean retryCommitSecondaryKeys;

  private final TxnKVClient kvClient;
  private final RegionManager regionManager;

  private final long txnPrewriteBatchSize;
  private final long txnCommitBatchSize;
  private final int writeBufferSize;
  private final int writeThreadPerTask;
  private final int prewriteMaxRetryTimes;
  private final ExecutorService executorService;

  public TwoPhaseCommitter(TiConfiguration conf, long startTime) {
    this.kvClient = TiSession.getInstance(conf).createTxnClient();
    this.regionManager = kvClient.getRegionManager();
    this.startTs = startTime;
    this.lockTTL = DEFAULT_BATCH_WRITE_LOCK_TTL;
    this.retryCommitSecondaryKeys = true;
    this.txnPrewriteBatchSize = TXN_COMMIT_BATCH_SIZE;
    this.txnCommitBatchSize = TXN_COMMIT_BATCH_SIZE;
    this.writeBufferSize = WRITE_BUFFER_SIZE;
    this.writeThreadPerTask = 1;
    this.prewriteMaxRetryTimes = 3;
    this.executorService = createExecutorService();
  }

  public TwoPhaseCommitter(
      TiConfiguration conf,
      long startTime,
      long lockTTL,
      long txnPrewriteBatchSize,
      long txnCommitBatchSize,
      int writeBufferSize,
      int writeThreadPerTask,
      boolean retryCommitSecondaryKeys,
      int prewriteMaxRetryTimes) {
    this.kvClient = TiSession.getInstance(conf).createTxnClient();
    this.regionManager = kvClient.getRegionManager();
    this.startTs = startTime;
    this.lockTTL = lockTTL;
    this.retryCommitSecondaryKeys = retryCommitSecondaryKeys;
    this.txnPrewriteBatchSize = txnPrewriteBatchSize;
    this.txnCommitBatchSize = txnCommitBatchSize;
    this.writeBufferSize = writeBufferSize;
    this.writeThreadPerTask = writeThreadPerTask;
    this.prewriteMaxRetryTimes = prewriteMaxRetryTimes;
    this.executorService = createExecutorService();
  }

  private ExecutorService createExecutorService() {
    return Executors.newFixedThreadPool(
        writeThreadPerTask,
        new ThreadFactoryBuilder().setNameFormat("2pc-pool-%d").setDaemon(true).build());
  }

  public void close() throws Exception {
    if (executorService != null) {
      executorService.shutdownNow();
    }
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
    this.doPrewritePrimaryKeyWithRetry(
        backOffer, ByteString.copyFrom(primaryKey), ByteString.copyFrom(value));
  }

  private void doPrewritePrimaryKeyWithRetry(BackOffer backOffer, ByteString key, ByteString value)
      throws TiBatchWriteException {
    Pair<TiRegion, Metapb.Store> pair = this.regionManager.getRegionStorePairByKey(key, backOffer);
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

    LOG.info(
        "prewrite primary key {} successfully", LogDesensitization.hide(KeyUtils.formatBytes(key)));
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
    Pair<TiRegion, Metapb.Store> pair = this.regionManager.getRegionStorePairByKey(key, backOffer);
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

    LOG.info(
        "commit primary key {} successfully", LogDesensitization.hide(KeyUtils.formatBytes(key)));
  }

  /**
   * 2pc - prewrite secondary keys
   *
   * @param primaryKey
   * @param pairs
   * @return
   */
  public void prewriteSecondaryKeys(
      byte[] primaryKey, Iterator<BytePairWrapper> pairs, int maxBackOfferMS)
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

    doPrewriteSecondaryKeys(ByteString.copyFrom(primaryKey), byteStringKeys, maxBackOfferMS);
  }

  private void doPrewriteSecondaryKeys(
      ByteString primaryKey, Iterator<Pair<ByteString, ByteString>> pairs, int maxBackOfferMS)
      throws TiBatchWriteException {
    try {
      int taskBufferSize = writeThreadPerTask * 2;
      int totalSize = 0, cnt = 0;
      Pair<ByteString, ByteString> pair;
      ExecutorCompletionService<Void> completionService =
          new ExecutorCompletionService<>(executorService);
      while (pairs.hasNext()) {
        int size = 0;
        ByteString[] keyBytes = new ByteString[writeBufferSize];
        ByteString[] valueBytes = new ByteString[writeBufferSize];
        while (size < writeBufferSize && pairs.hasNext()) {
          pair = pairs.next();
          keyBytes[size] = pair.first;
          valueBytes[size] = pair.second;
          size++;
        }
        int curSize = size;
        cnt++;
        if (cnt > taskBufferSize) {
          // consume one task if reaches task limit
          completionService.take().get();
        }
        BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(maxBackOfferMS);
        completionService.submit(
            () -> {
              doPrewriteSecondaryKeysInBatchesWithRetry(
                  backOffer, primaryKey, keyBytes, valueBytes, curSize, 0);
              return null;
            });

        totalSize = totalSize + size;
      }

      for (int i = 0; i < Math.min(taskBufferSize, cnt); i++) {
        completionService.take().get();
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TiBatchWriteException("Current thread interrupted.", e);
    } catch (ExecutionException e) {
      throw new TiBatchWriteException("Execution exception met.", e);
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
    GroupKeyResult groupResult = Utils.groupKeysByRegion(this.regionManager, keys, size, backOffer);
    List<BatchKeys> batchKeyList = new LinkedList<>();
    Map<Pair<TiRegion, Metapb.Store>, List<ByteString>> groupKeyMap = groupResult.getGroupsResult();

    for (Map.Entry<Pair<TiRegion, Metapb.Store>, List<ByteString>> entry : groupKeyMap.entrySet()) {
      TiRegion tiRegion = entry.getKey().first;
      Metapb.Store store = entry.getKey().second;
      this.appendBatchBySize(batchKeyList, tiRegion, store, entry.getValue(), true, mutations);
    }

    // For prewrite, stop sending other requests after receiving first error.
    for (BatchKeys batchKeys : batchKeyList) {
      TiRegion oldRegion = batchKeys.getRegion();
      TiRegion currentRegion =
          this.regionManager.getRegionByKey(oldRegion.getStartKey(), backOffer);
      if (oldRegion.equals(currentRegion)) {
        doPrewriteSecondaryKeySingleBatchWithRetry(backOffer, primaryKey, batchKeys, mutations);
      } else {
        if (level > prewriteMaxRetryTimes) {
          throw new TiBatchWriteException(
              String.format(
                  "> max retry number %s, oldRegion=%s, currentRegion=%s",
                  prewriteMaxRetryTimes, oldRegion, currentRegion));
        }
        LOG.info(
            String.format(
                "oldRegion=%s != currentRegion=%s, will re-fetch region info and retry",
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
    LOG.info(
        "start prewrite secondary key, row={}, size={}KB, regionId={}",
        batchKeys.getKeys().size(),
        batchKeys.getSizeInKB(),
        batchKeys.getRegion().getId());

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
      LOG.info("prewrite secondary key fail, will backoff and retry");
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
    LOG.info(
        "prewrite secondary key successfully, row={}, size={}KB, regionId={}",
        batchKeys.getKeys().size(),
        batchKeys.getSizeInKB(),
        batchKeys.getRegion().getId());
  }

  private void appendBatchBySize(
      List<BatchKeys> batchKeyList,
      TiRegion tiRegion,
      Metapb.Store store,
      List<ByteString> keys,
      boolean sizeIncludeValue,
      Map<ByteString, Kvrpcpb.Mutation> mutations) {
    long commitBatchSize = sizeIncludeValue ? txnPrewriteBatchSize : txnCommitBatchSize;

    int start;
    int end;
    if (keys == null) {
      return;
    }
    int len = keys.size();
    for (start = 0; start < len; start = end) {
      int sizeInBytes = 0;
      for (end = start; end < len && sizeInBytes < commitBatchSize; end++) {
        if (sizeIncludeValue) {
          sizeInBytes += this.keyValueSize(keys.get(end), mutations);
        } else {
          sizeInBytes += this.keySize(keys.get(end));
        }
      }
      BatchKeys batchKeys = new BatchKeys(tiRegion, store, keys.subList(start, end), sizeInBytes);
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
  public void commitSecondaryKeys(Iterator<ByteWrapper> keys, long commitTs, int commitBackOfferMS)
      throws TiBatchWriteException {

    Iterator<ByteString> byteStringKeys =
        new Iterator<ByteString>() {

          @Override
          public boolean hasNext() {
            return keys.hasNext();
          }

          @Override
          public ByteString next() {
            return ByteString.copyFrom(keys.next().getBytes());
          }
        };

    doCommitSecondaryKeys(byteStringKeys, commitTs, commitBackOfferMS);
  }

  private void doCommitSecondaryKeys(
      Iterator<ByteString> keys, long commitTs, int commitBackOfferMS)
      throws TiBatchWriteException {
    try {
      int taskBufferSize = writeThreadPerTask * 2;
      int totalSize = 0, cnt = 0;
      ExecutorCompletionService<Void> completionService =
          new ExecutorCompletionService<>(executorService);
      while (keys.hasNext()) {
        int size = 0;
        ByteString[] keyBytes = new ByteString[writeBufferSize];
        while (size < writeBufferSize && keys.hasNext()) {
          keyBytes[size] = keys.next();
          size++;
        }
        int curSize = size;
        cnt++;
        if (cnt > taskBufferSize) {
          // consume one task if reaches task limit
          completionService.take().get();
        }
        BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(commitBackOfferMS);
        completionService.submit(
            () -> {
              doCommitSecondaryKeysWithRetry(backOffer, keyBytes, curSize, commitTs);
              return null;
            });

        totalSize = totalSize + size;
      }

      for (int i = 0; i < Math.min(taskBufferSize, cnt); i++) {
        completionService.take().get();
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TiBatchWriteException("Current thread interrupted.", e);
    } catch (ExecutionException e) {
      throw new TiBatchWriteException("Execution exception met.", e);
    }
  }

  private void doCommitSecondaryKeysWithRetry(
      BackOffer backOffer, ByteString[] keys, int size, long commitTs)
      throws TiBatchWriteException {
    if (keys == null || keys.length == 0 || size <= 0) {
      return;
    }

    // groups keys by region
    GroupKeyResult groupResult = Utils.groupKeysByRegion(this.regionManager, keys, size, backOffer);
    List<BatchKeys> batchKeyList = new ArrayList<>();
    Map<Pair<TiRegion, Metapb.Store>, List<ByteString>> groupKeyMap = groupResult.getGroupsResult();

    for (Map.Entry<Pair<TiRegion, Metapb.Store>, List<ByteString>> entry : groupKeyMap.entrySet()) {
      TiRegion tiRegion = entry.getKey().first;
      Metapb.Store store = entry.getKey().second;
      this.appendBatchBySize(batchKeyList, tiRegion, store, entry.getValue(), false, null);
    }

    for (BatchKeys batchKeys : batchKeyList) {
      doCommitSecondaryKeySingleBatchWithRetry(backOffer, batchKeys, commitTs);
    }
  }

  private void doCommitSecondaryKeySingleBatchWithRetry(
      BackOffer backOffer, BatchKeys batchKeys, long commitTs) throws TiBatchWriteException {
    LOG.info(
        "start commit secondary key, row={}, size={}KB, regionId={}",
        batchKeys.getKeys().size(),
        batchKeys.getSizeInKB(),
        batchKeys.getRegion().getId());
    List<ByteString> keysCommit = batchKeys.getKeys();
    ByteString[] keys = new ByteString[keysCommit.size()];
    keysCommit.toArray(keys);
    // send rpc request to tikv server
    ClientRPCResult commitResult =
        this.kvClient.commit(
            backOffer, keys, this.startTs, commitTs, batchKeys.getRegion(), batchKeys.getStore());
    if (retryCommitSecondaryKeys && commitResult.isRetry()) {
      doCommitSecondaryKeysWithRetry(backOffer, keys, keysCommit.size(), commitTs);
    } else if (!commitResult.isSuccess()) {
      String error =
          String.format("Txn commit secondary key error, regionId=%s", batchKeys.getRegion());
      LOG.warn(error);
      throw new TiBatchWriteException("commit secondary key error", commitResult.getException());
    }
    LOG.info(
        "commit {} rows successfully, size={}KB, regionId={}",
        batchKeys.getKeys().size(),
        batchKeys.getSizeInKB(),
        batchKeys.getRegion().getId());
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
