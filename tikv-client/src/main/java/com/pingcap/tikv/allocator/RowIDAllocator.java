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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pingcap.tikv.allocator;

import com.pingcap.tikv.ClientSession;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.codec.MetaCodec;
import com.pingcap.tikv.meta.TiTableInfo;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.TiSession;
import org.tikv.common.exception.AllocateRowIDOverflowException;
import org.tikv.common.exception.TiBatchWriteException;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.shade.com.google.common.primitives.UnsignedLongs;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.txn.TwoPhaseCommitter;

/**
 * RowIDAllocator read current start from TiKV and write back 'start+step' back to TiKV. It designs
 * to allocate all id for data to be written at once, hence it does not need run inside a txn.
 *
 * <p>(start, end] is allocated
 */
public final class RowIDAllocator implements Serializable {

  private final long shardBits;
  private final boolean isUnsigned;
  private final long dbId;
  private final TiConfiguration conf;
  private final long step;
  private long end;
  private final long autoRandomPartition;
  private final RowIDAllocatorType allocatorType;
  private final long RowIDAllocatorTTL = 10000;

  private static final Logger LOG = LoggerFactory.getLogger(RowIDAllocator.class);

  private RowIDAllocator(
      long shardBits,
      boolean isUnsigned,
      long dbId,
      long step,
      TiConfiguration conf,
      TiTimestamp timestamp,
      RowIDAllocatorType allocatorType) {
    this.shardBits = shardBits;
    this.isUnsigned = isUnsigned;
    this.dbId = dbId;
    this.step = step;
    this.conf = conf;
    this.autoRandomPartition = new Random(timestamp.getVersion()).nextLong();
    this.allocatorType = allocatorType;
  }

  public long getAutoIncId(long index) {
    return index + getStart();
  }

  /**
   * @param index should >= 1
   * @return
   */
  public long getShardRowId(long index) {
    return getShardRowId(shardBits, index, index + getStart(), isUnsigned);
  }

  public long getAutoRandomId(long index) {
    return getShardRowId(shardBits, autoRandomPartition, index + getStart(), isUnsigned);
  }

  static long getShardRowId(long shardBits, long partitionIndex, long rowID, boolean isUnsigned) {
    if (shardBits <= 0 || shardBits >= 16) {
      return rowID;
    }
    int signBitLength = isUnsigned ? 0 : 1;
    // assert rowID < Math.pow(2, 64 - maxShardRowIDBits)

    long partition = partitionIndex & ((1L << shardBits) - 1);
    return rowID | (partition << (64 - shardBits - signBitLength));
  }

  public static RowIDAllocator createRowIDAllocator(
      long dbId,
      TiTableInfo tableInfo,
      TiConfiguration conf,
      long step,
      TiTimestamp timestamp,
      RowIDAllocatorType allocatorType) {
    long shardBits = 0;
    boolean isUnsigned = false;
    switch (allocatorType) {
      case AUTO_INCREMENT:
        isUnsigned = tableInfo.isAutoIncColUnsigned();
        // AUTO_INC doesn't have shard bits.
        break;
      case AUTO_RANDOM:
        isUnsigned = tableInfo.isAutoRandomColUnsigned();
        shardBits = tableInfo.getAutoRandomBits();
        break;
      case IMPLICIT_ROWID:
        // IMPLICIT_ROWID is always signed.
        shardBits = tableInfo.getMaxShardRowIDBits();
        break;
      default:
        throw new IllegalArgumentException("Unsupported RowIDAllocatorType: " + allocatorType);
    }
    return RowIDAllocator.create(
        dbId, tableInfo, conf, timestamp, isUnsigned, shardBits, step, allocatorType);
  }

  public static RowIDAllocator create(
      long dbId,
      TiTableInfo table,
      TiConfiguration conf,
      TiTimestamp timestamp,
      boolean unsigned,
      long shardBits,
      long step,
      RowIDAllocatorType allocatorType) {
    BackOffer backOffer =
        ConcreteBackOffer.newCustomBackOff(TiConfiguration.ROW_ID_ALLOCATOR_BACKOFF);
    while (true) {
      try {
        return doCreate(dbId, table, conf, timestamp, unsigned, shardBits, step, allocatorType);
      } catch (AllocateRowIDOverflowException | IllegalArgumentException e) {
        throw e;
      } catch (Exception e) {
        LOG.warn("error during allocating row id", e);
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoServerBusy, e);
      }
    }
  }

  private static RowIDAllocator doCreate(
      long dbId,
      TiTableInfo table,
      TiConfiguration conf,
      TiTimestamp timestamp,
      boolean unsigned,
      long shardBits,
      long step,
      RowIDAllocatorType allocatorType) {
    RowIDAllocator allocator =
        new RowIDAllocator(shardBits, unsigned, dbId, step, conf, timestamp, allocatorType);
    if (unsigned) {
      allocator.initUnsigned(
          ClientSession.getInstance(conf).createSnapshot(), table.getId(), shardBits);
    } else {
      allocator.initSigned(
          ClientSession.getInstance(conf).createSnapshot(), table.getId(), shardBits);
    }

    return allocator;
  }

  public long getStart() {
    return end - step;
  }

  public long getEnd() {
    return end;
  }

  // set key value pairs to tikv via two phase committer protocol.
  private void set(@Nonnull List<BytePairWrapper> pairs, @Nonnull TiTimestamp timestamp) {
    Iterator<BytePairWrapper> iterator = pairs.iterator();
    if (!iterator.hasNext()) {
      return;
    }
    TiSession session = ClientSession.getInstance(conf).getTiKVSession();
    TwoPhaseCommitter twoPhaseCommitter =
        new TwoPhaseCommitter(session, timestamp.getVersion(), RowIDAllocatorTTL);
    BytePairWrapper primaryPair = iterator.next();
    twoPhaseCommitter.prewritePrimaryKey(
        ConcreteBackOffer.newCustomBackOff(TiConfiguration.PREWRITE_MAX_BACKOFF),
        primaryPair.getKey(),
        primaryPair.getValue());

    if (iterator.hasNext()) {
      twoPhaseCommitter.prewriteSecondaryKeys(
          primaryPair.getKey(), iterator, TiConfiguration.PREWRITE_MAX_BACKOFF);
    }

    twoPhaseCommitter.commitPrimaryKey(
        ConcreteBackOffer.newCustomBackOff(TiConfiguration.BATCH_COMMIT_BACKOFF),
        primaryPair.getKey(),
        session.getTimestamp().getVersion());

    try {
      twoPhaseCommitter.close();
    } catch (Throwable ignored) {
    }
  }

  private Optional<BytePairWrapper> getMetaToUpdate(
      ByteString key, byte[] oldVal, Snapshot snapshot) {
    // 1. encode hash meta key
    // 2. load meta via hash meta key from TiKV
    // 3. update meta's filed count and set it back to TiKV
    CodecDataOutput cdo = new CodecDataOutput();
    ByteString metaKey = MetaCodec.encodeHashMetaKey(cdo, key.toByteArray());
    long fieldCount = 0;
    ByteString metaVal = snapshot.get(metaKey);

    // decode long from bytes
    // big endian the 8 bytes
    if (!metaVal.isEmpty()) {
      try {
        fieldCount = IntegerCodec.readULong(new CodecDataInput(metaVal.toByteArray()));
      } catch (Exception ignored) {
        LOG.warn("metaDecode failed, field is ignored." + KeyUtils.formatBytesUTF8(metaVal));
      }
    }

    // update meta field count only oldVal is null
    if (oldVal == null || oldVal.length == 0) {
      fieldCount++;
      cdo.reset();
      cdo.writeLong(fieldCount);

      return Optional.of(new BytePairWrapper(metaKey.toByteArray(), cdo.toBytes()));
    }
    return Optional.empty();
  }

  private long updateHash(
      ByteString key,
      ByteString field,
      Function<byte[], byte[]> calculateNewVal,
      Snapshot snapshot) {
    // 1. encode hash data key
    // 2. get value in byte from get operation
    // 3. calculate new value via calculateNewVal
    // 4. check old value equals to new value or not
    // 5. set the new value back to TiKV via 2pc
    // 6. encode a hash meta key
    // 7. update a hash meta field count if needed

    CodecDataOutput cdo = new CodecDataOutput();
    MetaCodec.encodeHashDataKey(cdo, key.toByteArray(), field.toByteArray());
    ByteString dataKey = cdo.toByteString();
    byte[] oldVal = snapshot.get(dataKey.toByteArray());

    byte[] newVal = calculateNewVal.apply(oldVal);
    if (Arrays.equals(newVal, oldVal)) {
      // not need to update
      return 0L;
    }

    List<BytePairWrapper> pairs = new ArrayList<>(2);
    pairs.add(new BytePairWrapper(dataKey.toByteArray(), newVal));
    getMetaToUpdate(key, oldVal, snapshot).ifPresent(pairs::add);
    set(pairs, snapshot.getTimestamp());
    return Long.parseLong(new String(newVal));
  }

  private static boolean isDBExisted(long dbId, Snapshot snapshot) {
    ByteString dbKey = MetaCodec.encodeDatabaseID(dbId);
    ByteString json = MetaCodec.hashGet(MetaCodec.KEY_DBs, dbKey, snapshot);
    return json != null && !json.isEmpty();
  }

  private static boolean isTableExisted(long dbId, long tableId, Snapshot snapshot) {
    ByteString dbKey = MetaCodec.encodeDatabaseID(dbId);
    ByteString tableKey = MetaCodec.tableKey(tableId);
    return !MetaCodec.hashGet(dbKey, tableKey, snapshot).isEmpty();
  }

  public static boolean shardRowBitsOverflow(
      long base, long step, long shardRowBits, boolean reservedSignBit) {
    long signBit = reservedSignBit ? 1 : 0;
    long mask = ((1L << shardRowBits) - 1) << (64 - shardRowBits - signBit);
    if (reservedSignBit) {
      return ((base + step) & mask) > 0;
    } else {
      return Long.compareUnsigned((base + step) & mask, 0) > 0;
    }
  }

  /**
   * read current row id from TiKV and write the calculated value back to TiKV. The calculation rule
   * is start(read from TiKV) + step.
   */
  public long updateAllocateId(
      long dbId, long tableId, long step, Snapshot snapshot, long shard, boolean hasSignedBit) {
    if (isDBExisted(dbId, snapshot) && isTableExisted(dbId, tableId, snapshot)) {
      ByteString idField = getIdField(tableId, allocatorType);
      return updateHash(
          MetaCodec.encodeDatabaseID(dbId),
          idField,
          (oldVal) -> {
            long base = 0;
            if (oldVal != null && oldVal.length != 0) {
              base = Long.parseLong(new String(oldVal));
            }
            if (shard >= 1 && shardRowBitsOverflow(base, step, shard, hasSignedBit)) {
              throw new AllocateRowIDOverflowException(base, step, shard);
            }
            base += step;
            return String.valueOf(base).getBytes();
          },
          snapshot);
    }

    throw new IllegalArgumentException("table or database is not existed");
  }

  public static ByteString getIdField(long tableId, RowIDAllocatorType allocatorType) {
    switch (allocatorType) {
      case AUTO_INCREMENT:
      case IMPLICIT_ROWID:
        return MetaCodec.autoTableIDKey(tableId);
      case AUTO_RANDOM:
        return MetaCodec.autoRandomTableIDKey(tableId);
      default:
        throw new IllegalArgumentException("Unsupported RowIDAllocatorType: " + allocatorType);
    }
  }

  /** read current row id from TiKV according to database id and table id. */
  public static long getAllocateId(
      long dbId, long tableId, Snapshot snapshot, RowIDAllocatorType allocatorType) {
    if (isDBExisted(dbId, snapshot) && isTableExisted(dbId, tableId, snapshot)) {
      ByteString dbKey = MetaCodec.encodeDatabaseID(dbId);
      ByteString idField = getIdField(tableId, allocatorType);
      ByteString val = MetaCodec.hashGet(dbKey, idField, snapshot);
      if (val.isEmpty()) {
        return 0L;
      }
      return Long.parseLong(val.toStringUtf8());
    }

    throw new IllegalArgumentException("table or database is not existed");
  }

  private void initSigned(Snapshot snapshot, long tableId, long shard) {
    // get new start from TiKV, and calculate new end and set it back to TiKV.
    long newStart = getAllocateId(dbId, tableId, snapshot, allocatorType);
    long tmpStep = Math.min(Long.MAX_VALUE - newStart, step);
    if (tmpStep != step) {
      throw new TiBatchWriteException("cannot allocate ids for this write");
    }
    if (newStart == Long.MAX_VALUE) {
      throw new TiBatchWriteException("cannot allocate more ids since it ");
    }
    end = updateAllocateId(dbId, tableId, tmpStep, snapshot, shard, true);
  }

  private void initUnsigned(Snapshot snapshot, long tableId, long shard) {
    // get new start from TiKV, and calculate new end and set it back to TiKV.
    long newStart = getAllocateId(dbId, tableId, snapshot, allocatorType);
    // for unsigned long, -1L is max value.
    long tmpStep = UnsignedLongs.min(-1L - newStart, step);
    if (tmpStep != step) {
      throw new TiBatchWriteException("cannot allocate ids for this write");
    }
    // when compare unsigned long, the min value is largest value.
    if (UnsignedLongs.compare(newStart, -1L) == 0) {
      throw new TiBatchWriteException(
          "cannot allocate more ids since the start reaches " + "unsigned long's max value ");
    }
    end = updateAllocateId(dbId, tableId, tmpStep, snapshot, shard, false);
  }

  public enum RowIDAllocatorType {
    AUTO_INCREMENT,
    AUTO_RANDOM,
    IMPLICIT_ROWID
  }
}
