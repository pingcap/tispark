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
package com.pingcap.tikv.allocator;

import static com.pingcap.tikv.util.BackOffer.ROW_ID_ALLOCATOR_BACKOFF;

import com.google.common.primitives.UnsignedLongs;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.TwoPhaseCommitter;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.MetaCodec;
import com.pingcap.tikv.exception.AllocateRowIDOverflowException;
import com.pingcap.tikv.exception.TiBatchWriteException;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.util.BackOffFunction;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RowIDAllocator read current start from TiKV and write back 'start+step' back to TiKV. It designs
 * to allocate all id for data to be written at once, hence it does not need run inside a txn.
 *
 * <p>(start, end] is allocated
 */
public final class RowIDAllocator implements Serializable {
  private final long maxShardRowIDBits;
  private final long dbId;
  private final TiConfiguration conf;
  private final long step;
  private long end;

  private static final Logger LOG = LoggerFactory.getLogger(RowIDAllocator.class);

  private RowIDAllocator(long maxShardRowIDBits, long dbId, long step, TiConfiguration conf) {
    this.maxShardRowIDBits = maxShardRowIDBits;
    this.dbId = dbId;
    this.step = step;
    this.conf = conf;
  }

  public long getAutoIncId(long index) {
    return index + getStart();
  }

  /**
   * @param index should >= 1
   * @return
   */
  public long getShardRowId(long index) {
    return getShardRowId(maxShardRowIDBits, index, index + getStart());
  }

  static long getShardRowId(long maxShardRowIDBits, long partitionIndex, long rowID) {
    if (maxShardRowIDBits <= 0 || maxShardRowIDBits >= 16) {
      return rowID;
    }

    // assert rowID < Math.pow(2, 64 - maxShardRowIDBits)

    long partition = partitionIndex & ((1L << maxShardRowIDBits) - 1);
    return rowID | (partition << (64 - maxShardRowIDBits - 1));
  }

  public static RowIDAllocator create(
      long dbId, TiTableInfo table, TiConfiguration conf, boolean unsigned, long step) {
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(ROW_ID_ALLOCATOR_BACKOFF);
    while (true) {
      try {
        return doCreate(dbId, table, conf, unsigned, step);
      } catch (AllocateRowIDOverflowException | IllegalArgumentException e) {
        throw e;
      } catch (Exception e) {
        LOG.warn("error during allocating row id", e);
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoServerBusy, e);
      }
    }
  }

  private static RowIDAllocator doCreate(
      long dbId, TiTableInfo table, TiConfiguration conf, boolean unsigned, long step) {
    RowIDAllocator allocator = new RowIDAllocator(table.getMaxShardRowIDBits(), dbId, step, conf);
    if (unsigned) {
      allocator.initUnsigned(
          TiSession.getInstance(conf).createSnapshot(),
          table.getId(),
          table.getMaxShardRowIDBits());
    } else {
      allocator.initSigned(
          TiSession.getInstance(conf).createSnapshot(),
          table.getId(),
          table.getMaxShardRowIDBits());
    }

    return allocator;
  }

  public long getStart() {
    return end - step;
  }

  public long getEnd() {
    return end;
  }

  // set key value pair to tikv via two phase committer protocol.
  private void set(ByteString key, byte[] value) {
    TiSession session = TiSession.getInstance(conf);
    TwoPhaseCommitter twoPhaseCommitter =
        new TwoPhaseCommitter(conf, session.getTimestamp().getVersion());

    twoPhaseCommitter.prewritePrimaryKey(
        ConcreteBackOffer.newCustomBackOff(BackOffer.PREWRITE_MAX_BACKOFF),
        key.toByteArray(),
        value);

    twoPhaseCommitter.commitPrimaryKey(
        ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_COMMIT_BACKOFF),
        key.toByteArray(),
        session.getTimestamp().getVersion());

    try {
      twoPhaseCommitter.close();
    } catch (Throwable ignored) {
    }
  }

  private void updateMeta(ByteString key, byte[] oldVal, Snapshot snapshot) {
    // 1. encode hash meta key
    // 2. load meta via hash meta key from TiKV
    // 3. update meta's filed count and set it back to TiKV
    CodecDataOutput cdo = new CodecDataOutput();
    ByteString metaKey = MetaCodec.encodeHashMetaKey(cdo, key.toByteArray());
    long fieldCount;
    ByteString metaVal = snapshot.get(metaKey);

    // decode long from bytes
    // big endian the 8 bytes
    fieldCount = new CodecDataInput(metaVal.toByteArray()).readLong();

    // update meta field count only oldVal is null
    if (oldVal == null || oldVal.length == 0) {
      fieldCount++;
      cdo.reset();
      cdo.writeLong(fieldCount);

      set(metaKey, cdo.toBytes());
    }
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

    set(dataKey, newVal);
    updateMeta(key, oldVal, snapshot);
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
  public long udpateAllocateId(
      long dbId, long tableId, long step, Snapshot snapshot, long shard, boolean hasSignedBit) {
    if (isDBExisted(dbId, snapshot) && isTableExisted(dbId, tableId, snapshot)) {
      return updateHash(
          MetaCodec.encodeDatabaseID(dbId),
          MetaCodec.autoTableIDKey(tableId),
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

  /** read current row id from TiKV according to database id and table id. */
  public static long getAllocateId(long dbId, long tableId, Snapshot snapshot) {
    if (isDBExisted(dbId, snapshot) && isTableExisted(dbId, tableId, snapshot)) {
      ByteString dbKey = MetaCodec.encodeDatabaseID(dbId);
      ByteString tblKey = MetaCodec.autoTableIDKey(tableId);
      ByteString val = MetaCodec.hashGet(dbKey, tblKey, snapshot);
      if (val.isEmpty()) return 0L;
      return Long.parseLong(val.toStringUtf8());
    }

    throw new IllegalArgumentException("table or database is not existed");
  }

  private void initSigned(Snapshot snapshot, long tableId, long shard) {
    // get new start from TiKV, and calculate new end and set it back to TiKV.
    long newStart = getAllocateId(dbId, tableId, snapshot);
    long tmpStep = Math.min(Long.MAX_VALUE - newStart, step);
    if (tmpStep != step) {
      throw new TiBatchWriteException("cannot allocate ids for this write");
    }
    if (newStart == Long.MAX_VALUE) {
      throw new TiBatchWriteException("cannot allocate more ids since it ");
    }
    end = udpateAllocateId(dbId, tableId, tmpStep, snapshot, shard, true);
  }

  private void initUnsigned(Snapshot snapshot, long tableId, long shard) {
    // get new start from TiKV, and calculate new end and set it back to TiKV.
    long newStart = getAllocateId(dbId, tableId, snapshot);
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
    end = udpateAllocateId(dbId, tableId, tmpStep, snapshot, shard, false);
  }
}
