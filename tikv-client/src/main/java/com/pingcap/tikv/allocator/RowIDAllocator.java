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

import com.google.common.primitives.UnsignedLongs;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.TwoPhaseCommitter;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.MetaCodec;
import com.pingcap.tikv.exception.TiBatchWriteException;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.util.Arrays;
import java.util.function.Function;

/**
 * RowIDAllocator read current start from TiKV and write back 'start+step' back to TiKV. It designs
 * to allocate all id for data to be written at once, hence it does not need run inside a txn.
 */
public final class RowIDAllocator {
  private long end;
  private final long dbId;
  private long step;
  private final TiConfiguration conf;

  private RowIDAllocator(long dbId, long step, TiConfiguration conf) {
    this.dbId = dbId;
    this.step = step;
    this.conf = conf;
  }

  public static RowIDAllocator create(
      long dbId, long tableId, TiConfiguration conf, boolean unsigned, long step) {
    RowIDAllocator allocator = new RowIDAllocator(dbId, step, conf);
    if (unsigned) {
      allocator.initUnsigned(TiSession.getInstance(conf).createSnapshot(), tableId);
    } else {
      allocator.initSigned(TiSession.getInstance(conf).createSnapshot(), tableId);
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

  private boolean isDBExisted(long dbId, Snapshot snapshot) {
    ByteString dbKey = MetaCodec.encodeDatabaseID(dbId);
    ByteString json = MetaCodec.hashGet(MetaCodec.KEY_DBs, dbKey, snapshot);
    return json != null && !json.isEmpty();
  }

  private boolean isTableExisted(long dbId, long tableId, Snapshot snapshot) {
    ByteString dbKey = MetaCodec.encodeDatabaseID(dbId);
    ByteString tableKey = MetaCodec.tableKey(tableId);
    return !MetaCodec.hashGet(dbKey, tableKey, snapshot).isEmpty();
  }
  /**
   * read current row id from TiKV and write the calculated value back to TiKV. The calculation rule
   * is start(read from TiKV) + step.
   */
  public long getAutoTableId(long dbId, long tableId, long step, Snapshot snapshot) {
    if (isDBExisted(dbId, snapshot) && isTableExisted(dbId, tableId, snapshot)) {
      return updateHash(
          MetaCodec.encodeDatabaseID(dbId),
          MetaCodec.autoTableIDKey(tableId),
          (oldVal) -> {
            long base = 0;
            if (oldVal != null && oldVal.length != 0) {
              base = Long.parseLong(new String(oldVal));
            }

            base += step;
            return String.valueOf(base).getBytes();
          },
          snapshot);
    }

    throw new IllegalArgumentException("table or database is not existed");
  }

  /** read current row id from TiKV according to database id and table id. */
  public long getAutoTableId(long dbId, long tableId, Snapshot snapshot) {
    if (isDBExisted(dbId, snapshot) && isTableExisted(dbId, tableId, snapshot)) {
      ByteString dbKey = MetaCodec.encodeDatabaseID(dbId);
      ByteString tblKey = MetaCodec.autoTableIDKey(tableId);
      ByteString val = MetaCodec.hashGet(dbKey, tblKey, snapshot);
      if (val.isEmpty()) return 0L;
      return Long.parseLong(val.toStringUtf8());
    }

    throw new IllegalArgumentException("table or database is not existed");
  }

  private void initSigned(Snapshot snapshot, long tableId) {
    long newEnd;
    // get new start from TiKV, and calculate new end and set it back to TiKV.
    long newStart = getAutoTableId(dbId, tableId, snapshot);
    long tmpStep = Math.min(Long.MAX_VALUE - newStart, step);
    if (tmpStep != step) {
      throw new TiBatchWriteException("cannot allocate ids for this write");
    }
    if (newStart == Long.MAX_VALUE) {
      throw new TiBatchWriteException("cannot allocate more ids since it ");
    }
    newEnd = getAutoTableId(dbId, tableId, tmpStep, snapshot);

    end = newEnd;
  }

  private void initUnsigned(Snapshot snapshot, long tableId) {
    long newEnd;
    // get new start from TiKV, and calculate new end and set it back to TiKV.
    long newStart = getAutoTableId(dbId, tableId, snapshot);
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
    newEnd = getAutoTableId(dbId, tableId, tmpStep, snapshot);

    end = newEnd;
  }
}
