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

package com.pingcap.tikv.catalog;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.codec.Codec.BytesCodec;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.util.Pair;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.log4j.Logger;
import org.tikv.kvproto.Kvrpcpb;

public class CatalogTransaction {
  protected static final Logger logger = Logger.getLogger(CatalogTransaction.class);
  private final Snapshot snapshot;
  private final byte[] prefix;

  private static final byte[] META_PREFIX = new byte[] {'m'};

  private static final byte HASH_DATA_FLAG = 'h';
  private static final byte HASH_META_FLAG = 'H';
  private static final byte STR_DATA_FLAG = 's';

  private static ByteString KEY_DBs = ByteString.copyFromUtf8("DBs");
  private static String KEY_TABLE = "Table";
  private static ByteString KEY_SCHEMA_VERSION = ByteString.copyFromUtf8("SchemaVersionKey");

  private static final String ENCODED_DB_PREFIX = "DB";
  private static final String KEY_TID = "TID";

  CatalogTransaction(Snapshot snapshot) {
    this.snapshot = snapshot;
    this.prefix = META_PREFIX;
  }

  private void encodeStringDataKey(CodecDataOutput cdo, byte[] key) {
    cdo.write(prefix);
    BytesCodec.writeBytes(cdo, key);
    IntegerCodec.writeULong(cdo, STR_DATA_FLAG);
  }

  private void encodeHashDataKey(CodecDataOutput cdo, byte[] key, byte[] field) {
    cdo.write(prefix);
    BytesCodec.writeBytes(cdo, key);
    IntegerCodec.writeULong(cdo, HASH_DATA_FLAG);
    BytesCodec.writeBytes(cdo, field);
  }

  private ByteString encodeHashMetaKey(CodecDataOutput cdo, byte[] key) {
    cdo.write(prefix);
    BytesCodec.writeBytes(cdo, key);
    IntegerCodec.writeULong(cdo, HASH_META_FLAG);
    return cdo.toByteString();
  }

  private void encodeHashDataKeyPrefix(CodecDataOutput cdo, byte[] key) {
    cdo.write(prefix);
    BytesCodec.writeBytes(cdo, key);
    IntegerCodec.writeULong(cdo, HASH_DATA_FLAG);
  }

  private Pair<ByteString, ByteString> decodeHashDataKey(ByteString rawKey) {
    checkArgument(
        KeyUtils.hasPrefix(rawKey, ByteString.copyFrom(prefix)),
        "invalid encoded hash data key prefix: " + new String(prefix));
    CodecDataInput cdi = new CodecDataInput(rawKey.toByteArray());
    cdi.skipBytes(prefix.length);
    byte[] key = BytesCodec.readBytes(cdi);
    long typeFlag = IntegerCodec.readULong(cdi);
    if (typeFlag != HASH_DATA_FLAG) {
      throw new TiClientInternalException("Invalid hash data flag: " + typeFlag);
    }
    byte[] field = BytesCodec.readBytes(cdi);
    return Pair.create(ByteString.copyFrom(key), ByteString.copyFrom(field));
  }

  private static ByteString autoTableIDKey(long tableId) {
    return ByteString.copyFrom(String.format("%s:%d", KEY_TID, tableId).getBytes());
  }

  private static ByteString tableKey(long tableId) {
    return ByteString.copyFrom(String.format("%s:%d", KEY_TABLE, tableId).getBytes());
  }

  private static ByteString encodeDatabaseID(long id) {
    return ByteString.copyFrom(String.format("%s:%d", ENCODED_DB_PREFIX, id).getBytes());
  }

  private boolean isDBExisted(long dbId) {
    return getDatabase(dbId) != null;
  }

  private boolean isTableExisted(long dbId, long tableId) {
    ByteString dbKey = encodeDatabaseID(dbId);
    ByteString tableKey = tableKey(tableId);
    return !hashGet(dbKey, tableKey).isEmpty();
  }

  private void updateMeta(ByteString key, byte[] oldVal) {
    // 1. encode hash meta key
    // 2. load meta via hash meta key from TiKV
    // 3. update meta's filed count and set it back to TiKV
    CodecDataOutput cdo = new CodecDataOutput();
    ByteString metaKey = encodeHashMetaKey(cdo, key.toByteArray());
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
      snapshot.set(metaKey, cdo.toByteString());
    }
  }

  private long updateHash(
      ByteString key, ByteString field, Function<byte[], byte[]> calculateNewVal) {
    // 1. encode hash data key
    // 2. get value in byte from get operation
    // 3. calculate new value via calculateNewVal
    // 4. check old value equals to new value or not
    // 5. set the new value back to TiKV via 2pc
    // 6. encode a hash meta key
    // 7. update a hash meta field count if needed

    CodecDataOutput cdo = new CodecDataOutput();
    encodeHashDataKey(cdo, key.toByteArray(), field.toByteArray());
    ByteString dataKey = cdo.toByteString();
    byte[] oldVal = snapshot.get(dataKey.toByteArray());

    byte[] newVal = calculateNewVal.apply(oldVal);
    if (Arrays.equals(newVal, oldVal)) {
      // not need to update
      return 0L;
    }
    snapshot.set(dataKey, ByteString.copyFrom(newVal));

    updateMeta(key, oldVal);
    return Long.parseLong(new String(newVal));
  }

  public long getAutoTableId(long dbId, long tableId, long step) {
    if (isDBExisted(dbId) && isTableExisted(dbId, tableId)) {
      return updateHash(
          encodeDatabaseID(dbId),
          autoTableIDKey(tableId),
          (oldVal) -> {
            long base = 0;
            if (oldVal != null && oldVal.length != 0) {
              base = Long.parseLong(new String(oldVal));
            }

            base += step;
            return String.valueOf(base).getBytes();
          });
    }

    throw new IllegalArgumentException("table or database is not existed");
  }

  public long getAutoTableId(long dbId, long tableId) {
    ByteString dbKey = encodeDatabaseID(dbId);
    ByteString tblKey = autoTableIDKey(tableId);
    ByteString val = hashGet(dbKey, tblKey);
    if (val.isEmpty()) return 0L;
    return Long.parseLong(val.toStringUtf8());
  }

  private ByteString hashGet(ByteString key, ByteString field) {
    CodecDataOutput cdo = new CodecDataOutput();
    encodeHashDataKey(cdo, key.toByteArray(), field.toByteArray());
    return snapshot.get(cdo.toByteString());
  }

  private ByteString bytesGet(ByteString key) {
    CodecDataOutput cdo = new CodecDataOutput();
    encodeStringDataKey(cdo, key.toByteArray());
    return snapshot.get(cdo.toByteString());
  }

  private List<Pair<ByteString, ByteString>> hashGetFields(ByteString key) {
    CodecDataOutput cdo = new CodecDataOutput();
    encodeHashDataKeyPrefix(cdo, key.toByteArray());
    ByteString encodedKey = cdo.toByteString();

    Iterator<Kvrpcpb.KvPair> iterator = snapshot.scan(encodedKey);
    List<Pair<ByteString, ByteString>> fields = new ArrayList<>();
    while (iterator.hasNext()) {
      Kvrpcpb.KvPair kv = iterator.next();
      if (kv == null || kv.getKey() == null) {
        continue;
      }
      if (!KeyUtils.hasPrefix(kv.getKey(), encodedKey)) {
        break;
      }
      fields.add(Pair.create(decodeHashDataKey(kv.getKey()).second, kv.getValue()));
    }

    return fields;
  }

  long getLatestSchemaVersion() {
    ByteString versionBytes = bytesGet(KEY_SCHEMA_VERSION);
    CodecDataInput cdi = new CodecDataInput(versionBytes.toByteArray());
    return Long.parseLong(new String(cdi.toByteArray(), StandardCharsets.UTF_8));
  }

  public List<TiDBInfo> getDatabases() {
    List<Pair<ByteString, ByteString>> fields = hashGetFields(KEY_DBs);
    ImmutableList.Builder<TiDBInfo> builder = ImmutableList.builder();
    for (Pair<ByteString, ByteString> pair : fields) {
      builder.add(parseFromJson(pair.second, TiDBInfo.class));
    }
    return builder.build();
  }

  TiDBInfo getDatabase(long id) {
    ByteString dbKey = encodeDatabaseID(id);
    ByteString json = hashGet(KEY_DBs, dbKey);
    if (json == null || json.isEmpty()) {
      return null;
    }
    return parseFromJson(json, TiDBInfo.class);
  }

  List<TiTableInfo> getTables(long dbId) {
    ByteString dbKey = encodeDatabaseID(dbId);
    List<Pair<ByteString, ByteString>> fields = hashGetFields(dbKey);
    ImmutableList.Builder<TiTableInfo> builder = ImmutableList.builder();
    for (Pair<ByteString, ByteString> pair : fields) {
      if (KeyUtils.hasPrefix(pair.first, ByteString.copyFromUtf8(KEY_TABLE))) {
        builder.add(parseFromJson(pair.second, TiTableInfo.class));
      }
    }
    return builder.build();
  }

  public static <T> T parseFromJson(ByteString json, Class<T> cls) {
    Objects.requireNonNull(json, "json is null");
    Objects.requireNonNull(cls, "cls is null");

    logger.debug(String.format("Parse Json %s : %s", cls.getSimpleName(), json.toStringUtf8()));
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(json.toStringUtf8(), cls);
    } catch (JsonParseException | JsonMappingException e) {
      String errMsg =
          String.format(
              "Invalid JSON value for Type %s: %s\n", cls.getSimpleName(), json.toStringUtf8());
      throw new TiClientInternalException(errMsg, e);
    } catch (Exception e1) {
      throw new TiClientInternalException("Error parsing Json", e1);
    }
  }
}
