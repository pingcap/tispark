/*
 * Copyright 2020 PingCAP, Inc.
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

package com.pingcap.tikv.codec;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.codec.Codec.BytesCodec;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.util.Pair;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.KvPair;

public class MetaCodec {
  public static final String ENCODED_DB_PREFIX = "DB";
  public static final String KEY_TID = "TID";
  private static final byte[] META_PREFIX = new byte[] {'m'};
  private static final byte HASH_DATA_FLAG = 'h';
  private static final byte HASH_META_FLAG = 'H';
  private static final byte STR_DATA_FLAG = 's';
  public static ByteString KEY_DBs = ByteString.copyFromUtf8("DBs");
  public static String KEY_TABLE = "Table";
  public static ByteString KEY_SCHEMA_VERSION = ByteString.copyFromUtf8("SchemaVersionKey");

  public static void encodeStringDataKey(CodecDataOutput cdo, byte[] key) {
    cdo.write(META_PREFIX);
    BytesCodec.writeBytes(cdo, key);
    IntegerCodec.writeULong(cdo, STR_DATA_FLAG);
  }

  public static void encodeHashDataKey(CodecDataOutput cdo, byte[] key, byte[] field) {
    cdo.write(META_PREFIX);
    BytesCodec.writeBytes(cdo, key);
    IntegerCodec.writeULong(cdo, HASH_DATA_FLAG);
    BytesCodec.writeBytes(cdo, field);
  }

  public static ByteString encodeHashMetaKey(CodecDataOutput cdo, byte[] key) {
    cdo.write(META_PREFIX);
    BytesCodec.writeBytes(cdo, key);
    IntegerCodec.writeULong(cdo, HASH_META_FLAG);
    return cdo.toByteString();
  }

  public static void encodeHashDataKeyPrefix(CodecDataOutput cdo, byte[] key) {
    cdo.write(META_PREFIX);
    BytesCodec.writeBytes(cdo, key);
    IntegerCodec.writeULong(cdo, HASH_DATA_FLAG);
  }

  public static Pair<ByteString, ByteString> decodeHashDataKey(ByteString rawKey) {
    checkArgument(
        KeyUtils.hasPrefix(rawKey, ByteString.copyFrom(META_PREFIX)),
        "invalid encoded hash data key prefix: " + new String(META_PREFIX));
    CodecDataInput cdi = new CodecDataInput(rawKey.toByteArray());
    cdi.skipBytes(META_PREFIX.length);
    byte[] key = BytesCodec.readBytes(cdi);
    long typeFlag = IntegerCodec.readULong(cdi);
    if (typeFlag != HASH_DATA_FLAG) {
      throw new TiClientInternalException("Invalid hash data flag: " + typeFlag);
    }
    byte[] field = BytesCodec.readBytes(cdi);
    return Pair.create(ByteString.copyFrom(key), ByteString.copyFrom(field));
  }

  public static ByteString autoTableIDKey(long tableId) {
    return ByteString.copyFrom(String.format("%s:%d", KEY_TID, tableId).getBytes());
  }

  public static ByteString tableKey(long tableId) {
    return ByteString.copyFrom(String.format("%s:%d", KEY_TABLE, tableId).getBytes());
  }

  public static ByteString encodeDatabaseID(long id) {
    return ByteString.copyFrom(String.format("%s:%d", ENCODED_DB_PREFIX, id).getBytes());
  }

  public static ByteString hashGet(ByteString key, ByteString field, Snapshot snapshot) {
    CodecDataOutput cdo = new CodecDataOutput();
    MetaCodec.encodeHashDataKey(cdo, key.toByteArray(), field.toByteArray());
    return snapshot.get(cdo.toByteString());
  }

  public static ByteString bytesGet(ByteString key, Snapshot snapshot) {
    CodecDataOutput cdo = new CodecDataOutput();
    MetaCodec.encodeStringDataKey(cdo, key.toByteArray());
    return snapshot.get(cdo.toByteString());
  }

  public static List<Pair<ByteString, ByteString>> hashGetFields(
      ByteString key, Snapshot snapshot) {
    CodecDataOutput cdo = new CodecDataOutput();
    MetaCodec.encodeHashDataKeyPrefix(cdo, key.toByteArray());
    ByteString encodedKey = cdo.toByteString();

    Iterator<KvPair> iterator = snapshot.scanPrefix(encodedKey);
    List<Pair<ByteString, ByteString>> fields = new ArrayList<>();
    while (iterator.hasNext()) {
      Kvrpcpb.KvPair kv = iterator.next();
      if (kv == null || kv.getKey() == null) {
        continue;
      }
      fields.add(Pair.create(MetaCodec.decodeHashDataKey(kv.getKey()).second, kv.getValue()));
    }

    return fields;
  }
}
