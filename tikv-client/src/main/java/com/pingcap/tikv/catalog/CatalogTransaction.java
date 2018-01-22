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
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.util.Pair;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.log4j.Logger;

public class CatalogTransaction {
  protected static final Logger logger = Logger.getLogger(Catalog.class);
  private final Snapshot snapshot;
  private final byte[] prefix;

  private static final byte[] META_PREFIX = new byte[] {'m'};

  private static final byte HASH_DATA_FLAG = 'h';
  private static final byte STR_DATA_FLAG = 's';

  private static ByteString KEY_DB = ByteString.copyFromUtf8("DBs");
  private static ByteString KEY_TABLE = ByteString.copyFromUtf8("Table");
  private static ByteString KEY_SCHEMA_VERSION =  ByteString.copyFromUtf8("SchemaVersionKey");

  private static final String DB_PREFIX = "DB";

  public CatalogTransaction(Snapshot snapshot) {
    this.snapshot = snapshot;
    this.prefix = META_PREFIX;
  }

  private void encodeStringDataKey(CodecDataOutput cdo, byte[] key) {
    cdo.write(prefix);
    BytesCodec.writeBytes(cdo, key);
    IntegerCodec.writeULong(cdo, STR_DATA_FLAG);
  }

  private void encodeHashDataKey(CodecDataOutput cdo, byte[] key, byte[] field) {
    encodeHashDataKeyPrefix(cdo, key);
    BytesCodec.writeBytes(cdo, field);
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
      if (!KeyUtils.hasPrefix(kv.getKey(), encodedKey)) {
        break;
      }
      fields.add(Pair.create(decodeHashDataKey(kv.getKey()).second, kv.getValue()));
    }

    return fields;
  }

  private static ByteString encodeDatabaseID(long id) {
    return ByteString.copyFrom(String.format("%s:%d", DB_PREFIX, id).getBytes());
  }

  public long getLatestSchemaVersion() {
    ByteString versionBytes = bytesGet(KEY_SCHEMA_VERSION);
    CodecDataInput cdi = new CodecDataInput(versionBytes.toByteArray());
    return Long.parseLong(new String(cdi.toByteArray(), StandardCharsets.UTF_8));
  }

  public List<TiDBInfo> getDatabases() {
    List<Pair<ByteString, ByteString>> fields = hashGetFields(KEY_DB);
    ImmutableList.Builder<TiDBInfo> builder = ImmutableList.builder();
    for (Pair<ByteString, ByteString> pair : fields) {
      builder.add(parseFromJson(pair.second, TiDBInfo.class));
    }
    return builder.build();
  }

  public TiDBInfo getDatabase(long id) {
    ByteString dbKey = encodeDatabaseID(id);
    ByteString json = hashGet(KEY_DB, dbKey);
    if (json == null || json.isEmpty()) {
      return null;
    }
    return parseFromJson(json, TiDBInfo.class);
  }

  public List<TiTableInfo> getTables(long dbId) {
    ByteString dbKey = encodeDatabaseID(dbId);
    List<Pair<ByteString, ByteString>> fields = hashGetFields(dbKey);
    ImmutableList.Builder<TiTableInfo> builder = ImmutableList.builder();
    for (Pair<ByteString, ByteString> pair : fields) {
      if (KeyUtils.hasPrefix(pair.first, KEY_TABLE)) {
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
