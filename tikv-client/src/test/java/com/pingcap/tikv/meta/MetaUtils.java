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

package com.pingcap.tikv.meta;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.GrpcUtils;
import com.pingcap.tikv.KVMockServer;
import com.pingcap.tikv.PDMockServer;
import com.pingcap.tikv.codec.Codec.BytesCodec;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.TiPartitionInfo.PartitionType;
import com.pingcap.tikv.types.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.tikv.kvproto.Metapb;

public class MetaUtils {
  public static class TableBuilder {
    long autoId = 1;
    private boolean pkHandle;
    private boolean isCommonHandle;
    private String name;
    private final List<TiColumnInfo> columns = new ArrayList<>();
    private final List<TiIndexInfo> indices = new ArrayList<>();
    private TiPartitionInfo partInfo;
    private Long tid = null;
    private long version = 0L;
    private final long updateTimestamp = 0L;
    private final long maxShardRowIDBits = 0L;
    private final long autoRandomBits = 0L;

    public TableBuilder() {}

    public static TableBuilder newBuilder() {
      return new TableBuilder();
    }

    private long newId() {
      return autoId++;
    }

    public TableBuilder name(String name) {
      this.name = name;
      return this;
    }

    public TableBuilder tableId(long id) {
      this.tid = id;
      return this;
    }

    public TableBuilder addColumn(String name, DataType type) {
      return addColumn(name, type, false);
    }

    public TableBuilder addColumn(String name, DataType type, long colId) {
      return addColumn(name, type, false, colId);
    }

    public TableBuilder addColumn(String name, DataType type, boolean pk, long colId) {
      for (TiColumnInfo c : columns) {
        if (c.matchName(name)) {
          throw new TiClientInternalException("duplicated name: " + name);
        }
      }

      TiColumnInfo col = new TiColumnInfo(colId, name, columns.size(), type, pk);
      columns.add(col);
      return this;
    }

    public TableBuilder addColumn(String name, DataType type, boolean pk) {
      return addColumn(name, type, pk, newId());
    }

    public TableBuilder addPartition(
        String expr, PartitionType type, List<TiPartitionDef> partitionDefs, List<CIStr> columns) {
      this.partInfo =
          new TiPartitionInfo(
              TiPartitionInfo.partTypeToLong(type), expr, columns, true, partitionDefs);
      return this;
    }

    public TableBuilder appendIndex(
        long iid, String indexName, List<String> colNames, boolean isPk) {
      List<TiIndexColumn> indexCols =
          colNames
              .stream()
              .map(name -> columns.stream().filter(c -> c.matchName(name)).findFirst())
              .flatMap(col -> col.isPresent() ? Stream.of(col.get()) : Stream.empty())
              .map(TiColumnInfo::toIndexColumn)
              .collect(Collectors.toList());

      TiIndexInfo index =
          new TiIndexInfo(
              iid,
              CIStr.newCIStr(indexName),
              CIStr.newCIStr(name),
              ImmutableList.copyOf(indexCols),
              false,
              isPk,
              SchemaState.StatePublic.getStateCode(),
              "",
              IndexType.IndexTypeBtree.getTypeCode(),
              false,
              false);
      indices.add(index);
      return this;
    }

    public TableBuilder appendIndex(String indexName, List<String> colNames, boolean isPk) {
      return appendIndex(newId(), indexName, colNames, isPk);
    }

    public TableBuilder setPkHandle(boolean pkHandle) {
      this.pkHandle = pkHandle;
      return this;
    }

    public TableBuilder setIsCommonHandle(boolean isCommonHandle) {
      this.isCommonHandle = isCommonHandle;
      return this;
    }

    public TableBuilder version(long version) {
      this.version = version;
      return this;
    }

    public TiTableInfo build() {
      if (tid == null) {
        tid = newId();
      }
      if (name == null) {
        name = "Table" + tid;
      }
      return new TiTableInfo(
          tid,
          CIStr.newCIStr(name),
          "",
          "",
          pkHandle,
          isCommonHandle,
          columns,
          indices,
          "",
          0,
          0,
          0,
          0,
          partInfo,
          null,
          null,
          version,
          updateTimestamp,
          maxShardRowIDBits,
          null,
          autoRandomBits);
    }
  }

  public static class MetaMockHelper {
    public static final String LOCAL_ADDR = "127.0.0.1";
    public static int MEMBER_ID = 1;
    public static int STORE_ID = 1;
    public static Metapb.Region region =
        Metapb.Region.newBuilder()
            .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
            .setId(1)
            .setStartKey(ByteString.EMPTY)
            .setEndKey(ByteString.EMPTY)
            .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(1))
            .build();
    private final KVMockServer kvServer;
    private final PDMockServer pdServer;

    public MetaMockHelper(PDMockServer pdServer, KVMockServer kvServer) {
      this.kvServer = kvServer;
      this.pdServer = pdServer;
    }

    public void preparePDForRegionRead() {
      pdServer.addGetMemberResp(
          GrpcUtils.makeGetMembersResponse(
              pdServer.getClusterId(),
              GrpcUtils.makeMember(MEMBER_ID, "http://" + LOCAL_ADDR + ":" + pdServer.port)));

      pdServer.addGetStoreResp(
          GrpcUtils.makeGetStoreResponse(
              pdServer.getClusterId(),
              GrpcUtils.makeStore(
                  STORE_ID, LOCAL_ADDR + ":" + kvServer.getPort(), Metapb.StoreState.Up)));

      pdServer.addGetRegionResp(GrpcUtils.makeGetRegionResponse(pdServer.getClusterId(), region));
    }

    private ByteString getDBKey(long id) {
      CodecDataOutput cdo = new CodecDataOutput();
      cdo.write(new byte[] {'m'});
      BytesCodec.writeBytes(cdo, "DBs".getBytes());
      IntegerCodec.writeULong(cdo, 'h');
      BytesCodec.writeBytes(cdo, String.format("DB:%d", id).getBytes());
      return cdo.toByteString();
    }

    public void addDatabase(long id, String name) {
      String dbJson =
          String.format(
              "{\n"
                  + " \"id\":%d,\n"
                  + " \"db_name\":{\"O\":\"%s\",\"L\":\"%s\"},\n"
                  + " \"charset\":\"utf8\",\"collate\":\"utf8_bin\",\"state\":5\n"
                  + "}",
              id, name, name.toLowerCase());

      kvServer.put(getDBKey(id), ByteString.copyFromUtf8(dbJson));
    }

    public void dropDatabase(long id) {
      kvServer.remove(getDBKey(id));
    }

    private ByteString getKeyForTable(long dbId, long tableId) {
      ByteString dbKey = ByteString.copyFrom(String.format("%s:%d", "DB", dbId).getBytes());
      ByteString tableKey =
          ByteString.copyFrom(String.format("%s:%d", "Table", tableId).getBytes());

      CodecDataOutput cdo = new CodecDataOutput();
      cdo.write(new byte[] {'m'});
      BytesCodec.writeBytes(cdo, dbKey.toByteArray());
      IntegerCodec.writeULong(cdo, 'h');
      BytesCodec.writeBytes(cdo, tableKey.toByteArray());
      return cdo.toByteString();
    }

    private ByteString getSchemaVersionKey() {
      CodecDataOutput cdo = new CodecDataOutput();
      cdo.write(new byte[] {'m'});
      BytesCodec.writeBytes(cdo, "SchemaVersionKey".getBytes());
      IntegerCodec.writeULong(cdo, 's');
      return cdo.toByteString();
    }

    public void setSchemaVersion(long version) {
      CodecDataOutput cdo = new CodecDataOutput();
      cdo.write(new byte[] {'m'});
      BytesCodec.writeBytes(cdo, "SchemaVersionKey".getBytes());
      IntegerCodec.writeULong(cdo, 's');
      kvServer.put(getSchemaVersionKey(), ByteString.copyFromUtf8(String.format("%d", version)));
    }

    public void addTable(int dbId, int tableId, String tableName) {
      String tableJson =
          String.format(
              "\n"
                  + "{\n"
                  + "   \"id\": %d,\n"
                  + "   \"name\": {\n"
                  + "      \"O\": \"%s\",\n"
                  + "      \"L\": \"%s\"\n"
                  + "   },\n"
                  + "   \"charset\": \"\",\n"
                  + "   \"collate\": \"\",\n"
                  + "   \"cols\": [\n"
                  + "      {\n"
                  + "         \"id\": 1,\n"
                  + "         \"name\": {\n"
                  + "            \"O\": \"c1\",\n"
                  + "            \"L\": \"c1\"\n"
                  + "         },\n"
                  + "         \"offset\": 0,\n"
                  + "         \"origin_default\": null,\n"
                  + "         \"default\": null,\n"
                  + "         \"type\": {\n"
                  + "            \"Tp\": 3,\n"
                  + "            \"Flag\": 139,\n"
                  + "            \"Flen\": 11,\n"
                  + "            \"Decimal\": -1,\n"
                  + "            \"Charset\": \"binary\",\n"
                  + "            \"Collate\": \"binary\",\n"
                  + "            \"Elems\": null\n"
                  + "         },\n"
                  + "         \"state\": 5,\n"
                  + "         \"comment\": \"\"\n"
                  + "      }\n"
                  + "   ],\n"
                  + "   \"index_info\": [],\n"
                  + "   \"fk_info\": null,\n"
                  + "   \"state\": 5,\n"
                  + "   \"pk_is_handle\": true,\n"
                  + "   \"comment\": \"\",\n"
                  + "   \"auto_inc_id\": 0,\n"
                  + "   \"max_col_id\": 4,\n"
                  + "   \"max_idx_id\": 1\n"
                  + "}",
              tableId, tableName, tableName.toLowerCase());

      kvServer.put(getKeyForTable(dbId, tableId), ByteString.copyFromUtf8(tableJson));
    }

    public void dropTable(long dbId, long tableId) {
      kvServer.remove(getKeyForTable(dbId, tableId));
    }
  }
}
