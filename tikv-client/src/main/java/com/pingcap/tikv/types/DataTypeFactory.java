/*
 *
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
 *
 */

package com.pingcap.tikv.types;

import com.google.common.collect.ImmutableMap;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Map;

public class DataTypeFactory {
  private static final Map<MySQLType, Constructor<? extends DataType>> dataTypeCreatorMap;
  private static final Map<MySQLType, DataType> dataTypeInstanceMap;

  static {
    ImmutableMap.Builder<MySQLType, Constructor<? extends DataType>> builder =
        ImmutableMap.builder();
    ImmutableMap.Builder<MySQLType, DataType> instBuilder = ImmutableMap.builder();
    extractTypeMap(BitType.subTypes, BitType.class, builder, instBuilder);
    extractTypeMap(StringType.subTypes, StringType.class, builder, instBuilder);
    extractTypeMap(DateTimeType.subTypes, DateTimeType.class, builder, instBuilder);
    extractTypeMap(DateType.subTypes, DateType.class, builder, instBuilder);
    extractTypeMap(DecimalType.subTypes, DecimalType.class, builder, instBuilder);
    extractTypeMap(IntegerType.subTypes, IntegerType.class, builder, instBuilder);
    extractTypeMap(BytesType.subTypes, BytesType.class, builder, instBuilder);
    extractTypeMap(RealType.subTypes, RealType.class, builder, instBuilder);
    extractTypeMap(TimestampType.subTypes, TimestampType.class, builder, instBuilder);
    extractTypeMap(EnumType.subTypes, EnumType.class, builder, instBuilder);
    extractTypeMap(SetType.subTypes, SetType.class, builder, instBuilder);
    extractTypeMap(JsonType.subTypes, JsonType.class, builder, instBuilder);
    extractTypeMap(TimeType.subTypes, TimeType.class, builder, instBuilder);
    extractTypeMap(UninitializedType.subTypes, UninitializedType.class, builder, instBuilder);
    dataTypeCreatorMap = builder.build();
    dataTypeInstanceMap = instBuilder.build();
  }

  private static void extractTypeMap(
      MySQLType[] types,
      Class<? extends DataType> cls,
      ImmutableMap.Builder<MySQLType, Constructor<? extends DataType>> holderBuilder,
      ImmutableMap.Builder<MySQLType, DataType> instBuilder) {
    for (MySQLType type : types) {
      try {
        Constructor ctorByHolder = cls.getDeclaredConstructor(InternalTypeHolder.class);
        Constructor ctorByType = cls.getDeclaredConstructor(MySQLType.class);
        ctorByHolder.setAccessible(true);
        ctorByType.setAccessible(true);
        holderBuilder.put(type, ctorByHolder);
        instBuilder.put(type, (DataType) ctorByType.newInstance(type));
      } catch (Exception e) {
        throw new TypeException(
            String.format("Type %s does not have a proper constructor", cls.getName()), e);
      }
    }
  }

  public static DataType of(MySQLType type) {
    DataType dataType = dataTypeInstanceMap.get(type);
    if (dataType == null) {
      throw new TypeException("Type not found for " + type);
    }
    return dataType;
  }

  // Convert non-binary to string type
  private static MySQLType convertType(MySQLType type, InternalTypeHolder holder) {
    if (Arrays.asList(BytesType.subTypes).contains(type)
        && !Charset.CharsetBin.equals(holder.getCharset())) {
      return MySQLType.TypeVarchar;
    }
    if (Arrays.asList(StringType.subTypes).contains(type)
        && Charset.CharsetBin.equals(holder.getCharset())) {
      return MySQLType.TypeBlob;
    }
    return type;
  }

  public static DataType of(InternalTypeHolder holder) {
    MySQLType type = MySQLType.fromTypeCode(holder.getTp());
    type = convertType(type, holder);
    Constructor<? extends DataType> ctor = dataTypeCreatorMap.get(type);
    if (ctor == null) {
      throw new NullPointerException(
          "tp " + holder.getTp() + " passed in can not retrieved DataType info.");
    }
    try {
      return ctor.newInstance(holder);
    } catch (Exception e) {
      throw new TypeException("Cannot create type from " + holder.getTp(), e);
    }
  }
}
