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

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.Codec.BytesCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.InvalidCodecFormatException;
import com.pingcap.tikv.meta.TiColumnInfo;

/**
 * TODO: if we need to unify string type and binary types? Indeed they are encoded as the same
 * However, decode to string actually going through encoding/decoding by whatever charset.encoding
 * format we set, and essentially changed underlying data
 */
public class BytesType extends DataType {
  public static final BytesType BLOB = new BytesType(MySQLType.TypeBlob);
  public static final BytesType LONG_TEXT = new BytesType(MySQLType.TypeLongBlob);
  public static final BytesType MEDIUM_TEXT = new BytesType(MySQLType.TypeMediumBlob);
  public static final BytesType TEXT = new BytesType(MySQLType.TypeBlob);
  public static final BytesType TINY_BLOB = new BytesType(MySQLType.TypeTinyBlob);

  public static final MySQLType[] subTypes =
      new MySQLType[] {
        MySQLType.TypeBlob, MySQLType.TypeLongBlob,
        MySQLType.TypeMediumBlob, MySQLType.TypeTinyBlob
      };

  protected BytesType(MySQLType tp) {
    super(tp);
  }

  protected BytesType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  public long getSize() {
    if (isLengthUnSpecified()) {
      return getPrefixSize() + getDefaultDataSize();
    } else {
      return getPrefixSize() + getLength();
    }
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag == Codec.COMPACT_BYTES_FLAG) {
      return BytesCodec.readCompactBytes(cdi);
    } else if (flag == Codec.BYTES_FLAG) {
      return BytesCodec.readBytes(cdi);
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for : " + flag);
    }
  }

  @Override
  protected boolean isPrefixIndexSupported() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    byte[] bytes = Converter.convertToBytes(value);
    BytesCodec.writeBytesFully(cdo, bytes);
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    byte[] bytes = Converter.convertToBytes(value);
    BytesCodec.writeCompactBytesFully(cdo, bytes);
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    byte[] bytes = Converter.convertToBytes(value);
    BytesCodec.writeBytesRaw(cdo, bytes);
  }

  @Override
  public ExprType getProtoExprType() {
    return getCharset().equals(Charset.CharsetBin) ? ExprType.Bytes : ExprType.String;
  }

  /** {@inheritDoc} */
  @Override
  public Object getOriginDefaultValueNonNull(String value, long version) {
    return value;
  }
}
