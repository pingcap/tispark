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

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.ConvertOverflowException;
import com.pingcap.tikv.meta.TiColumnInfo;
import java.nio.charset.StandardCharsets;

public class StringType extends BytesType {

  public static final StringType VARCHAR = new StringType(MySQLType.TypeVarchar);
  public static final StringType CHAR = new StringType(MySQLType.TypeString);
  public static final StringType VAR_STRING = new StringType(MySQLType.TypeVarString);

  public static final MySQLType[] subTypes =
      new MySQLType[] {MySQLType.TypeVarchar, MySQLType.TypeString, MySQLType.TypeVarString};

  protected StringType(MySQLType tp) {
    super(tp);
  }

  public StringType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  public boolean isPushDownSupported() {
    return true;
  }

  @Override
  protected Object doConvertToTiDBType(Object value) throws ConvertNotSupportException {
    return convertToString(value);
  }

  @Override
  public String getName() {
    if (length != -1) {
      return String.format("VARCHAR(%d)", length);
    }
    return "VARCHAR";
  }

  private String convertToString(Object value) throws ConvertNotSupportException {
    String result;
    if (value instanceof Boolean) {
      if ((Boolean) value) {
        result = "1";
      } else {
        result = "0";
      }
    } else if (value instanceof Byte) {
      result = value.toString();
    } else if (value instanceof Short) {
      result = value.toString();
    } else if (value instanceof Integer) {
      result = value.toString();
    } else if (value instanceof Long) {
      result = value.toString();
    } else if (value instanceof Float || value instanceof Double) {
      // TODO: a little complicated, e.g.
      // 3.4028235E38 -> 340282350000000000000000000000000000000
      throw new ConvertNotSupportException(value.getClass().getName(), this.getClass().getName());
    } else if (value instanceof String) {
      result = value.toString();
    } else if (value instanceof java.math.BigDecimal) {
      result = value.toString();
    } else if (value instanceof java.sql.Date) {
      result = value.toString();
    } else if (value instanceof java.sql.Timestamp) {
      result = value.toString();
      if (((java.sql.Timestamp) value).getNanos() == 0) {
        // remove `.0` according to mysql's format
        int len = result.length();
        result = result.substring(0, len - 2);
      }
    } else {
      throw new ConvertNotSupportException(value.getClass().getName(), this.getClass().getName());
    }

    // check utf-8 length of result string
    if (result.codePointCount(0, result.length()) > this.getLength()) {
      throw ConvertOverflowException.newMaxLengthException(result, this.getLength());
    }

    return result;
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    return new String((byte[]) super.decodeNotNull(flag, cdi), StandardCharsets.UTF_8);
  }
}
