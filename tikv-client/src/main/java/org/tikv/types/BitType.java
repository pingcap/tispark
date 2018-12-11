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

package org.tikv.types;

import org.tikv.codec.Codec;
import org.tikv.codec.Codec.IntegerCodec;
import org.tikv.codec.CodecDataInput;
import org.tikv.exception.TypeException;
import org.tikv.meta.TiColumnInfo;

public class BitType extends IntegerType {
  public static final BitType BIT = new BitType(MySQLType.TypeBit);

  public static final MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeBit};

  private BitType(MySQLType tp) {
    super(tp);
  }

  protected BitType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    switch (flag) {
      case Codec.UVARINT_FLAG:
        return IntegerCodec.readUVarLong(cdi);
      case Codec.UINT_FLAG:
        return IntegerCodec.readULong(cdi);
      default:
        throw new TypeException("Invalid IntegerType flag: " + flag);
    }
  }

  @Override
  public boolean isUnsigned() {
    return true;
  }
}
